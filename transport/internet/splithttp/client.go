package splithttp

import (
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"

	"github.com/apernet/quic-go/http3"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/signal/done"
	"golang.org/x/net/http2"
)

// interface to abstract between use of browser dialer, vs net/http
type DialerClient interface {
	IsClosed() bool

	// ctx, url, sessionId, body, uploadOnly
	OpenStream(context.Context, string, string, io.Reader, bool) (io.ReadCloser, net.Addr, net.Addr, error)

	// ctx, url, sessionId, seqStr, body, contentLength
	PostPacket(context.Context, string, string, string, buf.MultiBuffer) error
}

// implements splithttp.DialerClient in terms of direct network connections
type DefaultDialerClient struct {
	transportConfig *Config
	client          *http.Client
	retired    atomic.Bool
	httpVersion string
	// pool of H1Conn for HTTP/1.1 uploads, created using dialUploadConn
	uploadPool     *uploadConnPool
	dialUploadConn func(ctxInner context.Context) (net.Conn, error)
}

func (c *DefaultDialerClient) IsClosed() bool {
	return c.retired.Load()
}

func isRequestScopedError(err error) bool {
	if stderrors.Is(err, context.Canceled) || stderrors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var streamErr http2.StreamError
	return stderrors.As(err, &streamErr)
}

func (c *DefaultDialerClient) maybeRetire(err error) {
	if isRequestScopedError(err) {
		return
	}
	c.retired.Store(true)
	c.CloseIdleConnections()
}

func (c *DefaultDialerClient) OpenStream(ctx context.Context, url string, sessionId string, body io.Reader, uploadOnly bool) (wrc io.ReadCloser, remoteAddr, localAddr net.Addr, err error) {
	// this is done when the TCP/UDP connection to the server was established,
	// and we can unblock the Dial function and print correct net addresses in
	// logs
	gotConn := done.New()
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(connInfo httptrace.GotConnInfo) {
			remoteAddr = connInfo.Conn.RemoteAddr()
			localAddr = connInfo.Conn.LocalAddr()
			gotConn.Close()
		},
	})

	method := "GET" // stream-down
	if body != nil {
		method = c.transportConfig.GetNormalizedUplinkHTTPMethod() // stream-up/one
	}
	req, err := http.NewRequestWithContext(context.WithoutCancel(ctx), method, url, body)
	if err != nil {
		errors.LogInfoInner(ctx, err, "failed to create HTTP request for "+url)
		return nil, nil, nil, err
	}
	c.transportConfig.FillStreamRequest(req, sessionId, "")

	wrc = &WaitReadCloser{wait: done.New()}
	go func() {
		resp, err := c.client.Do(req)
		if err != nil {
			if !uploadOnly { // stream-down is enough
				c.maybeRetire(err)
				errors.LogInfoInner(ctx, err, "failed to "+method+" "+url)
			}
			gotConn.Close()
			common.Close(body)
			wrc.Close()
			wrc.(*WaitReadCloser).finish()
			return
		}
		if resp.StatusCode != 200 && !uploadOnly {
			errors.LogInfo(ctx, "unexpected status ", resp.StatusCode)
		}
		if resp.StatusCode != 200 || uploadOnly { // stream-up
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close() // if it is called immediately, the upload will be interrupted also
			common.Close(body)
			wrc.Close()
			wrc.(*WaitReadCloser).finish()
			return
		}
		wrc.(*WaitReadCloser).Set(resp.Body)
	}()

	<-gotConn.Wait()
	return
}

func (c *DefaultDialerClient) PostPacket(ctx context.Context, url string, sessionId string, seqStr string, payload buf.MultiBuffer) error {
	method := c.transportConfig.GetNormalizedUplinkHTTPMethod()
	req, err := http.NewRequestWithContext(context.WithoutCancel(ctx), method, url, nil)
	if err != nil {
		return err
	}
	c.transportConfig.FillPacketRequest(req, sessionId, seqStr, payload)

	if c.httpVersion != "1.1" {
		resp, err := c.client.Do(req)
		if err != nil {
			c.maybeRetire(err)
			return err
		}

		io.Copy(io.Discard, resp.Body)
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			return errors.New("bad status code:", resp.Status)
		}
	} else {
		// stringify the entire HTTP/1.1 request so it can be
		// safely retried. if instead req.Write is called multiple
		// times, the body is already drained after the first
		// request
		requestBuff := new(bytes.Buffer)
		requestBuff.Grow(512 + int(req.ContentLength))
		common.Must(req.Write(requestBuff))

		var h1UploadConn *H1Conn
		for {
			var newConnection bool
			var err error
			h1UploadConn, newConnection, err = c.uploadPool.borrow(context.WithoutCancel(ctx))
			if err != nil {
				return err
			}

			if !newConnection {
				// TODO: Replace 0 here with a config value later
				// Or add some other condition for optimization purposes
				if h1UploadConn.UnreadedResponsesCount > 0 {
					resp, err := http.ReadResponse(h1UploadConn.RespBufReader, req)
					if err != nil {
						return fmt.Errorf("error while reading response: %s", err.Error())
					}
					io.Copy(io.Discard, resp.Body)
					defer resp.Body.Close()
					if resp.StatusCode != 200 {
						return fmt.Errorf("got non-200 error response code: %d", resp.StatusCode)
					}
				}
			}

			_, err = h1UploadConn.Write(requestBuff.Bytes())
			// if the write failed, we try another connection from
			// the pool, until the write on a new connection fails.
			// failed writes to a pooled connection are normal when
			// the connection has been closed in the meantime.
			if err == nil {
				break
			} else if newConnection {
				return err
			}
		}

		c.uploadPool.returnConn(h1UploadConn)
	}

	return nil
}

// CloseIdleConnections retires the client: no new requests should be issued
// on it, but requests already in flight are allowed to finish. Physical
// connections that are currently idle are closed right away, so that
// connections of retired clients do not linger until IdleConnTimeout.
func (c *DefaultDialerClient) CloseIdleConnections() {
	if transport, ok := c.client.Transport.(interface{ CloseIdleConnections() }); ok {
		transport.CloseIdleConnections()
	}
	c.uploadPool.closeIdle()
}

// Close implements common.Closable. For HTTP/3 the whole transport including
// in-flight requests is terminated. HTTP/1.1 and HTTP/2 connections cannot be
// force-closed through their transports, so for them this is equivalent to
// CloseIdleConnections: idle connections are closed, in-flight requests are
// allowed to finish.
func (c *DefaultDialerClient) Close() error {
	if h3Transport, ok := c.client.Transport.(*http3.Transport); ok {
		h3Transport.Close()
		return nil
	}
	c.CloseIdleConnections()
	return nil
}

type WaitReadCloser struct {
	wait   *done.Instance
	reader atomic.Pointer[io.ReadCloser]

	// onFinish is called exactly once, when the request that produced this
	// reader has fully finished: either its response body was closed, or it
	// failed before a response arrived. It may be registered after the
	// request was issued, including after the reader was already closed
	// without ever receiving a response: the callback then fires as soon as
	// the pending request finishes.
	mu       sync.Mutex
	finished bool
	onFinish func()
}

// finish marks the request as finished and fires the callback.
func (w *WaitReadCloser) finish() {
	w.mu.Lock()
	if w.finished {
		w.mu.Unlock()
		return
	}
	w.finished = true
	f := w.onFinish
	w.mu.Unlock()
	if f != nil {
		f()
	}
}

// OnFinish registers f to be called when the request has finished. If it
// already has, f is called immediately.
func (w *WaitReadCloser) OnFinish(f func()) {
	w.mu.Lock()
	already := w.finished
	if !already {
		w.onFinish = f
	}
	w.mu.Unlock()
	if already {
		f()
	}
}

func (w *WaitReadCloser) Set(rc io.ReadCloser) {
	w.reader.Store(&rc)
	if w.wait.Done() {
		if p := w.reader.Swap(nil); p != nil {
			(*p).Close()
		}
		w.finish()
	}
	w.wait.Close()
}

func (w *WaitReadCloser) Read(b []byte) (int, error) {
	rc := w.reader.Load()
	if rc == nil {
		<-w.wait.Wait()
		if rc = w.reader.Load(); rc == nil {
			return 0, io.ErrClosedPipe
		}
	}
	return (*rc).Read(b)
}

func (w *WaitReadCloser) Close() error {
	w.wait.Close()
	if p := w.reader.Swap(nil); p != nil {
		err := (*p).Close()
		w.finish()
		return err
	}
	return nil
}
