package splithttp

import (
	"context"
	"sync"

	"github.com/xtls/xray-core/common/net"
)

// uploadConnPool is a pool of HTTP/1.1 upload connections. Unlike sync.Pool,
// it keeps track of every connection it handed out, so that idle connections
// can actually be closed when the owning client is retired, instead of
// lingering until the remote side closes them.
type uploadConnPool struct {
	mu     sync.Mutex
	idle   []*H1Conn
	closed bool
	dial   func(ctxInner context.Context) (net.Conn, error)
}

// borrow returns an idle connection, or dials a new one if the pool is empty.
// The second return value reports whether the connection is new.
func (p *uploadConnPool) borrow(ctx context.Context) (*H1Conn, bool, error) {
	p.mu.Lock()
	if n := len(p.idle); n > 0 {
		h1UploadConn := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return h1UploadConn, false, nil
	}
	p.mu.Unlock()

	newConn, err := p.dial(ctx)
	if err != nil {
		return nil, false, err
	}
	return NewH1Conn(newConn), true, nil
}

// returnConn parks a successfully used connection for later reuse. If the
// pool was closed in the meantime, the connection is closed instead: it
// belonged to a retired client and must not be reused.
func (p *uploadConnPool) returnConn(h1UploadConn *H1Conn) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		h1UploadConn.Close()
		return
	}
	p.idle = append(p.idle, h1UploadConn)
	p.mu.Unlock()
}

// closeIdle closes all idle connections and prevents further reuse. Borrowed
// connections are closed by returnConn once they are no longer in use.
func (p *uploadConnPool) closeIdle() {
	p.mu.Lock()
	p.closed = true
	idle := p.idle
	p.idle = nil
	p.mu.Unlock()
	for _, h1UploadConn := range idle {
		h1UploadConn.Close()
	}
}
