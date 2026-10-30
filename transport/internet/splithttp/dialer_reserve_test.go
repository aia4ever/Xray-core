package splithttp

import (
	"context"
	"io"
	"sync/atomic"
	"testing"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
)

type fakeDialerClient struct {
	closed     atomic.Bool
	closedIdle atomic.Bool
}

func (f *fakeDialerClient) IsClosed() bool {
	return false
}

func (f *fakeDialerClient) OpenStream(context.Context, string, string, io.Reader, bool) (io.ReadCloser, net.Addr, net.Addr, error) {
	return nil, nil, nil, nil
}

func (f *fakeDialerClient) PostPacket(context.Context, string, string, string, buf.MultiBuffer) error {
	return nil
}

func (f *fakeDialerClient) Close() error {
	f.closed.Store(true)
	return nil
}

func (f *fakeDialerClient) CloseIdleConnections() {
	f.closedIdle.Store(true)
}

// getHTTPClient must return the XMUX client with a running reference already
// taken under globalDialerAccess. Without that reservation, a concurrent
// getHTTPClient can sweep the client in the window between its selection and
// the AddRunning of the caller: the sweep marks it unused, sees Running == 0
// and closes its connection, while the first caller keeps issuing requests on
// the closed client.
func TestGetHTTPClientReservesXmuxClient(t *testing.T) {
	dest := net.TCPDestination(net.DomainAddress("reservation.test"), 443)
	settings := &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{},
	}
	key := dialerConf{dest, settings}

	var conns []*fakeDialerClient
	xmuxManager := NewXmuxManager(XmuxConfig{
		MaxConnections:   &RangeConfig{From: 1, To: 1},
		HMaxRequestTimes: &RangeConfig{From: 1, To: 1},
	}, func() XmuxConn {
		conn := &fakeDialerClient{}
		conns = append(conns, conn)
		return conn
	})

	globalDialerAccess.Lock()
	if globalDialerMap == nil {
		globalDialerMap = make(map[dialerConf]*XmuxManager)
	}
	globalDialerMap[key] = xmuxManager
	globalDialerAccess.Unlock()
	defer func() {
		globalDialerAccess.Lock()
		delete(globalDialerMap, key)
		globalDialerAccess.Unlock()
	}()

	httpClient, xmuxClient := getHTTPClient(context.Background(), dest, settings)
	if xmuxClient == nil || httpClient != DialerClient(conns[0]) {
		t.Fatal("expected the first XMUX client to be selected")
	}
	if got := xmuxClient.Running.Load(); got != 1 {
		t.Fatal("expected Running == 1 right after getHTTPClient, got ", got)
	}

	// exhaust the request budget the way a Dial on this client would, then
	// select again: the sweep in the second call retires the first client,
	// but must not close it while the reservation is held
	xmuxClient.LeftRequests.Store(0)
	_, xmuxClient2 := getHTTPClient(context.Background(), dest, settings)
	defer xmuxClient2.DoneRunning()
	if xmuxClient2 == xmuxClient {
		t.Fatal("expected a new XMUX client after the request budget was exhausted")
	}
	if len(conns) != 2 {
		t.Fatal("expected the manager to create a second client, got ", len(conns))
	}
	if conns[0].closed.Load() || conns[0].closedIdle.Load() {
		t.Fatal("the retired client was closed while a stream was still using it")
	}

	// releasing the last reference is what retires the client's connections
	xmuxClient.DoneRunning()
	if !conns[0].closedIdle.Load() {
		t.Fatal("expected the retired client's idle connections to be closed after the last reference was released")
	}
}

func TestXmuxClientRequestReferences(t *testing.T) {
	conn := &fakeDialerClient{}
	xmuxClient := &XmuxClient{XmuxConn: conn}

	xmuxClient.AddRequest()
	xmuxClient.NotUsed.Store(true)
	xmuxClient.maybeClose()
	if conn.closedIdle.Load() {
		t.Fatal("the client was closed while a request was still in flight")
	}

	xmuxClient.DoneRequest()
	if !conn.closedIdle.Load() {
		t.Fatal("expected the client to be closed after the last request finished")
	}
}
