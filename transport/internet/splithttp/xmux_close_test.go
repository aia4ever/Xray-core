package splithttp_test

import (
	"context"
	"io"
	stdnet "net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/finalmask"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/internet/tls"
)

// countingConn keeps track of physical connections opened by the XHTTP
// client, so tests can assert that connections of retired XMUX clients are
// actually closed and do not linger until IdleConnTimeout.
type countingConn struct {
	stdnet.Conn
	closed  atomic.Bool
	onClose func()
}

func (c *countingConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		c.onClose()
	}
	return c.Conn.Close()
}

func dialCountingConn(active *atomic.Int32) func(context.Context, net.Destination) (net.Conn, error) {
	return func(ctx context.Context, dest net.Destination) (net.Conn, error) {
		conn, err := internet.DialSystem(ctx, dest, nil)
		if err != nil {
			return nil, err
		}
		active.Add(1)
		return &countingConn{Conn: conn, onClose: func() { active.Add(-1) }}, nil
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, active *atomic.Int32, want int32, desc string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if active.Load() == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condition not met within timeout: %s (active=%d, want=%d)", desc, active.Load(), want)
}

// With maxConnections=1 and hMaxRequestTimes=2, every second request rotates
// to a new XMUX client. The retired client must not keep its physical
// connection open until IdleConnTimeout (300s): once its last stream is
// closed, the connection must be closed too.
func Test_ListenXHAndDial_RetiredXmuxClientClosesConnection(t *testing.T) {
	if runtime.GOARCH == "arm64" {
		return
	}

	listenPort := tcp.PickPort()

	ct, ctHash := cert.MustGenerate(nil, cert.CommonName("localhost"))

	var active atomic.Int32
	serverSettings := &internet.MemoryStreamConfig{
		ProtocolName: "splithttp",
		ProtocolSettings: &Config{
			Path: "/sh",
		},
		SecurityType: "tls",
		SecuritySettings: &tls.Config{
			Certificate:          []*tls.Certificate{tls.ParseCertificate(ct)},
			PinnedPeerCertSha256: [][]byte{ctHash[:]},
		},
	}
	clientSettings := &internet.MemoryStreamConfig{
		ProtocolName: "splithttp",
		ProtocolSettings: &Config{
			Path:               "/sh",
			ScMaxEachPostBytes: &RangeConfig{From: 1 << 10, To: 1 << 10},
			Xmux: &XmuxConfig{
				MaxConnections:   &RangeConfig{From: 1, To: 1},
				HMaxRequestTimes: &RangeConfig{From: 2, To: 2},
			},
		},
		SecurityType: "tls",
		SecuritySettings: &tls.Config{
			Certificate:          []*tls.Certificate{tls.ParseCertificate(ct)},
			PinnedPeerCertSha256: [][]byte{ctHash[:]},
		},
		FinalMask: finalmask.NewFinalMask(nil, nil, dialCountingConn(&active), nil, nil, nil),
	}
	listen, err := ListenXH(context.Background(), net.LocalHostIP, listenPort, serverSettings, func(conn stat.Connection) {
		go func(c stat.Connection) {
			defer c.Close()

			var b [1024]byte
			c.SetReadDeadline(time.Now().Add(2 * time.Second))
			_, err := c.Read(b[:])
			if err != nil {
				return
			}

			common.Must2(c.Write([]byte("Response")))
		}(conn)
	})
	common.Must(err)
	defer listen.Close()

	ctx := context.Background()
	dest := net.TCPDestination(net.DomainAddress("localhost"), listenPort)

	// stream 1: the GET goes to the first XMUX client, the first POST
	// rotates to a second one and retires the first
	conn, err := Dial(ctx, dest, clientSettings)
	common.Must(err)
	_, err = conn.Write([]byte("Test connection 1"))
	common.Must(err)

	var b [1024]byte
	n, err := io.ReadFull(conn, b[:len("Response")])
	common.Must(err)
	if string(b[:n]) != "Response" {
		t.Error("response: ", string(b[:n]))
	}
	common.Must(conn.Close())

	// the retired client's connection must be closed; only the connection
	// of the new client may stay open
	waitForCondition(t, 5*time.Second, &active, 1, "retired XMUX client did not close its connection")

	// a second stream still works and rotates again
	conn, err = Dial(ctx, dest, clientSettings)
	common.Must(err)
	_, err = conn.Write([]byte("Test connection 2"))
	common.Must(err)
	n, err = io.ReadFull(conn, b[:len("Response")])
	common.Must(err)
	if string(b[:n]) != "Response" {
		t.Error("response: ", string(b[:n]))
	}
	common.Must(conn.Close())

	waitForCondition(t, 5*time.Second, &active, 1, "second retired XMUX client did not close its connection")
}
