package splithttp_test

import (
	"context"
	gotls "crypto/tls"
	"io"
	stdnet "net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/finalmask"
	. "github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/tls"
	"golang.org/x/net/http2"
)

// startH2TLSServer serves handler over HTTP/2 with TLS, using the certificate
// the test client pins. Tests get full control over response timing and
// stream handling, which the XHTTP hub does not expose.
func startH2TLSServer(t *testing.T, handler http.Handler) (net.Destination, *cert.Certificate, [32]byte) {
	t.Helper()

	listener, err := stdnet.Listen("tcp", "127.0.0.1:0")
	common.Must(err)

	generatedCert, certHash := cert.MustGenerate(nil, cert.CommonName("localhost"))
	certPEM, keyPEM := generatedCert.ToPEM()
	keyPair, err := gotls.X509KeyPair(certPEM, keyPEM)
	common.Must(err)
	tlsConfig := &gotls.Config{
		Certificates: []gotls.Certificate{keyPair},
		NextProtos:   []string{"h2"},
	}
	h2Server := &http2.Server{}

	var once sync.Once
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				tlsConn := gotls.Server(conn, tlsConfig)
				if err := tlsConn.Handshake(); err != nil {
					t.Logf("test server TLS handshake failed: %v", err)
					conn.Close()
					return
				}
				if proto := tlsConn.ConnectionState().NegotiatedProtocol; proto != "h2" {
					t.Logf("test server negotiated %q instead of h2", proto)
					tlsConn.Close()
					return
				}
				h2Server.ServeConn(tlsConn, &http2.ServeConnOpts{Handler: handler})
			}()
		}
	}()
	t.Cleanup(func() {
		once.Do(func() { listener.Close() })
	})

	port := net.Port(listener.Addr().(*stdnet.TCPAddr).Port)
	return net.TCPDestination(net.DomainAddress("localhost"), port), generatedCert, certHash
}

// dialCountingConns tracks both the connections currently open and the total
// number of physical connections dialed by the XHTTP client.
func dialCountingConns(active *atomic.Int32, total *atomic.Int32) func(context.Context, net.Destination) (net.Conn, error) {
	inner := dialCountingConn(active)
	return func(ctx context.Context, dest net.Destination) (net.Conn, error) {
		conn, err := inner(ctx, dest)
		if err == nil {
			total.Add(1)
		}
		return conn, err
	}
}

func h2ClientSettings(serverCert *cert.Certificate, serverCertHash [32]byte, active *atomic.Int32, total *atomic.Int32) *internet.MemoryStreamConfig {
	return &internet.MemoryStreamConfig{
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
			Certificate:          []*tls.Certificate{tls.ParseCertificate(serverCert)},
			PinnedPeerCertSha256: [][]byte{serverCertHash[:]},
		},
		FinalMask: finalmask.NewFinalMask(nil, nil, dialCountingConns(active, total), nil, nil, nil),
	}
}

// With maxConnections=1 and hMaxRequestTimes=2, the second upload POST rotates
// to a new XMUX client, retiring the first. The POST already issued on the
// retired client can still be awaiting its response: it must not be aborted,
// and the retired client's connection must be closed as soon as its last POST
// finishes, not left to linger until IdleConnTimeout.
func Test_ListenXHAndDial_InFlightUploadOutlivesRotation(t *testing.T) {
	if runtime.GOARCH == "arm64" {
		return
	}

	release := make(chan struct{})
	var posts atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet: // the download stream
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		case http.MethodPost: // an upload packet
			posts.Add(1)
			select {
			case <-release:
			case <-r.Context().Done():
			}
			io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		}
	})

	dest, serverCert, serverCertHash := startH2TLSServer(t, handler)
	var active atomic.Int32
	var total atomic.Int32
	clientSettings := h2ClientSettings(serverCert, serverCertHash, &active, &total)

	ctx := context.Background()
	conn, err := Dial(ctx, dest, clientSettings)
	common.Must(err)

	// with scMaxEachPostBytes=1KB, a 1.5KB write is split into exactly two
	// POSTs: the first one is issued on the initial XMUX client, the second
	// one rotates to a new client and retires the first while the first POST
	// is still in flight, its response withheld by the server
	common.Must2(conn.Write(make([]byte, 3<<10/2)))

	deadline := time.Now().Add(5 * time.Second)
	for (posts.Load() < 2 || active.Load() < 2) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if posts.Load() < 2 {
		t.Fatal("expected two upload POSTs, got ", posts.Load())
	}
	if active.Load() != 2 {
		t.Fatal("expected two physical connections (retired client + new client), got ", active.Load())
	}

	// close the stream: the retired client must stay alive for its in-flight
	// POST, even though nothing else references it anymore
	common.Must(conn.Close())
	time.Sleep(300 * time.Millisecond)
	if active.Load() != 2 {
		t.Fatal("the retired client's connection was closed while its POST was still in flight, got ", active.Load())
	}

	// let the POSTs finish: the retired client's connection must close now
	close(release)
	waitForCondition(t, 5*time.Second, &active, 1, "retired client did not close its connection after its last POST finished")
}
