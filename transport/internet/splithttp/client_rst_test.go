package splithttp

import (
	"context"
	gotls "crypto/tls"
	stderrors "errors"
	"fmt"
	"io"
	stdnet "net"
	"net/http"
	"testing"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"golang.org/x/net/http2"
)

func startRSTTLSServer(t *testing.T) net.Destination {
	t.Helper()

	listener, err := stdnet.Listen("tcp", "127.0.0.1:0")
	common.Must(err)

	generatedCert, _ := cert.MustGenerate(nil, cert.CommonName("localhost"))
	certPEM, keyPEM := generatedCert.ToPEM()
	keyPair, err := gotls.X509KeyPair(certPEM, keyPEM)
	common.Must(err)
	tlsConfig := &gotls.Config{
		Certificates: []gotls.Certificate{keyPair},
		NextProtos:   []string{"h2"},
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				tlsConn := gotls.Server(conn, tlsConfig)
				if err := tlsConn.Handshake(); err != nil {
					return
				}
				if tlsConn.ConnectionState().NegotiatedProtocol != "h2" {
					return
				}
				preface := make([]byte, len(http2.ClientPreface))
				if _, err := io.ReadFull(tlsConn, preface); err != nil || string(preface) != http2.ClientPreface {
					return
				}
				framer := http2.NewFramer(tlsConn, tlsConn)
				for {
					frame, err := framer.ReadFrame()
					if err != nil {
						return
					}
					switch f := frame.(type) {
					case *http2.SettingsFrame:
						if !f.IsAck() {
							framer.WriteSettings()
							framer.WriteSettingsAck()
						}
					case *http2.HeadersFrame:
						if err := framer.WriteRSTStream(f.StreamID, http2.ErrCodeCancel); err != nil {
							return
						}
					case *http2.PingFrame:
						if !f.IsAck() {
							framer.WritePing(true, f.Data)
						}
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { listener.Close() })

	port := net.Port(listener.Addr().(*stdnet.TCPAddr).Port)
	return net.TCPDestination(net.DomainAddress("localhost"), port)
}

func newRealH2TestClient(dest net.Destination) *DefaultDialerClient {
	transport := &http2.Transport{
		DialTLSContext: func(ctx context.Context, network string, addr string, cfg *gotls.Config) (stdnet.Conn, error) {
			rawConn, err := stdnet.Dial("tcp", dest.NetAddr())
			if err != nil {
				return nil, err
			}
			tlsConn := gotls.Client(rawConn, &gotls.Config{
				InsecureSkipVerify: true,
				ServerName:         "localhost",
				NextProtos:         []string{"h2"},
			})
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				rawConn.Close()
				return nil, err
			}
			if tlsConn.ConnectionState().NegotiatedProtocol != "h2" {
				tlsConn.Close()
				return nil, fmt.Errorf("negotiated %q instead of h2", tlsConn.ConnectionState().NegotiatedProtocol)
			}
			return tlsConn, nil
		},
		IdleConnTimeout: net.ConnIdleTimeout,
	}
	return &DefaultDialerClient{
		transportConfig: &Config{},
		client:          &http.Client{Transport: transport},
		httpVersion:     "2",
		uploadPool:      &uploadConnPool{},
	}
}

func TestDefaultDialerClientStreamResetDoesNotRetireClient(t *testing.T) {
	dest := startRSTTLSServer(t)
	c := newRealH2TestClient(dest)

	err := c.PostPacket(context.Background(), "https://localhost/", "session", "0", nil)
	if err == nil {
		t.Fatal("expected the POST to fail")
	}
	if c.IsClosed() {
		t.Fatal("a stream reset retired the client")
	}

	wrc, _, _, err := c.OpenStream(context.Background(), "https://localhost/", "session", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrc.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the stream reader to resolve with an error")
	}
	if c.IsClosed() {
		t.Fatal("a stream reset retired the client")
	}

	err = c.PostPacket(context.Background(), "https://localhost/", "session", "1", nil)
	if err == nil {
		t.Fatal("expected the second POST to fail")
	}
	var streamErr http2.StreamError
	if !stderrors.As(err, &streamErr) {
		t.Fatal("expected the POST error to still match http2.StreamError, got: ", err)
	}
}
