package internet

import (
	"context"
	"reflect"
	"sync"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/net/cnc"
	"github.com/xtls/xray-core/transport/internet/finalmask"
)

// MemoryStreamConfig is a parsed form of StreamConfig. It is used to reduce the number of Protobuf parses.
type MemoryStreamConfig struct {
	Destination      *net.Destination
	ProtocolName     string
	ProtocolSettings interface{}
	SecurityType     string
	SecuritySettings interface{}
	FinalMask        *finalmask.FinalMask
	QuicParams       *QuicParams
	SocketSettings   *SocketConfig
	DownloadSettings *MemoryStreamConfig

	mu      sync.Mutex
	closed  bool
	onClose []func()
}

// RegisterOnClose registers f to run once when the config is closed. It
// returns false if the config was already closed, in which case f is not
// registered and not run: the caller must undo whatever it did that the
// callback would have undone. Callbacks run without any lock held, so they
// may acquire any lock, including ones held by callers of this function.
func (m *MemoryStreamConfig) RegisterOnClose(f func()) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false
	}
	m.onClose = append(m.onClose, f)
	return true
}

// Close implements common.Closable. It runs the registered close callbacks
// and closes the DownloadSettings as well. Closing twice is a no-op.
func (m *MemoryStreamConfig) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	callbacks := m.onClose
	m.onClose = nil
	downloadSettings := m.DownloadSettings
	m.mu.Unlock()

	for _, f := range callbacks {
		f()
	}
	if downloadSettings != nil {
		downloadSettings.Close()
	}
	return nil
}

// SetDownloadSettings sets the DownloadSettings if they are not set yet, and
// returns the settings that are in effect afterwards.
func (m *MemoryStreamConfig) SetDownloadSettings(ds *MemoryStreamConfig) *MemoryStreamConfig {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.DownloadSettings == nil {
		m.DownloadSettings = ds
	}
	return m.DownloadSettings
}

// ToMemoryStreamConfig converts a StreamConfig to MemoryStreamConfig. It returns a default non-nil MemoryStreamConfig for nil input.
func ToMemoryStreamConfig(s *StreamConfig) (*MemoryStreamConfig, error) {
	ets, err := s.GetEffectiveTransportSettings()
	if err != nil {
		return nil, err
	}

	mss := &MemoryStreamConfig{
		ProtocolName:     s.GetEffectiveProtocol(),
		ProtocolSettings: ets,
	}

	if s != nil {
		if s.Address != nil {
			mss.Destination = &net.Destination{
				Address: s.Address.AsAddress(),
				Port:    net.Port(s.Port),
				Network: net.Network_TCP,
			}
		}
		mss.SocketSettings = s.SocketSettings
	}

	if s != nil && s.HasSecuritySettings() {
		ess, err := s.GetEffectiveSecuritySettings()
		if err != nil {
			return nil, err
		}
		mss.SecurityType = s.SecurityType
		mss.SecuritySettings = ess
	}
	if s != nil && (len(s.Tcpmasks) != 0 || len(s.Udpmasks) != 0) {
		var tcpMasks []finalmask.TCPMask
		var udpMasks []finalmask.UDPMask

		for i := range s.Tcpmasks {
			instance := common.Must2(s.Tcpmasks[i].GetInstance())
			tcpMasks = append(tcpMasks, instance.(finalmask.TCPMask))
		}
		for i := range s.Udpmasks {
			instance := common.Must2(s.Udpmasks[i].GetInstance())
			udpMasks = append(udpMasks, instance.(finalmask.UDPMask))
		}

		dialTCP := func(ctx context.Context, dest net.Destination) (net.Conn, error) {
			return DialSystem(ctx, dest, mss.SocketSettings)
		}
		listen := func(ctx context.Context, addr net.Addr) (net.Listener, error) {
			return ListenSystem(ctx, addr, mss.SocketSettings)
		}
		dialUDP := func(ctx context.Context, dest net.Destination) (net.PacketConn, net.Addr, error) {
			conn, err := DialSystem(ctx, dest, mss.SocketSettings)
			if err != nil {
				return nil, nil, err
			}
			var newConn net.PacketConn
			var udpAddr net.Addr
			switch c := conn.(type) {
			case *net.PacketConnWrapper:
				newConn = c.PacketConn
				udpAddr = conn.RemoteAddr()
			case *cnc.Connection:
				newConn = &FakePacketConn{Conn: c}
				udpAddr = &net.UDPAddr{IP: []byte{0, 0, 0, 0}, Port: 0}
			default:
				panic(reflect.TypeOf(c))
			}
			return newConn, udpAddr, nil
		}
		listenPacket := func(ctx context.Context, addr net.Addr) (net.PacketConn, error) {
			return ListenSystemPacket(ctx, addr, mss.SocketSettings)
		}
		mss.FinalMask = finalmask.NewFinalMask(tcpMasks, udpMasks, dialTCP, listen, dialUDP, listenPacket)
	}

	if s != nil && s.QuicParams != nil {
		mss.QuicParams = s.QuicParams
	}

	return mss, nil
}
