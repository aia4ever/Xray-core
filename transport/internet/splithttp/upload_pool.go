package splithttp

import (
	"context"
	"sync"

	"github.com/xtls/xray-core/common/net"
)

type uploadConnPool struct {
	mu     sync.Mutex
	idle   []*H1Conn
	closed bool
	dial   func(ctxInner context.Context) (net.Conn, error)
}

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
