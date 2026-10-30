package splithttp

import (
	"context"
	"net"
	"sync"
	"testing"
)

func TestUploadConnPoolCloseIdle(t *testing.T) {
	var mu sync.Mutex
	dialed := 0
	closed := 0

	p := &uploadConnPool{
		dial: func(ctx context.Context) (net.Conn, error) {
			c1, c2 := net.Pipe()
			go func() {
				buf := make([]byte, 1024)
				for {
					if _, err := c2.Read(buf); err != nil {
						return
					}
				}
			}()
			mu.Lock()
			dialed++
			mu.Unlock()
			return &closeTrackingConn{Conn: c1, onClose: func() {
				mu.Lock()
				closed++
				mu.Unlock()
			}}, nil
		},
	}

	conn1, isNew, err := p.borrow(context.Background())
	if err != nil || !isNew {
		t.Fatal("expected a new connection, got", isNew, err)
	}
	p.returnConn(conn1)
	conn2, isNew, err := p.borrow(context.Background())
	if err != nil || isNew || conn2 != conn1 {
		t.Fatal("expected the pooled connection to be reused")
	}
	p.returnConn(conn2)
	p.closeIdle()
	mu.Lock()
	if closed != 1 {
		t.Fatal("expected the idle connection to be closed, closed =", closed)
	}
	mu.Unlock()
	p.returnConn(conn1)
	mu.Lock()
	if closed != 2 {
		t.Fatal("expected the returned connection to be closed, closed =", closed)
	}
	mu.Unlock()
	conn3, isNew, err := p.borrow(context.Background())
	if err != nil || !isNew {
		t.Fatal("expected a new connection after closeIdle")
	}
	mu.Lock()
	if dialed != 2 {
		t.Fatal("expected 2 dials, got", dialed)
	}
	mu.Unlock()
	conn3.Close()
}

type closeTrackingConn struct {
	net.Conn
	onClose func()
}

func (c *closeTrackingConn) Close() error {
	c.onClose()
	return c.Conn.Close()
}
