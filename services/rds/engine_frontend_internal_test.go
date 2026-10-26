package rds

import (
	"context"
	"net"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFrontendListenerWaitGroupOrdering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		start func(*InMemoryBackend, *liveDB) error
		front func(*liveDB) (*sync.WaitGroup, int)
		name  string
		kind  string
	}{
		{
			name:  "proxy",
			kind:  enginePostgres,
			start: (*InMemoryBackend).startProxy,
			front: func(lb *liveDB) (*sync.WaitGroup, int) {
				if p := lb.proxy.Load(); p != nil {
					return &p.wg, p.port
				}

				return nil, 0
			},
		},
		{
			name:  "forwarder",
			kind:  engineMySQL,
			start: (*InMemoryBackend).startForwarder,
			front: func(lb *liveDB) (*sync.WaitGroup, int) {
				if f := lb.fwd.Load(); f != nil {
					return &f.wg, f.port
				}

				return nil, 0
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			{
				upstream, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
				require.NoError(t, err)

				defer func() { _ = upstream.Close() }()

				b := NewInMemoryBackend("123456789012", "us-east-1")
				b.EnableEngine(EngineConfig{})

				_, upstreamPort, err := net.SplitHostPort(upstream.Addr().String())
				require.NoError(t, err)

				port, err := strconv.Atoi(upstreamPort)
				require.NoError(t, err)

				lb := &liveDB{
					gone: make(chan struct{}), kind: tt.kind, port: port,
					key: unitInstancePrefix + "x", resourceID: "x",
				}

				var dialErr error

				waited, dialed := make(chan struct{}), make(chan struct{})

				go func() {
					defer close(waited)

					wg, frontPort := tt.front(lb)
					for wg == nil {
						runtime.Gosched()
						wg, frontPort = tt.front(lb)
					}

					conn, derr := (&net.Dialer{}).DialContext(context.Background(), "tcp",
						net.JoinHostPort("127.0.0.1", strconv.Itoa(frontPort)))
					dialErr = derr
					close(dialed)

					if derr == nil {
						defer func() { _ = conn.Close() }()
					}

					wg.Wait()
				}()

				require.NoError(t, tt.start(b, lb))
				<-dialed

				lb.release()
				<-waited
				require.NoError(t, dialErr)
				b.Close()
			}
		})
	}
}
