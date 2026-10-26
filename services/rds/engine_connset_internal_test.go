package rds

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const readGuard = 5 * time.Second

var errFakeListenerClosed = errors.New("fake listener closed")

// lateAcceptListener hands out one connection only after Close, modelling an accept that
// completes while shutdown is already sweeping.
type lateAcceptListener struct {
	conn   net.Conn
	closed chan struct{}
	once   sync.Once
	served bool
}

func (l *lateAcceptListener) Accept() (net.Conn, error) {
	<-l.closed

	if l.served {
		return nil, errFakeListenerClosed
	}

	l.served = true

	return l.conn, nil
}

func (l *lateAcceptListener) Close() error {
	l.once.Do(func() { close(l.closed) })

	return nil
}

func (l *lateAcceptListener) Addr() net.Addr { return &net.TCPAddr{} }

func TestListenerClosesConnAcceptedAfterStop(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(b *InMemoryBackend, lb *liveDB, ln net.Listener) (stop func(), loop func(), wg *sync.WaitGroup)
		name string
	}{
		{
			name: "forwarder",
			run: func(b *InMemoryBackend, lb *liveDB, ln net.Listener) (func(), func(), *sync.WaitGroup) {
				f := &tcpForwarder{be: b, lb: lb, ln: ln}

				return f.close, f.acceptLoop, &f.wg
			},
		},
		{
			name: "proxy",
			run: func(b *InMemoryBackend, lb *liveDB, ln net.Listener) (func(), func(), *sync.WaitGroup) {
				p := &pgProxy{be: b, lb: lb, ln: ln}

				return p.close, p.acceptLoop, &p.wg
			},
		},
		{
			name: "endpoint",
			run: func(b *InMemoryBackend, lb *liveDB, ln net.Listener) (func(), func(), *sync.WaitGroup) {
				l := &endpointListener{be: b, lb: lb, ln: ln}

				return l.close, l.acceptLoop, &l.wg
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("123456789012", "us-east-1")
			b.EnableEngine(EngineConfig{})

			t.Cleanup(b.Close)

			client, server := net.Pipe()

			defer func() { _ = client.Close() }()

			ln := &lateAcceptListener{conn: server, closed: make(chan struct{})}
			upstream, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)

			defer func() { _ = upstream.Close() }()

			lb := &liveDB{
				gone: make(chan struct{}), kind: engineMySQL, port: upstream.Addr().(*net.TCPAddr).Port,
				key: unitInstancePrefix + "x", resourceID: "x",
			}

			stop, loop, wg := tt.run(b, lb, ln)

			stop()
			loop()

			_ = client.SetReadDeadline(time.Now().Add(readGuard))

			_, err = client.Read(make([]byte, 1))
			require.Error(t, err)
			require.NotErrorIs(t, err, os.ErrDeadlineExceeded)

			wg.Wait()
		})
	}
}
