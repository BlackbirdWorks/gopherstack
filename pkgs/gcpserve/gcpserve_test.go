package gcpserve_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

type pool struct{ released []int }

func (p *pool) Acquire(string) (int, error) { return 4242, nil }
func (p *pool) Release(port int) error {
	p.released = append(p.released, port)

	return nil
}

func TestAcquireReleasePort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pool     *pool
		name     string
		wantPort int
	}{
		{name: "from pool", pool: &pool{}, wantPort: 4242},
		{name: "free port without pool"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var pp gcpserve.PortPool
			if tt.pool != nil {
				pp = tt.pool
			}

			port, err := gcpserve.AcquirePort(t.Context(), pp, "x")
			require.NoError(t, err)

			if tt.wantPort != 0 {
				assert.Equal(t, tt.wantPort, port)
			} else {
				assert.Positive(t, port)
			}

			gcpserve.ReleasePort(pp, port)

			if tt.pool != nil {
				assert.Equal(t, []int{port}, tt.pool.released)
			}
		})
	}
}

func TestWaitReadyAndProxy(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("host=" + r.Host))
	}))
	t.Cleanup(srv.Close)

	_, portStr, err := net.SplitHostPort(srv.Listener.Addr().String())
	require.NoError(t, err)

	port := srv.Listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, gcpserve.WaitReady(t.Context(), port))

	front := httptest.NewServer(gcpserve.Proxy(port, httpclient.NewTransport()))
	t.Cleanup(front.Close)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, front.URL, nil)
	require.NoError(t, err)

	req.Host = "fn.localhost"

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	buf := make([]byte, 64)
	n, _ := resp.Body.Read(buf)
	assert.Equal(t, "host=fn.localhost", string(buf[:n]))
	assert.NotEmpty(t, portStr)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	require.Error(t, gcpserve.WaitReady(ctx, 1))
}

func TestWaitReadyRequiresHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		answers bool
		wantErr bool
	}{
		{name: "tcp only like a published port", wantErr: true},
		{name: "http answers", answers: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var lc net.ListenConfig

			l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)

			if tt.answers {
				srv := &http.Server{Handler: http.NotFoundHandler(), ReadHeaderTimeout: time.Second}
				go func() { _ = srv.Serve(l) }()

				t.Cleanup(func() { _ = srv.Close() })
			} else {
				t.Cleanup(func() { _ = l.Close() })

				go func() {
					for {
						c, aerr := l.Accept()
						if aerr != nil {
							return
						}

						_ = c.Close()
					}
				}()
			}

			// Only the failing case needs a deadline; the success case must not race one.
			ctx := t.Context()

			if tt.wantErr {
				var cancel context.CancelFunc

				ctx, cancel = context.WithTimeout(ctx, 300*time.Millisecond)
				defer cancel()
			}

			err = gcpserve.WaitReady(ctx, l.Addr().(*net.TCPAddr).Port)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
