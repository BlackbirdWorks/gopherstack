package gcpserve_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
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

	front := httptest.NewServer(gcpserve.Proxy(port, http.DefaultTransport))
	t.Cleanup(front.Close)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, front.URL, nil)
	require.NoError(t, err)

	req.Host = "fn.localhost"

	resp, err := http.DefaultClient.Do(req)
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
