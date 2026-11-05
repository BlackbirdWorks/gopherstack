package elbv2_test

import (
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

const proxyPath = "/proxied"

func TestDataPlane_CloseDropsPooledTargetConnections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		close func(b *elbv2.InMemoryBackend, h *elbv2.Handler)
		name  string
	}{
		{name: "backend_close", close: func(b *elbv2.InMemoryBackend, _ *elbv2.Handler) { b.Close() }},
		{name: "handler_shutdown", close: func(_ *elbv2.InMemoryBackend, h *elbv2.Handler) { h.Shutdown(t.Context()) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// The shared health prober also dials the target and other parallel tests
			// close its pool, so track only the connection that served the proxied path.
			var proxied atomic.Value

			closed := make(chan struct{}, 8)
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == proxyPath {
					proxied.Store(r.RemoteAddr)
				}

				_, _ = w.Write([]byte("ok"))
			}))
			srv.Config.ConnState = func(c net.Conn, s http.ConnState) {
				if addr, _ := proxied.Load().(string); s == http.StateClosed && addr == c.RemoteAddr().String() {
					closed <- struct{}{}
				}
			}
			srv.Start()
			t.Cleanup(srv.Close)

			f := newDPFixture(t)
			tg := f.tg(t, "idle-tg", elbv2types.TargetTypeEnumIp, "")
			f.register(t, tg, srv.Listener.Addr().String())
			f.listener(t, forwardAction(tg))

			res := f.do(t, dpReq{path: proxyPath})
			require.Equal(t, http.StatusOK, res.status)

			select {
			case <-closed:
				require.FailNow(t, "target connection closed before backend close")
			default:
			}

			be, ok := f.h.Backend.(*elbv2.InMemoryBackend)
			require.True(t, ok)
			tt.close(be, f.h)

			select {
			case <-closed:
			case <-time.After(10 * time.Second):
				assert.Fail(t, "pooled target connection survived close")
			}
		})
	}
}
