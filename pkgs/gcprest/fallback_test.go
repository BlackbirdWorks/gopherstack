package gcprest_test

import (
	"net"
	"net/http"
	"testing"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest"
)

func TestServeWithFallback(t *testing.T) {
	t.Parallel()

	srv := grpc.NewServer()
	longrunningpb.RegisterOperationsServer(srv, fakeOps{})

	var lc net.ListenConfig

	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	fallback := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("fallback " + r.URL.Path))
	})

	done := make(chan struct{})

	go func() {
		defer close(done)

		_ = gcprest.ServeWithFallback(srv, l, fallback)
	}()

	t.Cleanup(func() {
		srv.Stop()
		<-done
	})

	base := "http://" + l.Addr().String()

	tests := []struct {
		name   string
		path   string
		body   string
		status int
	}{
		{"v2 root", "/v2/", "fallback /v2/", http.StatusTeapot},
		{"v2 bare", "/v2", "fallback /v2", http.StatusTeapot},
		{"v2 nested", "/v2/p/r/img/manifests/latest", "fallback /v2/p/r/img/manifests/latest", http.StatusTeapot},
		{"similar prefix is REST", "/v2beta/x", "", http.StatusNotFound},
		{"rest still served", "/v1/projects/p/locations/l/operations/op-1", "", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body := do(t, http.MethodGet, base+tt.path, "")
			assert.Equal(t, tt.status, status)

			if tt.body != "" {
				assert.Equal(t, tt.body, body)
			}
		})
	}
}
