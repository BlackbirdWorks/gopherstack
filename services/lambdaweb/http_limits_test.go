package lambdaweb_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const headLimit = 128 * 1024

func TestDataPlane_RequestHead(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		proto    string
		padExtra int
		want     int
	}{
		{name: "head at limit", proto: "HTTP/1.1", padExtra: 0, want: http.StatusOK},
		{name: "head one byte over", proto: "HTTP/1.1", padExtra: 1, want: http.StatusRequestHeaderFieldsTooLarge},
		{name: "http 1.0", proto: "HTTP/1.0", want: http.StatusHTTPVersionNotSupported},
		{name: "http 2", proto: "HTTP/2.0", want: http.StatusHTTPVersionNotSupported},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d := newDataEnv(t)

			r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
			r.Host = d.domain

			if tc.proto == "HTTP/1.0" {
				r.ProtoMajor, r.ProtoMinor = 1, 0
			}

			if tc.proto == "HTTP/2.0" {
				r.ProtoMajor, r.ProtoMinor = 2, 0
			}

			fixed := len("GET / HTTP/1.1\r\n") + len("Host: ") + len(d.domain) + len("\r\n") +
				len("X-Pad: \r\n") + len("\r\n")
			r.Header.Set("X-Pad", strings.Repeat("a", headLimit-fixed+tc.padExtra))

			w := httptest.NewRecorder()
			require.True(t, d.backend.ServeEndpoint(w, r))
			assert.Equal(t, tc.want, w.Code)
		})
	}
}

func TestDataPlane_ConnectionNamedHeadersRemoved(t *testing.T) {
	t.Parallel()

	d := newDataEnv(t)

	resp := d.do(t, http.MethodGet, d.domain, "/", func(r *http.Request) {
		r.Header.Set("Connection", "X-Hop, X-Other")
		r.Header.Set("X-Hop", "1")
		r.Header.Set("X-Other", "2")
		r.Header.Set("X-Keep", "3")
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	app, _ := d.launcher.apps.Load(d.rev1)
	seen := <-app.(*fakeApp).seen
	assert.Empty(t, seen.Header.Get("X-Hop"))
	assert.Empty(t, seen.Header.Get("X-Other"))
	assert.Equal(t, "3", seen.Header.Get("X-Keep"))
}
