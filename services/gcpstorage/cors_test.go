package gcpstorage_test

import (
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

func TestCORS(t *testing.T) {
	t.Parallel()

	rules := []storage.CORS{{
		Origins: []string{"https://app.example"}, Methods: []string{"GET", "PUT"},
		ResponseHeaders: []string{"X-Goog-Meta-A", "Content-Type"}, MaxAge: 2 * time.Minute,
	}, {
		Origins: []string{"*"}, Methods: []string{"HEAD"},
	}}

	tests := []struct {
		headers map[string]string
		want    map[string]string
		name    string
		method  string
		path    string
		absent  []string
		status  int
	}{
		{
			name:   "json preflight",
			method: http.MethodOptions,
			path:   "/storage/v1/b/cors-bkt",
			status: http.StatusOK,
			headers: map[string]string{
				"Origin":                         "https://x.example",
				"Access-Control-Request-Method":  "PATCH",
				"Access-Control-Request-Headers": "x-a",
			},
			want: map[string]string{
				"Access-Control-Allow-Origin":  "https://x.example",
				"Access-Control-Allow-Methods": "DELETE,GET,HEAD,PATCH,POST,PUT",
				"Access-Control-Allow-Headers": "x-a",
				"Access-Control-Max-Age":       "3600",
			},
		},
		{
			name: "json preflight without method", method: http.MethodOptions, path: "/storage/v1/b/cors-bkt",
			status:  http.StatusNotFound,
			headers: map[string]string{"Origin": "https://x.example"},
			want:    map[string]string{"Access-Control-Allow-Credentials": "true"},
		},
		{
			name: "json simple request", method: http.MethodGet, path: "/storage/v1/b/cors-bkt", status: http.StatusOK,
			headers: map[string]string{"Origin": "https://x.example"},
			want:    map[string]string{"Access-Control-Allow-Origin": "https://x.example"},
		},
		{
			name:   "xml preflight match",
			method: http.MethodOptions,
			path:   "/cors-bkt/o",
			status: http.StatusOK,
			headers: map[string]string{
				"Origin":                         "https://app.example",
				"Access-Control-Request-Method":  "PUT",
				"Access-Control-Request-Headers": "content-type",
			},
			want: map[string]string{
				"Access-Control-Allow-Origin": "https://app.example", "Access-Control-Allow-Methods": "PUT",
				"Access-Control-Allow-Headers": "content-type", "Access-Control-Max-Age": "120",
			},
		},
		{
			name:    "xml preflight wildcard origin default max age",
			method:  http.MethodOptions,
			path:    "/cors-bkt/o",
			status:  http.StatusOK,
			headers: map[string]string{"Origin": "https://other.example", "Access-Control-Request-Method": "HEAD"},
			want: map[string]string{
				"Access-Control-Allow-Origin": "https://other.example",
				"Access-Control-Max-Age":      "3600",
			},
		},
		{
			name:    "xml preflight method mismatch",
			method:  http.MethodOptions,
			path:    "/cors-bkt/o",
			status:  http.StatusOK,
			headers: map[string]string{"Origin": "https://app.example", "Access-Control-Request-Method": "DELETE"},
			absent:  []string{"Access-Control-Allow-Origin", "Access-Control-Allow-Methods"},
		},
		{
			name:   "xml preflight header mismatch",
			method: http.MethodOptions,
			path:   "/cors-bkt/o",
			status: http.StatusOK,
			headers: map[string]string{
				"Origin":                         "https://app.example",
				"Access-Control-Request-Method":  "GET",
				"Access-Control-Request-Headers": "x-secret",
			},
			absent: []string{"Access-Control-Allow-Origin"},
		},
		{
			name:    "xml preflight origin mismatch",
			method:  http.MethodOptions,
			path:    "/cors-bkt/o",
			status:  http.StatusOK,
			headers: map[string]string{"Origin": "https://evil.example", "Access-Control-Request-Method": "GET"},
			absent:  []string{"Access-Control-Allow-Origin"},
		},
		{
			name: "xml simple request", method: http.MethodGet, path: "/cors-bkt/o", status: http.StatusOK,
			headers: map[string]string{"Origin": "https://app.example"},
			want: map[string]string{
				"Access-Control-Allow-Origin":   "https://app.example",
				"Access-Control-Expose-Headers": "X-Goog-Meta-A,Content-Type",
			},
		},
		{
			name:    "xml simple request origin mismatch",
			method:  http.MethodGet,
			path:    "/cors-bkt/o",
			status:  http.StatusOK,
			headers: map[string]string{"Origin": "https://evil.example"},
			absent:  []string{"Access-Control-Allow-Origin"},
		},
		{
			name:   "no origin header",
			method: http.MethodGet,
			path:   "/cors-bkt/o",
			status: http.StatusOK,
			absent: []string{"Access-Control-Allow-Origin"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newSDK(t)
			b := e.bucket(t, "cors-bkt", &storage.BucketAttrs{CORS: rules})
			write(t, b.Object("o"), []byte("x"), 0, nil)

			req, err := http.NewRequestWithContext(t.Context(), tt.method, e.url+tt.path, nil)
			require.NoError(t, err)

			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			resp, err := httpclient.New(10 * time.Second).Do(req)
			require.NoError(t, err)
			t.Cleanup(func() { _ = resp.Body.Close() })

			assert.Equal(t, tt.status, resp.StatusCode)

			for k, v := range tt.want {
				assert.Equal(t, v, resp.Header.Get(k), k)
			}

			for _, k := range tt.absent {
				assert.Empty(t, resp.Header.Get(k), k)
			}
		})
	}
}
