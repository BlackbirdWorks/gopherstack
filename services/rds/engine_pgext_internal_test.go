package rds

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func TestPgExtImages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
		want    string
		vector  string
	}{
		{"default", "", "postgis/postgis:17-3.5", "pgvector/pgvector:0.8.6-pg17-bookworm"},
		{"v13", "13.20", "postgis/postgis:13-3.5", "pgvector/pgvector:0.8.6-pg13-bookworm"},
		{"v16", "16.4", "postgis/postgis:16-3.5", "pgvector/pgvector:0.8.6-pg16-bookworm"},
		{"unsupported", "9.6.1", "postgis/postgis:17-3.5", "pgvector/pgvector:0.8.6-pg17-bookworm"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, pgExtImage(pgMajor(tt.version)))
			assert.Equal(t, tt.vector, pgVectorSourceImage(pgMajor(tt.version)))
		})
	}
}

func TestApplyPgExtSpec(t *testing.T) {
	t.Parallel()

	dir := "/cache"

	tests := []struct {
		name       string
		wantImage  string
		entrypoint []string
		enabled    bool
		wantExt    bool
	}{
		{name: "stock untouched", enabled: false, wantImage: "postgres:16"},
		{name: "primary", enabled: true, wantImage: "postgis/postgis:16-3.5", wantExt: true},
		{
			name: "replica", enabled: true, entrypoint: []string{"sh", "-c", "echo replica", "pg-replica"},
			wantImage: "postgis/postgis:16-3.5", wantExt: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := &dbEngine{cfg: EngineConfig{PostgresExtensions: tt.enabled, APIEndpoint: "http://x"}}
			e.cfg.RoleLookup = func(string, string) bool { return true }
			require.NoError(t, e.startCallout())
			t.Cleanup(e.stopCallout)

			lb := &liveDB{kind: enginePostgres, version: "16.4", key: "i:a"}
			lb.extDir.Store(&dir)

			s := container.Spec{Image: "postgres:16", Cmd: []string{"postgres", "-c", "x=1"}, Entrypoint: tt.entrypoint}
			e.applyPgExt(&s, lb)

			assert.Equal(t, tt.wantImage, s.Image)

			if !tt.wantExt {
				assert.Empty(t, s.Mounts)
				assert.Empty(t, s.Env)

				return
			}

			assert.Contains(t, s.Mounts, "/cache/aws:/gs-ext/aws:ro")
			assert.Contains(t, s.Mounts, "/cache/vector-16:/gs-ext/vector:ro")
			require.Len(t, s.Env, 3)
			assert.Contains(t, strings.Join(s.Entrypoint, " "), "gs-ext/aws")
			assert.Equal(t, "sh", s.Entrypoint[0])
		})
	}
}

func TestCalloutServer(t *testing.T) {
	t.Parallel()

	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Amz-Executed-Version", "7")
		_, _ = io.WriteString(w, r.Method+" "+r.URL.RequestURI()+" "+r.Header.Get("X-Amz-Invocation-Type"))
	}))
	t.Cleanup(api.Close)

	roles := map[string]bool{"i:a|s3Import": true, "i:a|Lambda": true}
	e := &dbEngine{cfg: EngineConfig{
		PostgresExtensions: true, APIEndpoint: api.URL,
		RoleLookup: func(key, feature string) bool { return roles[key+"|"+feature] },
	}}
	require.NoError(t, e.startCallout())
	t.Cleanup(e.stopCallout)

	tok := e.calloutToken(&liveDB{key: "i:a"})
	e.calloutMu.Lock()
	listenIP, port := e.callout.listenIP, e.callout.port
	e.calloutMu.Unlock()

	assert.False(t, net.ParseIP(listenIP).IsUnspecified(), "callout must not bind a wildcard address: %s", listenIP)

	base := "http://" + net.JoinHostPort(listenIP, strconv.Itoa(port))

	tests := []struct {
		name       string
		method     string
		path       string
		token      string
		wantBody   string
		wantStatus int
	}{
		{name: "bad token", method: "GET", path: "/s3/b/k", token: "nope", wantStatus: http.StatusUnauthorized},
		{
			name: "import allowed", method: "GET", path: "/s3/b/dir/k.csv", token: tok,
			wantStatus: http.StatusOK, wantBody: "GET /b/dir/k.csv ",
		},
		{name: "export denied", method: "PUT", path: "/s3/b/k", token: tok, wantStatus: http.StatusForbidden},
		{
			name: "export with credentials", method: "PUT", path: "/s3/b/k?cred=1", token: tok,
			wantStatus: http.StatusOK,
		},
		{
			name: "lambda allowed", method: "POST", path: "/lambda/fn?Qualifier=v1", token: tok,
			wantStatus: http.StatusOK, wantBody: "POST /2015-03-31/functions/fn/invocations?Qualifier=v1 Event",
		},
		{name: "auth lambda", method: "POST", path: "/auth/Lambda", token: tok, wantStatus: http.StatusOK},
		{
			name: "auth export denied", method: "POST", path: "/auth/s3Export", token: tok,
			wantStatus: http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequestWithContext(t.Context(), tt.method, base+tt.path, strings.NewReader("x"))
			require.NoError(t, err)

			req.Header.Set(calloutAuthHeader, tt.token)
			req.Header.Set("X-Amz-Invocation-Type", "Event")

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			body, _ := io.ReadAll(resp.Body)
			require.NoError(t, resp.Body.Close())
			assert.Equal(t, tt.wantStatus, resp.StatusCode)

			if tt.wantBody != "" {
				assert.Equal(t, tt.wantBody, string(body))
			}

			if tt.wantStatus == http.StatusForbidden {
				assert.Contains(t, string(body), "AddRoleToDBInstance")
			}
		})
	}
}
