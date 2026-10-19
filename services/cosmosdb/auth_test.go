package cosmosdb_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func authNow() time.Time { return time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC) }

func authHandler(t *testing.T, validate bool) *cosmosdb.Handler {
	t.Helper()

	h := newTestHandler(t)
	h.ValidateAuth = validate
	cosmosdb.SetHandlerClock(h, authNow)

	return h
}

func serve(t *testing.T, h *cosmosdb.Handler, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))

	return rec
}

func newReq(t *testing.T, method, path, body string) *http.Request {
	t.Helper()

	return httptest.NewRequestWithContext(t.Context(), method, path, bytes.NewReader([]byte(body)))
}

func TestAuth_ValidateEnforcedOnEveryRoute(t *testing.T) {
	t.Parallel()

	routes := []struct {
		name, method, path, body string
	}{
		{"account root", http.MethodGet, "/", ""},
		{"list databases", http.MethodGet, "/dbs", ""},
		{"create database", http.MethodPost, "/dbs", `{"id":"d"}`},
		{"get database", http.MethodGet, "/dbs/d", ""},
		{"delete database", http.MethodDelete, "/dbs/d", ""},
		{"list containers", http.MethodGet, "/dbs/d/colls", ""},
		{"create container", http.MethodPost, "/dbs/d/colls", `{"id":"c","partitionKey":{"paths":["/pk"]}}`},
		{"get container", http.MethodGet, "/dbs/d/colls/c", ""},
		{"create document", http.MethodPost, "/dbs/d/colls/c/docs", `{"id":"1","pk":"a"}`},
		{"list documents", http.MethodGet, "/dbs/d/colls/c/docs", ""},
		{"get document", http.MethodGet, "/dbs/d/colls/c/docs/1", ""},
		{"list tables", http.MethodGet, "/Tables", ""},
		{"create table", http.MethodPost, "/Tables", `{"TableName":"t"}`},
	}

	for _, rt := range routes {
		t.Run(rt.name, func(t *testing.T) {
			t.Parallel()

			h := authHandler(t, true)

			rec := serve(t, h, newReq(t, rt.method, rt.path, rt.body))
			assert.Equal(t, http.StatusUnauthorized, rec.Code, "anonymous request must be rejected")

			signed := newReq(t, rt.method, rt.path, rt.body)
			require.NoError(t, h.SignRequest(signed))

			rec = serve(t, h, signed)
			assert.NotEqual(t, http.StatusUnauthorized, rec.Code, "correctly signed request must pass auth")
		})
	}
}

func TestAuth_Rejections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(r *http.Request)
		name   string
		skew   time.Duration
	}{
		{name: "date too old", skew: -16 * time.Minute},
		{name: "date too new", skew: 16 * time.Minute},
		{name: "missing date", mutate: func(r *http.Request) { r.Header.Del("X-Ms-Date") }},
		{name: "unparsable date", mutate: func(r *http.Request) { r.Header.Set("X-Ms-Date", "yesterday") }},
		{name: "tampered path", mutate: func(r *http.Request) { r.URL.Path = "/dbs/other" }},
		{name: "wrong verb", mutate: func(r *http.Request) { r.Method = http.MethodDelete }},
		{name: "garbage authorization", mutate: func(r *http.Request) { r.Header.Set("Authorization", "nope") }},
		{
			name:   "empty signature",
			mutate: func(r *http.Request) { r.Header.Set("Authorization", "type%3Dmaster%26ver%3D1.0%26sig%3D") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := authHandler(t, true)
			req := newReq(t, http.MethodGet, "/dbs/d", "")
			require.NoError(t, h.SignRequest(req))

			if tt.skew != 0 {
				cosmosdb.SetHandlerClock(h, func() time.Time { return authNow().Add(-tt.skew) })
			}

			if tt.mutate != nil {
				tt.mutate(req)
			}

			rec := serve(t, h, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		})
	}
}

func TestAuth_SkewWithinToleranceAccepted(t *testing.T) {
	t.Parallel()

	h := authHandler(t, true)
	req := newReq(t, http.MethodGet, "/dbs/d", "")
	require.NoError(t, h.SignRequest(req))

	cosmosdb.SetHandlerClock(h, func() time.Time { return authNow().Add(14 * time.Minute) })

	assert.Equal(t, http.StatusNotFound, serve(t, h, req).Code)
}

func TestAuth_DefaultOffIsPermissive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		header string
	}{
		{name: "no header"},
		{name: "garbage header", header: "total-nonsense"},
		{name: "bad signature", header: "type%3Dmaster%26ver%3D1.0%26sig%3Dwrong"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := authHandler(t, false)
			req := newReq(t, http.MethodGet, "/dbs", "")

			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}

			assert.Equal(t, http.StatusOK, serve(t, h, req).Code)
		})
	}
}

func TestAuth_TableAPISharedKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		want    int
		wantNot int
	}{
		{name: "valid shared key", key: cosmosdb.DefaultMasterKey, wantNot: http.StatusUnauthorized},
		{name: "wrong shared key", key: "d3Jvbmcta2V5", want: http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := authHandler(t, true)
			req := newReq(t, http.MethodGet, "/Tables", "")
			req.Header.Set("X-Ms-Date", authNow().Format(http.TimeFormat))
			req.Header.Set("X-Ms-Version", "2019-02-02")

			sig, err := azureauth.SignSharedKey(req, "acct", tt.key)
			require.NoError(t, err)
			req.Header.Set("Authorization", "SharedKey acct:"+sig)

			rec := serve(t, h, req)

			if tt.want != 0 {
				assert.Equal(t, tt.want, rec.Code)
			} else {
				assert.NotEqual(t, tt.wantNot, rec.Code)
			}
		})
	}
}
