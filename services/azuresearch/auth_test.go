package azuresearch_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuresearch"
)

func TestKeyPermissions(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedHotels(t)

	cases := []struct {
		body   any
		name   string
		method string
		path   string
		key    string
		want   int
	}{
		{nil, "no key", http.MethodGet, "/indexes?", "", http.StatusForbidden},
		{nil, "wrong key", http.MethodGet, "/indexes?", "WRONG", http.StatusForbidden},
		{nil, "admin lists indexes", http.MethodGet, "/indexes?", "admin", http.StatusOK},
		{nil, "query key cannot list indexes", http.MethodGet, "/indexes?", "query", http.StatusForbidden},
		{hotelIndex(), "query key cannot create index", http.MethodPost, "/indexes?", "query", http.StatusForbidden},
		{nil, "query key cannot delete index", http.MethodDelete, "/indexes('hotels')?", "query", http.StatusForbidden},
		{
			map[string]any{"value": []any{map[string]any{"HotelId": "9"}}},
			"query key cannot write docs",
			http.MethodPost,
			"/indexes('hotels')/docs/search.index?",
			"query",
			http.StatusForbidden,
		},
		{nil, "query key can search", http.MethodGet, "/indexes('hotels')/docs?search=motel&", "query", http.StatusOK},
		{
			map[string]any{"search": "motel"},
			"query key can post search",
			http.MethodPost,
			"/indexes('hotels')/docs/search.post.search?",
			"query",
			http.StatusOK,
		},
		{nil, "query key can look up", http.MethodGet, "/indexes('hotels')/docs('1')?", "query", http.StatusOK},
		{nil, "query key can count", http.MethodGet, "/indexes('hotels')/docs/$count?", "query", http.StatusOK},
		{
			nil,
			"query key can suggest",
			http.MethodGet,
			"/indexes('hotels')/docs/search.suggest?search=mo&suggesterName=sg&",
			"query",
			http.StatusOK,
		},
		{nil, "query key cannot read stats", http.MethodGet, "/servicestats?", "query", http.StatusForbidden},
		{nil, "query key cannot list indexers", http.MethodGet, "/indexers?", "query", http.StatusForbidden},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			key := map[string]string{"admin": s.admin, "query": s.query}[tc.key]
			if key == "" {
				key = tc.key
			}

			r := s.do(t, tc.method, tc.path+apiVer, key, tc.body)
			assert.Equal(t, tc.want, r.status, string(r.raw))
		})
	}
}

func TestAPIVersion(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")

	cases := []struct {
		name string
		path string
		want int
	}{
		{"missing", "/indexes", http.StatusBadRequest},
		{"unsupported", "/indexes?api-version=1999-01-01", http.StatusBadRequest},
		{"stable", "/indexes?api-version=2024-07-01", http.StatusOK},
		{"older stable", "/indexes?api-version=2023-11-01", http.StatusOK},
		{"preview", "/indexes?api-version=2024-05-01-Preview", http.StatusOK},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, s.do(t, http.MethodGet, tc.path, s.admin, nil).status)
		})
	}

	r := s.do(t, http.MethodGet, "/indexes", s.admin, nil)
	errObj, _ := r.body["error"].(map[string]any)
	assert.Contains(t, errObj["message"], "api-version")
}

func TestAuthModes(t *testing.T) {
	t.Parallel()

	cfg := func(mut func(*azuresearch.ServiceConfig)) azuresearch.ServiceConfig {
		c := azuresearch.ServiceConfig{
			SKU: "standard", HostingMode: "default", PublicNetworkAccess: "enabled", AuthMode: "apiKeyOnly",
			ReplicaCount: 1, PartitionCount: 1,
		}
		mut(&c)

		return c
	}

	setAAD := func(mode string) func(*azuresearch.ServiceConfig) {
		return func(c *azuresearch.ServiceConfig) { c.AuthMode, c.AADFailureMode = "aadOrApiKey", mode }
	}

	cases := []struct {
		mut           func(*azuresearch.ServiceConfig)
		name          string
		want          int
		useKey        bool
		bearer        bool
		wantChallenge bool
	}{
		{mut: func(*azuresearch.ServiceConfig) {}, name: "api key only accepts key", want: http.StatusOK, useKey: true},
		{
			mut:    func(*azuresearch.ServiceConfig) {},
			name:   "api key only rejects bearer",
			want:   http.StatusForbidden,
			bearer: true,
		},
		{mut: setAAD(""), name: "aad or key accepts bearer", want: http.StatusOK, bearer: true},
		{mut: setAAD(""), name: "aad or key accepts key", want: http.StatusOK, useKey: true},
		{
			mut: setAAD("http401WithBearerChallenge"), name: "challenge when no credentials",
			want: http.StatusUnauthorized, wantChallenge: true,
		},
		{mut: setAAD("http403"), name: "403 when no credentials", want: http.StatusForbidden},
		{
			mut:    func(c *azuresearch.ServiceConfig) { c.DisableLocalAuth = true },
			name:   "local auth disabled rejects key",
			want:   http.StatusForbidden,
			useKey: true,
		},
		{
			mut:    func(c *azuresearch.ServiceConfig) { c.DisableLocalAuth = true },
			name:   "local auth disabled accepts bearer",
			want:   http.StatusOK,
			bearer: true,
		},
		{
			mut:    func(c *azuresearch.ServiceConfig) { c.PublicNetworkAccess = "disabled" },
			name:   "public access disabled",
			want:   http.StatusForbidden,
			useKey: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t, "standard")
			_, err := s.b.PutService(testService, cfg(tc.mut))
			require.NoError(t, err)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, s.url+"/indexes?"+apiVer, nil)
			require.NoError(t, err)

			if tc.useKey {
				req.Header.Set("Api-Key", s.admin)
			}

			if tc.bearer {
				req.Header.Set("Authorization", "Bearer token")
			}

			res, err := testclient.New(t).Do(req)
			require.NoError(t, err)
			res.Body.Close()

			assert.Equal(t, tc.want, res.StatusCode)
			assert.Equal(t, tc.wantChallenge, res.Header.Get("WWW-Authenticate") != "")
		})
	}
}

func TestKeyLifecycle(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	oldAdmin := s.admin

	keys, err := s.b.RegenerateAdminKey(testService, "primary")
	require.NoError(t, err)
	assert.NotEqual(t, oldAdmin, keys.Primary)

	assert.Equal(
		t,
		http.StatusForbidden,
		s.do(t, http.MethodGet, "/indexes?"+apiVer, oldAdmin, nil).status,
		"regenerated key is revoked",
	)
	assert.Equal(t, http.StatusOK, s.do(t, http.MethodGet, "/indexes?"+apiVer, keys.Primary, nil).status)

	_, err = s.b.RegenerateAdminKey(testService, "tertiary")
	require.ErrorIs(t, err, azuresearch.ErrKeyKind)

	qk, err := s.b.CreateQueryKey(testService, "reader")
	require.NoError(t, err)
	assert.Equal(
		t,
		http.StatusNotFound,
		s.do(t, http.MethodGet, "/indexes('x')/docs?"+apiVer, qk.Key, nil).status,
		"authenticated, index missing",
	)

	_, err = s.b.CreateQueryKey(testService, "reader")
	require.ErrorIs(t, err, azuresearch.ErrQueryKeyNameTaken)

	require.NoError(t, s.b.DeleteQueryKey(testService, qk.Key))
	require.ErrorIs(t, s.b.DeleteQueryKey(testService, qk.Key), azuresearch.ErrQueryKeyNotFound)
	assert.Equal(t, http.StatusForbidden, s.do(t, http.MethodGet, "/indexes('x')/docs?"+apiVer, qk.Key, nil).status)
}

func TestMultiServiceHostRouting(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")

	_, err := s.b.PutService("other", azuresearch.ServiceConfig{
		SKU:                 "basic",
		HostingMode:         "default",
		PublicNetworkAccess: "enabled",
		AuthMode:            "apiKeyOnly",
		ReplicaCount:        1,
		PartitionCount:      1,
	})
	require.NoError(t, err)

	otherKeys, err := s.b.Keys("other")
	require.NoError(t, err)

	s.must(t, http.MethodPost, "/indexes?"+apiVer, hotelIndex(), http.StatusCreated)

	own := s.do(t, http.MethodGet, "/indexes?"+apiVer, otherKeys.Primary, nil)
	require.Equal(t, http.StatusOK, own.status)
	assert.Empty(t, own.body["value"], "each key addresses its own service")

	mine := s.do(t, http.MethodGet, "/indexes?"+apiVer, s.admin, nil)
	assert.Len(t, mine.body["value"], 1)
}
