package azurecontainerregistry_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
	acr "github.com/blackbirdworks/gopherstack/services/azurecontainerregistry"
)

func TestConformance(t *testing.T) {
	t.Parallel()

	e := newEnv(t, t.TempDir())
	creds := e.registry(t, "conformance", true)

	ocitest.Run(t, e.client(t, "conformance", basic(creds.Username, creds.Password1)), "", "/v2/_catalog", "")
}

func TestAuth(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	creds := e.registry(t, "authreg", true)
	e.registry(t, "noadmin", false)

	refresh, err := e.h.Backend.IssueRefreshToken("authreg", host("authreg"))
	require.NoError(t, err)

	access, err := e.h.Backend.IssueAccessToken("authreg", host("authreg"), []string{"repository:x:pull"})
	require.NoError(t, err)

	other, err := e.h.Backend.IssueAccessToken("noadmin", host("noadmin"), nil)
	require.NoError(t, err)

	tests := []struct {
		prep   func(*http.Request)
		name   string
		reg    string
		status int
	}{
		{name: "none", reg: "authreg", status: http.StatusUnauthorized},
		{name: "admin password", reg: "authreg", prep: basic("authreg", creds.Password1), status: http.StatusOK},
		{name: "admin password2", reg: "authreg", prep: basic("authreg", creds.Password2), status: http.StatusOK},
		{name: "user case", reg: "authreg", prep: basic("AuthReg", creds.Password1), status: http.StatusOK},
		{name: "wrong password", reg: "authreg", prep: basic("authreg", "nope"), status: http.StatusUnauthorized},
		{name: "wrong user", reg: "authreg", prep: basic("someone", creds.Password1), status: http.StatusUnauthorized},
		{name: "refresh token", reg: "authreg", prep: basic(acr.NullUsername, refresh), status: http.StatusOK},
		{name: "access token basic", reg: "authreg", prep: basic("x", access), status: http.StatusOK},
		{name: "bearer access", reg: "authreg", prep: bearer(access), status: http.StatusOK},
		{name: "bearer refresh", reg: "authreg", prep: bearer(refresh), status: http.StatusUnauthorized},
		{name: "bearer other registry", reg: "authreg", prep: bearer(other), status: http.StatusUnauthorized},
		{name: "garbage bearer", reg: "authreg", prep: bearer("a.b.c"), status: http.StatusUnauthorized},
		{
			name:   "admin disabled",
			reg:    "noadmin",
			prep:   basic("noadmin", creds.Password1),
			status: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := e.client(t, tt.reg, tt.prep).Do(http.MethodGet, "/v2/", nil, nil)
			assert.Equal(t, tt.status, got.Status)

			if tt.status == http.StatusUnauthorized {
				assert.Contains(t, got.Header.Get("WWW-Authenticate"), `Bearer realm="http://`+host(tt.reg))
				assert.Contains(t, got.Header.Get("WWW-Authenticate"), `/oauth2/token",service="`+host(tt.reg)+`"`)
			}
		})
	}
}

func TestRegenerateInvalidatesOldPassword(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	old := e.registry(t, "regenreg", true)

	next, err := e.h.Backend.RegenerateCredential("regenreg", "password")
	require.NoError(t, err)
	require.NotEqual(t, old.Password1, next.Password1)
	require.Equal(t, old.Password2, next.Password2)

	tests := []struct {
		name   string
		pass   string
		status int
	}{
		{"old rejected", old.Password1, http.StatusUnauthorized},
		{"new accepted", next.Password1, http.StatusOK},
		{"other kept", old.Password2, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := e.client(t, "regenreg", basic("regenreg", tt.pass)).Do(http.MethodGet, "/v2/", nil, nil)
			assert.Equal(t, tt.status, got.Status)
		})
	}
}

func TestTokenFlows(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	creds := e.registry(t, "tokenreg", true)
	anon := e.client(t, "tokenreg", nil)

	form := func(kv ...string) []byte {
		v := url.Values{}
		for i := 0; i < len(kv); i += 2 {
			v.Set(kv[i], kv[i+1])
		}

		return []byte(v.Encode())
	}
	formHdr := map[string]string{"Content-Type": "application/x-www-form-urlencoded"}

	exch := anon.Do(http.MethodPost, "/oauth2/exchange", form(
		"grant_type", "access_token", "service", host("tokenreg"), "access_token", "aad-token"), formHdr)
	require.Equal(t, http.StatusOK, exch.Status, string(exch.Body))

	refresh, _ := decode(t, exch.Body)["refresh_token"].(string)
	require.NotEmpty(t, refresh)

	t.Run("refresh to access", func(t *testing.T) {
		t.Parallel()

		got := anon.Do(http.MethodPost, "/oauth2/token", form(
			"grant_type", "refresh_token", "service", host("tokenreg"),
			"scope", "repository:app:pull,push", "refresh_token", refresh), formHdr)
		require.Equal(t, http.StatusOK, got.Status, string(got.Body))

		access, _ := decode(t, got.Body)["access_token"].(string)
		require.NotEmpty(t, access)

		pushed := e.client(t, "tokenreg", bearer(access)).Do(http.MethodGet, "/v2/_catalog", nil, nil)
		assert.Equal(t, http.StatusOK, pushed.Status)
	})

	t.Run("docker token endpoint", func(t *testing.T) {
		t.Parallel()

		got := e.client(t, "tokenreg", basic("tokenreg", creds.Password1)).Do(
			http.MethodGet, "/oauth2/token?service="+host("tokenreg")+"&scope=repository:app:pull", nil, nil)
		require.Equal(t, http.StatusOK, got.Status)
		assert.NotEmpty(t, decode(t, got.Body)["access_token"])
	})

	t.Run("docker token wrong password", func(t *testing.T) {
		t.Parallel()

		got := e.client(t, "tokenreg", basic("tokenreg", "bad")).Do(http.MethodGet, "/oauth2/token", nil, nil)
		assert.Equal(t, http.StatusUnauthorized, got.Status)
	})

	t.Run("refresh token as docker password", func(t *testing.T) {
		t.Parallel()

		got := e.client(t, "tokenreg", basic(acr.NullUsername, refresh)).Do(http.MethodGet, "/oauth2/token", nil, nil)
		assert.Equal(t, http.StatusOK, got.Status)
	})

	t.Run("bad refresh", func(t *testing.T) {
		t.Parallel()

		got := anon.Do(http.MethodPost, "/oauth2/token", form(
			"grant_type", "refresh_token", "refresh_token", "junk"), formHdr)
		assert.Equal(t, http.StatusUnauthorized, got.Status)
	})

	t.Run("exchange without token", func(t *testing.T) {
		t.Parallel()

		got := anon.Do(http.MethodPost, "/oauth2/exchange", form("grant_type", "access_token"), formHdr)
		assert.Equal(t, http.StatusBadRequest, got.Status)
	})

	t.Run("exchange bad grant", func(t *testing.T) {
		t.Parallel()

		got := anon.Do(http.MethodPost, "/oauth2/exchange", form("grant_type", "password"), formHdr)
		assert.Equal(t, http.StatusBadRequest, got.Status)
	})

	t.Run("exchange refresh grant", func(t *testing.T) {
		t.Parallel()

		got := anon.Do(
			http.MethodPost,
			"/oauth2/exchange",
			form("grant_type", "refresh_token", "refresh_token", refresh),
			formHdr,
		)
		assert.Equal(t, http.StatusOK, got.Status)
	})
}

func TestHostRouting(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	a := e.registry(t, "firstreg", true)
	b := e.registry(t, "secondreg", true)

	e.client(t, "firstreg", basic(a.Username, a.Password1)).PushImage("only-in-first", "t")

	tests := []struct {
		prep   func(*http.Request)
		name   string
		host   string
		status int
	}{
		{name: "first host", host: host("firstreg"), prep: basic(a.Username, a.Password1), status: http.StatusOK},
		{name: "second host", host: host("secondreg"), prep: basic(b.Username, b.Password1), status: http.StatusOK},
		{
			name:   "first creds on second",
			host:   host("secondreg"),
			prep:   basic(a.Username, a.Password1),
			status: http.StatusUnauthorized,
		},
		{
			name:   "ambiguous ip host",
			host:   "127.0.0.1:5000",
			prep:   basic(a.Username, a.Password1),
			status: http.StatusNotFound,
		},
		{
			name:   "unknown label",
			host:   "nothere.azurecr.localhost",
			prep:   basic(a.Username, a.Password1),
			status: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := ocitest.Client{T: t, Base: e.srv.URL, Host: tt.host, Prepare: tt.prep}
			assert.Equal(t, tt.status, c.Do(http.MethodGet, "/v2/", nil, nil).Status)
		})
	}

	second := e.client(t, "secondreg", basic(b.Username, b.Password1))
	assert.Equal(t, http.StatusNotFound, second.Do(http.MethodGet, "/v2/only-in-first/tags/list", nil, nil).Status)
}

func TestSingleRegistryFallback(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	creds := e.registry(t, "soloreg", true)

	c := ocitest.Client{T: t, Base: e.srv.URL, Prepare: basic(creds.Username, creds.Password1)}
	c.PushImage("img", "latest")

	assert.Equal(t, http.StatusOK, c.Do(http.MethodGet, "/v2/img/manifests/latest", nil, nil).Status)
}

func TestUnknownPath(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.registry(t, "pathreg", true)

	got := e.client(t, "pathreg", nil).Do(http.MethodGet, "/nope", nil, nil)
	assert.Equal(t, http.StatusNotFound, got.Status)
	assert.Contains(t, string(got.Body), "NOT_FOUND")
}
