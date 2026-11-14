package azurecontainerregistry_test

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	acr "github.com/blackbirdworks/gopherstack/services/azurecontainerregistry"
)

func newTokenEnv(t *testing.T, name string) (*env, string) {
	t.Helper()

	e := newEnv(t, "")
	creds := e.registry(t, name, true)
	admin := e.client(t, name, basic(creds.Username, creds.Password1))
	admin.PushImage("samples/app", "v1")
	admin.PushImage("samples/team/svc", "v1")
	admin.PushImage("other", "v1")

	return e, name
}

func TestRepositoryTokenAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		method  string
		path    string
		actions []string
		want    int
	}{
		{name: "read pulls", actions: []string{"repositories/samples/app/content/read"},
			method: http.MethodGet, path: "/v2/samples/app/manifests/v1", want: http.StatusOK},
		{name: "read cannot push", actions: []string{"repositories/samples/app/content/read"},
			method: http.MethodPost, path: "/v2/samples/app/blobs/uploads/", want: http.StatusUnauthorized},
		{name: "write pushes", actions: []string{"repositories/samples/app/content/write"},
			method: http.MethodPost, path: "/v2/samples/app/blobs/uploads/", want: http.StatusAccepted},
		{name: "other repo denied", actions: []string{"repositories/samples/app/content/read"},
			method: http.MethodGet, path: "/v2/other/manifests/v1", want: http.StatusUnauthorized},
		{name: "prefix wildcard", actions: []string{"repositories/samples/*/content/read"},
			method: http.MethodGet, path: "/v2/samples/app/manifests/v1", want: http.StatusOK},
		{name: "wildcard nested", actions: []string{"repositories/samples/*/content/read"},
			method: http.MethodGet, path: "/v2/samples/team/svc/manifests/v1", want: http.StatusOK},
		{name: "wildcard other prefix", actions: []string{"repositories/samples/team/*/content/read"},
			method: http.MethodGet, path: "/v2/samples/app/manifests/v1", want: http.StatusUnauthorized},
		{name: "root wildcard", actions: []string{"repositories/*/content/read"},
			method: http.MethodGet, path: "/v2/other/manifests/v1", want: http.StatusOK},
		{name: "catalog denied", actions: []string{"repositories/*/content/read"},
			method: http.MethodGet, path: "/v2/_catalog", want: http.StatusUnauthorized},
		{name: "delete needs content delete", actions: []string{"repositories/other/content/read"},
			method: http.MethodDelete, path: "/v2/other/manifests/v1", want: http.StatusUnauthorized},
		{name: "metadata read lists tags", actions: []string{"repositories/other/metadata/read"},
			method: http.MethodGet, path: "/acr/v1/other/_tags", want: http.StatusOK},
		{name: "content read has no metadata", actions: []string{"repositories/other/content/read"},
			method: http.MethodGet, path: "/acr/v1/other/_tags", want: http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, reg := newTokenEnv(t, "tokreg")
			b := e.h.Backend

			_, err := b.SetScopeMap(reg, "map1", "", tt.actions)
			require.NoError(t, err)
			require.NoError(t, b.SetToken(reg, "tok1", "map1", true))

			pw, err := b.GenerateTokenPassword(reg, "tok1", "password1", nil)
			require.NoError(t, err)

			got := e.client(t, reg, basic("tok1", pw.Value)).Do(tt.method, tt.path, nil, nil)
			assert.Equal(t, tt.want, got.Status, string(got.Body))
		})
	}
}

func TestRepositoryTokenWildcardsAdd(t *testing.T) {
	t.Parallel()

	e, reg := newTokenEnv(t, "wildreg")
	b := e.h.Backend

	_, err := b.SetScopeMap(reg, "map1", "", []string{
		"repositories/samples/*/content/read", "repositories/samples/team/*/content/write",
		"repositories/samples/team/svc/content/delete",
	})
	require.NoError(t, err)
	require.NoError(t, b.SetToken(reg, "tok1", "map1", true))

	pw, err := b.GenerateTokenPassword(reg, "tok1", "password2", nil)
	require.NoError(t, err)

	c := e.client(t, reg, basic("tok1", pw.Value))
	assert.Equal(t, http.StatusOK, c.Do(http.MethodGet, "/v2/samples/team/svc/manifests/v1", nil, nil).Status)
	assert.Equal(
		t,
		http.StatusAccepted,
		c.Do(http.MethodPost, "/v2/samples/team/other/blobs/uploads/", nil, nil).Status,
	)
	assert.Equal(t, http.StatusUnauthorized, c.Do(http.MethodPost, "/v2/samples/app/blobs/uploads/", nil, nil).Status)
}

func TestRepositoryTokenLifecycle(t *testing.T) {
	t.Parallel()

	e, reg := newTokenEnv(t, "liferg")
	b := e.h.Backend
	pull := []string{"repositories/other/content/read"}

	_, err := b.SetScopeMap(reg, "map1", "", pull)
	require.NoError(t, err)
	require.NoError(t, b.SetToken(reg, "tok1", "map1", true))

	soon := time.Now().Add(time.Hour)
	pw, err := b.GenerateTokenPassword(reg, "tok1", "password1", &soon)
	require.NoError(t, err)

	get := func(user, pass string) int {
		return e.client(t, reg, basic(user, pass)).Do(http.MethodGet, "/v2/other/manifests/v1", nil, nil).Status
	}

	assert.Equal(t, http.StatusOK, get("tok1", pw.Value))
	assert.Equal(t, http.StatusUnauthorized, get("tok1", "wrong"))
	assert.Equal(t, http.StatusUnauthorized, get("missing", pw.Value))

	require.NoError(t, b.SetToken(reg, "tok1", "map1", false))
	assert.Equal(t, http.StatusUnauthorized, get("tok1", pw.Value))
	require.NoError(t, b.SetToken(reg, "tok1", "map1", true))
	assert.Equal(t, http.StatusOK, get("tok1", pw.Value))

	_, err = b.SetScopeMap(reg, "map1", "", []string{"repositories/samples/app/content/read"})
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, get("tok1", pw.Value))

	require.ErrorIs(t, b.DeleteScopeMap(reg, "map1"), acr.ErrScopeMapInUse)
	require.NoError(t, b.DeleteToken(reg, "tok1"))
	assert.Equal(t, http.StatusUnauthorized, get("tok1", pw.Value))
	require.NoError(t, b.DeleteScopeMap(reg, "map1"))
}

func TestRepositoryTokenPasswordExpiry(t *testing.T) {
	t.Parallel()

	e, reg := newTokenEnv(t, "exprg")
	b := e.h.Backend

	_, err := b.SetScopeMap(reg, "map1", "", []string{"repositories/other/content/read"})
	require.NoError(t, err)
	require.NoError(t, b.SetToken(reg, "tok1", "map1", true))

	past := time.Now().Add(-time.Minute)
	pw, err := b.GenerateTokenPassword(reg, "tok1", "password1", &past)
	require.NoError(t, err)

	got := e.client(t, reg, basic("tok1", pw.Value)).Do(http.MethodGet, "/v2/other/manifests/v1", nil, nil)
	assert.Equal(t, http.StatusUnauthorized, got.Status)

	listed, err := b.TokenPasswords(reg, "tok1")
	require.NoError(t, err)
	require.Len(t, listed, 1)
	assert.Empty(t, listed[0].Value)
}

func TestRepositoryTokenDockerFlow(t *testing.T) {
	t.Parallel()

	e, reg := newTokenEnv(t, "flowreg")
	b := e.h.Backend

	_, err := b.SetScopeMap(reg, "map1", "", []string{"repositories/other/content/read"})
	require.NoError(t, err)
	require.NoError(t, b.SetToken(reg, "tok1", "map1", true))

	pw, err := b.GenerateTokenPassword(reg, "tok1", "password1", nil)
	require.NoError(t, err)

	q := url.Values{"service": {host(reg)}, "scope": {"repository:other:pull,push", "repository:samples/app:pull"}}
	got := e.client(t, reg, basic("tok1", pw.Value)).Do(http.MethodGet, "/oauth2/token?"+q.Encode(), nil, nil)
	require.Equal(t, http.StatusOK, got.Status, string(got.Body))

	access, _ := decode(t, got.Body)["access_token"].(string)
	require.NotEmpty(t, access)

	scopes, ok := b.AccessTokenScopes(access, reg)
	require.True(t, ok)
	assert.Equal(t, []string{"repository:other:pull"}, scopes)

	bad := e.client(t, reg, basic("tok1", "nope")).Do(http.MethodGet, "/oauth2/token?"+q.Encode(), nil, nil)
	assert.Equal(t, http.StatusUnauthorized, bad.Status)
}

func TestScopeMapValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		mapName string
		actions []string
	}{
		{name: "valid", actions: []string{"repositories/a/b/content/read"}, mapName: "m1"},
		{name: "root wildcard", actions: []string{"repositories/*/metadata/write"}, mapName: "m1"},
		{name: "middle wildcard", actions: []string{"repositories/a/*/b/content/read"}, mapName: "m1",
			wantErr: acr.ErrInvalidScopeMap},
		{name: "no slash wildcard", actions: []string{"repositories/a*/content/read"}, mapName: "m1",
			wantErr: acr.ErrInvalidScopeMap},
		{name: "two wildcards", actions: []string{"repositories/a/*/*/content/read"}, mapName: "m1",
			wantErr: acr.ErrInvalidScopeMap},
		{name: "unknown verb", actions: []string{"repositories/a/content/pull"}, mapName: "m1",
			wantErr: acr.ErrInvalidScopeMap},
		{name: "empty", mapName: "m1", wantErr: acr.ErrInvalidScopeMap},
		{name: "system map", actions: []string{"repositories/a/content/read"}, mapName: "_repositories_pull",
			wantErr: acr.ErrScopeMapReadOnly},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, "")
			e.registry(t, "valreg", false)

			_, err := e.h.Backend.SetScopeMap("valreg", tt.mapName, "", tt.actions)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSystemScopeMaps(t *testing.T) {
	t.Parallel()

	e, reg := newTokenEnv(t, "sysreg")
	b := e.h.Backend

	maps, err := b.ScopeMaps(reg)
	require.NoError(t, err)
	require.Len(t, maps, 3)
	assert.Equal(t, "_repositories_admin", maps[0].Name)

	require.NoError(t, b.SetToken(reg, "puller", "_repositories_pull", true))

	pw, err := b.GenerateTokenPassword(reg, "puller", "password1", nil)
	require.NoError(t, err)

	c := e.client(t, reg, basic("puller", pw.Value))
	assert.Equal(t, http.StatusOK, c.Do(http.MethodGet, "/v2/other/manifests/v1", nil, nil).Status)
	assert.Equal(t, http.StatusUnauthorized, c.Do(http.MethodPost, "/v2/other/blobs/uploads/", nil, nil).Status)
	require.ErrorIs(t, b.SetToken(reg, "x", "missing", true), acr.ErrScopeMapNotFound)
}

func TestExchangeValidatesEntraToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		token  string
		tenant string
		want   int
	}{
		{name: "valid", token: "good", tenant: "t1", want: http.StatusOK},
		{name: "rejected", token: "bad", tenant: "t1", want: http.StatusUnauthorized},
		{name: "wrong tenant", token: "good", tenant: "t2", want: http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, "")
			e.registry(t, "exchreg", false)
			e.h.ValidateToken = func(token, tenant string) error {
				if token != "good" || tenant != "t1" {
					return assert.AnError
				}

				return nil
			}

			body := url.Values{
				"grant_type": {"access_token"}, "access_token": {tt.token}, "tenant": {tt.tenant},
				"service": {host("exchreg")},
			}.Encode()

			got := e.client(t, "exchreg", nil).Do(http.MethodPost, "/oauth2/exchange", []byte(body),
				map[string]string{"Content-Type": "application/x-www-form-urlencoded"})
			assert.Equal(t, tt.want, got.Status, string(got.Body))
		})
	}
}
