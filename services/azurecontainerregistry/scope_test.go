package azurecontainerregistry_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokenScopeEnforced(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	creds := e.registry(t, "scopereg", true)
	admin := e.client(t, "scopereg", basic(creds.Username, creds.Password1))
	img := admin.PushImage("app", "v1")
	admin.PushImage("other", "v1")

	tests := []struct {
		name      string
		method    string
		path      string
		wantScope string
		scopes    []string
		want      int
		viaBasic  bool
	}{
		{name: "pull allows get", method: http.MethodGet, path: "/v2/app/manifests/v1", want: http.StatusOK,
			scopes: []string{"repository:app:pull"}},
		{name: "pull allows head", method: http.MethodHead, path: "/v2/app/manifests/v1", want: http.StatusOK,
			scopes: []string{"repository:app:pull"}},
		{name: "pull blocks blob upload", method: http.MethodPost, path: "/v2/app/blobs/uploads/",
			want: http.StatusUnauthorized, scopes: []string{"repository:app:pull"},
			wantScope: "repository:app:pull,push"},
		{name: "pull blocks manifest put", method: http.MethodPut, path: "/v2/app/manifests/v2",
			want: http.StatusUnauthorized, scopes: []string{"repository:app:pull"},
			wantScope: "repository:app:pull,push"},
		{name: "pull blocks delete", method: http.MethodDelete, path: "/v2/app/manifests/" + img.Digest,
			want: http.StatusUnauthorized, scopes: []string{"repository:app:pull"},
			wantScope: "repository:app:delete"},
		{name: "push allows upload", method: http.MethodPost, path: "/v2/app/blobs/uploads/",
			want: http.StatusAccepted, scopes: []string{"repository:app:push"}},
		{name: "push blocks get", method: http.MethodGet, path: "/v2/app/manifests/v1",
			want: http.StatusUnauthorized, scopes: []string{"repository:app:push"},
			wantScope: "repository:app:pull"},
		{name: "pull push blocks delete", method: http.MethodDelete, path: "/v2/app/manifests/" + img.Digest,
			want: http.StatusUnauthorized, scopes: []string{"repository:app:pull,push"},
			wantScope: "repository:app:delete"},
		{name: "other repository blocked", method: http.MethodGet, path: "/v2/other/manifests/v1",
			want: http.StatusUnauthorized, scopes: []string{"repository:app:pull,push,delete"},
			wantScope: "repository:other:pull"},
		{name: "no scopes blocked", method: http.MethodGet, path: "/v2/app/manifests/v1",
			want: http.StatusUnauthorized, wantScope: "repository:app:pull"},
		{name: "second scope entry grants", method: http.MethodGet, path: "/v2/other/manifests/v1",
			want: http.StatusOK, scopes: []string{"repository:app:pull", "repository:other:pull"}},
		{name: "wildcard action", method: http.MethodPost, path: "/v2/app/blobs/uploads/",
			want: http.StatusAccepted, scopes: []string{"repository:app:*"}},
		{name: "mount needs source pull", method: http.MethodPost,
			path: "/v2/app/blobs/uploads/?mount=sha256:abc&from=other", want: http.StatusUnauthorized,
			scopes: []string{"repository:app:push"}, wantScope: "repository:app:pull,push repository:other:pull"},
		{name: "catalog needs registry scope", method: http.MethodGet, path: "/v2/_catalog",
			want: http.StatusUnauthorized, scopes: []string{"repository:app:pull"},
			wantScope: "registry:catalog:*"},
		{name: "catalog allowed", method: http.MethodGet, path: "/v2/_catalog", want: http.StatusOK,
			scopes: []string{"registry:catalog:*"}},
		{name: "base check needs only auth", method: http.MethodGet, path: "/v2/", want: http.StatusOK},
		{name: "scoped basic password blocked", method: http.MethodGet, path: "/v2/app/manifests/v1",
			want: http.StatusUnauthorized, scopes: []string{"repository:other:pull"}, viaBasic: true,
			wantScope: "repository:app:pull"},
		{name: "scoped basic password allowed", method: http.MethodGet, path: "/v2/app/manifests/v1",
			want: http.StatusOK, scopes: []string{"repository:app:pull"}, viaBasic: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tok, err := e.h.Backend.IssueAccessToken("scopereg", host("scopereg"), tt.scopes)
			require.NoError(t, err)

			prep := bearer(tok)
			if tt.viaBasic {
				prep = basic("anyone", tok)
			}

			got := e.client(t, "scopereg", prep).Do(tt.method, tt.path, nil, nil)
			assert.Equal(t, tt.want, got.Status, string(got.Body))

			if tt.want != http.StatusUnauthorized {
				return
			}

			challenge := got.Header.Get("WWW-Authenticate")
			assert.Contains(t, challenge, `scope="`+tt.wantScope+`"`)
			assert.Contains(t, challenge, `error="insufficient_scope"`)
			assert.True(t, strings.HasPrefix(challenge, "Bearer realm="))

			errs, _ := decode(t, got.Body)["errors"].([]any)
			require.Len(t, errs, 1)

			first, _ := errs[0].(map[string]any)
			assert.Equal(t, "UNAUTHORIZED", first["code"])
			assert.NotEmpty(t, first["detail"])
		})
	}
}

func TestTokenScopeDoesNotLimitCredentials(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	creds := e.registry(t, "fullreg", true)
	c := e.client(t, "fullreg", basic(creds.Username, creds.Password1))
	img := c.PushImage("app", "v1")

	del := c.Do(http.MethodDelete, "/v2/app/manifests/"+img.Digest, nil, nil)
	assert.Equal(t, http.StatusAccepted, del.Status)
}

func TestAnonymousPull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		method    string
		path      string
		want      int
		anonymous bool
	}{
		{
			name:   "disabled refuses pull",
			method: http.MethodGet,
			path:   "/v2/app/manifests/v1",
			want:   http.StatusUnauthorized,
		},
		{name: "enabled allows pull", anonymous: true, method: http.MethodGet, path: "/v2/app/manifests/v1",
			want: http.StatusOK},
		{name: "enabled allows head", anonymous: true, method: http.MethodHead, path: "/v2/app/manifests/v1",
			want: http.StatusOK},
		{name: "enabled refuses push", anonymous: true, method: http.MethodPost, path: "/v2/app/blobs/uploads/",
			want: http.StatusUnauthorized},
		{name: "enabled refuses delete", anonymous: true, method: http.MethodDelete, path: "/v2/app/manifests/v1",
			want: http.StatusUnauthorized},
		{name: "enabled refuses catalog", anonymous: true, method: http.MethodGet, path: "/v2/_catalog",
			want: http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, "")
			creds := e.registry(t, "anonreg", true)
			e.client(t, "anonreg", basic(creds.Username, creds.Password1)).PushImage("app", "v1")
			require.NoError(t, e.h.Backend.SetAnonymousPull("anonreg", tt.anonymous))

			resp := e.client(t, "anonreg", nil).Do(tt.method, tt.path, nil, nil)
			require.Equal(t, tt.want, resp.Status)
		})
	}
}

func TestAnonymousTokenEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		scope     string
		want      int
		anonymous bool
		wantPull  bool
	}{
		{name: "disabled refuses", scope: "repository:app:pull", want: http.StatusUnauthorized},
		{name: "enabled issues pull token", anonymous: true, scope: "repository:app:pull,push", want: http.StatusOK,
			wantPull: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, "")
			creds := e.registry(t, "anonreg", true)
			admin := e.client(t, "anonreg", basic(creds.Username, creds.Password1))
			admin.PushImage("app", "v1")
			require.NoError(t, e.h.Backend.SetAnonymousPull("anonreg", tt.anonymous))

			anon := e.client(t, "anonreg", nil)
			resp := anon.Do(http.MethodGet, "/oauth2/token?service=anonreg.azurecr.localhost&scope="+tt.scope, nil, nil)
			require.Equal(t, tt.want, resp.Status)

			if !tt.wantPull {
				return
			}

			tok := decode(t, resp.Body)["access_token"].(string)
			bearerClient := e.client(t, "anonreg", bearer(tok))

			assert.Equal(t, http.StatusOK, bearerClient.Do(http.MethodGet, "/v2/app/manifests/v1", nil, nil).Status)
			assert.Equal(t, http.StatusUnauthorized,
				bearerClient.Do(http.MethodPost, "/v2/app/blobs/uploads/", nil, nil).Status)
		})
	}
}

func TestACRMetadataScopeEnforced(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	creds := e.registry(t, "metascope", true)
	admin := e.client(t, "metascope", basic(creds.Username, creds.Password1))
	admin.PushImage("app", "v1")
	admin.PushImage("app", "v2")

	tests := []struct {
		name      string
		method    string
		path      string
		body      []byte
		wantScope string
		scopes    []string
		want      int
	}{
		{name: "read ok", method: http.MethodGet, path: "/acr/v1/app/_tags", want: http.StatusOK,
			scopes: []string{"repository:app:metadata_read"}},
		{name: "pull cannot read", method: http.MethodGet, path: "/acr/v1/app/_tags", want: http.StatusUnauthorized,
			scopes: []string{"repository:app:pull"}, wantScope: "repository:app:metadata_read"},
		{name: "other repo", method: http.MethodGet, path: "/acr/v1/app", want: http.StatusUnauthorized,
			scopes: []string{"repository:other:metadata_read"}, wantScope: "repository:app:metadata_read"},
		{name: "read cannot patch", method: http.MethodPatch, path: "/acr/v1/app/_tags/v1",
			want: http.StatusUnauthorized, scopes: []string{"repository:app:metadata_read"},
			wantScope: "repository:app:metadata_write"},
		{name: "write patches", method: http.MethodPatch, path: "/acr/v1/app/_tags/v1",
			body: []byte(`{"value":{"deleteEnabled":true}}`), want: http.StatusOK,
			scopes: []string{"repository:app:metadata_write"}},
		{name: "untag needs write", method: http.MethodDelete, path: "/acr/v1/app/_tags/v2",
			want: http.StatusUnauthorized, scopes: []string{"repository:app:delete"},
			wantScope: "repository:app:metadata_write"},
		{name: "repo delete needs delete", method: http.MethodDelete, path: "/acr/v1/app",
			want: http.StatusUnauthorized, scopes: []string{"repository:app:metadata_write"},
			wantScope: "repository:app:delete"},
		{name: "catalog needs registry scope", method: http.MethodGet, path: "/acr/v1/_catalog",
			want: http.StatusUnauthorized, scopes: []string{"repository:app:metadata_read"},
			wantScope: "registry:catalog:*"},
		{name: "wildcard action", method: http.MethodGet, path: "/acr/v1/app", want: http.StatusOK,
			scopes: []string{"repository:app:*"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tok, err := e.h.Backend.IssueAccessToken("metascope", host("metascope"), tt.scopes)
			require.NoError(t, err)

			got := e.client(t, "metascope", bearer(tok)).Do(tt.method, tt.path, tt.body, nil)
			assert.Equal(t, tt.want, got.Status, string(got.Body))

			if tt.want == http.StatusUnauthorized {
				challenge := got.Header.Get("WWW-Authenticate")
				assert.Contains(t, challenge, `scope="`+tt.wantScope+`"`)
				assert.Contains(t, challenge, `error="insufficient_scope"`)
			}
		})
	}
}
