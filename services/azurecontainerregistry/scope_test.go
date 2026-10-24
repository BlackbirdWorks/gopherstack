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
