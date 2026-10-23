package gcpartifactregistry_test

import (
	"encoding/base64"
	"net/http"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
)

func TestDockerConformance(t *testing.T) {
	t.Parallel()

	for _, dir := range []string{"", t.TempDir()} {
		t.Run(map[bool]string{true: "memory", false: "disk"}[dir == ""], func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, dir)
			e.createRepo(t, "conf", dockerRepo())

			ocitest.Run(t, e.docker(t), project+"/conf/", "/v2/"+project+"/conf/_catalog", "")
		})
	}
}

func TestDockerAuth(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "authrepo", dockerRepo())

	basic := func(user, pass string) func(*http.Request) {
		return func(r *http.Request) {
			r.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(user+":"+pass)))
		}
	}
	bearer := func(r *http.Request) { r.Header.Set("Authorization", "Bearer some-token") }

	tests := []struct {
		prep   func(*http.Request)
		name   string
		status int
	}{
		{nil, "none", http.StatusUnauthorized},
		{basic("oauth2accesstoken", "ya29.token"), "oauth2accesstoken", http.StatusOK},
		{basic("_json_key", "{}"), "json key", http.StatusOK},
		{basic("anyone", "anything"), "any basic", http.StatusOK},
		{bearer, "bearer", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := ocitest.Client{T: t, Base: "http://127.0.0.1:" + e.port, Prepare: tt.prep}
			got := c.Do(http.MethodGet, "/v2/", nil, nil)
			assert.Equal(t, tt.status, got.Status)

			if tt.status == http.StatusUnauthorized {
				assert.Contains(
					t,
					got.Header.Get("WWW-Authenticate"),
					`Bearer realm="http://127.0.0.1:`+e.port+`/v2/token"`,
				)
			}
		})
	}

	t.Run("token endpoint", func(t *testing.T) {
		t.Parallel()

		c := ocitest.Client{T: t, Base: "http://127.0.0.1:" + e.port}
		got := c.Do(http.MethodGet, "/v2/token?service=x&scope=repository:"+project+"/authrepo/app:pull", nil, nil)
		require.Equal(t, http.StatusOK, got.Status)
		assert.NotEmpty(t, decodeJSON(t, got.Body)["token"])
	})
}

func TestDockerRouting(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "dockerrepo", dockerRepo())
	e.createRepo(t, "mavenrepo", &artifactregistrypb.Repository{Format: artifactregistrypb.Repository_MAVEN})

	tests := []struct {
		name   string
		path   string
		host   string
		status int
	}{
		{"known repo", "/v2/" + project + "/dockerrepo/app/tags/list", "", http.StatusNotFound},
		{"unknown repo", "/v2/" + project + "/nope/app/tags/list", "", http.StatusNotFound},
		{"non-docker repo", "/v2/" + project + "/mavenrepo/app/tags/list", "", http.StatusNotFound},
		{"unknown project", "/v2/other/dockerrepo/app/tags/list", "", http.StatusNotFound},
		{"missing image", "/v2/" + project + "/dockerrepo", "", http.StatusNotFound},
		{"api version check", "/v2/", "", http.StatusOK},
		{
			"matching regional host",
			"/v2/" + project + "/dockerrepo/_catalog",
			"us-central1-docker.pkg.dev",
			http.StatusOK,
		},
		{
			"other regional host",
			"/v2/" + project + "/dockerrepo/_catalog",
			"europe-west1-docker.pkg.dev",
			http.StatusNotFound,
		},
		{
			"regional host with port",
			"/v2/" + project + "/dockerrepo/_catalog",
			"us-central1-docker.pkg.dev:443",
			http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := e.docker(t)
			c.Host = tt.host
			assert.Equal(t, tt.status, c.Do(http.MethodGet, tt.path, nil, nil).Status)
		})
	}
}

func TestImmutableTags(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "frozen", dockerRepo(func(r *artifactregistrypb.Repository) {
		r.FormatConfig = &artifactregistrypb.Repository_DockerConfig{
			DockerConfig: &artifactregistrypb.Repository_DockerRepositoryConfig{ImmutableTags: true},
		}
	}))

	c := e.docker(t)
	repo := project + "/frozen/app"
	first := c.PushImage(repo, "v1")

	// Same content under the same tag is rejected too: the tag exists.
	put := c.Do(http.MethodPut, "/v2/"+repo+"/manifests/v1", first.Body,
		map[string]string{"Content-Type": ocitest.ManifestMediaType})
	assert.Equal(t, http.StatusConflict, put.Status)

	// A new tag, or a push by digest, is fine.
	c.PushImage(repo, "v2")

	byDigest := c.Do(http.MethodPut, "/v2/"+repo+"/manifests/"+first.Digest, first.Body,
		map[string]string{"Content-Type": ocitest.ManifestMediaType})
	assert.Equal(t, http.StatusCreated, byDigest.Status)
}

func TestImageDirIsPersisted(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	before := newEnv(t, dir)
	before.createRepo(t, "kept", dockerRepo())
	img := pushTo(before.docker(t), "kept", "app", "v1")

	snap := before.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	after := newEnv(t, dir)
	require.NoError(t, after.handler.Restore(t.Context(), snap))

	got := after.docker(t).Do(http.MethodGet, "/v2/"+project+"/kept/app/manifests/v1", nil, nil)
	require.Equal(t, http.StatusOK, got.Status)
	assert.Equal(t, img.Body, got.Body)

	repo, err := after.client.GetRepository(
		t.Context(),
		&artifactregistrypb.GetRepositoryRequest{Name: repoName("kept")},
	)
	require.NoError(t, err)
	assert.Equal(t, artifactregistrypb.Repository_DOCKER, repo.GetFormat())

	ver, err := after.client.GetVersion(t.Context(), &artifactregistrypb.GetVersionRequest{
		Name: pkgName("kept", "app") + "/versions/" + img.Digest,
	})
	require.NoError(t, err)
	assert.Len(t, ver.GetRelatedTags(), 1)

	require.Error(t, after.handler.Restore(t.Context(), []byte("not json")))

	after.handler.Reset()

	_, err = after.client.GetRepository(t.Context(), &artifactregistrypb.GetRepositoryRequest{Name: repoName("kept")})
	require.Error(t, err)
}
