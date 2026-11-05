package gcpartifactregistry_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
)

func dockerUpstream(uri string) *apb {
	return &apb{RemoteSource: &artifactregistrypb.RemoteRepositoryConfig_DockerRepository_{
		DockerRepository: &artifactregistrypb.RemoteRepositoryConfig_DockerRepository{
			Upstream: &artifactregistrypb.RemoteRepositoryConfig_DockerRepository_CustomRepository_{
				CustomRepository: &artifactregistrypb.RemoteRepositoryConfig_DockerRepository_CustomRepository{
					Uri: uri,
				},
			},
		},
	}}
}

// tokenRegistry is a registry that answers unauthenticated reads with a Bearer challenge, as Docker Hub does.
func tokenRegistry(t *testing.T, challenges *atomic.Int32) *httptest.Server {
	t.Helper()

	reg := ociregistry.New(t.Context(), ociregistry.Options{InstanceID: "upstream"})

	var srv *httptest.Server

	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			_, _ = w.Write([]byte(`{"token":"anon"}`))

			return
		}

		if r.Method == http.MethodGet && r.Header.Get("Authorization") != "Bearer anon" {
			challenges.Add(1)
			w.Header().
				Set("WWW-Authenticate", `Bearer realm="`+srv.URL+`/token",service="upstream",scope="repository:app:pull"`)
			w.WriteHeader(http.StatusUnauthorized)

			return
		}

		reg.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	return srv
}

func TestRemoteDockerRepositoryPullsThrough(t *testing.T) {
	t.Parallel()

	var challenges atomic.Int32

	up := tokenRegistry(t, &challenges)
	img := ocitest.Client{T: t, Base: up.URL}.PushImage("app", "v1")

	e := newEnv(t, "")
	e.createRepo(t, "dock-remote", remoteRepo(artifactregistrypb.Repository_DOCKER, dockerUpstream(up.URL)))

	c := e.docker(t)
	accept := map[string]string{"Accept": ocitest.ManifestMediaType}
	path := "/v2/" + project + "/dock-remote/app/manifests/v1"

	off := c.Do(http.MethodGet, path, nil, accept)
	assert.Equal(t, http.StatusBadGateway, off.Status, "upstream access is off by default")

	e.handler.Backend.SetRemoteUpstreams(true)

	got := c.Do(http.MethodGet, path, nil, accept)
	require.Equal(t, http.StatusOK, got.Status, string(got.Body))
	assert.Equal(t, img.Body, got.Body)
	assert.Positive(t, challenges.Load(), "the Bearer challenge was answered")

	blob := c.Do(http.MethodGet, "/v2/"+project+"/dock-remote/app/blobs/"+ocitest.Digest(img.Layer), nil, nil)
	require.Equal(t, http.StatusOK, blob.Status)
	assert.Equal(t, img.Layer, blob.Body)

	assert.Equal(
		t,
		[]string{img.Digest},
		versionIDs(t, e, "dock-remote", "app"),
		"the pulled image is a version of the package",
	)

	up.Close()
	e.handler.Backend.SetRemoteUpstreams(false)

	cached := c.Do(http.MethodGet, "/v2/"+project+"/dock-remote/app/manifests/"+img.Digest, nil, accept)
	assert.Equal(t, http.StatusOK, cached.Status, "a digest already pulled is served without the upstream")

	missing := c.Do(http.MethodGet, "/v2/"+project+"/dock-remote/app/manifests/v2", nil, accept)
	assert.GreaterOrEqual(t, missing.Status, http.StatusBadRequest)
	assert.True(
		t,
		strings.Contains(string(missing.Body), "UNAVAILABLE") || strings.Contains(string(missing.Body), "UNKNOWN"),
	)
}

func TestVirtualDockerRepositoryReadsUpstreams(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	e.createRepo(t, "dock-a", dockerRepo())
	e.createRepo(t, "dock-b", dockerRepo())
	e.createRepo(
		t,
		"dock-virt",
		virtualRepo(artifactregistrypb.Repository_DOCKER, [2]any{"dock-a", 1}, [2]any{"dock-b", 5}),
	)

	c := e.docker(t)
	onlyA := pushTo(c, "dock-a", "svc", "v1")
	inB := pushTo(c, "dock-b", "svc", "v2")

	accept := map[string]string{"Accept": ocitest.ManifestMediaType}

	for tag, want := range map[string]ocitest.Image{"v1": onlyA, "v2": inB} {
		got := c.Do(http.MethodGet, "/v2/"+project+"/dock-virt/svc/manifests/"+tag, nil, accept)
		require.Equal(t, http.StatusOK, got.Status, tag)
		assert.Equal(t, want.Body, got.Body, tag)
	}

	blob := c.Do(http.MethodGet, "/v2/"+project+"/dock-virt/svc/blobs/"+ocitest.Digest(onlyA.Layer), nil, nil)
	assert.Equal(t, http.StatusOK, blob.Status)

	miss := c.Do(http.MethodGet, "/v2/"+project+"/dock-virt/svc/manifests/nope", nil, accept)
	assert.Equal(t, http.StatusNotFound, miss.Status)

	push := c.Do(http.MethodPost, "/v2/"+project+"/dock-virt/svc/blobs/uploads/", nil, nil)
	assert.Equal(t, http.StatusMethodNotAllowed, push.Status, "virtual repositories are read-only")
}
