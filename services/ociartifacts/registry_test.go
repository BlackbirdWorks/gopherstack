package ociartifacts_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/artifacts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
	"github.com/blackbirdworks/gopherstack/services/ociartifacts"
)

type tokenReply struct {
	Token       string `json:"token"`
	AccessToken string `json:"access_token"`
	Scope       string `json:"scope"`
	ExpiresIn   int    `json:"expires_in"`
}

func (e *env) token(t *testing.T, authz, scope string) (int, tokenReply) {
	t.Helper()

	c := e.anonymous(t)
	headers := map[string]string{}

	if authz != "" {
		headers["Authorization"] = authz
	}

	resp := c.Do(http.MethodGet, "/20180419/docker/token?service=localhost&scope="+scope, nil, headers)

	var out tokenReply

	if resp.Status == http.StatusOK {
		require.NoError(t, json.Unmarshal(resp.Body, &out))
	}

	return resp.Status, out
}

func (e *env) bearer(t *testing.T, token string) ocitest.Client {
	t.Helper()

	return ocitest.Client{T: t, Base: e.base, Prepare: func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer "+token)
	}}
}

func TestRegistryConformance(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ocitest.Run(t, e.registry(t), testNamespace+"/", "", "")
}

func TestRegistryBasicAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		authz  string
		status int
	}{
		{"none", "", http.StatusUnauthorized},
		{"valid", basic(loginUser, loginToken), http.StatusOK},
		{"federated user", basic(testNamespace+"/oracleidentitycloudservice/bob", loginToken), http.StatusOK},
		{"namespace case", basic("GopherStack/alice", loginToken), http.StatusOK},
		{"wrong namespace", basic("other/alice", loginToken), http.StatusUnauthorized},
		{"no namespace", basic("alice", loginToken), http.StatusUnauthorized},
		{"empty user", basic(testNamespace+"/", loginToken), http.StatusUnauthorized},
		{"empty token", basic(loginUser, ""), http.StatusUnauthorized},
		{"bad base64", "Basic !!!", http.StatusUnauthorized},
		{"bad bearer", "Bearer nope", http.StatusUnauthorized},
		{"unknown scheme", "Digest x", http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			headers := map[string]string{}

			if tt.authz != "" {
				headers["Authorization"] = tt.authz
			}

			resp := e.anonymous(t).Do(http.MethodGet, "/v2/", nil, headers)
			assert.Equal(t, tt.status, resp.Status)
			assert.Equal(t, "registry/2.0", resp.Header.Get("Docker-Distribution-Api-Version"))

			if tt.status == http.StatusUnauthorized {
				assert.Contains(
					t,
					resp.Header.Get("WWW-Authenticate"),
					`Bearer realm="`+e.base+`/20180419/docker/token"`,
				)
				assert.Contains(t, string(resp.Body), "UNAUTHORIZED")
			} else {
				assert.JSONEq(t, "{}", string(resp.Body))
			}

			// A failed login must not echo the credentials.
			assert.NotContains(t, string(resp.Body), loginToken)
		})
	}
}

func TestTokenEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		authz     string
		scope     string
		wantScope string
		wantCode  int
	}{
		{"login without scope", basic(loginUser, loginToken), "", "", http.StatusOK},
		{
			"push scope",
			basic(loginUser, loginToken),
			"repository:gopherstack/app:pull,push",
			"repository:gopherstack/app:pull,push",
			http.StatusOK,
		},
		{"anonymous pull", "", "repository:gopherstack/app:pull", "repository:gopherstack/app:pull", http.StatusOK},
		{"anonymous push dropped", "", "repository:gopherstack/app:push", "", http.StatusOK},
		{
			"anonymous push narrowed",
			"",
			"repository:gopherstack/app:pull,push",
			"repository:gopherstack/app:pull",
			http.StatusOK,
		},
		{
			"bad credentials",
			basic("nobody/alice", loginToken),
			"repository:gopherstack/app:pull",
			"",
			http.StatusUnauthorized,
		},
		{"empty token", basic(loginUser, ""), "", "", http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			status, reply := e.token(t, tt.authz, tt.scope)
			require.Equal(t, tt.wantCode, status)

			if status != http.StatusOK {
				return
			}

			assert.NotEmpty(t, reply.Token)
			assert.Equal(t, reply.Token, reply.AccessToken)
			assert.Equal(t, 3600, reply.ExpiresIn)
			assert.Equal(t, tt.wantScope, reply.Scope)
			assert.NotContains(t, reply.Token, loginToken)
		})
	}
}

func TestTokenFlowPushPullAndScope(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	_, push := e.token(t, basic(loginUser, loginToken), "repository:gopherstack/flow:pull,push")
	pc := e.bearer(t, push.Token)
	img := pc.PushImage(testNamespace+"/flow", "v1")

	_, pull := e.token(t, basic(loginUser, loginToken), "repository:gopherstack/flow:pull")
	rc := e.bearer(t, pull.Token)

	got := rc.Do(http.MethodGet, "/v2/"+testNamespace+"/flow/manifests/v1", nil, nil)
	require.Equal(t, http.StatusOK, got.Status)
	assert.Equal(t, img.Body, got.Body)

	denied := rc.Do(http.MethodPost, "/v2/"+testNamespace+"/flow/blobs/uploads/", nil, nil)
	assert.Equal(t, http.StatusUnauthorized, denied.Status)
	assert.Contains(t, denied.Header.Get("WWW-Authenticate"), `error="insufficient_scope"`)

	other := rc.Do(http.MethodGet, "/v2/"+testNamespace+"/elsewhere/manifests/v1", nil, nil)
	assert.Equal(t, http.StatusUnauthorized, other.Status)

	tampered := e.bearer(t, push.Token+"x")
	assert.Equal(t, http.StatusUnauthorized, tampered.Do(http.MethodGet, "/v2/", nil, nil).Status)
}

func TestTokenExpires(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	_, reply := e.token(t, basic(loginUser, loginToken), "repository:gopherstack/x:pull")
	c := e.bearer(t, reply.Token)

	assert.Equal(t, http.StatusOK, c.Do(http.MethodGet, "/v2/", nil, nil).Status)

	e.clock.Advance(3601 * 1_000_000_000)

	assert.Equal(t, http.StatusUnauthorized, c.Do(http.MethodGet, "/v2/", nil, nil).Status)
}

func TestAutoCreateOnFirstPush(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	c := e.registry(t)
	c.PushImage(testNamespace+"/team/new-app", "v1")

	resp, err := e.client.ListContainerRepositories(t.Context(), artifacts.ListContainerRepositoriesRequest{
		CompartmentId: new(ociartifacts.DefaultTenancy),
	})
	require.NoError(t, err)
	require.Len(t, resp.Items, 1)

	repo := resp.Items[0]
	assert.Equal(t, "team/new-app", *repo.DisplayName)
	assert.Equal(t, 1, *repo.ImageCount)
	assert.Equal(t, 1, *repo.LayerCount)
	assert.Positive(t, *repo.LayersSizeInBytes)
	assert.False(t, *repo.IsPublic)

	full, err := e.client.GetContainerRepository(
		t.Context(),
		artifacts.GetContainerRepositoryRequest{RepositoryId: repo.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, "alice@example.com", *full.CreatedBy)
	assert.NotNil(t, full.TimeLastPushed)
}

func TestFirstPushCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		repo       string
		wantCode   string
		wantStatus int
		wantRepos  int
		createOff  bool
	}{
		{name: "creates when enabled", repo: testNamespace + "/fresh", wantStatus: http.StatusAccepted, wantRepos: 1},
		{
			name: "denied when disabled", createOff: true, repo: testNamespace + "/fresh",
			wantStatus: http.StatusForbidden, wantCode: "DENIED",
		},
		{
			name: "invalid name", repo: testNamespace + "/Fresh",
			wantStatus: http.StatusBadRequest, wantCode: "NAME_INVALID",
		},
		{
			name: "wrong namespace", repo: "someone-else/fresh",
			wantStatus: http.StatusNotFound, wantCode: "NAME_UNKNOWN",
		},
		{name: "no namespace", repo: "fresh", wantStatus: http.StatusNotFound, wantCode: "NAME_UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})

			if tt.createOff {
				_, err := e.client.UpdateContainerConfiguration(
					t.Context(),
					artifacts.UpdateContainerConfigurationRequest{
						CompartmentId:                  new(testCompartment),
						IsRepositoryCreatedOnFirstPush: new(false),
					},
				)
				require.NoError(t, err)
			}

			resp := e.registry(t).Do(http.MethodPost, "/v2/"+tt.repo+"/blobs/uploads/", nil, nil)
			require.Equal(t, tt.wantStatus, resp.Status, string(resp.Body))

			if tt.wantCode != "" {
				assert.Contains(t, string(resp.Body), tt.wantCode)
			}

			list, err := e.client.ListContainerRepositories(t.Context(), artifacts.ListContainerRepositoriesRequest{
				CompartmentId: new(ociartifacts.DefaultTenancy),
			})
			require.NoError(t, err)
			assert.Len(t, list.Items, tt.wantRepos)
		})
	}

	t.Run("pull of unknown repo does not create it", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		resp := e.registry(t).Do(http.MethodGet, "/v2/"+testNamespace+"/ghost/manifests/latest", nil, nil)
		assert.Equal(t, http.StatusNotFound, resp.Status)
		assert.Contains(t, string(resp.Body), "NAME_UNKNOWN")

		list, err := e.client.ListContainerRepositories(t.Context(), artifacts.ListContainerRepositoriesRequest{
			CompartmentId: new(ociartifacts.DefaultTenancy),
		})
		require.NoError(t, err)
		assert.Empty(t, list.Items)
	})

	t.Run("catalog is not offered", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		assert.Equal(t, http.StatusNotFound, e.registry(t).Do(http.MethodGet, "/v2/_catalog", nil, nil).Status)
	})
}

func TestImmutableRepository(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.createRepo(t, "locked", func(d *artifacts.CreateContainerRepositoryDetails) { d.IsImmutable = new(true) })

	c := e.registry(t)
	repo := testNamespace + "/locked"
	first := c.PushImage(repo, "v1")

	// A different image under the same tag is rejected.
	layer := c.PushBlob(repo, []byte("other layer"), 0)
	cfg := c.PushBlob(repo, []byte(`{"architecture":"amd64","os":"linux"}`), 0)
	other := manifestFor(t, cfg, layer, 11, 36)

	denied := c.Do(
		http.MethodPut,
		"/v2/"+repo+"/manifests/v1",
		other,
		map[string]string{"Content-Type": ocitest.ManifestMediaType},
	)
	assert.Equal(t, http.StatusConflict, denied.Status)
	assert.Contains(t, string(denied.Body), "TAG_INVALID")

	still := c.Do(http.MethodGet, "/v2/"+repo+"/manifests/v1", nil, nil)
	assert.Equal(t, first.Body, still.Body)

	// The same image again is idempotent, a new tag and a digest push are allowed.
	same := c.Do(
		http.MethodPut,
		"/v2/"+repo+"/manifests/v1",
		first.Body,
		map[string]string{"Content-Type": ocitest.ManifestMediaType},
	)
	assert.Equal(t, http.StatusCreated, same.Status)

	fresh := c.Do(
		http.MethodPut,
		"/v2/"+repo+"/manifests/v2",
		other,
		map[string]string{"Content-Type": ocitest.ManifestMediaType},
	)
	assert.Equal(t, http.StatusCreated, fresh.Status)

	byDigest := c.Do(
		http.MethodPut,
		"/v2/"+repo+"/manifests/"+ocitest.Digest(other),
		other,
		map[string]string{"Content-Type": ocitest.ManifestMediaType},
	)
	assert.Equal(t, http.StatusCreated, byDigest.Status)

	assert.Len(t, e.listImages(t, nil), 2)
}

func TestMutableRepositoryMovesTag(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.createRepo(t, "movable", nil)

	c := e.registry(t)
	repo := testNamespace + "/movable"
	c.PushImage(repo, "latest")
	e.clock.Advance(1_000_000_000)

	layer := c.PushBlob(repo, []byte("newer layer"), 0)
	cfg := c.PushBlob(repo, []byte(`{"architecture":"arm64","os":"linux"}`), 0)
	newer := manifestFor(t, cfg, layer, 11, 37)

	put := c.Do(
		http.MethodPut,
		"/v2/"+repo+"/manifests/latest",
		newer,
		map[string]string{"Content-Type": ocitest.ManifestMediaType},
	)
	require.Equal(t, http.StatusCreated, put.Status)

	images := e.listImages(t, nil)
	require.Len(t, images, 2)

	byVersion := e.listImages(t, func(r *artifacts.ListContainerImagesRequest) { r.Version = new("latest") })
	require.Len(t, byVersion, 1)
	assert.Equal(t, ocitest.Digest(newer), *byVersion[0].Digest)

	untagged := e.listImages(t, func(r *artifacts.ListContainerImagesRequest) { r.IsVersioned = new(false) })
	require.Len(t, untagged, 1)
	assert.Nil(t, untagged[0].Version)
	assert.Contains(t, *untagged[0].DisplayName, "movable:latest@sha256:")
}

func TestPublicRepositoryAnonymousPull(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.createRepo(t, "open", func(d *artifacts.CreateContainerRepositoryDetails) { d.IsPublic = new(true) })
	e.createRepo(t, "closed", nil)

	rc := e.registry(t)
	open := rc.PushImage(testNamespace+"/open", "v1")
	rc.PushImage(testNamespace+"/closed", "v1")

	_, anon := e.token(t, "", "repository:gopherstack/open:pull,push")
	ac := e.bearer(t, anon.Token)

	tests := []struct {
		name   string
		client ocitest.Client
		method string
		path   string
		want   int
	}{
		{"anonymous pulls public manifest", ac, http.MethodGet, "/v2/gopherstack/open/manifests/v1", http.StatusOK},
		{"anonymous heads public manifest", ac, http.MethodHead, "/v2/gopherstack/open/manifests/v1", http.StatusOK},
		{"anonymous lists public tags", ac, http.MethodGet, "/v2/gopherstack/open/tags/list", http.StatusOK},
		{"anonymous cannot push", ac, http.MethodPost, "/v2/gopherstack/open/blobs/uploads/", http.StatusUnauthorized},
		{
			"anonymous cannot delete",
			ac,
			http.MethodDelete,
			"/v2/gopherstack/open/manifests/" + open.Digest,
			http.StatusUnauthorized,
		},
		{
			"anonymous cannot pull private",
			ac,
			http.MethodGet,
			"/v2/gopherstack/closed/manifests/v1",
			http.StatusUnauthorized,
		},
		{
			"anonymous cannot probe unknown",
			ac,
			http.MethodGet,
			"/v2/gopherstack/ghost/manifests/v1",
			http.StatusUnauthorized,
		},
		{
			"no credentials at all",
			e.anonymous(t),
			http.MethodGet,
			"/v2/gopherstack/open/manifests/v1",
			http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.client.T = t
			assert.Equal(t, tt.want, tt.client.Do(tt.method, tt.path, nil, nil).Status)
		})
	}

	t.Run("anonymous blob pull", func(t *testing.T) {
		t.Parallel()

		ac.T = t
		resp := ac.Do(http.MethodGet, "/v2/gopherstack/open/blobs/"+ocitest.Digest(open.Layer), nil, nil)
		require.Equal(t, http.StatusOK, resp.Status)
		assert.Equal(t, open.Layer, resp.Body)
	})

	t.Run("making the repository private revokes access", func(t *testing.T) {
		t.Parallel()

		e2 := newEnv(t, envOpts{})
		repo := e2.createRepo(t, "flip", func(d *artifacts.CreateContainerRepositoryDetails) { d.IsPublic = new(true) })
		e2.registry(t).PushImage(testNamespace+"/flip", "v1")

		_, tok := e2.token(t, "", "repository:gopherstack/flip:pull")
		c := e2.bearer(t, tok.Token)
		assert.Equal(t, http.StatusOK, c.Do(http.MethodGet, "/v2/gopherstack/flip/manifests/v1", nil, nil).Status)

		_, err := e2.client.UpdateContainerRepository(t.Context(), artifacts.UpdateContainerRepositoryRequest{
			RepositoryId: repo.Id,
			IsPublic:     new(false),
		})
		require.NoError(t, err)

		assert.Equal(
			t,
			http.StatusUnauthorized,
			c.Do(http.MethodGet, "/v2/gopherstack/flip/manifests/v1", nil, nil).Status,
		)
	})
}

func TestImageListingAndDetails(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	repo := e.createRepo(t, "svc/web", nil)
	c := e.registry(t)
	name := testNamespace + "/svc/web"

	img := c.PushImage(name, "v1")

	// A second tag on the same manifest is one image with two versions.
	e.clock.Advance(1_000_000_000)

	put := c.Do(
		http.MethodPut,
		"/v2/"+name+"/manifests/stable",
		img.Body,
		map[string]string{"Content-Type": ocitest.ManifestMediaType},
	)
	require.Equal(t, http.StatusCreated, put.Status)

	images := e.listImages(t, nil)
	require.Len(t, images, 1)

	sum := images[0]
	assert.Equal(t, ocitest.Digest(img.Body), *sum.Digest)
	assert.Equal(t, "svc/web:stable", *sum.DisplayName)
	assert.Equal(t, "stable", *sum.Version)
	assert.Equal(t, "svc/web", *sum.RepositoryName)
	assert.Equal(t, *repo.Id, *sum.RepositoryId)
	assert.Equal(t, testCompartment, *sum.CompartmentId)
	assert.Equal(t, artifacts.ContainerImageLifecycleStateAvailable, sum.LifecycleState)

	full, err := e.client.GetContainerImage(t.Context(), artifacts.GetContainerImageRequest{ImageId: sum.Id})
	require.NoError(t, err)
	require.Len(t, full.Versions, 2)
	assert.Equal(t, "v1", *full.Versions[0].Version)
	assert.Equal(t, "stable", *full.Versions[1].Version)
	assert.Equal(t, "alice@example.com", *full.Versions[0].CreatedBy)
	require.Len(t, full.Layers, 1)
	assert.Equal(t, ocitest.Digest(img.Layer), *full.Layers[0].Digest)
	assert.EqualValues(t, len(img.Layer), *full.Layers[0].SizeInBytes)
	assert.EqualValues(t, len(img.Layer), *full.LayersSizeInBytes)
	assert.Equal(t, len(img.Body), *full.ManifestSizeInBytes)
	assert.Zero(t, *full.PullCount)
	assert.NotNil(t, full.Etag)

	filters := []struct {
		mut  func(*artifacts.ListContainerImagesRequest)
		name string
		want int
	}{
		{func(r *artifacts.ListContainerImagesRequest) { r.RepositoryId = repo.Id }, "repository id", 1},
		{
			func(r *artifacts.ListContainerImagesRequest) { r.RepositoryId = new("ocid1.containerrepo.oc1..x") },
			"other repository id",
			0,
		},
		{
			func(r *artifacts.ListContainerImagesRequest) { r.RepositoryName = new("svc/*") },
			"repository name wildcard",
			1,
		},
		{func(r *artifacts.ListContainerImagesRequest) { r.RepositoryName = new("other*") }, "repository name miss", 0},
		{func(r *artifacts.ListContainerImagesRequest) { r.Version = new("v1") }, "version exact", 1},
		{func(r *artifacts.ListContainerImagesRequest) { r.Version = new("st*") }, "version wildcard", 1},
		{func(r *artifacts.ListContainerImagesRequest) { r.ImageDigest = new(ocitest.Digest(img.Body)) }, "digest", 1},
		{func(r *artifacts.ListContainerImagesRequest) { r.DisplayName = new("svc/web:stable") }, "display name", 1},
		{func(r *artifacts.ListContainerImagesRequest) { r.ImageId = sum.Id }, "image id", 1},
		{func(r *artifacts.ListContainerImagesRequest) { r.IsVersioned = new(true) }, "versioned", 1},
		{func(r *artifacts.ListContainerImagesRequest) { r.IsVersioned = new(false) }, "unversioned", 0},
		{func(r *artifacts.ListContainerImagesRequest) { r.CompartmentId = new(otherCompart) }, "other compartment", 0},
	}

	for _, f := range filters {
		t.Run(f.name, func(t *testing.T) {
			t.Parallel()

			assert.Len(t, e.listImages(t, f.mut), f.want)
		})
	}

	got, err := e.client.GetContainerRepository(
		t.Context(),
		artifacts.GetContainerRepositoryRequest{RepositoryId: repo.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, 1, *got.ImageCount)
	assert.Equal(t, 1, *got.LayerCount)
	assert.EqualValues(t, len(img.Layer), *got.LayersSizeInBytes)
}

func TestPullCountAndLastPulled(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.createRepo(t, "pulls", nil)

	c := e.registry(t)
	name := testNamespace + "/pulls"
	c.PushImage(name, "v1")

	for range 3 {
		require.Equal(t, http.StatusOK, c.Do(http.MethodGet, "/v2/"+name+"/manifests/v1", nil, nil).Status)
	}

	// HEAD is not a pull.
	require.Equal(t, http.StatusOK, c.Do(http.MethodHead, "/v2/"+name+"/manifests/v1", nil, nil).Status)

	images := e.listImages(t, nil)
	require.Len(t, images, 1)

	full, err := e.client.GetContainerImage(t.Context(), artifacts.GetContainerImageRequest{ImageId: images[0].Id})
	require.NoError(t, err)
	assert.EqualValues(t, 3, *full.PullCount)
	require.NotNil(t, full.TimeLastPulled)
	assert.WithinDuration(t, e.clock.Now(), full.TimeLastPulled.Time, time.Second)
}

func TestImageDeleteRestoreRemoveVersion(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.createRepo(t, "lifecycle", nil)

	c := e.registry(t)
	name := testNamespace + "/lifecycle"
	c.PushImage(name, "v1")

	images := e.listImages(t, nil)
	require.Len(t, images, 1)

	id := images[0].Id
	digest := *images[0].Digest

	// Add a second tag, then remove it again.
	body := c.Do(http.MethodGet, "/v2/"+name+"/manifests/v1", nil, nil).Body
	require.Equal(
		t,
		http.StatusCreated,
		c.Do(
			http.MethodPut,
			"/v2/"+name+"/manifests/extra",
			body,
			map[string]string{"Content-Type": ocitest.ManifestMediaType},
		).Status,
	)

	removed, err := e.client.RemoveContainerVersion(t.Context(), artifacts.RemoveContainerVersionRequest{
		ImageId: id, Version: new("extra"),
	})
	require.NoError(t, err)
	require.Len(t, removed.Versions, 1)
	assert.Equal(t, "v1", *removed.Version)
	assert.Equal(t, http.StatusNotFound, c.Do(http.MethodGet, "/v2/"+name+"/manifests/extra", nil, nil).Status,
		"a removed version no longer resolves")
	assert.Equal(t, http.StatusOK, c.Do(http.MethodGet, "/v2/"+name+"/manifests/v1", nil, nil).Status)

	_, err = e.client.RemoveContainerVersion(t.Context(), artifacts.RemoveContainerVersionRequest{
		ImageId: id, Version: new("nope"),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.RestoreContainerImage(t.Context(), artifacts.RestoreContainerImageRequest{ImageId: id})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode())

	// Delete hides the image and stops pulls.
	_, err = e.client.DeleteContainerImage(
		t.Context(),
		artifacts.DeleteContainerImageRequest{ImageId: id, IfMatch: new("stale")},
	)
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteContainerImage(t.Context(), artifacts.DeleteContainerImageRequest{ImageId: id})
	require.NoError(t, err)

	gone, err := e.client.GetContainerImage(t.Context(), artifacts.GetContainerImageRequest{ImageId: id})
	require.NoError(t, err)
	assert.Equal(t, artifacts.ContainerImageLifecycleStateDeleted, gone.LifecycleState)
	assert.Empty(t, e.listImages(t, nil))
	assert.Len(
		t,
		e.listImages(t, func(r *artifacts.ListContainerImagesRequest) { r.LifecycleState = new("DELETED") }),
		1,
	)

	for _, ref := range []string{"v1", digest} {
		resp := c.Do(http.MethodGet, "/v2/"+name+"/manifests/"+ref, nil, nil)
		assert.Equal(t, http.StatusNotFound, resp.Status, ref)
		assert.Contains(t, string(resp.Body), "MANIFEST_UNKNOWN")
	}

	_, err = e.client.DeleteContainerImage(t.Context(), artifacts.DeleteContainerImageRequest{ImageId: id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	repos, err := e.client.ListContainerRepositories(t.Context(), artifacts.ListContainerRepositoriesRequest{
		CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.Len(t, repos.Items, 1)
	assert.Zero(t, *repos.Items[0].ImageCount)

	// Restore under a new version.
	restored, err := e.client.RestoreContainerImage(t.Context(), artifacts.RestoreContainerImageRequest{
		ImageId: id, Version: new("back"),
	})
	require.NoError(t, err)
	assert.Equal(t, artifacts.ContainerImageLifecycleStateAvailable, restored.LifecycleState)
	assert.Equal(t, "back", *restored.Version)

	for _, ref := range []string{"back", "v1", digest} {
		assert.Equal(t, http.StatusOK, c.Do(http.MethodGet, "/v2/"+name+"/manifests/"+ref, nil, nil).Status, ref)
	}
}

func TestLookupContainerImageByURI(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	img := e.registry(t).PushImage(testNamespace+"/team/lookup", "v1")
	digest := ocitest.Digest(img.Body)

	tests := []struct {
		name string
		uri  string
		want int
	}{
		{"by tag", testNamespace + "/team/lookup:v1", http.StatusOK},
		{"by digest", testNamespace + "/team/lookup@" + digest, http.StatusOK},
		{"unknown tag", testNamespace + "/team/lookup:v9", http.StatusNotFound},
		{"unknown repo", testNamespace + "/team/none:v1", http.StatusNotFound},
		{"wrong namespace", "other/team/lookup:v1", http.StatusNotFound},
		{"no reference", testNamespace + "/team/lookup", http.StatusNotFound},
		{"empty", "", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := e.client.LookupContainerImageByUri(t.Context(), artifacts.LookupContainerImageByUriRequest{
				ImageUri: new(tt.uri),
			})
			if tt.want == http.StatusOK {
				require.NoError(t, err)
				assert.Equal(t, digest, *resp.Digest)

				return
			}

			assert.Equal(t, tt.want, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestUpdateImageTags(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.createRepo(t, "tagged", nil)
	e.registry(t).PushImage(testNamespace+"/tagged", "v1")

	images := e.listImages(t, nil)
	require.Len(t, images, 1)

	upd, err := e.client.UpdateContainerImage(t.Context(), artifacts.UpdateContainerImageRequest{
		ImageId:      images[0].Id,
		FreeformTags: map[string]string{"scan": "clean"}})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"scan": "clean"}, upd.FreeformTags)

	_, err = e.client.UpdateContainerImage(t.Context(), artifacts.UpdateContainerImageRequest{
		ImageId: images[0].Id, IfMatch: new("stale"),
		UpdateContainerImageDetails: artifacts.UpdateContainerImageDetails{},
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())
}

func TestRegistryDeleteUpdatesListing(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.createRepo(t, "dp", nil)

	c := e.registry(t)
	name := testNamespace + "/dp"
	img := c.PushImage(name, "v1")

	require.Len(t, e.listImages(t, nil), 1)

	del := c.Do(http.MethodDelete, "/v2/"+name+"/manifests/"+ocitest.Digest(img.Body), nil, nil)
	require.Equal(t, http.StatusAccepted, del.Status, string(del.Body))
	assert.Empty(t, e.listImages(t, nil))

	// Deleting from a repository that does not exist is unknown, and never creates it.
	miss := c.Do(http.MethodDelete, "/v2/"+testNamespace+"/nothere/manifests/"+ocitest.Digest(img.Body), nil, nil)
	assert.Equal(t, http.StatusNotFound, miss.Status)
}

func TestDeleteRepositoryRemovesImages(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	repo := e.createRepo(t, "doomed", nil)
	c := e.registry(t)
	name := testNamespace + "/doomed"
	img := c.PushImage(name, "v1")

	_, err := e.client.DeleteContainerRepository(
		t.Context(),
		artifacts.DeleteContainerRepositoryRequest{RepositoryId: repo.Id},
	)
	require.NoError(t, err)

	assert.Empty(t, e.listImages(t, nil))
	assert.Equal(t, http.StatusNotFound, c.Do(http.MethodGet, "/v2/"+name+"/manifests/v1", nil, nil).Status)

	// A new repository with the same name starts empty even though its name was used before.
	e.createRepo(t, "doomed", nil)
	assert.Equal(
		t,
		http.StatusNotFound,
		c.Do(http.MethodGet, "/v2/"+name+"/manifests/"+ocitest.Digest(img.Body), nil, nil).Status,
	)
}

func manifestFor(t *testing.T, cfgDigest, layerDigest string, layerSize, cfgSize int) []byte {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"schemaVersion": 2, "mediaType": ocitest.ManifestMediaType,
		"config": map[string]any{
			"mediaType": "application/vnd.docker.container.image.v1+json", "size": cfgSize, "digest": cfgDigest,
		},
		"layers": []any{map[string]any{
			"mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip", "size": layerSize, "digest": layerDigest,
		}},
	})
	require.NoError(t, err)

	return body
}

func TestRegistryTagDeleteKeepsImage(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.createRepo(t, "untag", nil)

	c := e.registry(t)
	name := testNamespace + "/untag"
	img := c.PushImage(name, "v1")

	require.Equal(
		t,
		http.StatusCreated,
		c.Do(
			http.MethodPut,
			"/v2/"+name+"/manifests/v2",
			img.Body,
			map[string]string{"Content-Type": ocitest.ManifestMediaType},
		).Status,
	)

	del := c.Do(http.MethodDelete, "/v2/"+name+"/manifests/v2", nil, nil)
	require.Equal(t, http.StatusAccepted, del.Status, string(del.Body))

	images := e.listImages(t, nil)
	require.Len(t, images, 1)

	full, err := e.client.GetContainerImage(t.Context(), artifacts.GetContainerImageRequest{ImageId: images[0].Id})
	require.NoError(t, err)
	require.Len(t, full.Versions, 1)
	assert.Equal(t, "v1", *full.Versions[0].Version)
}

func TestCustomNamespace(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.backend.SetNamespace("TenantNS")

	c := ocitest.Client{T: t, Base: e.base, Prepare: func(r *http.Request) {
		r.Header.Set("Authorization", basic("tenantns/alice", loginToken))
	}}
	c.PushImage("tenantns/svc", "v1")

	resp, err := e.client.GetContainerConfiguration(
		t.Context(),
		artifacts.GetContainerConfigurationRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	assert.Equal(t, "TenantNS", *resp.Namespace)
	assert.Equal(t, "TenantNS", e.backend.Namespace())

	// The default namespace no longer logs in.
	assert.Equal(t, http.StatusUnauthorized, e.registry(t).Do(http.MethodGet, "/v2/", nil, nil).Status)
}
