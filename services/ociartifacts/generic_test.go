package ociartifacts_test

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/artifacts"
	"github.com/oracle/oci-go-sdk/v65/genericartifactscontent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sha256Upper(b []byte) string {
	sum := sha256.Sum256(b)

	return strings.ToUpper(hex.EncodeToString(sum[:]))
}

func TestGenericRepositoryLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	repo := e.createGenericRepo(t, false)
	assert.Equal(t, "generic-repo", *repo.DisplayName)
	assert.False(t, *repo.IsImmutable)
	assert.Equal(t, artifacts.RepositoryLifecycleStateAvailable, repo.LifecycleState)

	got, err := e.client.GetRepository(t.Context(), artifacts.GetRepositoryRequest{RepositoryId: repo.Id})
	require.NoError(t, err)

	g, ok := got.Repository.(artifacts.GenericRepository)
	require.True(t, ok)
	assert.Equal(t, *repo.Id, *g.Id)

	upd, err := e.client.UpdateRepository(t.Context(), artifacts.UpdateRepositoryRequest{
		RepositoryId: repo.Id, IfMatch: got.Etag,
		UpdateRepositoryDetails: artifacts.UpdateGenericRepositoryDetails{
			DisplayName: new("renamed"), Description: new("binaries"), FreeformTags: map[string]string{"a": "b"},
		}})
	require.NoError(t, err)

	u, ok := upd.Repository.(artifacts.GenericRepository)
	require.True(t, ok)
	assert.Equal(t, "renamed", *u.DisplayName)
	assert.Equal(t, "binaries", *u.Description)

	_, err = e.client.UpdateRepository(t.Context(), artifacts.UpdateRepositoryRequest{
		RepositoryId: repo.Id, IfMatch: new("stale"),
		UpdateRepositoryDetails: artifacts.UpdateGenericRepositoryDetails{Description: new("x")},
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	list, err := e.client.ListRepositories(t.Context(), artifacts.ListRepositoriesRequest{
		CompartmentId: new(testCompartment), DisplayName: new("renamed"), IsImmutable: new(false)})
	require.NoError(t, err)
	assert.Len(t, list.Items, 1)

	empty, err := e.client.ListRepositories(t.Context(), artifacts.ListRepositoriesRequest{
		CompartmentId: new(testCompartment), IsImmutable: new(true)})
	require.NoError(t, err)
	assert.Empty(t, empty.Items)

	_, err = e.client.ChangeRepositoryCompartment(t.Context(), artifacts.ChangeRepositoryCompartmentRequest{
		RepositoryId:  repo.Id,
		CompartmentId: new(otherCompart),
	})
	require.NoError(t, err)

	moved, err := e.client.ListRepositories(
		t.Context(),
		artifacts.ListRepositoriesRequest{CompartmentId: new(otherCompart)},
	)
	require.NoError(t, err)
	assert.Len(t, moved.Items, 1)

	_, err = e.client.DeleteRepository(t.Context(), artifacts.DeleteRepositoryRequest{RepositoryId: repo.Id})
	require.NoError(t, err)

	_, err = e.client.GetRepository(t.Context(), artifacts.GetRepositoryRequest{RepositoryId: repo.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestCreateGenericRepositoryValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			"wrong type",
			`{"repositoryType":"MAVEN","compartmentId":"` + testCompartment + `","isImmutable":false}`,
			"InvalidParameter",
		},
		{
			"no immutable flag",
			`{"repositoryType":"GENERIC","compartmentId":"` + testCompartment + `"}`,
			"MissingParameter",
		},
		{"no compartment", `{"repositoryType":"GENERIC","isImmutable":false}`, "MissingParameter"},
		{
			"bad name",
			`{"repositoryType":"GENERIC","compartmentId":"` + testCompartment + `","isImmutable":false,"displayName":""}`,
			"InvalidParameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			status, body := rawDo(t, http.MethodPost, e.base+"/20160918/repositories", tt.body)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Contains(t, body, tt.want)
		})
	}

	t.Run("generated name", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		status, body := rawDo(t, http.MethodPost, e.base+"/20160918/repositories",
			`{"repositoryType":"GENERIC","compartmentId":"`+testCompartment+`","isImmutable":false}`)
		require.Equal(t, http.StatusOK, status)
		assert.Contains(t, body, `"displayName":"repository`)
	})
}

func TestGenericArtifactUploadDownload(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{register: true, enforce: true})
	repo := e.createGenericRepo(t, false)
	payload := []byte("binary\x00payload\n")

	art, err := e.upload(t, *repo.Id, "tools/cli/linux-amd64", "1.0.0", payload)
	require.NoError(t, err)
	assert.Equal(t, sha256Upper(payload), *art.Sha256)
	assert.EqualValues(t, len(payload), *art.SizeInBytes)
	assert.Equal(t, "tools/cli/linux-amd64:1.0.0", *art.DisplayName)
	assert.Equal(t, "tools/cli/linux-amd64", *art.ArtifactPath)
	assert.Equal(t, genericartifactscontent.GenericArtifactLifecycleStateAvailable, art.LifecycleState)

	download := func(byID bool) []byte {
		t.Helper()

		var body io.ReadCloser

		if byID {
			resp, dlErr := e.content.GetGenericArtifactContent(
				t.Context(),
				genericartifactscontent.GetGenericArtifactContentRequest{ArtifactId: art.Id},
			)
			require.NoError(t, dlErr)

			body = resp.Content
		} else {
			resp, dlErr := e.content.GetGenericArtifactContentByPath(
				t.Context(),
				genericartifactscontent.GetGenericArtifactContentByPathRequest{
					RepositoryId: repo.Id, ArtifactPath: new("tools/cli/linux-amd64"), Version: new("1.0.0")},
			)
			require.NoError(t, dlErr)

			body = resp.Content
		}

		defer body.Close()

		out, dlErr := io.ReadAll(body)
		require.NoError(t, dlErr)

		return out
	}

	assert.Equal(t, payload, download(true))
	assert.Equal(t, payload, download(false))

	// Overwriting in a mutable repository keeps the artifact id and changes the digest.
	next := []byte("second revision")
	again, err := e.upload(t, *repo.Id, "tools/cli/linux-amd64", "1.0.0", next)
	require.NoError(t, err)
	assert.Equal(t, *art.Id, *again.Id)
	assert.Equal(t, sha256Upper(next), *again.Sha256)

	meta, err := e.client.GetGenericArtifactByPath(t.Context(), artifacts.GetGenericArtifactByPathRequest{
		RepositoryId: repo.Id, ArtifactPath: new("tools/cli/linux-amd64"), Version: new("1.0.0")})
	require.NoError(t, err)
	assert.Equal(t, *art.Id, *meta.Id)
	assert.EqualValues(t, len(next), *meta.SizeInBytes)

	byID, err := e.client.GetGenericArtifact(t.Context(), artifacts.GetGenericArtifactRequest{ArtifactId: art.Id})
	require.NoError(t, err)
	assert.Equal(t, sha256Upper(next), *byID.Sha256)

	// If-Match guards the overwrite.
	_, err = e.content.PutGenericArtifactContentByPath(
		t.Context(),
		genericartifactscontent.PutGenericArtifactContentByPathRequest{
			RepositoryId: repo.Id, ArtifactPath: new("tools/cli/linux-amd64"), Version: new("1.0.0"),
			GenericArtifactContentBody: io.NopCloser(strings.NewReader("x")), IfMatch: new("stale"),
		},
	)
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	upd, err := e.client.UpdateGenericArtifact(t.Context(), artifacts.UpdateGenericArtifactRequest{
		ArtifactId:   art.Id,
		FreeformTags: map[string]string{"k": "v"},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "v"}, upd.FreeformTags)

	upd2, err := e.client.UpdateGenericArtifactByPath(t.Context(), artifacts.UpdateGenericArtifactByPathRequest{
		RepositoryId: repo.Id, ArtifactPath: new("tools/cli/linux-amd64"), Version: new("1.0.0"),
		FreeformTags: map[string]string{"k": "w"},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "w"}, upd2.FreeformTags)

	_, err = e.client.DeleteGenericArtifact(
		t.Context(),
		artifacts.DeleteGenericArtifactRequest{ArtifactId: art.Id, IfMatch: new("stale")},
	)
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteGenericArtifact(t.Context(), artifacts.DeleteGenericArtifactRequest{ArtifactId: art.Id})
	require.NoError(t, err)

	_, err = e.content.GetGenericArtifactContent(
		t.Context(),
		genericartifactscontent.GetGenericArtifactContentRequest{ArtifactId: art.Id},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.upload(t, *repo.Id, "a/b", "2", []byte("two"))
	require.NoError(t, err)

	_, err = e.client.DeleteGenericArtifactByPath(t.Context(), artifacts.DeleteGenericArtifactByPathRequest{
		RepositoryId: repo.Id, ArtifactPath: new("a/b"), Version: new("2")})
	require.NoError(t, err)

	_, err = e.client.GetGenericArtifactByPath(t.Context(), artifacts.GetGenericArtifactByPathRequest{
		RepositoryId: repo.Id, ArtifactPath: new("a/b"), Version: new("2")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestGenericArtifactImmutableAndKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		version   string
		wantHTTP  int
		immutable bool
		prefill   bool
	}{
		{name: "new artifact", path: "p", version: "1", wantHTTP: http.StatusOK},
		{name: "immutable new", immutable: true, path: "p", version: "1", wantHTTP: http.StatusOK},
		{
			name:      "immutable overwrite",
			immutable: true,
			path:      "p",
			version:   "1",
			prefill:   true,
			wantHTTP:  http.StatusConflict,
		},
		{
			name:      "immutable other version",
			immutable: true,
			path:      "p",
			version:   "2",
			prefill:   true,
			wantHTTP:  http.StatusOK,
		},
		{name: "mutable overwrite", path: "p", version: "1", prefill: true, wantHTTP: http.StatusOK},
		{name: "empty version", path: "p", version: " ", wantHTTP: http.StatusOK},
		{name: "path too long", path: strings.Repeat("a", 256), version: "1", wantHTTP: http.StatusBadRequest},
		{name: "version too long", path: "p", version: strings.Repeat("v", 256), wantHTTP: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			repo := e.createGenericRepo(t, tt.immutable)

			if tt.prefill {
				_, err := e.upload(t, *repo.Id, "p", "1", []byte("old"))
				require.NoError(t, err)
			}

			_, err := e.upload(t, *repo.Id, tt.path, tt.version, []byte("new"))
			if tt.wantHTTP == http.StatusOK {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.wantHTTP, svcErr(t, err).GetHTTPStatusCode())
		})
	}

	t.Run("unknown repository", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		_, err := e.upload(t, "ocid1.artifactrepository.oc1..nope", "p", "1", []byte("x"))
		assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
	})
}

func TestListGenericArtifacts(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	repo := e.createGenericRepo(t, false)
	other := e.createGenericRepo(t, false)

	for _, a := range []struct{ path, version string }{
		{"app/server", "1.0"}, {"app/server", "1.1"}, {"app/client", "2.0"}, {"docs/readme", "1.0"},
	} {
		e.clock.Advance(1_000_000_000)
		_, err := e.upload(t, *repo.Id, a.path, a.version, []byte(a.path+a.version))
		require.NoError(t, err)
	}

	_, err := e.upload(t, *other.Id, "app/server", "9", []byte("x"))
	require.NoError(t, err)

	names := func(r artifacts.ListGenericArtifactsRequest) []string {
		resp, listErr := e.client.ListGenericArtifacts(t.Context(), r)
		require.NoError(t, listErr)

		out := make([]string, 0, len(resp.Items))
		for _, it := range resp.Items {
			out = append(out, *it.DisplayName)
		}

		return out
	}

	base := func() artifacts.ListGenericArtifactsRequest {
		return artifacts.ListGenericArtifactsRequest{CompartmentId: new(testCompartment), RepositoryId: repo.Id}
	}

	tests := []struct {
		name string
		mut  func(*artifacts.ListGenericArtifactsRequest)
		want []string
	}{
		{
			"newest first",
			func(*artifacts.ListGenericArtifactsRequest) {},
			[]string{"docs/readme:1.0", "app/client:2.0", "app/server:1.1", "app/server:1.0"},
		},
		{"name asc", func(r *artifacts.ListGenericArtifactsRequest) {
			r.SortBy = artifacts.ListGenericArtifactsSortByDisplayname
		}, []string{"app/client:2.0", "app/server:1.0", "app/server:1.1", "docs/readme:1.0"}},
		{
			"path prefix",
			func(r *artifacts.ListGenericArtifactsRequest) { r.ArtifactPath = new("app/") },
			[]string{"app/client:2.0", "app/server:1.1", "app/server:1.0"},
		},
		{
			"version prefix",
			func(r *artifacts.ListGenericArtifactsRequest) { r.Version = new("1.") },
			[]string{"docs/readme:1.0", "app/server:1.1", "app/server:1.0"},
		},
		{
			"display name",
			func(r *artifacts.ListGenericArtifactsRequest) { r.DisplayName = new("app/client:2.0") },
			[]string{"app/client:2.0"},
		},
		{
			"sha256",
			func(r *artifacts.ListGenericArtifactsRequest) { r.Sha256 = new(sha256Upper([]byte("docs/readme1.0"))) },
			[]string{"docs/readme:1.0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := base()
			tt.mut(&r)
			assert.Equal(t, tt.want, names(r))
		})
	}

	t.Run("pagination", func(t *testing.T) {
		t.Parallel()

		var got []string

		var page *string

		for range 6 {
			r := base()
			r.Limit, r.Page = new(3), page
			resp, pageErr := e.client.ListGenericArtifacts(t.Context(), r)
			require.NoError(t, pageErr)

			for _, it := range resp.Items {
				got = append(got, *it.DisplayName)
			}

			if resp.OpcNextPage == nil {
				break
			}

			page = resp.OpcNextPage
		}

		assert.Len(t, got, 4)
	})

	t.Run("wrong compartment", func(t *testing.T) {
		t.Parallel()

		r := base()
		r.CompartmentId = new(otherCompart)
		assert.Empty(t, names(r))
	})
}

func TestDeleteGenericRepositoryRemovesArtifacts(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	repo := e.createGenericRepo(t, false)
	art, err := e.upload(t, *repo.Id, "p", "1", []byte("x"))
	require.NoError(t, err)

	_, err = e.client.DeleteRepository(
		t.Context(),
		artifacts.DeleteRepositoryRequest{RepositoryId: repo.Id, IfMatch: new("stale")},
	)
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteRepository(t.Context(), artifacts.DeleteRepositoryRequest{RepositoryId: repo.Id})
	require.NoError(t, err)

	_, err = e.client.GetGenericArtifact(t.Context(), artifacts.GetGenericArtifactRequest{ArtifactId: art.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}
