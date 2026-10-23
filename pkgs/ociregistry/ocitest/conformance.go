package ocitest

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	chunkedBlobSize = 5000
	blobPattern     = 251
	blobChunks      = 3
	pageSize        = 2
)

// Run executes the conformance checks against c. Repositories are named prefix+"<case>", so a registry that
// scopes repositories (for example project/repo/) passes its scope as prefix. catalog is the repository listing
// path, empty when there is none; names in that listing carry listedPrefix instead of prefix.
func Run(t *testing.T, c Client, prefix, catalog, listedPrefix string) {
	t.Helper()

	c.T = t

	tests := []struct {
		run  func(t *testing.T, c Client, repo string)
		name string
	}{
		{name: "monolithic", run: checkMonolithic},
		{name: "chunked", run: checkChunked},
		{name: "manifest", run: checkManifest},
		{name: "head", run: checkHead},
		{name: "delete", run: checkDelete},
		{name: "tags", run: checkTags},
		{name: "unknown", run: checkUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cc := c
			cc.T = t
			tt.run(t, cc, prefix+"conf-"+tt.name)
		})
	}

	if catalog != "" {
		t.Run("catalog", func(t *testing.T) {
			t.Parallel()

			cc := c
			cc.T = t
			checkCatalog(t, cc, prefix, catalog, listedPrefix)
		})
	}
}

func checkMonolithic(t *testing.T, c Client, repo string) {
	t.Helper()

	data := []byte("monolithic blob contents")
	digest := c.PushBlob(repo, data, 0)

	got := c.Do(http.MethodGet, "/v2/"+repo+"/blobs/"+digest, nil, nil)
	require.Equal(t, http.StatusOK, got.Status)
	assert.Equal(t, data, got.Body)
}

func checkChunked(t *testing.T, c Client, repo string) {
	t.Helper()

	data := make([]byte, chunkedBlobSize)
	for i := range data {
		data[i] = byte(i % blobPattern)
	}

	digest := c.PushBlob(repo, data, blobChunks)

	got := c.Do(http.MethodGet, "/v2/"+repo+"/blobs/"+digest, nil, nil)
	require.Equal(t, http.StatusOK, got.Status)
	assert.Equal(t, data, got.Body)
}

func checkManifest(t *testing.T, c Client, repo string) {
	t.Helper()

	img := c.PushImage(repo, "v1")

	byTag := c.Do(http.MethodGet, "/v2/"+repo+"/manifests/v1", nil, map[string]string{"Accept": ManifestMediaType})
	require.Equal(t, http.StatusOK, byTag.Status)
	assert.Equal(t, img.Body, byTag.Body)
	assert.Equal(t, img.Digest, byTag.Header.Get("Docker-Content-Digest"))
	assert.Equal(t, ManifestMediaType, byTag.Header.Get("Content-Type"))

	byDigest := c.Do(http.MethodGet, "/v2/"+repo+"/manifests/"+img.Digest, nil, nil)
	require.Equal(t, http.StatusOK, byDigest.Status)
	assert.Equal(t, img.Body, byDigest.Body)
}

func checkHead(t *testing.T, c Client, repo string) {
	t.Helper()

	img := c.PushImage(repo, "latest")

	m := c.Do(http.MethodHead, "/v2/"+repo+"/manifests/latest", nil, map[string]string{"Accept": ManifestMediaType})
	require.Equal(t, http.StatusOK, m.Status)
	assert.Equal(t, img.Digest, m.Header.Get("Docker-Content-Digest"))
	assert.Empty(t, m.Body)

	b := c.Do(http.MethodHead, "/v2/"+repo+"/blobs/"+Digest(img.Layer), nil, nil)
	require.Equal(t, http.StatusOK, b.Status)

	missing := c.Do(http.MethodHead, "/v2/"+repo+"/manifests/nope", nil, nil)
	assert.Equal(t, http.StatusNotFound, missing.Status)
}

func checkDelete(t *testing.T, c Client, repo string) {
	t.Helper()

	img := c.PushImage(repo, "gone")

	del := c.Do(http.MethodDelete, "/v2/"+repo+"/manifests/"+img.Digest, nil, nil)
	require.Equal(t, http.StatusAccepted, del.Status, string(del.Body))

	after := c.Do(http.MethodGet, "/v2/"+repo+"/manifests/gone", nil, nil)
	assert.Equal(t, http.StatusNotFound, after.Status)
}

func checkTags(t *testing.T, c Client, repo string) {
	t.Helper()

	want := []string{"a", "b", "c", "d", "e"}
	for _, tag := range want {
		c.PushImage(repo, tag)
	}

	assert.Equal(t, want, c.Names("/v2/"+repo+"/tags/list", "tags", pageSize))
}

func checkUnknown(t *testing.T, c Client, repo string) {
	t.Helper()

	assert.Equal(t, http.StatusNotFound, c.Do(http.MethodGet, "/v2/"+repo+"/manifests/x", nil, nil).Status)
	assert.Equal(t, http.StatusNotFound,
		c.Do(http.MethodGet, "/v2/"+repo+"/blobs/"+Digest([]byte("none")), nil, nil).Status)
}

func checkCatalog(t *testing.T, c Client, prefix, catalog, listedPrefix string) {
	t.Helper()

	want := []string{listedPrefix + "cat-1", listedPrefix + "cat-2", listedPrefix + "cat-3"}
	for _, n := range []string{"cat-1", "cat-2", "cat-3"} {
		c.PushImage(prefix+n, "t")
	}

	got := c.Names(catalog, "repositories", pageSize)
	assert.Subset(t, got, want)
}
