package ociregistry_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
)

func newServer(t *testing.T, rootDir string) (*httptest.Server, http.Handler) {
	t.Helper()

	reg := ociregistry.New(t.Context(), ociregistry.Options{InstanceID: "test", RootDir: rootDir})
	t.Cleanup(func() { ociregistry.Shutdown(reg) })

	srv := httptest.NewServer(ociregistry.Limit(reg, 0))
	t.Cleanup(srv.Close)

	return srv, reg
}

func tempDir(t *testing.T) string {
	t.Helper()

	return t.TempDir()
}

func TestConformance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rootDir func(t *testing.T) string
		name    string
	}{
		{name: "memory", rootDir: func(*testing.T) string { return "" }},
		{name: "filesystem", rootDir: tempDir},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv, _ := newServer(t, tt.rootDir(t))
			ocitest.Run(t, ocitest.Client{T: t, Base: srv.URL}, "", "/v2/_catalog", "")
		})
	}
}

func TestClient(t *testing.T) {
	t.Parallel()

	srv, reg := newServer(t, "")
	oc := ocitest.Client{T: t, Base: srv.URL}
	img := oc.PushImage("team/app", "v1")
	oc.PushImage("team/app", "v2")
	oc.PushImage("other", "x")

	c := ociregistry.NewClient(reg)

	t.Run("catalog", func(t *testing.T) {
		t.Parallel()

		got, err := c.Catalog(t.Context())
		require.NoError(t, err)
		assert.Equal(t, []string{"other", "team/app"}, got)
	})

	t.Run("tags", func(t *testing.T) {
		t.Parallel()

		got, err := c.Tags(t.Context(), "team/app")
		require.NoError(t, err)
		assert.Equal(t, []string{"v1", "v2"}, got)
	})

	t.Run("tags of unknown repo", func(t *testing.T) {
		t.Parallel()

		got, err := c.Tags(t.Context(), "nope")
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("describe", func(t *testing.T) {
		t.Parallel()

		m, err := c.GetManifest(t.Context(), "team/app", "v1")
		require.NoError(t, err)
		assert.Equal(t, img.Digest, m.Digest)

		info := c.Describe(t.Context(), "team/app", m)
		assert.Equal(t, "amd64", info.Architecture)
		assert.Equal(t, "linux", info.OS)
		assert.Equal(t, ocitest.ManifestMediaType, info.MediaType)
		assert.Greater(t, info.Size, int64(len(img.Layer)))
	})

	t.Run("missing manifest", func(t *testing.T) {
		t.Parallel()

		_, err := c.GetManifest(t.Context(), "team/app", "nope")
		require.ErrorIs(t, err, ociregistry.ErrNotFound)
	})

	t.Run("blob over limit", func(t *testing.T) {
		t.Parallel()

		_, err := c.GetBlob(t.Context(), "team/app", ocitest.Digest(img.Layer), tinyLimit)
		require.ErrorIs(t, err, ociregistry.ErrTooLarge)
	})
}

func TestClientPutAndDelete(t *testing.T) {
	t.Parallel()

	srv, reg := newServer(t, "")
	ocitest.Client{T: t, Base: srv.URL}.PushImage("other", "x")

	c := ociregistry.NewClient(reg)

	m, err := c.GetManifest(t.Context(), "other", "x")
	require.NoError(t, err)

	digest, err := c.PutManifest(t.Context(), "other", "copy", m.MediaType, m.Body)
	require.NoError(t, err)
	assert.Equal(t, m.Digest, digest)

	tags, err := c.Tags(t.Context(), "other")
	require.NoError(t, err)
	assert.Equal(t, []string{"copy", "x"}, tags)

	require.NoError(t, c.DeleteManifest(t.Context(), "other", m.Digest))

	_, err = c.GetManifest(t.Context(), "other", "x")
	require.ErrorIs(t, err, ociregistry.ErrNotFound)
}

const tinyLimit = 10

func TestLimit(t *testing.T) {
	t.Parallel()

	var read int

	h := ociregistry.Limit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 64)

		for {
			n, err := r.Body.Read(buf)
			read += n

			if err != nil {
				if read > 0 {
					w.WriteHeader(http.StatusRequestEntityTooLarge)
				}

				return
			}
		}
	}), 8)

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/", strings.NewReader(strings.Repeat("a", 100)))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	assert.LessOrEqual(t, read, 8)
}
