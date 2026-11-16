package azurecontainerregistry_test

import (
	"encoding/json/v2"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
)

type pushedImage struct {
	manifest []byte
	digest   string
	config   string
	layers   []string
}

func pushLayered(t *testing.T, c ocitest.Client, repo, tag, label string, shared []byte) pushedImage {
	t.Helper()

	cfg, err := json.Marshal(map[string]any{"architecture": "amd64", "os": "linux", "label": label}, jsonx.V1())
	require.NoError(t, err)

	img := pushedImage{config: c.PushBlob(repo, cfg, 0)}
	layers := make([]any, 0, 2)

	own := c.PushBlob(repo, []byte("layer "+label), 0)
	for _, d := range []string{own, c.PushBlob(repo, shared, 0)} {
		img.layers = append(img.layers, d)
		layers = append(layers, map[string]any{
			"mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip", "size": 1, "digest": d,
		})
	}

	img.manifest, err = json.Marshal(map[string]any{
		"schemaVersion": 2, "mediaType": ocitest.ManifestMediaType,
		"config": map[string]any{
			"mediaType": "application/vnd.docker.container.image.v1+json", "size": len(cfg), "digest": img.config,
		},
		"layers": layers,
	}, jsonx.V1())
	require.NoError(t, err)

	put := c.Do(http.MethodPut, "/v2/"+repo+"/manifests/"+tag, img.manifest,
		map[string]string{"Content-Type": ocitest.ManifestMediaType})
	require.Equal(t, http.StatusCreated, put.Status, string(put.Body))

	img.digest = ocitest.Digest(img.manifest)

	return img
}

func TestDeleteManifestCollectsUnreferencedBlobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		remove func(c ocitest.Client, a pushedImage)
		name   string
	}{
		{name: "v2 delete", remove: func(c ocitest.Client, a pushedImage) {
			del := c.Do(http.MethodDelete, "/v2/app/manifests/"+a.digest, nil, nil)
			require.Equal(t, http.StatusAccepted, del.Status)
		}},
		{name: "acr delete", remove: func(c ocitest.Client, a pushedImage) {
			require.Equal(t, http.StatusAccepted,
				c.Do(http.MethodDelete, "/acr/v1/app/_manifests/"+a.digest, nil, nil).Status)
		}},
		{name: "repository delete", remove: func(c ocitest.Client, _ pushedImage) {
			require.Equal(t, http.StatusAccepted, c.Do(http.MethodDelete, "/acr/v1/app", nil, nil).Status)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, "")
			creds := e.registry(t, "gcreg", true)
			c := e.client(t, "gcreg", basic(creds.Username, creds.Password1))

			sharedData := []byte("shared layer")
			shared := ocitest.Digest(sharedData)
			keep := pushLayered(t, c, "other", "k", "keep", sharedData)
			a := pushLayered(t, c, "app", "a", "a", sharedData)

			tt.remove(c, a)

			blob := func(repo, d string) int { return c.Do(http.MethodHead, "/v2/"+repo+"/blobs/"+d, nil, nil).Status }

			assert.Equal(t, http.StatusNotFound, blob("app", a.config), "config")
			assert.Equal(t, http.StatusNotFound, blob("app", a.layers[0]), "own layer")
			assert.Equal(t, http.StatusOK, blob("other", shared), "shared layer is still referenced")
			assert.Equal(t, http.StatusOK, blob("other", keep.config), "other config")
			assert.Equal(t, http.StatusOK, c.Do(http.MethodGet, "/v2/other/manifests/"+keep.digest, nil, nil).Status)
		})
	}
}
