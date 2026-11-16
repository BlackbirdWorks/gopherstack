package gcpartifactregistry_test

import (
	"encoding/json/v2"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
)

func pushWithConfig(t *testing.T, c ocitest.Client, repo, tag string, cfg map[string]any) string {
	t.Helper()

	layer := []byte("layer " + tag)
	layerDigest := c.PushBlob(repo, layer, 0)
	cfg["architecture"], cfg["os"] = "amd64", "linux"
	cfg["rootfs"] = map[string]any{"type": "layers", "diff_ids": []string{layerDigest}}

	raw, err := json.Marshal(cfg, jsonx.V1())
	require.NoError(t, err)

	cfgDigest := c.PushBlob(repo, raw, 0)

	body, err := json.Marshal(map[string]any{
		"schemaVersion": 2, "mediaType": ocitest.ManifestMediaType,
		"config": map[string]any{
			"mediaType": "application/vnd.docker.container.image.v1+json", "size": len(raw), "digest": cfgDigest,
		},
		"layers": []any{map[string]any{
			"mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip", "size": len(layer), "digest": layerDigest,
		}},
	}, jsonx.V1())
	require.NoError(t, err)

	put := c.Do(http.MethodPut, "/v2/"+repo+"/manifests/"+tag, body, map[string]string{
		"Content-Type": ocitest.ManifestMediaType,
	})
	require.Equal(t, http.StatusCreated, put.Status, string(put.Body))

	return ocitest.Digest(body)
}

func TestDockerImageBuildTime(t *testing.T) {
	t.Parallel()

	built := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)

	tests := []struct {
		cfg   map[string]any
		name  string
		built bool
	}{
		{name: "created", cfg: map[string]any{"created": built.Format(time.RFC3339)}, built: true},
		{name: "none", cfg: map[string]any{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, "")
			e.createRepo(t, "bt", dockerRepo())

			digest := pushWithConfig(t, e.docker(t), project+"/bt/app", "v1", tt.cfg)
			ctx := t.Context()

			img, err := e.client.GetDockerImage(ctx, &artifactregistrypb.GetDockerImageRequest{
				Name: repoName("bt") + "/dockerImages/app@" + digest,
			})
			require.NoError(t, err)

			ver, err := e.client.GetVersion(ctx, &artifactregistrypb.GetVersionRequest{
				Name: pkgName("bt", "app") + "/versions/" + digest, View: artifactregistrypb.VersionView_FULL,
			})
			require.NoError(t, err)

			meta := ver.GetMetadata().AsMap()

			if !tt.built {
				assert.Nil(t, img.GetBuildTime())
				assert.NotContains(t, meta, "buildTime")

				return
			}

			assert.True(t, built.Equal(img.GetBuildTime().AsTime()))
			assert.Equal(t, built.Format(time.RFC3339Nano), meta["buildTime"])
		})
	}
}
