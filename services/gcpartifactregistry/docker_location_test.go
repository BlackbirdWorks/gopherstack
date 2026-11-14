package gcpartifactregistry_test

import (
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
)

const otherLoc = "projects/" + project + "/locations/europe-west1"

func (e *env) dockerImageCount(t *testing.T, parent string) int {
	t.Helper()

	it := e.client.ListDockerImages(t.Context(), &artifactregistrypb.ListDockerImagesRequest{Parent: parent})
	n := 0

	for {
		_, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return n
		}

		require.NoError(t, err)

		n++
	}
}

func TestDockerRepositoryIDPerLocation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, t.TempDir())
	e.createRepo(t, "shared", dockerRepo())

	op, err := e.client.CreateRepository(t.Context(), &artifactregistrypb.CreateRepositoryRequest{
		Parent: otherLoc, RepositoryId: "shared", Repository: dockerRepo(),
	})
	require.NoError(t, err)

	_, err = op.Wait(t.Context())
	require.NoError(t, err)

	us, eu := e.docker(t), e.docker(t)
	us.Host, eu.Host = "us-central1-docker.pkg.dev", "europe-west1-docker.pkg.dev"

	usImg := pushTo(us, "shared", "app", "v1")
	euImg := pushTo(eu, "shared", "other", "v1")

	assert.Equal(t, []string{"app"}, us.Names("/v2/"+project+"/shared/_catalog", "repositories", 10))
	assert.Equal(t, []string{"other"}, eu.Names("/v2/"+project+"/shared/_catalog", "repositories", 10))
	assert.Equal(t, 1, e.dockerImageCount(t, repoName("shared")))
	assert.Equal(t, 1, e.dockerImageCount(t, otherLoc+"/repositories/shared"))

	repo, err := e.client.GetRepository(t.Context(), &artifactregistrypb.GetRepositoryRequest{Name: repoName("shared")})
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(repo.GetRegistryUri(), "/"+project+"/shared"), repo.GetRegistryUri())

	assert.Equal(t, 200, us.Do("GET", "/v2/"+project+"/shared/app/manifests/v1", nil, nil).Status)
	assert.Equal(t, 404, us.Do("GET", "/v2/"+project+"/shared/other/manifests/v1", nil, nil).Status)
	assert.NotEqual(t, usImg.Digest, euImg.Digest)

	del, err := e.client.DeleteRepository(t.Context(), &artifactregistrypb.DeleteRepositoryRequest{
		Name: repoName("shared"),
	})
	require.NoError(t, err)
	require.NoError(t, del.Wait(t.Context()))

	assert.Equal(t, 200, eu.Do("GET", "/v2/"+project+"/shared/other/manifests/v1", nil, nil).Status)
}

func TestLegacyDockerLayoutRestore(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	before := newEnv(t, dir)
	before.createRepo(t, "legacy", dockerRepo())
	img := pushTo(before.docker(t), "legacy", "app", "v1")

	var snap map[string]any

	require.NoError(t, json.Unmarshal(before.handler.Snapshot(t.Context()), &snap, jsonx.V1()))
	before.handler.Backend.Close()

	delete(snap, "layout")

	oldLedger := map[string]any{}
	for k, v := range snap["ledger"].(map[string]any) {
		oldLedger[strings.Replace(k, "/us-central1/", "/", 1)] = v
	}

	snap["ledger"] = oldLedger

	repos := filepath.Join(dir, "images", "docker", "registry", "v2", "repositories", project)
	require.NoError(t, os.Rename(filepath.Join(repos, "us-central1", "legacy"), filepath.Join(repos, "legacy")))
	require.NoError(t, os.Remove(filepath.Join(repos, "us-central1")))

	raw, err := json.Marshal(snap, jsonx.V1())
	require.NoError(t, err)

	after := newEnv(t, dir)
	require.NoError(t, after.handler.Restore(t.Context(), raw))

	got := after.docker(t).Do("GET", "/v2/"+project+"/legacy/app/manifests/v1", nil, nil)
	require.Equal(t, 200, got.Status, string(got.Body))
	assert.Equal(t, img.Body, got.Body)

	blob := after.docker(t).Do("GET", "/v2/"+project+"/legacy/app/blobs/"+ocitest.Digest(img.Layer), nil, nil)
	assert.Equal(t, 200, blob.Status)
	assert.Equal(t, 1, after.dockerImageCount(t, repoName("legacy")))

	_, err = os.Stat(filepath.Join(repos, "us-central1", "legacy"))
	require.NoError(t, err)
	assert.Contains(t, string(after.handler.Snapshot(t.Context())), `"layout":2`)
}

func TestGCRHostCreatesRepositoryOnPush(t *testing.T) {
	t.Parallel()

	tests := []struct {
		host     string
		repoID   string
		location string
	}{
		{"gcr.io", "gcr.io", "us"},
		{"us.gcr.io", "us.gcr.io", "us"},
		{"eu.gcr.io", "eu.gcr.io", "europe"},
		{"asia.gcr.io", "asia.gcr.io", "asia"},
	}

	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, "")
			c := e.docker(t)
			c.Host = tt.host

			name := "projects/" + project + "/locations/" + tt.location + "/repositories/" + tt.repoID
			_, err := e.client.GetRepository(t.Context(), &artifactregistrypb.GetRepositoryRequest{Name: name})
			require.Error(t, err)

			assert.Equal(
				t,
				404,
				c.Do("GET", "/v2/"+project+"/app/manifests/v1", nil, nil).Status,
				"reads do not create",
			)

			img := c.PushImage(project+"/app", "v1")

			repo, err := e.client.GetRepository(t.Context(), &artifactregistrypb.GetRepositoryRequest{Name: name})
			require.NoError(t, err)
			assert.Equal(t, artifactregistrypb.Repository_DOCKER, repo.GetFormat())
			assert.Equal(t, tt.host+"/"+project, repo.GetRegistryUri())

			got := c.Do("GET", "/v2/"+project+"/app/manifests/v1", nil, nil)
			require.Equal(t, 200, got.Status)
			assert.Equal(t, img.Body, got.Body)
			assert.Equal(t, 1, e.dockerImageCount(t, name))
		})
	}
}
