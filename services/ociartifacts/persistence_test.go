package ociartifacts_test

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/artifacts"
	"github.com/oracle/oci-go-sdk/v65/genericartifactscontent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/services/ociartifacts"
)

var _ persistence.Persistable = (*ociartifacts.Handler)(nil)

func TestSnapshotRestoreWithBlobs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	src := newEnv(t, envOpts{dataDir: dir, register: true})
	repo := src.createRepo(t, "persist/app", func(d *artifacts.CreateContainerRepositoryDetails) {
		d.IsPublic = new(true)
		d.FreeformTags = map[string]string{"k": "v"}
	})
	img := src.registry(t).PushImage(testNamespace+"/persist/app", "v1")

	images := src.listImages(t, nil)
	require.Len(t, images, 1)

	_, err := src.client.CreateContainerImageSignature(t.Context(), artifacts.CreateContainerImageSignatureRequest{
		CreateContainerImageSignatureDetails: sigDetails(images[0].Id)})
	require.NoError(t, err)

	gen := src.createGenericRepo(t, true)
	_, err = src.upload(t, *gen.Id, "bin/tool", "1", []byte("tool bytes"))
	require.NoError(t, err)

	_, err = src.client.UpdateContainerConfiguration(t.Context(), artifacts.UpdateContainerConfigurationRequest{
		CompartmentId:                  new(testCompartment),
		IsRepositoryCreatedOnFirstPush: new(false),
	})
	require.NoError(t, err)

	snap := src.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)
	src.handler.Shutdown(context.Background())

	dst := newEnv(t, envOpts{dataDir: dir})
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	got, err := dst.client.GetContainerRepository(
		t.Context(),
		artifacts.GetContainerRepositoryRequest{RepositoryId: repo.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, "persist/app", *got.DisplayName)
	assert.True(t, *got.IsPublic)
	assert.Equal(t, map[string]string{"k": "v"}, got.FreeformTags)
	assert.Equal(t, 1, *got.ImageCount)

	restored := dst.listImages(t, nil)
	require.Len(t, restored, 1)
	assert.Equal(t, *images[0].Id, *restored[0].Id)
	assert.Equal(t, "v1", *restored[0].Version)

	pull := dst.registry(t).Do(http.MethodGet, "/v2/"+testNamespace+"/persist/app/manifests/v1", nil, nil)
	require.Equal(t, http.StatusOK, pull.Status)
	assert.Equal(t, img.Body, pull.Body)

	sigs, err := dst.client.ListContainerImageSignatures(
		t.Context(),
		artifacts.ListContainerImageSignaturesRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	assert.Len(t, sigs.Items, 1)

	dl, err := dst.content.GetGenericArtifactContentByPath(
		t.Context(),
		genericartifactscontent.GetGenericArtifactContentByPathRequest{
			RepositoryId: gen.Id, ArtifactPath: new("bin/tool"), Version: new("1")},
	)
	require.NoError(t, err)

	defer dl.Content.Close()

	body, err := io.ReadAll(dl.Content)
	require.NoError(t, err)
	assert.Equal(t, "tool bytes", string(body))

	cfg, err := dst.client.GetContainerConfiguration(
		t.Context(),
		artifacts.GetContainerConfigurationRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	assert.False(t, *cfg.IsRepositoryCreatedOnFirstPush)

	_, err = dst.backend.Auth.Register(testTenancy, testUser, newKeyPair(t).pubPEM)
	require.NoError(t, err)
	assert.NotEmpty(t, dst.backend.Auth.Records())
}

func TestRestoreRestoresSigningKeys(t *testing.T) {
	t.Parallel()

	src := newEnv(t, envOpts{register: true})
	snap := src.handler.Snapshot(t.Context())

	dst := newEnv(t, envOpts{})
	require.NoError(t, dst.handler.Restore(t.Context(), snap))
	assert.Len(t, dst.backend.Auth.Records(), 1)
}

func TestRestoreWithoutDataDirDropsImages(t *testing.T) {
	t.Parallel()

	src := newEnv(t, envOpts{})
	repo := src.createRepo(t, "volatile", nil)
	src.registry(t).PushImage(testNamespace+"/volatile", "v1")
	require.Len(t, src.listImages(t, nil), 1)

	snap := src.handler.Snapshot(t.Context())

	dst := newEnv(t, envOpts{})
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	_, err := dst.client.GetContainerRepository(
		t.Context(),
		artifacts.GetContainerRepositoryRequest{RepositoryId: repo.Id},
	)
	require.NoError(t, err)
	assert.Empty(t, dst.listImages(t, nil))
}

func TestRestoreRejectsBadSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    string
		wantErr bool
		empty   bool
	}{
		{"not json", "{", true, false},
		{"null repo", `{"version":1,"repos":{"a":null}}`, true, false},
		{"null image", `{"version":1,"images":{"a":null}}`, true, false},
		{"null signature", `{"version":1,"signatures":{"a":null}}`, true, false},
		{"null generic repo", `{"version":1,"genericRepos":{"a":null}}`, true, false},
		{"null artifact", `{"version":1,"artifacts":{"a":null}}`, true, false},
		{"other version starts empty", `{"version":99,"repos":{"a":{"id":"a","name":"x"}}}`, false, true},
		{"empty maps", `{"version":1}`, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.createRepo(t, "before", nil)

			err := e.handler.Restore(t.Context(), []byte(tt.data))
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			list, lerr := e.client.ListContainerRepositories(
				t.Context(),
				artifacts.ListContainerRepositoriesRequest{CompartmentId: new(testCompartment)},
			)
			require.NoError(t, lerr)
			assert.Empty(t, list.Items)
		})
	}
}

func TestResetClearsEverything(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	e := newEnv(t, envOpts{dataDir: dir})
	e.createRepo(t, "gone", nil)
	e.registry(t).PushImage(testNamespace+"/gone", "v1")
	e.createGenericRepo(t, false)

	_, err := e.client.UpdateContainerConfiguration(t.Context(), artifacts.UpdateContainerConfigurationRequest{
		CompartmentId:                  new(testCompartment),
		IsRepositoryCreatedOnFirstPush: new(false),
	})
	require.NoError(t, err)

	e.handler.Reset()

	repos, err := e.client.ListContainerRepositories(
		t.Context(),
		artifacts.ListContainerRepositoriesRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	assert.Empty(t, repos.Items)

	gens, err := e.client.ListRepositories(
		t.Context(),
		artifacts.ListRepositoriesRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	assert.Empty(t, gens.Items)

	cfg, err := e.client.GetContainerConfiguration(
		t.Context(),
		artifacts.GetContainerConfigurationRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	assert.True(t, *cfg.IsRepositoryCreatedOnFirstPush)

	// Blobs are gone with the repository: the same name starts empty.
	assert.Equal(t, http.StatusNotFound,
		e.registry(t).Do(http.MethodGet, "/v2/"+testNamespace+"/gone/manifests/v1", nil, nil).Status)

	c := e.registry(t)
	c.PushImage(testNamespace+"/gone", "v2")
	assert.Equal(t, http.StatusOK, c.Do(http.MethodGet, "/v2/"+testNamespace+"/gone/manifests/v2", nil, nil).Status)
	assert.Equal(
		t,
		http.StatusNotFound,
		c.Do(http.MethodGet, "/v2/"+testNamespace+"/gone/manifests/v1", nil, nil).Status,
	)
}
