package azurearm_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerinstance/armcontainerinstance/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

func TestACIAzureFileSyncStaysInVolume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		plant func(t *testing.T, dir, outside string)
		share func(s *aciStack)
		name  string
	}{
		{
			name: "symlink to a host file is not exported",
			plant: func(t *testing.T, dir, outside string) {
				t.Helper()
				require.NoError(t, os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(dir, "link")))
			},
		},
		{
			name: "symlinked directory is not walked",
			plant: func(t *testing.T, dir, outside string) {
				t.Helper()
				require.NoError(t, os.Symlink(outside, filepath.Join(dir, "dlink")))
			},
		},
		{
			name: "share file never writes through a container symlink",
			plant: func(t *testing.T, dir, outside string) {
				t.Helper()
				require.NoError(t, os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(dir, "link")))
			},
			share: func(s *aciStack) { s.shares.set("link", []byte("overwritten")) },
		},
		{
			name: "share file never writes through a container directory symlink",
			plant: func(t *testing.T, dir, outside string) {
				t.Helper()
				require.NoError(t, os.Symlink(outside, filepath.Join(dir, "dlink")))
			},
			share: func(s *aciStack) { s.shares.set("dlink/secret.txt", []byte("overwritten")) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			outside := t.TempDir()
			secret := filepath.Join(outside, "secret.txt")
			require.NoError(t, os.WriteFile(secret, []byte("host secret"), 0o600))

			s := newACIStack(t)
			s.shares.trees["acct/share1"] = azurearm.ACIShareTree{Files: map[string][]byte{}}

			dirCh := make(chan string, 1)
			s.rt.onStart = func(spec container.Spec) {
				dir, _, _ := splitMount(spec.Mounts[0])
				dirCh <- dir
			}

			turn := make(chan struct{})
			s.engine.SetProbeHooks(func(ctx context.Context, _ time.Duration) bool {
				select {
				case <-turn:
					return true
				case <-ctx.Done():
					return false
				}
			}, nil)

			_, err := s.create(t, "rg", "confine", aciOpts{
				policy:  armcontainerinstance.ContainerGroupRestartPolicyAlways,
				volumes: []*armcontainerinstance.Volume{azureFileVolume(false)},
				mounts:  []*armcontainerinstance.VolumeMount{{Name: new("files"), MountPath: new("/data")}},
			})
			require.NoError(t, err)

			dir := <-dirCh
			tt.plant(t, dir, outside)

			if tt.share != nil {
				tt.share(s)
			}

			for range 2 {
				select {
				case turn <- struct{}{}:
				case <-time.After(aciSyncTimeout):
					require.Fail(t, "the sync loop never asked for a turn")
				}
			}

			got, err := os.ReadFile(secret)
			require.NoError(t, err)
			assert.Equal(t, "host secret", string(got), "the host file is untouched")

			for name, body := range s.shares.contents("acct/share1") {
				assert.NotEqual(t, "host secret", body, "share file %s must not carry host content", name)
			}
		})
	}
}

func TestACIAzureFileFillStaysInVolume(t *testing.T) {
	t.Parallel()

	probe := "gopherstack-aci-escape-probe.txt"
	escaped := filepath.Join(os.TempDir(), probe) //nolint:usetesting // volume dirs are created under os.TempDir

	t.Cleanup(func() { _ = os.Remove(escaped) })

	s := newACIStack(t)
	s.shares.trees["acct/share1"] = azurearm.ACIShareTree{Files: map[string][]byte{"../" + probe: []byte("x")}}

	_, err := s.create(t, "rg", "fill", aciOpts{
		policy:  armcontainerinstance.ContainerGroupRestartPolicyAlways,
		volumes: []*armcontainerinstance.Volume{azureFileVolume(false)},
		mounts:  []*armcontainerinstance.VolumeMount{{Name: new("files"), MountPath: new("/data")}},
	})
	require.Error(t, err)

	_, statErr := os.Stat(escaped)
	assert.True(t, os.IsNotExist(statErr), "a share path with .. must not write outside the volume")
}

func TestReadACITreeBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		maxFiles int
		maxBytes int64
		wantErr  bool
	}{
		{name: "within bounds", maxFiles: 2, maxBytes: 10},
		{name: "too many files", maxFiles: 1, maxBytes: 10, wantErr: true},
		{name: "too many bytes", maxFiles: 2, maxBytes: 5, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, "a"), []byte("1234"), 0o600))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "b"), []byte("5678"), 0o600))

			tree, err := azurearm.ReadACITreeForTest(dir, tt.maxFiles, tt.maxBytes)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, tree.Files, 2)
		})
	}
}

func TestWriteACIDirConfined(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	dir := filepath.Join(parent, "vol")
	require.NoError(t, os.Mkdir(dir, 0o700))

	err := azurearm.WriteACIDirForTest(
		dir,
		azurearm.ACIShareTree{Files: map[string][]byte{"../escape.txt": []byte("x")}},
	)
	require.Error(t, err)

	_, statErr := os.Stat(filepath.Join(parent, "escape.txt"))
	assert.True(t, os.IsNotExist(statErr))
}

func TestACISyncSharesSerialized(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	shares := &fakeShares{trees: map[string]azurearm.ACIShareTree{
		"acct/share1": {Files: map[string][]byte{"a.txt": []byte("a")}},
	}}

	azurearm.SyncMountsForTest(shares, dir, "acct", "share1", 8)

	got, err := os.ReadFile(filepath.Join(dir, "a.txt"))
	require.NoError(t, err)
	assert.Equal(t, "a", string(got))
}
