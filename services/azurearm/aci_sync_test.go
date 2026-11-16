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

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const aciSyncTimeout = 30 * time.Second

func TestACIAzureFileLiveSync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seed      map[string][]byte
		container func(t *testing.T, dir string)
		share     func(s *aciStack)
		wantShare map[string]string
		wantDir   map[string]string
		name      string
		readOnly  bool
	}{
		{
			name: "container write reaches the share",
			container: func(t *testing.T, dir string) {
				t.Helper()
				writeFile(t, dir, "out.txt", "from container")
			},
			wantShare: map[string]string{"out.txt": "from container"},
			wantDir:   map[string]string{"out.txt": "from container"},
		},
		{
			name: "share write reaches the container",
			share: func(s *aciStack) {
				s.shares.set("new.txt", []byte("from share"))
			},
			wantShare: map[string]string{"new.txt": "from share"},
			wantDir:   map[string]string{"new.txt": "from share"},
		},
		{
			name: "share delete removes the file in the container", seed: map[string][]byte{"seed.txt": []byte("x")},
			share:     func(s *aciStack) { s.shares.remove("acct/share1", "seed.txt") },
			wantShare: map[string]string{}, wantDir: map[string]string{},
		},
		{
			name: "container delete removes the file in the share", seed: map[string][]byte{"seed.txt": []byte("x")},
			container: func(t *testing.T, dir string) {
				t.Helper()
				require.NoError(t, os.Remove(filepath.Join(dir, "seed.txt")))
			},
			wantShare: map[string]string{}, wantDir: map[string]string{},
		},
		{
			name: "container wins a conflict", seed: map[string][]byte{"f.txt": []byte("base")},
			container: func(t *testing.T, dir string) {
				t.Helper()
				writeFile(t, dir, "f.txt", "container")
			},
			share:     func(s *aciStack) { s.shares.set("f.txt", []byte("share")) },
			wantShare: map[string]string{"f.txt": "container"}, wantDir: map[string]string{"f.txt": "container"},
		},
		{
			name: "read only volume keeps share content", readOnly: true,
			seed: map[string][]byte{"f.txt": []byte("base")},
			container: func(t *testing.T, dir string) {
				t.Helper()
				writeFile(t, dir, "f.txt", "tampered")
			},
			share:     func(s *aciStack) { s.shares.set("g.txt", []byte("added")) },
			wantShare: map[string]string{"f.txt": "base", "g.txt": "added"},
			wantDir:   map[string]string{"f.txt": "base", "g.txt": "added"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newACIStack(t)
			s.shares.trees["acct/share1"] = azurearm.ACIShareTree{Files: tt.seed}

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

			_, err := s.create(t, "rg", "sync", aciOpts{
				policy:  armcontainerinstance.ContainerGroupRestartPolicyAlways,
				volumes: []*armcontainerinstance.Volume{azureFileVolume(tt.readOnly)},
				mounts:  []*armcontainerinstance.VolumeMount{{Name: new("files"), MountPath: new("/data")}},
			})
			require.NoError(t, err)

			dir := <-dirCh

			if tt.container != nil {
				tt.container(t, dir)
			}

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

			assert.Equal(t, tt.wantShare, s.shares.contents("acct/share1"))
			assert.Equal(t, tt.wantDir, readDir(t, dir))
		})
	}
}

func TestACIAzureFileKeyChecked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{name: "valid key", key: azureauth.DefaultAccountKey},
		{name: "wrong key", key: "d3Jvbmc=", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newACIStack(t)
			s.shares.trees["acct/share1"] = azurearm.ACIShareTree{Files: map[string][]byte{}}

			vol := azureFileVolume(false)
			vol.AzureFile.StorageAccountKey = &tt.key

			_, err := s.create(t, "rg", "keyed", aciOpts{
				policy:  armcontainerinstance.ContainerGroupRestartPolicyAlways,
				volumes: []*armcontainerinstance.Volume{vol},
				mounts:  []*armcontainerinstance.VolumeMount{{Name: new("files"), MountPath: new("/data")}},
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.Empty(t, s.rt.started(), "no container starts with a bad key")

				return
			}

			require.NoError(t, err)
			assert.Len(t, s.rt.started(), 1)
		})
	}
}

func azureFileVolume(readOnly bool) *armcontainerinstance.Volume {
	return &armcontainerinstance.Volume{Name: new("files"), AzureFile: &armcontainerinstance.AzureFileVolume{
		ShareName: new("share1"), StorageAccountName: new("acct"),
		StorageAccountKey: new(azureauth.DefaultAccountKey), ReadOnly: &readOnly,
	}}
}

func writeFile(t *testing.T, dir, name, body string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600))
}

func readDir(t *testing.T, dir string) map[string]string {
	t.Helper()

	out := map[string]string{}

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	for _, e := range entries {
		b, rerr := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, rerr)

		out[e.Name()] = string(b)
	}

	return out
}
