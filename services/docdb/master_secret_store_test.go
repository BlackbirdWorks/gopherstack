package docdb_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

type fakeSecretsStore struct {
	secrets map[string]string
	mu      sync.Mutex
}

func (f *fakeSecretsStore) CreateManagedSecret(region, name, _, body string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	arn := "arn:aws:secretsmanager:" + region + ":000000000000:secret:" + name + "-abc123"
	f.secrets[arn] = body

	return arn, nil
}

func (f *fakeSecretsStore) PutManagedSecretValue(_, arn, body string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.secrets[arn] = body

	return nil
}

func (f *fakeSecretsStore) DeleteManagedSecret(_, arn string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.secrets, arn)

	return nil
}

func TestMasterSecretStore(t *testing.T) {
	t.Parallel()

	managed := docdb.MasterSecretRequest{ManageMasterUserPassword: true, ManageSet: true}

	tests := []struct {
		name         string
		stopManaging bool
		rotate       bool
	}{
		{name: "delete_cluster"},
		{name: "stop_managing", stopManaging: true},
		{name: "rotate", rotate: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			f := &fakeSecretsStore{secrets: map[string]string{}}
			b.SetSecretsStore(f)

			c, err := b.CreateDBCluster(
				t.Context(), "ms-c1", "docdb", "", "admin", "", "", "", "", 0, false, false, 0, "", "", nil, nil,
				&docdb.CreateDBClusterOptions{MasterSecretRequest: managed},
			)
			require.NoError(t, err)
			require.Len(t, f.secrets, 1)
			assert.Contains(t, f.secrets[c.MasterUserSecretARN], `"username":"admin"`)

			if tt.rotate {
				before := f.secrets[c.MasterUserSecretARN]
				_, err = b.ModifyDBCluster(t.Context(), "ms-c1", "", nil, 0, "", "", &docdb.ModifyDBClusterOptions{
					RotateMasterUserPassword: true,
				})
				require.NoError(t, err)
				assert.NotEqual(t, before, f.secrets[c.MasterUserSecretARN])

				return
			}

			if tt.stopManaging {
				_, err = b.ModifyDBCluster(t.Context(), "ms-c1", "", nil, 0, "", "", &docdb.ModifyDBClusterOptions{
					MasterUserPassword: "newpassword1", ManageSet: true,
				})
				require.NoError(t, err)
				assert.Empty(t, f.secrets)

				return
			}

			_, err = b.DeleteDBCluster(t.Context(), "ms-c1", &docdb.DeleteDBClusterOptions{SkipFinalSnapshot: true})
			require.NoError(t, err)
			assert.Empty(t, f.secrets)
		})
	}
}
