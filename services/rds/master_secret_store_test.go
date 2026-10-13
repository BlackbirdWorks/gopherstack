package rds_test

import (
	"io/fs"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

type fakeSecretsStore struct {
	secrets map[string]string
	mu      sync.Mutex
	failNew bool
}

func newFakeSecretsStore() *fakeSecretsStore { return &fakeSecretsStore{secrets: map[string]string{}} }

func (f *fakeSecretsStore) CreateManagedSecret(region, name, _, body string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failNew {
		return "", fs.ErrPermission
	}

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

func (f *fakeSecretsStore) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.secrets)
}

func TestMasterSecretStore(t *testing.T) {
	t.Parallel()

	managed := rds.MasterSecretRequest{ManageMasterUserPassword: true, ManageSet: true}

	tests := []struct {
		run  func(t *testing.T, b *rds.InMemoryBackend, f *fakeSecretsStore)
		name string
	}{
		{name: "instance_create_and_delete", run: func(t *testing.T, b *rds.InMemoryBackend, f *fakeSecretsStore) {
			t.Helper()

			inst, err := b.CreateDBInstance(
				"ms-i1", "mysql", "db.t3.micro", "db", "admin", "", 20,
				rds.DBInstanceOptions{MasterSecretRequest: managed},
			)
			require.NoError(t, err)
			require.Equal(t, 1, f.count())
			assert.Contains(t, f.secrets[inst.MasterUserSecretARN], `"username":"admin"`)
			assert.Contains(t, f.secrets[inst.MasterUserSecretARN], `"password":"`)

			_, err = b.DeleteDBInstance("ms-i1")
			require.NoError(t, err)
			assert.Zero(t, f.count())
		}},
		{name: "instance_stop_managing", run: func(t *testing.T, b *rds.InMemoryBackend, f *fakeSecretsStore) {
			t.Helper()

			_, err := b.CreateDBInstance(
				"ms-i2", "mysql", "db.t3.micro", "db", "admin", "", 20,
				rds.DBInstanceOptions{MasterSecretRequest: managed},
			)
			require.NoError(t, err)
			require.Equal(t, 1, f.count())

			_, err = b.ModifyDBInstance("ms-i2", "", 0, rds.DBInstanceOptions{
				MasterUserPassword: "newpassword1",
				ManageSet:          true,
			})
			require.NoError(t, err)
			assert.Zero(t, f.count())
		}},
		{name: "instance_rotate", run: func(t *testing.T, b *rds.InMemoryBackend, f *fakeSecretsStore) {
			t.Helper()

			inst, err := b.CreateDBInstance(
				"ms-i4", "mysql", "db.t3.micro", "db", "admin", "", 20,
				rds.DBInstanceOptions{MasterSecretRequest: managed},
			)
			require.NoError(t, err)

			before := f.secrets[inst.MasterUserSecretARN]

			_, err = b.ModifyDBInstance("ms-i4", "", 0, rds.DBInstanceOptions{
				RotateMasterUserPassword: true,
			})
			require.NoError(t, err)
			assert.NotEqual(t, before, f.secrets[inst.MasterUserSecretARN])
			assert.Contains(t, f.secrets[inst.MasterUserSecretARN], `"username":"admin"`)
		}},
		{name: "rotate_unmanaged", run: func(t *testing.T, b *rds.InMemoryBackend, _ *fakeSecretsStore) {
			t.Helper()

			seedInstance(t, b, "ms-i5")

			_, err := b.ModifyDBInstance("ms-i5", "", 0, rds.DBInstanceOptions{
				RotateMasterUserPassword: true,
			})
			require.ErrorIs(t, err, rds.ErrInvalidParameterCombination)
		}},
		{name: "cluster_create_and_delete", run: func(t *testing.T, b *rds.InMemoryBackend, f *fakeSecretsStore) {
			t.Helper()

			c, err := b.CreateDBCluster(
				"ms-c1", "aurora-postgresql", "admin", "", "", 0, nil,
				rds.DBClusterOptions{MasterSecretRequest: managed},
			)
			require.NoError(t, err)
			require.Equal(t, 1, f.count())
			assert.Contains(t, f.secrets[c.MasterUserSecretARN], `"username":"admin"`)

			_, err = b.DeleteDBCluster("ms-c1")
			require.NoError(t, err)
			assert.Zero(t, f.count())
		}},
		{name: "create_fails_when_store_fails", run: func(t *testing.T, b *rds.InMemoryBackend, f *fakeSecretsStore) {
			t.Helper()

			f.failNew = true
			_, err := b.CreateDBInstance(
				"ms-i3", "mysql", "db.t3.micro", "db", "admin", "", 20,
				rds.DBInstanceOptions{MasterSecretRequest: managed},
			)
			require.Error(t, err)
			assert.Zero(t, f.count())
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			f := newFakeSecretsStore()
			b.SetSecretsStore(f)
			tt.run(t, b, f)
		})
	}
}
