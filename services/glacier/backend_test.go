package glacier_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

const (
	testAccountID = "000000000000"
	testRegion    = "us-east-1"
)

func TestInMemoryBackend_VaultCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
		wantErr   bool
	}{
		{
			name:      "create_and_describe",
			vaultName: "test-vault",
		},
		{
			name:      "delete_nonexistent",
			vaultName: "does-not-exist",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()

			if tt.wantErr {
				err := bk.DeleteVault(testAccountID, testRegion, tt.vaultName)
				require.Error(t, err)

				return
			}

			v, err := bk.CreateVault(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, err)
			assert.Equal(t, tt.vaultName, v.VaultName)
			assert.NotEmpty(t, v.VaultARN)
			assert.NotEmpty(t, v.CreationDate)

			got, err := bk.DescribeVault(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, err)
			assert.Equal(t, v.VaultName, got.VaultName)
			assert.Equal(t, v.VaultARN, got.VaultARN)

			err = bk.DeleteVault(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, err)

			_, err = bk.DescribeVault(testAccountID, testRegion, tt.vaultName)
			require.Error(t, err)
		})
	}
}

func TestInMemoryBackend_ListVaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		vaultNames []string
		wantCount  int
	}{
		{
			name:       "empty",
			vaultNames: nil,
			wantCount:  0,
		},
		{
			name:       "multiple_vaults",
			vaultNames: []string{"vault-a", "vault-b", "vault-c"},
			wantCount:  3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()

			for _, name := range tt.vaultNames {
				_, err := bk.CreateVault(testAccountID, testRegion, name)
				require.NoError(t, err)
			}

			vaults := bk.ListVaults(testAccountID, testRegion)
			assert.Len(t, vaults, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_ArchiveCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setup       func(*glacier.InMemoryBackend)
		vaultName   string
		description string
		wantErr     bool
	}{
		{
			name: "upload_and_delete",
			setup: func(bk *glacier.InMemoryBackend) {
				_, err := bk.CreateVault(testAccountID, testRegion, "vault")
				if err != nil {
					panic(err)
				}
			},
			vaultName:   "vault",
			description: "test archive",
		},
		{
			name:      "vault_not_found",
			vaultName: "no-such-vault",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(bk)
			}

			a, err := bk.UploadArchive(testAccountID, testRegion, tt.vaultName, tt.description, "checksum", 1024)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, a.ArchiveID)
			assert.Equal(t, tt.description, a.Description)

			v, err := bk.DescribeVault(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, err)
			assert.Equal(t, int64(1), v.NumberOfArchives)
			assert.Equal(t, int64(1024), v.SizeInBytes)

			err = bk.DeleteArchive(testAccountID, testRegion, tt.vaultName, a.ArchiveID)
			require.NoError(t, err)

			v, err = bk.DescribeVault(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, err)
			assert.Equal(t, int64(0), v.NumberOfArchives)
		})
	}
}

func TestInMemoryBackend_JobCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		jobType     string
		archiveID   string
		expectJobID bool
		wantErr     bool
	}{
		{
			name:        "inventory_retrieval",
			jobType:     "InventoryRetrieval",
			expectJobID: true,
		},
		{
			name:        "archive_retrieval",
			jobType:     "ArchiveRetrieval",
			archiveID:   "test-archive-id",
			expectJobID: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			_, err := bk.CreateVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			req := &glacier.ExportedInitiateJobRequest{
				Type:      tt.jobType,
				ArchiveID: tt.archiveID,
			}

			j, err := bk.InitiateJob(testAccountID, testRegion, "vault", req)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, j.JobID)
			assert.Equal(t, tt.jobType, j.Action)
			assert.True(t, j.Completed)

			got, err := bk.DescribeJob(testAccountID, testRegion, "vault", j.JobID)
			require.NoError(t, err)
			assert.Equal(t, j.JobID, got.JobID)

			jobs := bk.ListJobs(testAccountID, testRegion, "vault")
			assert.Len(t, jobs, 1)
		})
	}
}

func TestInMemoryBackend_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addTags    map[string]string
		wantTags   map[string]string
		name       string
		removeTags []string
	}{
		{
			name:     "add_tags",
			addTags:  map[string]string{"env": "test", "team": "infra"},
			wantTags: map[string]string{"env": "test", "team": "infra"},
		},
		{
			name:       "add_and_remove_tags",
			addTags:    map[string]string{"env": "test", "team": "infra"},
			removeTags: []string{"team"},
			wantTags:   map[string]string{"env": "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			_, err := bk.CreateVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			err = bk.AddTagsToVault(testAccountID, testRegion, "vault", tt.addTags)
			require.NoError(t, err)

			if len(tt.removeTags) > 0 {
				err = bk.RemoveTagsFromVault(testAccountID, testRegion, "vault", tt.removeTags)
				require.NoError(t, err)
			}

			tags, err := bk.ListTagsForVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)
			assert.Equal(t, tt.wantTags, tags)
		})
	}
}

func TestInMemoryBackend_Notifications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		snsTopic string
		events   []string
	}{
		{
			name:     "set_and_get",
			snsTopic: "arn:aws:sns:us-east-1:000000000000:test-topic",
			events:   []string{"ArchiveRetrievalCompleted", "InventoryRetrievalCompleted"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			_, err := bk.CreateVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			err = bk.SetVaultNotifications(testAccountID, testRegion, "vault", tt.snsTopic, tt.events)
			require.NoError(t, err)

			snsTopic, events, err := bk.GetVaultNotifications(testAccountID, testRegion, "vault")
			require.NoError(t, err)
			assert.Equal(t, tt.snsTopic, snsTopic)
			assert.Equal(t, tt.events, events)

			err = bk.DeleteVaultNotifications(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			snsTopic, _, err = bk.GetVaultNotifications(testAccountID, testRegion, "vault")
			require.NoError(t, err)
			assert.Empty(t, snsTopic)
		})
	}
}

func TestInMemoryBackend_AccessPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
	}{
		{
			name:   "set_and_get",
			policy: `{"Version":"2012-10-17","Statement":[]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			_, err := bk.CreateVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			err = bk.SetVaultAccessPolicy(testAccountID, testRegion, "vault", tt.policy)
			require.NoError(t, err)

			policy, err := bk.GetVaultAccessPolicy(testAccountID, testRegion, "vault")
			require.NoError(t, err)
			assert.Equal(t, tt.policy, policy)

			err = bk.DeleteVaultAccessPolicy(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			policy, err = bk.GetVaultAccessPolicy(testAccountID, testRegion, "vault")
			require.NoError(t, err)
			assert.Empty(t, policy)
		})
	}
}
