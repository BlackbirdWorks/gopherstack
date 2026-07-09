package glacier_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

func TestGlacier_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *glacier.InMemoryBackend)
		verify func(t *testing.T, b *glacier.InMemoryBackend)
		name   string
	}{
		{
			name:  "empty_backend",
			setup: func(_ *glacier.InMemoryBackend) {},
			verify: func(t *testing.T, b *glacier.InMemoryBackend) {
				t.Helper()

				vaults := b.ListVaults("123", "us-east-1")
				assert.Empty(t, vaults)
			},
		},
		{
			name: "vault_with_archive_preserved",
			setup: func(b *glacier.InMemoryBackend) {
				_, err := b.CreateVault("123", "us-east-1", "my-vault")
				if err != nil {
					return
				}

				_, _ = b.UploadArchive("123", "us-east-1", "my-vault", "desc", "hash", 1024, []byte("data"))
			},
			verify: func(t *testing.T, b *glacier.InMemoryBackend) {
				t.Helper()

				vaults := b.ListVaults("123", "us-east-1")
				require.Len(t, vaults, 1)
				assert.Equal(t, "my-vault", vaults[0].VaultName)
				assert.Equal(t, int64(1), vaults[0].NumberOfArchives)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			tt.setup(b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := glacier.NewInMemoryBackend()
			err := b2.Restore(t.Context(), snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}

// TestGlacier_PersistenceFullStateRoundTrip exercises every piece of backend
// state -- vaults (with inline archives), jobs, multipart uploads and their
// parts, vault locks, provisioned capacity, the account data-retrieval
// policy, tags, notifications, and the access policy -- through a single
// Snapshot/Restore round trip. This is the Phase 3.3 pkgs/store conversion's
// full-state coverage: every store.Table (vaults, jobs, multipartUploads,
// vaultLocks) and every raw map left behind (multipartParts,
// provisionedCapacity, dataRetrievalPolicies) is represented.
func TestGlacier_PersistenceFullStateRoundTrip(t *testing.T) {
	t.Parallel()

	const (
		accountID = "111122223333"
		region    = "us-west-2"
		vaultName = "full-state-vault"
	)

	b := glacier.NewInMemoryBackend()
	glacier.SetRetrievalDelay(b, 0)

	_, err := b.CreateVault(accountID, region, vaultName)
	require.NoError(t, err)

	archive, err := b.UploadArchive(accountID, region, vaultName, "desc", "archive-hash", 2048, []byte("archive-bytes"))
	require.NoError(t, err)

	job, err := b.InitiateJob(accountID, region, vaultName, &glacier.ExportedInitiateJobRequest{
		Type:      "archive-retrieval",
		ArchiveID: archive.ArchiveID,
	})
	require.NoError(t, err)

	upload, err := b.InitiateMultipartUpload(accountID, region, vaultName, "mpu-desc", 1<<20)
	require.NoError(t, err)
	require.NoError(t,
		b.UploadMultipartPart(accountID, region, vaultName, upload.MultipartUploadID, "0-1048575/*", "part-hash"),
	)

	require.NoError(t, b.SetVaultLock(accountID, region, vaultName, `{"Version":"2012-10-17"}`, "lock-id-1"))

	require.NoError(t, b.AddTagsToVault(accountID, region, vaultName, map[string]string{"env": "prod"}))
	require.NoError(t, b.SetVaultNotifications(accountID, region, vaultName, "arn:aws:sns:us-west-2:111122223333:topic",
		[]string{"ArchiveRetrievalCompleted"}))
	require.NoError(t, b.SetVaultAccessPolicy(accountID, region, vaultName, `{"Version":"2012-10-17"}`))

	b.SetDataRetrievalPolicy(accountID, []byte(`{"Rules":[]}`))

	_, err = b.PurchaseProvisionedCapacity(accountID)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := glacier.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Vault + inline archive.
	v, err := b2.DescribeVault(accountID, region, vaultName)
	require.NoError(t, err)
	assert.Equal(t, int64(1), v.NumberOfArchives)
	assert.Equal(t, int64(2048), v.SizeInBytes)

	archives, err := b2.ListArchives(accountID, region, vaultName)
	require.NoError(t, err)
	require.Len(t, archives, 1)
	assert.Equal(t, archive.ArchiveID, archives[0].ArchiveID)

	// Job (store.Table, byVault index).
	gotJob, err := b2.DescribeJob(accountID, region, vaultName, job.JobID)
	require.NoError(t, err)
	assert.Equal(t, job.JobID, gotJob.JobID)

	jobs, err := b2.ListJobs(accountID, region, vaultName)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	// Multipart upload + parts (upload is a store.Table; parts stay a raw map).
	uploads := b2.ListMultipartUploads(accountID, region, vaultName)
	require.Len(t, uploads, 1)
	assert.Equal(t, upload.MultipartUploadID, uploads[0].MultipartUploadID)

	parts, err := b2.ListParts(accountID, region, vaultName, upload.MultipartUploadID)
	require.NoError(t, err)
	require.Len(t, parts.Parts, 1)
	assert.Equal(t, "part-hash", parts.Parts[0].SHA256TreeHash)

	// Vault lock (store.Table).
	lock, err := b2.GetVaultLock(accountID, region, vaultName)
	require.NoError(t, err)
	assert.Equal(t, "lock-id-1", lock.LockID)
	assert.Equal(t, "InProgress", lock.State)

	// Tags, notifications, access policy (fields on the restored Vault).
	tags, err := b2.ListTagsForVault(accountID, region, vaultName)
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"])

	snsTopic, events, err := b2.GetVaultNotifications(accountID, region, vaultName)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:sns:us-west-2:111122223333:topic", snsTopic)
	assert.Equal(t, []string{"ArchiveRetrievalCompleted"}, events)

	policy, err := b2.GetVaultAccessPolicy(accountID, region, vaultName)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, policy)

	// Raw maps: data retrieval policy and provisioned capacity.
	assert.JSONEq(t, `{"Rules":[]}`, b2.GetDataRetrievalPolicy(accountID))
	assert.Len(t, b2.ListProvisionedCapacity(accountID), 1)

	// GetArchiveData is a pre-existing gap this conversion preserves
	// byte-for-byte: raw archive bytes were never part of any persisted map
	// before this conversion (only Archive metadata was), so they do not
	// survive a Snapshot/Restore round trip either.
	_, ok := b2.GetArchiveData(archive.ArchiveID)
	assert.False(t, ok)
}

// TestGlacier_PersistenceVersionGuard verifies that Restore discards an
// incompatible (missing/old) snapshot version by resetting to empty rather
// than erroring, and returns an error for genuinely malformed JSON.
func TestGlacier_PersistenceVersionGuard(t *testing.T) {
	t.Parallel()

	t.Run("incompatible_version_resets_to_empty", func(t *testing.T) {
		t.Parallel()

		b := glacier.NewInMemoryBackend()
		_, err := b.CreateVault("123", "us-east-1", "pre-existing")
		require.NoError(t, err)

		// A version-0 (pre-conversion-shaped) snapshot must be treated as
		// incompatible, not partially decoded.
		err = b.Restore(t.Context(), []byte(`{"version":0,"tables":{}}`))
		require.NoError(t, err)

		assert.Empty(t, b.ListVaults("123", "us-east-1"))
	})

	t.Run("malformed_json_errors", func(t *testing.T) {
		t.Parallel()

		b := glacier.NewInMemoryBackend()
		err := b.Restore(t.Context(), []byte(`{not valid json`))
		require.Error(t, err)
	})
}
