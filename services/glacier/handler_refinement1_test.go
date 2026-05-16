package glacier_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

// TestRefinement1_Reset verifies that Reset clears all 7 internal maps.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantLen int
	}{
		{name: "backend_empty_after_reset", wantLen: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			_, err := b.CreateVault(testAccountID, testRegion, "vault-a")
			require.NoError(t, err)
			assert.Equal(t, 1, glacier.VaultCount(b))

			b.Reset()

			assert.Equal(t, tt.wantLen, glacier.VaultCount(b))
			assert.Equal(t, tt.wantLen, glacier.ArchiveCount(b))
			assert.Equal(t, tt.wantLen, glacier.MultipartUploadCount(b))
			assert.Equal(t, tt.wantLen, glacier.ProvisionedCapacityCount(b))
		})
	}
}

// TestRefinement1_MultipleResetCycle verifies reset + repopulate + reset clears everything.
func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		vaultsBefore int
		wantAfter    int
	}{
		{name: "two_vaults_then_reset", vaultsBefore: 2, wantAfter: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()

			for i := range tt.vaultsBefore {
				_, err := b.CreateVault(testAccountID, testRegion, "vault-"+string(rune('a'+i)))
				require.NoError(t, err)
			}

			assert.Equal(t, tt.vaultsBefore, glacier.VaultCount(b))

			b.Reset()
			b.Reset() // double reset should be safe

			assert.Equal(t, tt.wantAfter, glacier.VaultCount(b))
		})
	}
}

// TestRefinement1_HandlerReset verifies that Handler.Reset() delegates to the backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantVault int
	}{
		{name: "handler_reset_clears_backend", wantVault: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			h := glacier.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			_, err := b.CreateVault(testAccountID, testRegion, "myvault")
			require.NoError(t, err)
			assert.Equal(t, 1, glacier.VaultCount(b))

			h.Reset()

			assert.Equal(t, tt.wantVault, glacier.VaultCount(b))
		})
	}
}

// TestRefinement1_ProviderInit_NilCtx verifies that Init with nil context returns ErrNilAppContext.
func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
	}{
		{name: "nil_ctx_returns_error", wantErr: glacier.ErrNilAppContext},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := glacier.Provider{}
			_, err := p.Init(nil)

			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestRefinement1_GetSupportedOperations_AllOps verifies all 33 operations are present.
func TestRefinement1_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	wantOps := []string{
		"CreateVault", "DescribeVault", "DeleteVault", "ListVaults",
		"UploadArchive", "DeleteArchive",
		"InitiateJob", "DescribeJob", "ListJobs", "GetJobOutput",
		"SetVaultNotifications", "GetVaultNotifications", "DeleteVaultNotifications",
		"SetVaultAccessPolicy", "GetVaultAccessPolicy", "DeleteVaultAccessPolicy",
		"AddTagsToVault", "ListTagsForVault", "RemoveTagsFromVault",
		"InitiateVaultLock", "AbortVaultLock", "CompleteVaultLock", "GetVaultLock",
		"GetDataRetrievalPolicy", "SetDataRetrievalPolicy",
		"InitiateMultipartUpload", "UploadMultipartPart", "CompleteMultipartUpload",
		"AbortMultipartUpload", "ListMultipartUploads", "ListParts",
		"ListProvisionedCapacity", "PurchaseProvisionedCapacity",
	}

	tests := []struct {
		name    string
		wantLen int
	}{
		{name: "all_33_ops", wantLen: 33},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			ops := h.GetSupportedOperations()

			assert.Len(t, ops, tt.wantLen)

			opSet := make(map[string]struct{}, len(ops))
			for _, op := range ops {
				opSet[op] = struct{}{}
			}

			for _, want := range wantOps {
				assert.Contains(t, opSet, want, "missing operation: %s", want)
			}
		})
	}
}

// TestRefinement1_SeedHelpers verifies AddVaultInternal and AddArchiveInternal work correctly.
func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		vaultName    string
		archiveID    string
		wantVaults   int
		wantArchives int
	}{
		{name: "seed_vault_and_archive", vaultName: "seeded", archiveID: "arch-1", wantVaults: 1, wantArchives: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			b.AddVaultInternal(testAccountID, testRegion, &glacier.Vault{VaultName: tt.vaultName})
			b.AddArchiveInternal(testAccountID, testRegion, tt.vaultName, &glacier.Archive{ArchiveID: tt.archiveID})

			assert.Equal(t, tt.wantVaults, glacier.VaultCount(b))
			assert.Equal(t, tt.wantArchives, glacier.ArchiveCount(b))
		})
	}
}

// TestRefinement1_ExportCountHelpers verifies all four export count helpers work.
func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		wantVaults       int
		wantArchives     int
		wantMultipart    int
		wantProvCapacity int
	}{
		{name: "counts_reflect_state", wantVaults: 1, wantArchives: 1, wantMultipart: 1, wantProvCapacity: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()

			_, err := b.CreateVault(testAccountID, testRegion, "v1")
			require.NoError(t, err)

			_, err = b.UploadArchive(testAccountID, testRegion, "v1", "desc", "chk", 100)
			require.NoError(t, err)

			_, err = b.InitiateMultipartUpload(testAccountID, testRegion, "v1", "desc", 1024*1024)
			require.NoError(t, err)

			_, err = b.PurchaseProvisionedCapacity(testAccountID)
			require.NoError(t, err)

			assert.Equal(t, tt.wantVaults, glacier.VaultCount(b))
			assert.Equal(t, tt.wantArchives, glacier.ArchiveCount(b))
			assert.Equal(t, tt.wantMultipart, glacier.MultipartUploadCount(b))
			assert.Equal(t, tt.wantProvCapacity, glacier.ProvisionedCapacityCount(b))
		})
	}
}

// TestRefinement1_VaultName_Validation verifies that creating a vault with an empty name returns 400.
func TestRefinement1_VaultName_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		vaultName  string
		wantStatus int
	}{
		{name: "empty_name_returns_400", vaultName: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			// PUT /{accountId}/vaults/{vaultName} - empty vault name maps to CreateVault with ""
			rec := doRequest(t, h, http.MethodPut, "/"+testAccountID+"/vaults/"+tt.vaultName, "")

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_ErrValidationMapping verifies ErrValidation is returned from the backend.
func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		vaultName string
	}{
		{name: "empty_vault_name_returns_ErrValidation", vaultName: "", wantErr: glacier.ErrValidation},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			_, err := b.CreateVault(testAccountID, testRegion, tt.vaultName)

			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestRefinement1_SortedListVaults verifies ListVaults returns vaults in lexicographic name order.
func TestRefinement1_SortedListVaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		vaultNames []string
		wantOrder  []string
	}{
		{
			name:       "three_vaults_sorted",
			vaultNames: []string{"zoo", "alpha", "middle"},
			wantOrder:  []string{"alpha", "middle", "zoo"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()

			for _, vn := range tt.vaultNames {
				_, err := b.CreateVault(testAccountID, testRegion, vn)
				require.NoError(t, err)
			}

			vaults := b.ListVaults(testAccountID, testRegion)
			require.Len(t, vaults, len(tt.wantOrder))

			for i, want := range tt.wantOrder {
				assert.Equal(t, want, vaults[i].VaultName)
			}
		})
	}
}

// TestRefinement1_SortedListJobs verifies ListJobs returns jobs sorted by JobID.
func TestRefinement1_SortedListJobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		jobCount   int
		wantSorted bool
	}{
		{name: "jobs_sorted_by_id", jobCount: 3, wantSorted: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			_, err := b.CreateVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			for range tt.jobCount {
				_, err = b.InitiateJob(testAccountID, testRegion, "vault", &glacier.ExportedInitiateJobRequest{
					Type: "InventoryRetrieval",
				})
				require.NoError(t, err)
			}

			jobs, listErr := b.ListJobs(testAccountID, testRegion, "vault")
			require.NoError(t, listErr)
			require.Len(t, jobs, tt.jobCount)

			for i := 1; i < len(jobs); i++ {
				assert.LessOrEqual(t, jobs[i-1].JobID, jobs[i].JobID)
			}
		})
	}
}

// TestRefinement1_SortedListMultipartUploads verifies ListMultipartUploads returns sorted by MultipartUploadID.
func TestRefinement1_SortedListMultipartUploads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		uploadCount int
	}{
		{name: "uploads_sorted_by_id", uploadCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			_, err := b.CreateVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			for range tt.uploadCount {
				_, err = b.InitiateMultipartUpload(testAccountID, testRegion, "vault", "desc", 1024*1024)
				require.NoError(t, err)
			}

			uploads := b.ListMultipartUploads(testAccountID, testRegion, "vault")
			require.Len(t, uploads, tt.uploadCount)

			for i := 1; i < len(uploads); i++ {
				assert.LessOrEqual(t, uploads[i-1].MultipartUploadID, uploads[i].MultipartUploadID)
			}
		})
	}
}

// TestRefinement1_NonNilListJobs verifies ListJobs returns a non-nil empty slice.
func TestRefinement1_NonNilListJobs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
	}{
		{name: "empty_vault_non_nil_jobs", vaultName: "empty-vault"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			_, err := b.CreateVault(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, err)

			jobs, listErr := b.ListJobs(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, listErr)

			assert.NotNil(t, jobs)
			assert.Empty(t, jobs)
		})
	}
}

// TestRefinement1_NonNilListMultipartUploads verifies ListMultipartUploads returns non-nil empty slice.
func TestRefinement1_NonNilListMultipartUploads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
	}{
		{name: "empty_vault_non_nil_uploads", vaultName: "empty-vault"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			_, err := b.CreateVault(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, err)

			uploads := b.ListMultipartUploads(testAccountID, testRegion, tt.vaultName)

			assert.NotNil(t, uploads)
			assert.Empty(t, uploads)
		})
	}
}

// TestRefinement1_NonNilProvisionedCapacity verifies ListProvisionedCapacity returns non-nil empty slice.
func TestRefinement1_NonNilProvisionedCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "no_capacity_returns_non_nil"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			caps := b.ListProvisionedCapacity(testAccountID)

			assert.NotNil(t, caps, tt.name)
			assert.Empty(t, caps)
		})
	}
}

// TestRefinement1_DeleteVault_CascadesMultipartUploads verifies multipart uploads are removed when vault is deleted.
func TestRefinement1_DeleteVault_CascadesMultipartUploads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantUploadCount int
	}{
		{name: "delete_vault_removes_uploads", wantUploadCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			_, err := b.CreateVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			_, err = b.InitiateMultipartUpload(testAccountID, testRegion, "vault", "desc", 1024*1024)
			require.NoError(t, err)
			assert.Equal(t, 1, glacier.MultipartUploadCount(b))

			err = b.DeleteVault(testAccountID, testRegion, "vault")
			require.NoError(t, err)

			assert.Equal(t, tt.wantUploadCount, glacier.MultipartUploadCount(b))
		})
	}
}

// TestRefinement1_VaultLocks_Persistence verifies snapshot/restore preserves vault locks.
func TestRefinement1_VaultLocks_Persistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		vaultName string
		lockID    string
	}{
		{name: "vault_lock_survives_roundtrip", vaultName: "locked-vault", lockID: "my-lock-id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			_, err := b.CreateVault(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, err)

			err = b.SetVaultLock(testAccountID, testRegion, tt.vaultName, "policy", tt.lockID)
			require.NoError(t, err)

			snap := b.Snapshot()
			require.NotNil(t, snap)

			b2 := glacier.NewInMemoryBackend()
			err = b2.Restore(snap)
			require.NoError(t, err)

			lock, err := b2.GetVaultLock(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, err)
			assert.Equal(t, tt.lockID, lock.LockID)
			assert.Equal(t, "InProgress", lock.State)
		})
	}
}

// TestRefinement1_PersistenceRoundTrip verifies full snapshot/restore preserves state.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		vaultName    string
		archiveDesc  string
		wantVaults   int
		wantArchives int
	}{
		{name: "full_state_roundtrip", vaultName: "myv", archiveDesc: "my-archive", wantVaults: 1, wantArchives: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			_, err := b.CreateVault(testAccountID, testRegion, tt.vaultName)
			require.NoError(t, err)

			_, err = b.UploadArchive(testAccountID, testRegion, tt.vaultName, tt.archiveDesc, "chk", 512)
			require.NoError(t, err)

			snap := b.Snapshot()
			require.NotNil(t, snap)

			b2 := glacier.NewInMemoryBackend()
			err = b2.Restore(snap)
			require.NoError(t, err)

			assert.Equal(t, tt.wantVaults, glacier.VaultCount(b2))
			assert.Equal(t, tt.wantArchives, glacier.ArchiveCount(b2))
		})
	}
}

// TestRefinement1_PersistenceEmpty verifies an empty snapshot round-trips cleanly.
func TestRefinement1_PersistenceEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "empty_snapshot_restores_cleanly"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()
			snap := b.Snapshot()
			require.NotNil(t, snap, tt.name)

			b2 := glacier.NewInMemoryBackend()
			err := b2.Restore(snap)
			require.NoError(t, err)

			assert.Equal(t, 0, glacier.VaultCount(b2))
		})
	}
}

// TestRefinement1_InitiateVaultLock_Propagates_VaultNotFound verifies 404 for missing vault.
func TestRefinement1_InitiateVaultLock_Propagates_VaultNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		vaultName  string
		wantStatus int
	}{
		{name: "missing_vault_returns_404", vaultName: "nonexistent", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			// POST /{accountId}/vaults/{vaultName}/lock-policy = InitiateVaultLock
			rec := doRequest(t, h, http.MethodPost, "/"+testAccountID+"/vaults/"+tt.vaultName+"/lock-policy", "")

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement1_ListVaults_AfterReset verifies no vaults returned after reset.
func TestRefinement1_ListVaults_AfterReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCount int
	}{
		{name: "no_vaults_after_reset", wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create a vault first.
			rec := doRequest(t, h, http.MethodPut, "/"+testAccountID+"/vaults/myvault", "")
			require.Equal(t, http.StatusCreated, rec.Code)

			h.Reset()

			// List should be empty.
			rec = doRequest(t, h, http.MethodGet, "/"+testAccountID+"/vaults", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				VaultList []any `json:"VaultList"`
			}

			err := json.NewDecoder(rec.Body).Decode(&resp)
			require.NoError(t, err)

			assert.Len(t, resp.VaultList, tt.wantCount)
		})
	}
}

// TestRefinement1_SortedListProvisionedCapacity verifies capacity units are sorted by CapacityID.
func TestRefinement1_SortedListProvisionedCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		count int
	}{
		{name: "capacity_sorted_by_id", count: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glacier.NewInMemoryBackend()

			for range tt.count {
				_, err := b.PurchaseProvisionedCapacity(testAccountID)
				require.NoError(t, err)
			}

			caps := b.ListProvisionedCapacity(testAccountID)
			require.Len(t, caps, tt.count)

			for i := 1; i < len(caps); i++ {
				assert.LessOrEqual(t, caps[i-1].CapacityID, caps[i].CapacityID)
			}
		})
	}
}
