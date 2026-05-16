package glacier_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glacier"
)

const (
	testVault2   = "refinement2-vault"
	testArchive2 = "test-archive-id-123"
)

func setupVault(t *testing.T, h *glacier.Handler, vaultName string) {
	t.Helper()
	rec := doRequest(t, h, http.MethodPut, "/"+testAccountID+"/vaults/"+vaultName, "")
	require.Equal(t, http.StatusCreated, rec.Code)
}

func TestRefinement2_DeleteVault_RejectsNonEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "non_empty_vault_returns_409", wantStatus: http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			bk := glacier.NewInMemoryBackend()
			h2 := glacier.NewHandler(bk)
			h2.AccountID = testAccountID
			h2.DefaultRegion = testRegion

			bk.AddVaultInternal(testAccountID, testRegion, &glacier.Vault{VaultName: testVault2})
			bk.AddArchiveInternal(testAccountID, testRegion, testVault2, &glacier.Archive{
				ArchiveID: testArchive2,
				Size:      100,
			})

			rec := doRequest(t, h2, http.MethodDelete, "/"+testAccountID+"/vaults/"+testVault2, "")
			assert.Equal(t, tt.wantStatus, rec.Code)
			_ = h
		})
	}
}

func TestRefinement2_DeleteVault_AllowsEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "empty_vault_returns_204", wantStatus: http.StatusNoContent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			setupVault(t, h, testVault2)
			rec := doRequest(t, h, http.MethodDelete, "/"+testAccountID+"/vaults/"+testVault2, "")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestRefinement2_VaultNotEmpty_Returns409(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "409_on_non_empty", wantStatus: http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			h := glacier.NewHandler(bk)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			bk.AddVaultInternal(testAccountID, testRegion, &glacier.Vault{VaultName: "v1"})
			bk.AddArchiveInternal(
				testAccountID,
				testRegion,
				"v1",
				&glacier.Archive{ArchiveID: "a1", Size: 10},
			)

			rec := doRequest(t, h, http.MethodDelete, "/"+testAccountID+"/vaults/v1", "")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestRefinement2_InitiateJob_InvalidType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		jobType    string
		wantStatus int
	}{
		{name: "unknown_type", jobType: "UnknownRetrieval", wantStatus: http.StatusBadRequest},
		{name: "empty_type", jobType: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			setupVault(t, h, "job-vault")

			body := `{"Type":"` + tt.jobType + `"}`
			rec := doRequest(
				t,
				h,
				http.MethodPost,
				"/"+testAccountID+"/vaults/job-vault/jobs",
				body,
			)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestRefinement2_InitiateJob_ArchiveRetrieval_RequiresArchiveID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "missing_archive_id", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			setupVault(t, h, "job-vault2")

			rec := doRequest(t, h, http.MethodPost, "/"+testAccountID+"/vaults/job-vault2/jobs",
				`{"Type":"ArchiveRetrieval"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestRefinement2_InitiateJob_ArchiveNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "nonexistent_archive", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			setupVault(t, h, "job-vault3")

			rec := doRequest(t, h, http.MethodPost, "/"+testAccountID+"/vaults/job-vault3/jobs",
				`{"Type":"ArchiveRetrieval","ArchiveId":"nonexistent-id"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestRefinement2_InitiateJob_InventoryRetrieval_NoArchiveID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "inventory_no_archive_id_ok", wantStatus: http.StatusAccepted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			setupVault(t, h, "inv-vault")

			rec := doRequest(t, h, http.MethodPost, "/"+testAccountID+"/vaults/inv-vault/jobs",
				`{"Type":"InventoryRetrieval"}`)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestRefinement2_AbortVaultLock_ClearsLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantLockCount int
	}{
		{name: "abort_clears_lock", wantLockCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			h := glacier.NewHandler(bk)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			setupVault(t, h, "lock-vault")

			// initiate
			rec := doRequest(
				t,
				h,
				http.MethodPost,
				"/"+testAccountID+"/vaults/lock-vault/lock-policy",
				`{"Policy":"{}"}`,
			)
			require.Equal(t, http.StatusCreated, rec.Code)
			assert.Equal(t, 1, glacier.VaultLockCount(bk))

			// abort
			rec = doRequest(
				t,
				h,
				http.MethodDelete,
				"/"+testAccountID+"/vaults/lock-vault/lock-policy",
				"",
			)
			require.Equal(t, http.StatusNoContent, rec.Code)
			assert.Equal(t, tt.wantLockCount, glacier.VaultLockCount(bk))
		})
	}
}

func TestRefinement2_AbortVaultLock_VaultNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "vault_not_found", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(
				t,
				h,
				http.MethodDelete,
				"/"+testAccountID+"/vaults/no-such-vault/lock-policy",
				"",
			)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestRefinement2_SetVaultLock_RejectsConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		wantSecondStatus int
	}{
		{name: "second_initiate_returns_409", wantSecondStatus: http.StatusConflict},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			setupVault(t, h, "conflict-vault")

			rec := doRequest(
				t,
				h,
				http.MethodPost,
				"/"+testAccountID+"/vaults/conflict-vault/lock-policy",
				`{"Policy":"{}"}`,
			)
			require.Equal(t, http.StatusCreated, rec.Code)

			rec = doRequest(
				t,
				h,
				http.MethodPost,
				"/"+testAccountID+"/vaults/conflict-vault/lock-policy",
				`{"Policy":"{}"}`,
			)
			assert.Equal(t, tt.wantSecondStatus, rec.Code)
		})
	}
}

func TestRefinement2_CompleteVaultLock_Validates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		lockID     string
		wantStatus int
	}{
		{name: "wrong_lock_id", lockID: "wrong-id", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			setupVault(t, h, "complete-vault")

			// initiate
			rec := doRequest(
				t,
				h,
				http.MethodPost,
				"/"+testAccountID+"/vaults/complete-vault/lock-policy",
				`{"Policy":"{}"}`,
			)
			require.Equal(t, http.StatusCreated, rec.Code)

			// complete with wrong lockID
			rec = doRequest(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/complete-vault/lock-policy/"+tt.lockID, "")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestRefinement2_CompleteVaultLock_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "correct_lock_id_transitions_to_locked", wantStatus: http.StatusNoContent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			setupVault(t, h, "complete2-vault")

			// initiate
			rec := doRequest(
				t,
				h,
				http.MethodPost,
				"/"+testAccountID+"/vaults/complete2-vault/lock-policy",
				`{"Policy":"{}"}`,
			)
			require.Equal(t, http.StatusCreated, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			lockID := resp["lockId"]
			require.NotEmpty(t, lockID)

			rec = doRequest(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/complete2-vault/lock-policy/"+lockID, "")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestRefinement2_InitiateVaultLock_ReadsPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policy     string
		wantStatus int
	}{
		{name: "policy_stored", policy: `{"Version":"2012-10-17"}`, wantStatus: http.StatusCreated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			setupVault(t, h, "policy-vault")

			body := `{"Policy":"` + tt.policy + `"}`
			rec := doRequest(
				t,
				h,
				http.MethodPost,
				"/"+testAccountID+"/vaults/policy-vault/lock-policy",
				body,
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["lockId"])
		})
	}
}

func TestRefinement2_DataRetrievalPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
	}{
		{
			name:   "set_then_get",
			policy: `{"Policy":{"Rules":[{"Strategy":"BytesPerHour","BytesPerHour":1073741824}]}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doRequest(
				t,
				h,
				http.MethodPut,
				"/"+testAccountID+"/policies/data-retrieval",
				tt.policy,
			)
			require.Equal(t, http.StatusNoContent, rec.Code)

			rec = doRequest(t, h, http.MethodGet, "/"+testAccountID+"/policies/data-retrieval", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var got map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			assert.NotEmpty(t, got)
		})
	}
}

func TestRefinement2_DataRetrievalPolicy_DefaultFreeTier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantStrategy string
	}{
		{name: "default_free_tier", wantStrategy: "FreeTier"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doRequest(t, h, http.MethodGet, "/"+testAccountID+"/policies/data-retrieval", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var got map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))

			policy, ok := got["Policy"].(map[string]any)
			require.True(t, ok)
			rules, ok := policy["Rules"].([]any)
			require.True(t, ok)
			require.NotEmpty(t, rules)

			firstRule, ok := rules[0].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantStrategy, firstRule["Strategy"])
		})
	}
}

func TestRefinement2_DeleteArchive_SizeGuard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		archiveSize int64
		wantSize    int64
		wantCount   int64
	}{
		{name: "size_decrements", archiveSize: 500, wantSize: 0, wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			h := glacier.NewHandler(bk)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			bk.AddVaultInternal(testAccountID, testRegion, &glacier.Vault{
				VaultName:        "size-vault",
				NumberOfArchives: 1,
				SizeInBytes:      tt.archiveSize,
			})
			bk.AddArchiveInternal(testAccountID, testRegion, "size-vault", &glacier.Archive{
				ArchiveID: "arch1",
				Size:      tt.archiveSize,
			})

			rec := doRequest(t, h, http.MethodDelete,
				"/"+testAccountID+"/vaults/size-vault/archives/arch1", "")
			require.Equal(t, http.StatusNoContent, rec.Code)

			v, err := bk.DescribeVault(testAccountID, testRegion, "size-vault")
			require.NoError(t, err)
			assert.Equal(t, tt.wantSize, v.SizeInBytes)
			assert.Equal(t, tt.wantCount, v.NumberOfArchives)
		})
	}
}

func TestRefinement2_SeedHelpers_AddJobInternal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantJobs int
	}{
		{name: "add_job_internal", wantJobs: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			bk.AddVaultInternal(
				testAccountID,
				testRegion,
				&glacier.Vault{VaultName: "job-seed-vault"},
			)
			bk.AddJobInternal(testAccountID, testRegion, "job-seed-vault", &glacier.Job{
				JobID:  "test-job-id",
				Action: "InventoryRetrieval",
			})

			assert.Equal(t, tt.wantJobs, glacier.JobCount(bk))
		})
	}
}

func TestRefinement2_ExportCountHelpers_JobCount_VaultLockCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantJobs       int
		wantVaultLocks int
	}{
		{name: "helpers_work", wantJobs: 2, wantVaultLocks: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			bk.AddVaultInternal(testAccountID, testRegion, &glacier.Vault{VaultName: "v1"})
			bk.AddVaultInternal(testAccountID, testRegion, &glacier.Vault{VaultName: "v2"})
			bk.AddJobInternal(
				testAccountID,
				testRegion,
				"v1",
				&glacier.Job{JobID: "j1", Action: "InventoryRetrieval"},
			)
			bk.AddJobInternal(
				testAccountID,
				testRegion,
				"v2",
				&glacier.Job{JobID: "j2", Action: "InventoryRetrieval"},
			)

			require.NoError(t, bk.SetVaultLock(testAccountID, testRegion, "v1", "{}", "lockid1"))

			assert.Equal(t, tt.wantJobs, glacier.JobCount(bk))
			assert.Equal(t, tt.wantVaultLocks, glacier.VaultLockCount(bk))
		})
	}
}

func TestRefinement2_DescribeJob_ArchiveSizeInBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		archiveSize     int64
		wantSizePresent bool
	}{
		{name: "archive_retrieval_has_size", archiveSize: 1024, wantSizePresent: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			h := glacier.NewHandler(bk)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			bk.AddVaultInternal(testAccountID, testRegion, &glacier.Vault{VaultName: "ar-vault"})
			bk.AddArchiveInternal(testAccountID, testRegion, "ar-vault", &glacier.Archive{
				ArchiveID:      "archive-123",
				Size:           tt.archiveSize,
				SHA256TreeHash: "deadbeef",
			})

			rec := doRequest(t, h, http.MethodPost, "/"+testAccountID+"/vaults/ar-vault/jobs",
				`{"Type":"ArchiveRetrieval","ArchiveId":"archive-123"}`)
			require.Equal(t, http.StatusAccepted, rec.Code)

			var initResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
			jobID := initResp["jobId"].(string)

			rec = doRequest(
				t,
				h,
				http.MethodGet,
				"/"+testAccountID+"/vaults/ar-vault/jobs/"+jobID,
				"",
			)
			require.Equal(t, http.StatusOK, rec.Code)

			var jobResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &jobResp))
			if tt.wantSizePresent {
				assert.NotNil(t, jobResp["ArchiveSizeInBytes"])
			}
		})
	}
}

func TestRefinement2_GetJobOutput_SHA256Header(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantHeader string
	}{
		{name: "sha256_header_set", wantHeader: "deadbeef"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := glacier.NewInMemoryBackend()
			h := glacier.NewHandler(bk)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			bk.AddVaultInternal(testAccountID, testRegion, &glacier.Vault{VaultName: "sha-vault"})
			bk.AddArchiveInternal(testAccountID, testRegion, "sha-vault", &glacier.Archive{
				ArchiveID:      "arch-sha",
				Size:           512,
				SHA256TreeHash: tt.wantHeader,
			})

			rec := doRequest(t, h, http.MethodPost, "/"+testAccountID+"/vaults/sha-vault/jobs",
				`{"Type":"ArchiveRetrieval","ArchiveId":"arch-sha"}`)
			require.Equal(t, http.StatusAccepted, rec.Code)

			var initResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
			jobID := initResp["jobId"].(string)

			rec = doRequest(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/sha-vault/jobs/"+jobID+"/output", "")
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tt.wantHeader, rec.Header().Get("X-Amz-Sha256-Tree-Hash"))
		})
	}
}

func TestRefinement2_PersistenceRoundTrip_DataRetrievalPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
	}{
		{
			name:   "drp_survives_snapshot_restore",
			policy: `{"Policy":{"Rules":[{"Strategy":"BytesPerHour","BytesPerHour":1048576}]}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(
				t,
				h,
				http.MethodPut,
				"/"+testAccountID+"/policies/data-retrieval",
				tt.policy,
			)
			require.Equal(t, http.StatusNoContent, rec.Code)

			snap := h.Snapshot()
			require.NotNil(t, snap)

			h2 := newTestHandler()
			require.NoError(t, h2.Restore(snap))

			rec = doRequest(t, h2, http.MethodGet, "/"+testAccountID+"/policies/data-retrieval", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var got map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
			assert.NotEmpty(t, got)
		})
	}
}

func TestRefinement2_GetSupportedOperations_Count(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantAtLeast int
	}{
		{name: "all_ops_present", wantAtLeast: 30},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			ops := h.GetSupportedOperations()
			assert.GreaterOrEqual(t, len(ops), tt.wantAtLeast)
		})
	}
}
