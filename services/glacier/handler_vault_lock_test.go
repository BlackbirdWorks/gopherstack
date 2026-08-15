package glacier_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/glacier"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetVaultLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantState    string
		wantStatus   int
		initiateLock bool
	}{
		{
			name:       "unlocked_vault",
			wantStatus: http.StatusOK,
			wantState:  "Unlocked",
		},
		{
			name:         "locked_vault",
			initiateLock: true,
			wantStatus:   http.StatusOK,
			wantState:    "InProgress",
		},
		{
			name:       "vault_not_found",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vaultName := "lock-vault"

			if tt.wantStatus != http.StatusNotFound {
				rec := doRequest(t, h, http.MethodPut, "/-/vaults/"+vaultName, "")
				require.Equal(t, http.StatusCreated, rec.Code)
			} else {
				vaultName = "nonexistent-vault"
			}

			if tt.initiateLock {
				rec := doRequest(t, h, http.MethodPost, "/-/vaults/"+vaultName+"/lock-policy", `{"Policy":"{}"}`)
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/-/vaults/"+vaultName+"/lock-policy", "")
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantState, resp["State"])
			}
		})
	}
}

// ----------------------------------------
// ListProvisionedCapacity
// ----------------------------------------

// TestInitiateVaultLock_Propagates_VaultNotFound verifies 404 for missing vault.
func TestInitiateVaultLock_Propagates_VaultNotFound(t *testing.T) {
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

func TestAbortVaultLock_ClearsLock(t *testing.T) {
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

			createVault(t, h, "lock-vault")

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

func TestAbortVaultLock_VaultNotFound(t *testing.T) {
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

func TestSetVaultLock_RejectsConflict(t *testing.T) {
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
			createVault(t, h, "conflict-vault")

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

func TestCompleteVaultLock_Validates(t *testing.T) {
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
			createVault(t, h, "complete-vault")

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

func TestCompleteVaultLock_Success(t *testing.T) {
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
			createVault(t, h, "complete2-vault")

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

func TestInitiateVaultLock_ReadsPolicy(t *testing.T) {
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
			createVault(t, h, "policy-vault")

			body := `{"Policy":"` + strings.ReplaceAll(tt.policy, `"`, `\"`) + `"}`
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

func TestVaultLock_ExpiryAllowsReinitiation(t *testing.T) {
	t.Parallel()

	bk := glacier.NewInMemoryBackend()
	h := glacier.NewHandler(bk)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	createVault(t, h, "expire-vault")

	// Initiate a vault lock.
	rec := doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/expire-vault/lock-policy",
		`{"Policy":"{\"Version\":\"2012-10-17\"}"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Backdating the lock expiry simulates the 24-hour window passing.
	glacier.SetVaultLockExpired(bk, testAccountID, testRegion, "expire-vault")

	// GetVaultLock should return Unlocked after expiry.
	rec = doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/expire-vault/lock-policy", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "Unlocked", resp["State"])
}

func TestVaultLock_ExpiredLockAllowsNewInitiation(t *testing.T) {
	t.Parallel()

	bk := glacier.NewInMemoryBackend()
	h := glacier.NewHandler(bk)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	createVault(t, h, "reinit-vault")

	rec := doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/reinit-vault/lock-policy",
		`{"Policy":"{}"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	glacier.SetVaultLockExpired(bk, testAccountID, testRegion, "reinit-vault")

	// Should be able to initiate a new lock after expiry.
	rec = doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/reinit-vault/lock-policy",
		`{"Policy":"{}"}`)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
}

func TestVaultLock_CompleteClearsExpirationDate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "complete-vault")

	rec := doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/complete-vault/lock-policy",
		`{"Policy":"{}"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var lockResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lockResp))
	lockID := lockResp["lockId"].(string)

	rec = doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/complete-vault/lock-policy/"+lockID, "")
	require.Equal(t, http.StatusNoContent, rec.Code)

	rec = doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/complete-vault/lock-policy", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "Locked", getResp["State"])
	// ExpirationDate must be absent after completing a lock per AWS spec.
	_, hasExpiration := getResp["ExpirationDate"]
	assert.False(t, hasExpiration, "ExpirationDate should be absent on a Locked vault")
}

// -------------------------------------------------------------------------
// Issue 14: Tag key validation (charset + aws: prefix)
// -------------------------------------------------------------------------

func TestVaultLock_LockedStateHasNoExpirationDate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "locked-vault")

	// Initiate lock — response should have lockId.
	rec := doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/locked-vault/lock-policy",
		`{"Policy":"{}"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var initResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
	lockID := initResp["lockId"].(string)

	// InProgress state should have ExpirationDate.
	rec = doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/locked-vault/lock-policy", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var inProgressResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &inProgressResp))
	assert.Equal(t, "InProgress", inProgressResp["State"])
	assert.NotEmpty(t, inProgressResp["ExpirationDate"], "InProgress lock should have ExpirationDate")

	// Complete the lock.
	rec = doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/locked-vault/lock-policy/"+lockID, "")
	require.Equal(t, http.StatusNoContent, rec.Code)

	// Locked state should NOT have ExpirationDate.
	rec = doRequest(t, h, http.MethodGet,
		"/"+testAccountID+"/vaults/locked-vault/lock-policy", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var lockedResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lockedResp))
	assert.Equal(t, "Locked", lockedResp["State"])

	exp, hasExp := lockedResp["ExpirationDate"]
	if hasExp {
		assert.Empty(t, exp, "ExpirationDate should be absent or empty after locking")
	}
}

// -------------------------------------------------------------------------
// Additional coverage: ListParts pagination
// -------------------------------------------------------------------------

func TestVaultLock_DoubleInitiateReturnsConflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "conflict-vault")

	// First initiate succeeds.
	rec := doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/conflict-vault/lock-policy",
		`{"Policy":"{}"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Second initiate while InProgress → 409 Conflict.
	rec = doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/conflict-vault/lock-policy",
		`{"Policy":"{}"}`)
	assert.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
}

func TestVaultLock_AbortThenReinitiate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createVault(t, h, "abort-reinit-vault")

	// Initiate.
	rec := doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/abort-reinit-vault/lock-policy",
		`{"Policy":"{}"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Abort.
	rec = doRequest(t, h, http.MethodDelete,
		"/"+testAccountID+"/vaults/abort-reinit-vault/lock-policy", "")
	require.Equal(t, http.StatusNoContent, rec.Code)

	// Re-initiate must succeed.
	rec = doRequest(t, h, http.MethodPost,
		"/"+testAccountID+"/vaults/abort-reinit-vault/lock-policy",
		`{"Policy":"{}"}`)
	assert.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
}

// -------------------------------------------------------------------------
// Additional: ProvisionedCapacity expiry reaping
// -------------------------------------------------------------------------

func TestVaultLock_FullLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		policy string
	}{
		{name: "initiate_complete_verify_locked", policy: `{"Version":"2012-10-17"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "lock-lifecycle-vault")

			// GetVaultLock on unlocked vault.
			rec := doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/lock-lifecycle-vault/lock-policy", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var lockState map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lockState))
			assert.Equal(t, "Unlocked", lockState["State"])

			// Initiate.
			body := `{"Policy":"` + strings.ReplaceAll(tt.policy, `"`, `\"`) + `"}`
			rec = doRequestWithHeaders(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/lock-lifecycle-vault/lock-policy", body, nil)
			require.Equal(t, http.StatusCreated, rec.Code)
			var initResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &initResp))
			lockID := initResp["lockId"]
			require.NotEmpty(t, lockID)

			// GetVaultLock shows InProgress.
			rec = doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/lock-lifecycle-vault/lock-policy", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lockState))
			assert.Equal(t, "InProgress", lockState["State"])
			assert.NotEmpty(t, lockState["ExpirationDate"])

			// Complete with correct lockID.
			rec = doRequestWithHeaders(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/lock-lifecycle-vault/lock-policy/"+lockID, "", nil)
			require.Equal(t, http.StatusNoContent, rec.Code)

			// GetVaultLock shows Locked.
			rec = doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/lock-lifecycle-vault/lock-policy", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lockState))
			assert.Equal(t, "Locked", lockState["State"], tt.name)
		})
	}
}

func TestVaultLock_WrongLockIDFails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "wrong_lock_id_returns_400"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "lock-wrong-id-vault")

			rec := doRequestWithHeaders(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/lock-wrong-id-vault/lock-policy", `{"Policy":"{}"}`, nil)
			require.Equal(t, http.StatusCreated, rec.Code)

			rec = doRequestWithHeaders(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/lock-wrong-id-vault/lock-policy/WRONGID", "", nil)
			assert.Equal(t, http.StatusBadRequest, rec.Code, tt.name)
		})
	}
}

func TestVaultLock_AbortRemovesLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "abort_then_state_is_unlocked"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "lock-abort-vault")

			rec := doRequestWithHeaders(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/lock-abort-vault/lock-policy", `{"Policy":"{}"}`, nil)
			require.Equal(t, http.StatusCreated, rec.Code)

			rec = doRequestWithHeaders(t, h, http.MethodDelete,
				"/"+testAccountID+"/vaults/lock-abort-vault/lock-policy", "", nil)
			require.Equal(t, http.StatusNoContent, rec.Code)

			rec = doRequestWithHeaders(t, h, http.MethodGet,
				"/"+testAccountID+"/vaults/lock-abort-vault/lock-policy", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)
			var lockState map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lockState))
			assert.Equal(t, "Unlocked", lockState["State"], tt.name)
		})
	}
}

func TestVaultLock_DoubleInitiateConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "second_initiate_returns_409"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			createVault(t, h, "double-lock-vault")

			rec1 := doRequestWithHeaders(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/double-lock-vault/lock-policy", `{"Policy":"{}"}`, nil)
			require.Equal(t, http.StatusCreated, rec1.Code)

			rec2 := doRequestWithHeaders(t, h, http.MethodPost,
				"/"+testAccountID+"/vaults/double-lock-vault/lock-policy", `{"Policy":"{}"}`, nil)
			assert.Equal(t, http.StatusConflict, rec2.Code, tt.name)
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 11. DataRetrievalPolicy full roundtrip
// ─────────────────────────────────────────────────────────────────────────────
