package backup_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestLegalHold(t *testing.T) {
	t.Parallel()
	b := backup.NewInMemoryBackend("000000000000", "us-east-1")

	lh, _ := b.CreateLegalHold("test hold", "test description", nil)
	assert.NotEmpty(t, lh.LegalHoldID)

	found, err := b.GetLegalHold(lh.LegalHoldID)
	require.NoError(t, err)
	assert.Equal(t, "test hold", found.Title)

	holds := b.ListLegalHolds()
	assert.NotEmpty(t, holds)

	rps := b.ListRecoveryPointsByLegalHold(lh.LegalHoldID)
	assert.Empty(t, rps)
}

// TestCancelLegalHold exercises creating and cancelling legal holds.
func TestCancelLegalHold(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops        func(t *testing.T, h *backup.Handler)
		name       string
		wantStatus int
	}{
		{
			name: "create_and_cancel",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				createRec := doREST(t, h, http.MethodPost, "/legal-holds", map[string]any{
					"Title":       "litigation hold",
					"Description": "hold for lawsuit XYZ",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				resp := parseResp(t, createRec)
				legalHoldID, ok := resp["LegalHoldId"].(string)
				require.True(t, ok)
				assert.NotEmpty(t, legalHoldID)
				assert.Equal(t, "ACTIVE", resp["Status"])

				cancelRec := doREST(t, h, http.MethodDelete, "/legal-holds/"+legalHoldID, nil)
				assert.Equal(t, http.StatusCreated, cancelRec.Code)
			},
		},
		{
			name: "cancel_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodDelete, "/legal-holds/nonexistent-id", nil)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "create_missing_title",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/legal-holds", map[string]any{
					"Description": "some description",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "create_missing_description",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodPost, "/legal-holds", map[string]any{
					"Title": "some title",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestBackupHandler()
			tt.ops(t, h)
		})
	}
}

// TestCreateLegalHold_RecoveryPointSelection exercises the real wire shape
// for RecoveryPointSelection and confirms ListRecoveryPointsByLegalHold
// actually filters by it end to end (through the HTTP layer, not just the
// backend method).
func TestCreateLegalHold_RecoveryPointSelection(t *testing.T) {
	t.Parallel()

	h, b := newHandlerAndBackend()
	mustVault(t, b, "sel-vault-a")
	mustVault(t, b, "sel-vault-b")
	mustRP(t, b, "sel-vault-a", "arn:aws:backup:::rp/a1", "arn:aws:ec2:::instance/i-a1", "EC2")
	mustRP(t, b, "sel-vault-b", "arn:aws:backup:::rp/b1", "arn:aws:ec2:::instance/i-b1", "EC2")

	createRec := doREST(t, h, http.MethodPost, "/legal-holds", map[string]any{
		"Title":       "vault-scoped hold",
		"Description": "covers sel-vault-a only",
		"RecoveryPointSelection": map[string]any{
			"VaultNames": []string{"sel-vault-a"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	created := parseResp(t, createRec)
	legalHoldID, ok := created["LegalHoldId"].(string)
	require.True(t, ok)

	getRec := doREST(t, h, http.MethodGet, "/legal-holds/"+legalHoldID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)
	got := parseResp(t, getRec)
	sel, ok := got["RecoveryPointSelection"].(map[string]any)
	require.True(t, ok)
	vaultNames, ok := sel["VaultNames"].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"sel-vault-a"}, vaultNames)

	listRec := doREST(t, h, http.MethodGet, "/legal-holds/"+legalHoldID+"/recovery-points", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	listResp := parseResp(t, listRec)
	items, ok := listResp["RecoveryPoints"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)
	entry, ok := items[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "sel-vault-a", entry["BackupVaultName"])
}
