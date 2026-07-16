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

	lh, _ := b.CreateLegalHold("test hold", "test description")
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
				assert.Equal(t, http.StatusOK, cancelRec.Code)
			},
		},
		{
			name: "cancel_not_found",
			ops: func(t *testing.T, h *backup.Handler) {
				t.Helper()
				rec := doREST(t, h, http.MethodDelete, "/legal-holds/nonexistent-id", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
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
