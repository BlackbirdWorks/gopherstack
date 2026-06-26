package guardduty_test

// Parity batch-B: fixes for GetFindingsStatistics key format, UpdateFindingsFeedback
// stub, and ArchiveFindings/UnarchiveFindings missing updatedAt updates.

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

// TestParity_GetFindingsStatistics_SeverityKeys verifies that countBySeverity
// uses the actual finding severity float as string key (e.g. "5.0") rather than
// the former incorrect label strings "Low"/"Medium"/"High".
func TestParity_GetFindingsStatistics_SeverityKeys(t *testing.T) {
	t.Parallel()

	b := guardduty.NewInMemoryBackend("111111111111", "us-east-1")
	det, err := b.CreateDetector(true, "ALL", nil, nil)
	require.NoError(t, err)
	detID := det.DetectorID

	// CreateSampleFindings produces findings with severity 5.0 by default.
	require.NoError(t, b.CreateSampleFindings(detID, nil))

	stats, err := b.GetFindingsStatistics(detID)
	require.NoError(t, err)

	fs, ok := stats["findingStatistics"].(map[string]any)
	require.True(t, ok, "findingStatistics must be a map")

	cbs, ok := fs["countBySeverity"].(map[string]int)
	require.True(t, ok, "countBySeverity must be map[string]int")

	// Key must be numeric severity string, not a label.
	assert.Contains(t, cbs, "5.0", "expected severity key '5.0'")
	assert.NotContains(t, cbs, "Low", "must not use label keys")
	assert.NotContains(t, cbs, "Medium", "must not use label keys")
	assert.NotContains(t, cbs, "High", "must not use label keys")
	assert.Equal(t, 1, cbs["5.0"])
}

// TestParity_GetFindingsStatistics_Empty verifies an empty detector returns
// an empty countBySeverity map, not one pre-populated with zero-count labels.
func TestParity_GetFindingsStatistics_Empty(t *testing.T) {
	t.Parallel()

	b := guardduty.NewInMemoryBackend("111111111111", "us-east-1")
	det, err := b.CreateDetector(true, "ALL", nil, nil)
	require.NoError(t, err)

	stats, err := b.GetFindingsStatistics(det.DetectorID)
	require.NoError(t, err)

	fs := stats["findingStatistics"].(map[string]any)
	cbs := fs["countBySeverity"].(map[string]int)
	assert.Empty(t, cbs, "no findings → empty countBySeverity")
}

// TestParity_UpdateFindingsFeedback verifies feedback is stored on the finding
// and updatedAt is refreshed — previously this was a complete stub.
func TestParity_UpdateFindingsFeedback(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		feedback string
	}{
		{"useful", "USEFUL"},
		{"not_useful", "NOT_USEFUL"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := guardduty.NewInMemoryBackend("111111111111", "us-east-1")
			det, err := b.CreateDetector(true, "ALL", nil, nil)
			require.NoError(t, err)
			detID := det.DetectorID

			require.NoError(t, b.CreateSampleFindings(detID, nil))

			ids, err := b.ListFindings(detID)
			require.NoError(t, err)
			require.NotEmpty(t, ids)

			before := time.Now().Add(-time.Second)
			require.NoError(t, b.UpdateFindingsFeedback(detID, ids, tc.feedback))

			findings, err := b.GetFindings(detID, ids)
			require.NoError(t, err)
			require.Len(t, findings, len(ids))

			for _, f := range findings {
				assert.Equal(t, tc.feedback, f.Service.UserFeedback,
					"userFeedback must be stored on finding")

				updatedAt, parseErr := time.Parse(time.RFC3339, f.UpdatedAt)
				require.NoError(t, parseErr, "updatedAt must be RFC3339")
				assert.True(t, updatedAt.After(before),
					"updatedAt must be refreshed after feedback")
			}
		})
	}
}

// TestParity_ArchiveFindings_UpdatesTimestamp verifies that ArchiveFindings
// refreshes the finding's updatedAt — previously it was not updated.
func TestParity_ArchiveFindings_UpdatesTimestamp(t *testing.T) {
	t.Parallel()

	b := guardduty.NewInMemoryBackend("111111111111", "us-east-1")
	det, err := b.CreateDetector(true, "ALL", nil, nil)
	require.NoError(t, err)
	detID := det.DetectorID

	require.NoError(t, b.CreateSampleFindings(detID, nil))

	ids, err := b.ListFindings(detID)
	require.NoError(t, err)
	require.NotEmpty(t, ids)

	before := time.Now().Add(-time.Second)

	require.NoError(t, b.ArchiveFindings(detID, ids))

	findings1, err := b.GetFindings(detID, ids)
	require.NoError(t, err)

	for _, f := range findings1 {
		assert.True(t, f.Service.Archived, "finding must be archived")

		updatedAt, parseErr := time.Parse(time.RFC3339, f.UpdatedAt)
		require.NoError(t, parseErr)
		assert.True(t, updatedAt.After(before),
			"updatedAt must be refreshed on archive")
	}
}

// TestParity_UnarchiveFindings_UpdatesTimestamp verifies that UnarchiveFindings
// refreshes the finding's updatedAt — previously it was not updated.
func TestParity_UnarchiveFindings_UpdatesTimestamp(t *testing.T) {
	t.Parallel()

	b := guardduty.NewInMemoryBackend("111111111111", "us-east-1")
	det, err := b.CreateDetector(true, "ALL", nil, nil)
	require.NoError(t, err)
	detID := det.DetectorID

	require.NoError(t, b.CreateSampleFindings(detID, nil))

	ids, err := b.ListFindings(detID)
	require.NoError(t, err)
	require.NotEmpty(t, ids)

	// Archive first.
	require.NoError(t, b.ArchiveFindings(detID, ids))

	before := time.Now().Add(-time.Second)
	require.NoError(t, b.UnarchiveFindings(detID, ids))

	findings, err := b.GetFindings(detID, ids)
	require.NoError(t, err)

	for _, f := range findings {
		assert.False(t, f.Service.Archived, "finding must be unarchived")

		updatedAt, parseErr := time.Parse(time.RFC3339, f.UpdatedAt)
		require.NoError(t, parseErr)
		assert.True(t, updatedAt.After(before),
			"updatedAt must be refreshed on unarchive")
	}
}

// TestParity_GetFindingsStatistics_Handler verifies the HTTP layer returns
// findingStatistics with numeric severity keys.
func TestParity_GetFindingsStatistics_Handler(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	detID := auditCreateDetector(t, h)

	// Create sample findings via HTTP.
	rec := auditDo(t, h, http.MethodPost, "/detector/"+detID+"/findings/create", map[string]any{
		"findingTypes": []string{"Recon:IAMUser/TorIPCaller"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = auditDo(t, h, http.MethodPost, "/detector/"+detID+"/findings/statistics", map[string]any{
		"findingStatisticTypes": []string{"COUNT_BY_SEVERITY"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	fs, ok := resp["findingStatistics"].(map[string]any)
	require.True(t, ok)

	cbs, ok := fs["countBySeverity"].(map[string]any)
	require.True(t, ok)

	// Must have a numeric severity key, not "Low"/"Medium"/"High".
	assert.NotContains(t, cbs, "Low")
	assert.NotContains(t, cbs, "High")
	assert.NotContains(t, cbs, "Medium")
}
