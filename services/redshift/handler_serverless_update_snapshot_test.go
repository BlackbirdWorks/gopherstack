package redshift_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerless_UpdateSnapshot_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name:       "missing snapshot name",
			body:       map[string]any{"retentionPeriod": 30},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
		{
			name:       "unknown snapshot",
			body:       map[string]any{"snapshotName": "no-such-snapshot"},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newServerlessHandler()

			rec := doServerlessOp(t, h, "UpdateSnapshot", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var errResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
			assert.Equal(t, tt.wantType, errResp["__type"])
		})
	}
}

// TestServerless_UpdateSnapshot_OmittedRetentionPeriodUnchanged proves
// UpdateSnapshotInput.RetentionPeriod is optional -- an absent value leaves
// the stored retention period unchanged rather than zeroing it.
func TestServerless_UpdateSnapshot_OmittedRetentionPeriodUnchanged(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	rec := doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "upd-snap-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "CreateSnapshot", map[string]any{
		"snapshotName":    "upd-snap",
		"namespaceName":   "upd-snap-ns",
		"retentionPeriod": 14,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "UpdateSnapshot", map[string]any{"snapshotName": "upd-snap"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	snap, _ := resp["snapshot"].(map[string]any)
	require.NotNil(t, snap)
	assert.InEpsilon(t, float64(14), snap["snapshotRetentionPeriod"], 0)
}
