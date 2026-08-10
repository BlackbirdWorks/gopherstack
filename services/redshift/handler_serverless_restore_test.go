package redshift_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerless_RestoreFromSnapshot(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()
	doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "rfs-ns"})
	doServerlessOp(t, h, "CreateWorkgroup", map[string]any{"workgroupName": "rfs-wg", "namespaceName": "rfs-ns"})
	rec := doServerlessOp(t, h, "CreateSnapshot", map[string]any{"snapshotName": "rfs-snap", "namespaceName": "rfs-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "RestoreFromSnapshot", map[string]any{
		"namespaceName": "rfs-ns",
		"workgroupName": "rfs-wg",
		"snapshotName":  "rfs-snap",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "rfs-snap", resp["snapshotName"])
	ns, _ := resp["namespace"].(map[string]any)
	require.NotNil(t, ns)
	assert.Equal(t, "rfs-ns", ns["namespaceName"])
}

func TestServerless_RestoreFromSnapshot_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name:       "missing fields",
			body:       map[string]any{"namespaceName": "x"},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
		{
			name: "missing snapshot identifier",
			body: map[string]any{
				"namespaceName": "x", "workgroupName": "y",
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
		{
			name: "unknown namespace",
			body: map[string]any{
				"namespaceName": "no-such-ns", "workgroupName": "no-such-wg", "snapshotName": "no-such-snap",
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newServerlessHandler()

			rec := doServerlessOp(t, h, "RestoreFromSnapshot", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var errResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
			assert.Equal(t, tt.wantType, errResp["__type"])
		})
	}
}

func TestServerless_RestoreFromSnapshot_WorkgroupNamespaceMismatch(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()
	doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "rfs-mismatch-a"})
	doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "rfs-mismatch-b"})
	doServerlessOp(t, h, "CreateWorkgroup", map[string]any{
		"workgroupName": "rfs-mismatch-wg", "namespaceName": "rfs-mismatch-a",
	})
	rec := doServerlessOp(t, h, "CreateSnapshot", map[string]any{
		"snapshotName": "rfs-mismatch-snap", "namespaceName": "rfs-mismatch-a",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "RestoreFromSnapshot", map[string]any{
		"namespaceName": "rfs-mismatch-b",
		"workgroupName": "rfs-mismatch-wg",
		"snapshotName":  "rfs-mismatch-snap",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.Equal(t, "ValidationException", errResp["__type"])
}

func TestServerless_ConvertRecoveryPointToSnapshot(t *testing.T) {
	t.Parallel()

	h, rp := seedNamespaceAndWorkgroup(t, "crp-ns", "crp-wg")
	recoveryPointID, _ := rp["recoveryPointId"].(string)
	require.NotEmpty(t, recoveryPointID)

	rec := doServerlessOp(t, h, "ConvertRecoveryPointToSnapshot", map[string]any{
		"recoveryPointId": recoveryPointID,
		"snapshotName":    "crp-snap",
		"retentionPeriod": 3,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	snap, _ := resp["snapshot"].(map[string]any)
	require.NotNil(t, snap)
	assert.Equal(t, "crp-snap", snap["snapshotName"])
	assert.Equal(t, "crp-ns", snap["namespaceName"])
	assert.InEpsilon(t, float64(3), snap["snapshotRetentionPeriod"], 0)

	rec = doServerlessOp(t, h, "GetSnapshot", map[string]any{"snapshotName": "crp-snap"})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestServerless_ConvertRecoveryPointToSnapshot_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name:       "missing fields",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
		{
			name:       "unknown recovery point",
			body:       map[string]any{"recoveryPointId": "nope", "snapshotName": "s1"},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newServerlessHandler()

			rec := doServerlessOp(t, h, "ConvertRecoveryPointToSnapshot", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var errResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
			assert.Equal(t, tt.wantType, errResp["__type"])
		})
	}
}

func TestServerless_ConvertRecoveryPointToSnapshot_DuplicateName(t *testing.T) {
	t.Parallel()

	h, rp := seedNamespaceAndWorkgroup(t, "crp-dup-ns", "crp-dup-wg")
	recoveryPointID, _ := rp["recoveryPointId"].(string)

	rec := doServerlessOp(t, h, "CreateSnapshot", map[string]any{
		"snapshotName": "crp-dup-snap", "namespaceName": "crp-dup-ns",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "ConvertRecoveryPointToSnapshot", map[string]any{
		"recoveryPointId": recoveryPointID,
		"snapshotName":    "crp-dup-snap",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.Equal(t, "ConflictException", errResp["__type"])
}
