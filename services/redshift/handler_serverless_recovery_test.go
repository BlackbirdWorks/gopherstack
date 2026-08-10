package redshift_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// seedNamespaceAndWorkgroup creates a namespace + workgroup and returns the
// single recovery point CreateWorkgroup generates automatically. Real AWS
// creates recovery points on a 30-minute schedule, never through an API
// call, so a workgroup must exist before any test has one to reference.
func seedNamespaceAndWorkgroup(
	t *testing.T, ns, wg string,
) (*redshift.ServerlessHandler, map[string]any) {
	t.Helper()

	h := newServerlessHandler()
	rec := doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": ns})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "CreateWorkgroup", map[string]any{"workgroupName": wg, "namespaceName": ns})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "ListRecoveryPoints", map[string]any{"namespaceName": ns})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&listResp))
	points, _ := listResp["recoveryPoints"].([]any)
	require.Len(t, points, 1, "CreateWorkgroup must generate exactly one recovery point")

	rp, _ := points[0].(map[string]any)
	require.NotNil(t, rp)

	return h, rp
}

func TestServerless_RecoveryPoint_CRUD(t *testing.T) {
	t.Parallel()

	h, rp := seedNamespaceAndWorkgroup(t, "rp-ns", "rp-wg")

	recoveryPointID, _ := rp["recoveryPointId"].(string)
	require.NotEmpty(t, recoveryPointID)
	assert.Equal(t, "rp-ns", rp["namespaceName"])
	assert.Equal(t, "rp-wg", rp["workgroupName"])
	assert.NotEmpty(t, rp["namespaceArn"])
	assert.NotEmpty(t, rp["recoveryPointCreateTime"])

	rec := doServerlessOp(t, h, "GetRecoveryPoint", map[string]any{"recoveryPointId": recoveryPointID})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&getResp))
	got, _ := getResp["recoveryPoint"].(map[string]any)
	require.NotNil(t, got)
	assert.Equal(t, recoveryPointID, got["recoveryPointId"])

	rec = doServerlessOp(t, h, "RestoreFromRecoveryPoint", map[string]any{
		"namespaceName":   "rp-ns",
		"workgroupName":   "rp-wg",
		"recoveryPointId": recoveryPointID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var restoreResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&restoreResp))
	assert.Equal(t, recoveryPointID, restoreResp["recoveryPointId"])
	ns, _ := restoreResp["namespace"].(map[string]any)
	require.NotNil(t, ns)
	assert.Equal(t, "rp-ns", ns["namespaceName"])
}

func TestServerless_RecoveryPoint_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantType   string
		wantStatus int
	}{
		{
			name:       "get missing id",
			op:         "GetRecoveryPoint",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
		{
			name:       "get unknown id",
			op:         "GetRecoveryPoint",
			body:       map[string]any{"recoveryPointId": "nope"},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
		{
			name:       "restore missing fields",
			op:         "RestoreFromRecoveryPoint",
			body:       map[string]any{"namespaceName": "x"},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
		{
			name: "restore unknown namespace",
			op:   "RestoreFromRecoveryPoint",
			body: map[string]any{
				"namespaceName": "no-such-ns", "workgroupName": "no-such-wg", "recoveryPointId": "nope",
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newServerlessHandler()

			rec := doServerlessOp(t, h, tt.op, tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var errResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
			assert.Equal(t, tt.wantType, errResp["__type"])
		})
	}
}

func TestServerless_RestoreFromRecoveryPoint_WorkgroupNamespaceMismatch(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	rec := doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "mismatch-ns-a"})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "mismatch-ns-b"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "CreateWorkgroup", map[string]any{
		"workgroupName": "mismatch-wg-a", "namespaceName": "mismatch-ns-a",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "ListRecoveryPoints", map[string]any{"namespaceName": "mismatch-ns-a"})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&listResp))
	points, _ := listResp["recoveryPoints"].([]any)
	require.Len(t, points, 1)
	rp, _ := points[0].(map[string]any)
	recoveryPointID, _ := rp["recoveryPointId"].(string)

	rec = doServerlessOp(t, h, "RestoreFromRecoveryPoint", map[string]any{
		"namespaceName":   "mismatch-ns-b",
		"workgroupName":   "mismatch-wg-a",
		"recoveryPointId": recoveryPointID,
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.Equal(t, "ValidationException", errResp["__type"])
}
