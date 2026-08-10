package redshift_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerless_TableRestore_FromSnapshotAndRecoveryPoint(t *testing.T) {
	t.Parallel()

	h, rp := seedNamespaceAndWorkgroup(t, "tr-ns", "tr-wg")
	recoveryPointID, _ := rp["recoveryPointId"].(string)
	require.NotEmpty(t, recoveryPointID)

	rec := doServerlessOp(t, h, "CreateSnapshot", map[string]any{
		"snapshotName": "tr-snap", "namespaceName": "tr-ns",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "RestoreTableFromSnapshot", map[string]any{
		"namespaceName":      "tr-ns",
		"workgroupName":      "tr-wg",
		"newTableName":       "new_tbl",
		"snapshotName":       "tr-snap",
		"sourceDatabaseName": "srcdb",
		"sourceTableName":    "srctbl",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var fromSnapResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fromSnapResp))
	status1, _ := fromSnapResp["tableRestoreStatus"].(map[string]any)
	require.NotNil(t, status1)
	assert.Equal(t, "SUCCEEDED", status1["status"])
	assert.Equal(t, "tr-snap", status1["snapshotName"])
	assert.Equal(t, "new_tbl", status1["newTableName"])
	assert.NotEmpty(t, status1["tableRestoreRequestId"])
	assert.NotEmpty(t, status1["requestTime"])
	requestID1, _ := status1["tableRestoreRequestId"].(string)

	rec = doServerlessOp(t, h, "RestoreTableFromRecoveryPoint", map[string]any{
		"namespaceName":      "tr-ns",
		"workgroupName":      "tr-wg",
		"newTableName":       "new_tbl2",
		"recoveryPointId":    recoveryPointID,
		"sourceDatabaseName": "srcdb",
		"sourceTableName":    "srctbl2",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var fromRPResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fromRPResp))
	status2, _ := fromRPResp["tableRestoreStatus"].(map[string]any)
	require.NotNil(t, status2)
	assert.Equal(t, recoveryPointID, status2["recoveryPointId"])
	requestID2, _ := status2["tableRestoreRequestId"].(string)
	require.NotEmpty(t, requestID2)
	assert.NotEqual(t, requestID1, requestID2)

	rec = doServerlessOp(t, h, "GetTableRestoreStatus", map[string]any{"tableRestoreRequestId": requestID1})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&getResp))
	got, _ := getResp["tableRestoreStatus"].(map[string]any)
	require.NotNil(t, got)
	assert.Equal(t, requestID1, got["tableRestoreRequestId"])

	rec = doServerlessOp(t, h, "ListTableRestoreStatus", map[string]any{"namespaceName": "tr-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&listResp))
	list, _ := listResp["tableRestoreStatuses"].([]any)
	assert.Len(t, list, 2)
}

func TestServerless_TableRestore_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantType   string
		wantStatus int
	}{
		{
			name:       "restore from snapshot missing fields",
			op:         "RestoreTableFromSnapshot",
			body:       map[string]any{"namespaceName": "x"},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
		{
			name: "restore from unknown snapshot",
			op:   "RestoreTableFromSnapshot",
			body: map[string]any{
				"namespaceName": "x", "workgroupName": "y", "newTableName": "z",
				"snapshotName": "no-such-snap", "sourceDatabaseName": "d", "sourceTableName": "t",
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
		{
			name:       "restore from recovery point missing fields",
			op:         "RestoreTableFromRecoveryPoint",
			body:       map[string]any{"namespaceName": "x"},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
		{
			name: "restore from unknown recovery point",
			op:   "RestoreTableFromRecoveryPoint",
			body: map[string]any{
				"namespaceName": "x", "workgroupName": "y", "newTableName": "z",
				"recoveryPointId": "no-such-rp", "sourceDatabaseName": "d", "sourceTableName": "t",
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
		{
			name:       "get missing id",
			op:         "GetTableRestoreStatus",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
		{
			name:       "get unknown id",
			op:         "GetTableRestoreStatus",
			body:       map[string]any{"tableRestoreRequestId": "nope"},
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
