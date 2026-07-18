package fsx_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

func TestFSx_StorageVirtualMachine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		svmName  string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "create SVM returns StorageVirtualMachineId",
			svmName:  "svm1",
			wantCode: http.StatusOK,
		},
		{
			name:     "missing FileSystemId returns 400",
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var body map[string]any
			if !tc.wantErr {
				fsID := createFS(t, h, "ONTAP")
				body = map[string]any{"FileSystemId": fsID, "Name": tc.svmName}
			} else {
				body = map[string]any{"Name": "svm1"}
			}

			rec := doFSxRequest(t, h, "CreateStorageVirtualMachine", body)
			require.Equal(t, tc.wantCode, rec.Code)

			if !tc.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				svm := out["StorageVirtualMachine"].(map[string]any)
				assert.Contains(t, svm["StorageVirtualMachineId"].(string), "svm-")
				assert.Equal(t, "AVAILABLE", svm["Lifecycle"])
				assert.Equal(t, tc.svmName, svm["Name"])
			}
		})
	}
}

func TestFSx_SVMLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("describe/update/delete cycle", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		b := fsx.GetBackend(h)
		fsID := createFS(t, h, "ONTAP")
		svmID := createSVM(t, h, fsID, "svm1")

		assert.Equal(t, 1, fsx.SVMCount(b))

		// describe
		rec := doFSxRequest(t, h, "DescribeStorageVirtualMachines", map[string]any{
			"StorageVirtualMachineIds": []string{svmID},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var dr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dr))
		assert.Len(t, dr["StorageVirtualMachines"].([]any), 1)

		// update
		rec2 := doFSxRequest(t, h, "UpdateStorageVirtualMachine", map[string]any{
			"StorageVirtualMachineId": svmID,
			"Subtype":                 "DP_DESTINATION",
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var ur map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &ur))
		assert.Equal(t, "DP_DESTINATION", ur["StorageVirtualMachine"].(map[string]any)["Subtype"])

		// delete
		rec3 := doFSxRequest(t, h, "DeleteStorageVirtualMachine", map[string]any{
			"StorageVirtualMachineId": svmID,
		})
		require.Equal(t, http.StatusOK, rec3.Code)
		var del map[string]any
		require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &del))
		assert.Equal(t, svmID, del["StorageVirtualMachineId"])
		assert.Equal(t, "DELETING", del["Lifecycle"])
		assert.Equal(t, 0, fsx.SVMCount(b))
	})
}
