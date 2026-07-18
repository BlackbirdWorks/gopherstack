package fsx_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

func TestFSx_DataRepositoryAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fsPath   string
		repoPath string
		wantCode int
	}{
		{
			name:     "create returns AssociationId",
			wantCode: http.StatusOK,
			fsPath:   "/data",
			repoPath: "s3://my-bucket/prefix",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			fsID := createFS(t, h, "LUSTRE")

			rec := doFSxRequest(t, h, "CreateDataRepositoryAssociation", map[string]any{
				"FileSystemId":       fsID,
				"FileSystemPath":     tc.fsPath,
				"DataRepositoryPath": tc.repoPath,
			})
			require.Equal(t, tc.wantCode, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			a := out["Association"].(map[string]any)
			assert.Contains(t, a["AssociationId"].(string), "dra-")
			assert.Equal(t, tc.fsPath, a["FileSystemPath"])
			assert.Equal(t, tc.repoPath, a["DataRepositoryPath"])
		})
	}
}

func TestFSx_DataRepositoryAssociationLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("describe/update/delete cycle", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		fsID := createFS(t, h, "LUSTRE")
		b := fsx.GetBackend(h)

		rec := doFSxRequest(t, h, "CreateDataRepositoryAssociation", map[string]any{
			"FileSystemId":       fsID,
			"FileSystemPath":     "/data",
			"DataRepositoryPath": "s3://bucket/prefix",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var cr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cr))
		id := cr["Association"].(map[string]any)["AssociationId"].(string)

		assert.Equal(t, 1, fsx.DRACount(b))

		// update
		rec2 := doFSxRequest(t, h, "UpdateDataRepositoryAssociation", map[string]any{
			"AssociationId":      id,
			"DataRepositoryPath": "s3://bucket/new",
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var ur map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &ur))
		assert.Equal(t, "s3://bucket/new", ur["Association"].(map[string]any)["DataRepositoryPath"])

		// describe
		rec3 := doFSxRequest(t, h, "DescribeDataRepositoryAssociations", map[string]any{
			"AssociationIds": []string{id},
		})
		require.Equal(t, http.StatusOK, rec3.Code)
		var dr map[string]any
		require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &dr))
		assert.Len(t, dr["Associations"].([]any), 1)

		// delete
		rec4 := doFSxRequest(t, h, "DeleteDataRepositoryAssociation", map[string]any{"AssociationId": id})
		require.Equal(t, http.StatusOK, rec4.Code)
		assert.Equal(t, 0, fsx.DRACount(b))
	})
}
