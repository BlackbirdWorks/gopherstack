package fsx_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/fsx"
)

func TestFSx_FileCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cacheType   string
		typeVersion string
		subnetIDs   []string
		capacity    int
		wantCode    int
		wantErr     bool
	}{
		{
			name:        "create LUSTRE cache",
			cacheType:   "LUSTRE",
			typeVersion: "2.12",
			subnetIDs:   []string{"subnet-1"},
			capacity:    1200,
			wantCode:    http.StatusOK,
		},
		{
			name:     "missing cache type returns 400",
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
		{
			name:      "missing FileCacheTypeVersion returns 400",
			cacheType: "LUSTRE",
			subnetIDs: []string{"subnet-1"},
			capacity:  1200,
			wantCode:  http.StatusBadRequest,
			wantErr:   true,
		},
		{
			name:        "missing SubnetIds returns 400",
			cacheType:   "LUSTRE",
			typeVersion: "2.12",
			capacity:    1200,
			wantCode:    http.StatusBadRequest,
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			body := map[string]any{"StorageCapacity": tc.capacity}
			if tc.cacheType != "" {
				body["FileCacheType"] = tc.cacheType
			}
			if tc.typeVersion != "" {
				body["FileCacheTypeVersion"] = tc.typeVersion
			}
			if tc.subnetIDs != nil {
				body["SubnetIds"] = tc.subnetIDs
			}

			rec := doFSxRequest(t, h, "CreateFileCache", body)
			require.Equal(t, tc.wantCode, rec.Code)

			if !tc.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				c := out["FileCache"].(map[string]any)
				assert.Contains(t, c["FileCacheId"].(string), "fc-")
				assert.Equal(t, "AVAILABLE", c["Lifecycle"])
				assert.InDelta(t, float64(tc.capacity), c["StorageCapacity"], 0.0001)
				assert.Equal(t, tc.typeVersion, c["FileCacheTypeVersion"],
					"FileCacheTypeVersion must be echoed back on CreateFileCache's response")
				assert.ElementsMatch(t, tc.subnetIDs, c["SubnetIds"])
			}
		})
	}
}

func TestFSx_FileCacheLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("describe/update/delete cycle", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		b := fsx.GetBackend(h)
		fcID := createFileCache(t, h, "LUSTRE")

		assert.Equal(t, 1, fsx.FileCacheCount(b))

		// describe by id
		rec := doFSxRequest(t, h, "DescribeFileCaches", map[string]any{
			"FileCacheIds": []string{fcID},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var dr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dr))
		assert.Len(t, dr["FileCaches"].([]any), 1)

		// update
		rec2 := doFSxRequest(t, h, "UpdateFileCache", map[string]any{
			"FileCacheId":        fcID,
			"StorageCapacityGiB": 2400,
		})
		require.Equal(t, http.StatusOK, rec2.Code)
		var ur map[string]any
		require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &ur))
		assert.InDelta(t, float64(2400), ur["FileCache"].(map[string]any)["StorageCapacity"], 0.0001)

		// delete
		rec3 := doFSxRequest(t, h, "DeleteFileCache", map[string]any{"FileCacheId": fcID})
		require.Equal(t, http.StatusOK, rec3.Code)
		var del map[string]any
		require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &del))
		assert.Equal(t, fcID, del["FileCacheId"])
		assert.Equal(t, "DELETING", del["Lifecycle"])
		assert.Equal(t, 0, fsx.FileCacheCount(b))
	})
}
