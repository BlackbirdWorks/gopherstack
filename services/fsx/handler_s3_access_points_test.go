package fsx_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFSx_S3AccessPoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		apName   string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "create access point",
			apName:   "my-ap",
			wantCode: http.StatusOK,
		},
		{
			name:     "missing Name returns 400",
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
				body = map[string]any{"Name": tc.apName, "FileSystemId": fsID}
			} else {
				fsID := createFS(t, h, "ONTAP")
				body = map[string]any{"FileSystemId": fsID}
			}

			rec := doFSxRequest(t, h, "CreateAndAttachS3AccessPoint", body)
			require.Equal(t, tc.wantCode, rec.Code)

			if !tc.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				ap := out["S3AccessPoint"].(map[string]any)
				assert.Equal(t, tc.apName, ap["Name"])
				assert.Equal(t, "AVAILABLE", ap["Lifecycle"])
			}
		})
	}
}

func TestFSx_S3AccessPointLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("describe/detach-delete cycle", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		fsID := createFS(t, h, "ONTAP")

		// create
		doFSxRequest(t, h, "CreateAndAttachS3AccessPoint", map[string]any{
			"Name":         "my-ap",
			"FileSystemId": fsID,
		})

		// describe
		rec := doFSxRequest(t, h, "DescribeS3AccessPointAttachments", map[string]any{
			"Names": []string{"my-ap"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var dr map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dr))
		assert.Len(t, dr["S3AccessPoints"].([]any), 1)

		// detach
		rec2 := doFSxRequest(t, h, "DetachAndDeleteS3AccessPoint", map[string]any{
			"Name":         "my-ap",
			"FileSystemId": fsID,
		})
		require.Equal(t, http.StatusOK, rec2.Code)

		// describe empty
		rec3 := doFSxRequest(t, h, "DescribeS3AccessPointAttachments", map[string]any{})
		require.Equal(t, http.StatusOK, rec3.Code)
		var dr2 map[string]any
		require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &dr2))
		assert.Empty(t, dr2["S3AccessPoints"].([]any))
	})
}
