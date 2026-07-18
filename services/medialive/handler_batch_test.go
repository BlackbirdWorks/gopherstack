package medialive_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatch_StartStopDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		path     string
		wantCode int
	}{
		{
			name:     "batch start channels",
			path:     "/prod/batch/start",
			wantCode: http.StatusOK,
			body:     map[string]any{"channelIds": []any{}},
		},
		{
			name:     "batch stop channels",
			path:     "/prod/batch/stop",
			wantCode: http.StatusOK,
			body:     map[string]any{"channelIds": []any{}},
		},
		{
			name:     "batch delete channels",
			path:     "/prod/batch/delete",
			wantCode: http.StatusOK,
			body:     map[string]any{"channelIds": []any{}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestBatch_StartStopKnownChannels(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	chID := createTestChannel(t, h)

	// Batch start
	rec := doRequest(t, h, http.MethodPost, "/prod/batch/start", map[string]any{
		"channelIds": []any{chID},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	successful := startResp["successful"].([]any)
	assert.Len(t, successful, 1)
	assert.Equal(t, chID, successful[0].(map[string]any)["id"])

	// Batch stop
	rec = doRequest(t, h, http.MethodPost, "/prod/batch/stop", map[string]any{
		"channelIds": []any{chID},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Batch delete (channel must be idle)
	rec = doRequest(t, h, http.MethodPost, "/prod/batch/delete", map[string]any{
		"channelIds": []any{chID},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var delResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &delResp))
	delSuccessful := delResp["successful"].([]any)
	assert.Len(t, delSuccessful, 1)
}

func TestBatch_StartNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/prod/batch/start", map[string]any{
		"channelIds": []any{"nonexistent"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	failed := resp["failed"].([]any)
	assert.Len(t, failed, 1)
}

// TestBatch_DeleteInputSecurityGroups exercises the bug fix documented in
// PARITY.md: BatchDeleteInput has an inputSecurityGroupIds field that the
// handler previously never parsed.
func TestBatch_DeleteInputSecurityGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/prod/inputSecurityGroups", map[string]any{})
	require.Equal(t, http.StatusCreated, rec.Code)
	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	groupID := created["securityGroup"].(map[string]any)["id"].(string)

	rec = doRequest(t, h, http.MethodPost, "/prod/batch/delete", map[string]any{
		"inputSecurityGroupIds": []any{groupID},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	successful := resp["successful"].([]any)
	require.Len(t, successful, 1)
	assert.Equal(t, groupID, successful[0].(map[string]any)["id"])

	rec = doRequest(t, h, http.MethodGet, "/prod/inputSecurityGroups/"+groupID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
