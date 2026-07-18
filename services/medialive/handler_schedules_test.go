package medialive_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchedule_DescribeDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	// Seed an action via BatchUpdateSchedule.
	rec := doRequest(t, h, http.MethodPut, "/prod/channels/"+channelID+"/schedule", map[string]any{
		"creates": map[string]any{
			"scheduleActions": []map[string]any{
				{"actionName": "a1", "scheduleActionSettings": map[string]any{}},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/prod/channels/"+channelID+"/schedule", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotNil(t, decodeBody(t, rec.Body.Bytes())["scheduleActions"])

	rec = doRequest(t, h, http.MethodDelete, "/prod/channels/"+channelID+"/schedule", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/prod/channels/missing/schedule", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatchUpdateSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	chID := createTestChannel(t, h)

	// Add schedule actions
	rec := doRequest(t, h, http.MethodPut, "/prod/channels/"+chID+"/schedule", map[string]any{
		"creates": map[string]any{
			"scheduleActions": []any{
				map[string]any{"actionName": "start-at-midnight"},
			},
		},
		"deletes": map[string]any{"actionNames": []any{}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	creates := resp["creates"].(map[string]any)["scheduleActions"].([]any)
	assert.Len(t, creates, 1)

	// Delete the action
	rec = doRequest(t, h, http.MethodPut, "/prod/channels/"+chID+"/schedule", map[string]any{
		"creates": map[string]any{"scheduleActions": []any{}},
		"deletes": map[string]any{"actionNames": []any{"start-at-midnight"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}
