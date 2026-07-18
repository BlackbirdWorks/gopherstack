package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateGetListDeleteDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		destName   string
		expression string
		wantStatus int
	}{
		{
			name:       "full_lifecycle",
			destName:   "my-dest",
			expression: "my-iot-rule",
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			body := `{"Name":"` + tt.destName + `","Expression":"` + tt.expression +
				`","ExpressionType":"RuleName","RoleArn":"arn:aws:iam::000000000000:role/r"}`

			// Create
			rec := doIoTWRequest(t, h, http.MethodPost, "/destinations", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			assert.Equal(t, tt.destName, createResp["Name"])

			// Get
			rec = doIoTWRequest(t, h, http.MethodGet, "/destinations/"+tt.destName, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.destName, getResp["Name"])
			assert.Equal(t, tt.expression, getResp["Expression"])

			// List
			rec = doIoTWRequest(t, h, http.MethodGet, "/destinations", "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			dests, ok := listResp["DestinationList"].([]any)
			require.True(t, ok)
			assert.Len(t, dests, 1)

			// Delete
			rec = doIoTWRequest(t, h, http.MethodDelete, "/destinations/"+tt.destName, "")
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Get after delete returns 404
			rec = doIoTWRequest(t, h, http.MethodGet, "/destinations/"+tt.destName, "")
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_UpdateDestination(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Create destination
	rec := doIoTWRequest(
		t,
		h,
		http.MethodPost,
		"/destinations",
		`{"Name":"dest1","Expression":"rule/test","ExpressionType":"RuleName","RoleArn":"arn:aws:iam::123:role/r"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Update
	rec = doIoTWRequest(
		t,
		h,
		http.MethodPatch,
		"/destinations/dest1",
		`{"Expression":"rule/updated","ExpressionType":"RuleName","RoleArn":"arn:aws:iam::123:role/r2","Description":"desc"}`,
	)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Verify
	rec = doIoTWRequest(t, h, http.MethodGet, "/destinations/dest1", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, "rule/updated", getResp["Expression"])
}
