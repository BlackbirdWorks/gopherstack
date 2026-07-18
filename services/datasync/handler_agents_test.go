package datasync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/datasync"
)

func TestDataSync_Agent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *datasync.Handler)
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateAgent returns AgentArn",
			action: "CreateAgent",
			body: map[string]any{
				"ActivationKey": "AAAAA-BBBBB-CCCCC-DDDDD-EEEEE",
				"AgentName":     "my-agent",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["AgentArn"], "arn:aws:datasync:us-east-1:000000000000:agent/")
			},
		},
		{
			name:     "CreateAgent missing ActivationKey returns 400",
			action:   "CreateAgent",
			body:     map[string]any{"AgentName": "x"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DescribeAgent returns agent details",
			action: "DescribeAgent",
			setup:  func(h *datasync.Handler) { createTestAgent(t, h) },
			body: func() any {
				return nil // populated in setup via describe after create
			}(),
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "ONLINE", resp["Status"])
			},
		},
		{
			name:     "DescribeAgent unknown ARN returns 404",
			action:   "DescribeAgent",
			body:     map[string]any{"AgentArn": "arn:aws:datasync:us-east-1:000000000000:agent/notexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DescribeAgent missing ARN returns 400",
			action:   "DescribeAgent",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "UpdateAgent unknown ARN returns 404",
			action: "UpdateAgent",
			body: map[string]any{
				"AgentArn": "arn:aws:datasync:us-east-1:000000000000:agent/notexist",
				"Name":     "new",
			},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "DeleteAgent unknown ARN returns 404",
			action:   "DeleteAgent",
			body:     map[string]any{"AgentArn": "arn:aws:datasync:us-east-1:000000000000:agent/notexist"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "ListAgents empty returns empty list",
			action:   "ListAgents",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				agents, ok := resp["Agents"].([]any)
				require.True(t, ok)
				assert.Empty(t, agents)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}

			body := tc.body
			// For DescribeAgent after setup, derive body from created agent.
			if tc.action == "DescribeAgent" && tc.setup != nil && tc.body == nil {
				agentArn := createTestAgent(t, h)
				body = map[string]any{"AgentArn": agentArn}
			}

			rec := doRequest(t, h, tc.action, body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestDataSync_AgentCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	agentArn := createTestAgent(t, h)
	assert.Equal(t, 1, datasync.AgentCount(h.Backend.(*datasync.InMemoryBackend)))

	// Describe
	rec := doRequest(t, h, "DescribeAgent", map[string]any{"AgentArn": agentArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "test-agent", descResp["Name"])

	// Update
	rec = doRequest(t, h, "UpdateAgent", map[string]any{"AgentArn": agentArn, "Name": "updated-agent"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify update
	rec = doRequest(t, h, "DescribeAgent", map[string]any{"AgentArn": agentArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "updated-agent", descResp["Name"])

	// List
	rec = doRequest(t, h, "ListAgents", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["Agents"], 1)

	// Delete
	rec = doRequest(t, h, "DeleteAgent", map[string]any{"AgentArn": agentArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, datasync.AgentCount(h.Backend.(*datasync.InMemoryBackend)))

	// Describe deleted returns 404
	rec = doRequest(t, h, "DescribeAgent", map[string]any{"AgentArn": agentArn})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
