package medialive_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

func TestInputSecurityGroup_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, http.MethodPost, "/prod/inputSecurityGroups", map[string]any{
		"whitelistRules": []any{
			map[string]any{"cidr": "10.0.0.0/8"},
		},
		"tags": map[string]any{"env": "test"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	sg := createResp["securityGroup"].(map[string]any)
	groupID := sg["id"].(string)

	assert.Contains(t, sg["arn"], "arn:aws:medialive:us-east-1:000000000000:inputSecurityGroup:")
	assert.Equal(t, "IDLE", sg["state"])
	assert.NotEmpty(t, groupID)

	rules := sg["whitelistRules"].([]any)
	assert.Len(t, rules, 1)
	assert.Equal(t, "10.0.0.0/8", rules[0].(map[string]any)["cidr"])

	assert.Equal(t, 1, medialive.InputSecurityGroupCount(h.Backend.(*medialive.InMemoryBackend)))

	// Describe
	rec = doRequest(t, h, http.MethodGet, "/prod/inputSecurityGroups/"+groupID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Update whitelist
	rec = doRequest(t, h, http.MethodPut, "/prod/inputSecurityGroups/"+groupID, map[string]any{
		"whitelistRules": []any{
			map[string]any{"cidr": "192.168.0.0/16"},
			map[string]any{"cidr": "10.0.0.0/8"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	updatedSG := updateResp["securityGroup"].(map[string]any)
	updatedRules := updatedSG["whitelistRules"].([]any)
	assert.Len(t, updatedRules, 2)

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/inputSecurityGroups", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["inputSecurityGroups"], 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/prod/inputSecurityGroups/"+groupID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, medialive.InputSecurityGroupCount(h.Backend.(*medialive.InMemoryBackend)))

	// Describe deleted returns 404
	rec = doRequest(t, h, http.MethodGet, "/prod/inputSecurityGroups/"+groupID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestListInputSecurityGroups_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/prod/inputSecurityGroups", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["inputSecurityGroups"])
}
