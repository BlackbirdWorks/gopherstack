package medialive_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

func TestInput_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	inputID := createTestInput(t, h)

	assert.Equal(t, 1, medialive.InputCount(h.Backend.(*medialive.InMemoryBackend)))

	// Describe
	rec := doRequest(t, h, http.MethodGet, "/prod/inputs/"+inputID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "test-input", descResp["name"])
	assert.Equal(t, "DETACHED", descResp["state"])
	assert.Contains(t, descResp["arn"], "arn:aws:medialive:us-east-1:000000000000:input:")

	// Update
	rec = doRequest(t, h, http.MethodPut, "/prod/inputs/"+inputID, map[string]any{
		"name": "updated-input",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	inp := updateResp["input"].(map[string]any)
	assert.Equal(t, "updated-input", inp["name"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/inputs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["inputs"], 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/prod/inputs/"+inputID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, medialive.InputCount(h.Backend.(*medialive.InMemoryBackend)))

	// Describe deleted returns 404
	rec = doRequest(t, h, http.MethodGet, "/prod/inputs/"+inputID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestInput_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/prod/inputs", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestListInputs_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/prod/inputs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["inputs"])
}

func TestCreatePartnerInput(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/prod/inputs", map[string]any{
		"name": "primary", "type": "UDP_PUSH",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	inputID := decodeBody(t, rec.Body.Bytes())["input"].(map[string]any)["id"].(string)

	rec = doRequest(t, h, http.MethodPost, "/prod/inputs/"+inputID+"/partners", map[string]any{})
	require.Equal(t, http.StatusCreated, rec.Code)
	partner := decodeBody(t, rec.Body.Bytes())["input"].(map[string]any)
	assert.NotEmpty(t, partner["id"])
	assert.NotEqual(t, inputID, partner["id"])

	rec = doRequest(t, h, http.MethodPost, "/prod/inputs/missing/partners", map[string]any{})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
