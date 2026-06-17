package medialive_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

func newTestHandler(t *testing.T) *medialive.Handler {
	t.Helper()

	backend := medialive.NewInMemoryBackend("000000000000", "us-east-1")

	return medialive.NewHandler(backend)
}

func doRequest(
	t *testing.T,
	h *medialive.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error

		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	if body != nil {
		req.ContentLength = int64(len(bodyBytes))
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func createTestChannel(t *testing.T, h *medialive.Handler) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/prod/channels", map[string]any{
		"name":         "test-channel",
		"channelClass": "STANDARD",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ch := resp["Channel"].(map[string]any)

	return ch["Id"].(string)
}

func createTestInput(t *testing.T, h *medialive.Handler) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/prod/inputs", map[string]any{
		"name": "test-input",
		"type": "UDP_PUSH",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	inp := resp["input"].(map[string]any)

	return inp["id"].(string)
}

func TestAudit1_Channel_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name:     "create returns channel with ARN and IDLE state",
			body:     map[string]any{"name": "my-channel", "channelClass": "STANDARD"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				ch := resp["Channel"].(map[string]any)
				assert.Contains(t, ch["Arn"], "arn:aws:medialive:us-east-1:000000000000:channel:")
				assert.Equal(t, "IDLE", ch["State"])
				assert.Equal(t, "STANDARD", ch["ChannelClass"])
				assert.NotEmpty(t, ch["Id"])
			},
		},
		{
			name:     "create missing Name returns 400",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/prod/channels", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestAudit1_Channel_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	assert.Equal(t, 1, medialive.ChannelCount(h.Backend.(*medialive.InMemoryBackend)))

	// Describe
	rec := doRequest(t, h, http.MethodGet, "/prod/channels/"+channelID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "test-channel", descResp["Name"])
	assert.Equal(t, "IDLE", descResp["State"])

	// Update
	rec = doRequest(t, h, http.MethodPut, "/prod/channels/"+channelID, map[string]any{
		"name": "updated-channel",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	ch := updateResp["Channel"].(map[string]any)
	assert.Equal(t, "updated-channel", ch["Name"])

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/channels", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["Channels"], 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/prod/channels/"+channelID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, medialive.ChannelCount(h.Backend.(*medialive.InMemoryBackend)))

	// Describe deleted returns 404
	rec = doRequest(t, h, http.MethodGet, "/prod/channels/"+channelID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAudit1_Channel_StartStop(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	// Start
	rec := doRequest(t, h, http.MethodPost, "/prod/channels/"+channelID+"/start", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	assert.Equal(t, "RUNNING", startResp["State"])

	// Start again returns conflict
	rec = doRequest(t, h, http.MethodPost, "/prod/channels/"+channelID+"/start", nil)
	assert.Equal(t, http.StatusConflict, rec.Code)

	// Stop
	rec = doRequest(t, h, http.MethodPost, "/prod/channels/"+channelID+"/stop", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var stopResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &stopResp))
	assert.Equal(t, "IDLE", stopResp["State"])
}

func TestAudit1_Channel_DeleteRunning(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	// Start channel
	doRequest(t, h, http.MethodPost, "/prod/channels/"+channelID+"/start", nil)

	// Delete running channel returns conflict
	rec := doRequest(t, h, http.MethodDelete, "/prod/channels/"+channelID, nil)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestAudit1_Channel_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"describe unknown returns 404", http.MethodGet, "/prod/channels/notexist"},
		{"update unknown returns 404", http.MethodPut, "/prod/channels/notexist"},
		{"delete unknown returns 404", http.MethodDelete, "/prod/channels/notexist"},
		{"start unknown returns 404", http.MethodPost, "/prod/channels/notexist/start"},
		{"stop unknown returns 404", http.MethodPost, "/prod/channels/notexist/stop"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := doRequest(t, h, tc.method, tc.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestAudit1_Input_CRUD(t *testing.T) {
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

func TestAudit1_Input_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/prod/inputs", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit1_InputSecurityGroup_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, http.MethodPost, "/prod/inputSecurityGroups", map[string]any{
		"WhitelistRules": []any{
			map[string]any{"Cidr": "10.0.0.0/8"},
		},
		"Tags": map[string]any{"env": "test"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	sg := createResp["SecurityGroup"].(map[string]any)
	groupID := sg["Id"].(string)

	assert.Contains(t, sg["Arn"], "arn:aws:medialive:us-east-1:000000000000:inputSecurityGroup:")
	assert.Equal(t, "IDLE", sg["State"])
	assert.NotEmpty(t, groupID)

	rules := sg["WhitelistRules"].([]any)
	assert.Len(t, rules, 1)
	assert.Equal(t, "10.0.0.0/8", rules[0].(map[string]any)["Cidr"])

	assert.Equal(t, 1, medialive.InputSecurityGroupCount(h.Backend.(*medialive.InMemoryBackend)))

	// Describe
	rec = doRequest(t, h, http.MethodGet, "/prod/inputSecurityGroups/"+groupID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Update whitelist
	rec = doRequest(t, h, http.MethodPut, "/prod/inputSecurityGroups/"+groupID, map[string]any{
		"WhitelistRules": []any{
			map[string]any{"Cidr": "192.168.0.0/16"},
			map[string]any{"Cidr": "10.0.0.0/8"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	updatedSG := updateResp["SecurityGroup"].(map[string]any)
	updatedRules := updatedSG["WhitelistRules"].([]any)
	assert.Len(t, updatedRules, 2)

	// List
	rec = doRequest(t, h, http.MethodGet, "/prod/inputSecurityGroups", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp["InputSecurityGroups"], 1)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/prod/inputSecurityGroups/"+groupID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, medialive.InputSecurityGroupCount(h.Backend.(*medialive.InMemoryBackend)))

	// Describe deleted returns 404
	rec = doRequest(t, h, http.MethodGet, "/prod/inputSecurityGroups/"+groupID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAudit1_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	channelID := createTestChannel(t, h)

	// Get ARN
	rec := doRequest(t, h, http.MethodGet, "/prod/channels/"+channelID, nil)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	resourceARN := descResp["Arn"].(string)

	// CreateTags
	rec = doRequest(t, h, http.MethodPost, "/prod/tags/"+resourceARN, map[string]any{
		"Tags": map[string]any{"env": "prod", "team": "platform"},
	})
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// ListTagsForResource
	rec = doRequest(t, h, http.MethodGet, "/prod/tags/"+resourceARN, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags := listResp["Tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	// DeleteTags
	req := httptest.NewRequest(http.MethodDelete, "/prod/tags/"+resourceARN+"?tagKeys=env", nil)
	rec2 := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec2)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// Verify tag removed
	rec = doRequest(t, h, http.MethodGet, "/prod/tags/"+resourceARN, nil)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags = listResp["Tags"].(map[string]any)
	assert.NotContains(t, tags, "env")
	assert.Equal(t, "platform", tags["team"])
}

func TestAudit1_ListChannels_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/prod/channels", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["Channels"])
}

func TestAudit1_ListInputs_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/prod/inputs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["inputs"])
}

func TestAudit1_ListInputSecurityGroups_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/prod/inputSecurityGroups", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["InputSecurityGroups"])
}
