package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_CreateApp_ErrorMapping verifies that handleCreateApp maps errors to
// the correct HTTP status codes. Before the fix, any backend error was always
// mapped to 500 regardless of type.
func TestParity_CreateApp_ErrorMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name:       "success_returns_201",
			body:       map[string]any{"Name": "my-app"},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "empty_name_returns_400_not_500",
			body:       map[string]any{"Name": ""},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "whitespace_name_returns_400_not_500",
			body:       map[string]any{"Name": "   "},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_name_key_returns_400_not_500",
			body:       map[string]any{"tags": map[string]string{"env": "prod"}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestParity_AppNotFound_Returns404 verifies that GetApp and DeleteApp return
// 404 (not 500) for nonexistent application IDs.
func TestParity_AppNotFound_Returns404(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{
			name:   "get_nonexistent_app",
			method: http.MethodGet,
			path:   "/v1/apps/does-not-exist",
		},
		{
			name:   "delete_nonexistent_app",
			method: http.MethodDelete,
			path:   "/v1/apps/does-not-exist",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doPinpointRequest(t, h, tc.method, tc.path, nil)

			assert.Equal(t, http.StatusNotFound, rec.Code, "body: %s", rec.Body.String())

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "NotFoundException", resp["__type"])
		})
	}
}

// TestParity_CreateApp_ResponseShape verifies the CreateApp response contains
// the required AWS fields: Id, Arn, Name, CreationDate.
func TestParity_CreateApp_ResponseShape(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps",
		map[string]any{"Name": "shape-test"})
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.NotEmpty(t, resp["Id"], "Id must be present")
	assert.NotEmpty(t, resp["Arn"], "Arn must be present")
	assert.Equal(t, "shape-test", resp["Name"])
	assert.NotEmpty(t, resp["CreationDate"], "CreationDate must be present")
}

// TestParity_GetApps_ReflectsCreatedApps verifies GetApps lists apps created via CreateApp.
func TestParity_GetApps_ReflectsCreatedApps(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	names := []string{"alpha", "beta", "gamma"}
	for _, n := range names {
		rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": n})
		require.Equal(t, http.StatusCreated, rec.Code, "create %q: %s", n, rec.Body.String())
	}

	rec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	item := resp["Item"].([]any)
	assert.Len(t, item, len(names))
}

// TestParity_DeleteApp_RemovesFromList verifies DeleteApp removes the app
// from subsequent GetApps responses.
func TestParity_DeleteApp_RemovesFromList(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	// Create two apps.
	createRec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps",
		map[string]any{"Name": "keep-me"})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	appID := created["Id"].(string)

	doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "also-here"})

	// Delete the first app.
	delRec := doPinpointRequest(t, h, http.MethodDelete, "/v1/apps/"+appID, nil)
	require.Equal(t, http.StatusOK, delRec.Code, delRec.Body.String())

	// List should now have only one app.
	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	item := listResp["Item"].([]any)
	assert.Len(t, item, 1)
}
