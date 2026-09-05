package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_CreateApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		appName   string
		tags      map[string]string
		wantName  string
		wantTagKV [2]string
		wantErr   bool
	}{
		{
			name:     "creates_app",
			appName:  "my-app",
			wantName: "my-app",
		},
		{
			name:      "creates_app_with_tags",
			appName:   "tagged-app",
			tags:      map[string]string{"env": "test"},
			wantName:  "tagged-app",
			wantTagKV: [2]string{"env", "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			app, err := b.CreateApp("us-east-1", "123456789012", tt.appName, tt.tags)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, app)
			assert.Equal(t, tt.wantName, app.Name)
			assert.NotEmpty(t, app.ID)
			assert.NotEmpty(t, app.ARN)
			assert.NotEmpty(t, app.CreationDate)

			if tt.wantTagKV[0] != "" {
				assert.Equal(t, tt.wantTagKV[1], app.Tags[tt.wantTagKV[0]])
			}
		})
	}
}

func TestBackend_GetApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appName string
		wantErr bool
	}{
		{
			name:    "gets_existing_app",
			appName: "my-app",
			wantErr: false,
		},
		{
			name:    "returns_error_for_missing_app",
			appName: "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var appID string

			if tt.appName != "" {
				app, err := b.CreateApp("us-east-1", "123456789012", tt.appName, nil)
				require.NoError(t, err)

				appID = app.ID
			} else {
				appID = "nonexistent-id"
			}

			got, err := b.GetApp(appID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, tt.appName, got.Name)
		})
	}
}

func TestBackend_DeleteApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		appName string
		wantErr bool
	}{
		{
			name:    "deletes_existing_app",
			appName: "to-delete",
			wantErr: false,
		},
		{
			name:    "returns_error_for_missing_app",
			appName: "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			var appID string

			if tt.appName != "" {
				app, err := b.CreateApp("us-east-1", "123456789012", tt.appName, nil)
				require.NoError(t, err)

				appID = app.ID
			} else {
				appID = "nonexistent-id"
			}

			deleted, err := b.DeleteApp(appID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.appName, deleted.Name)

			_, err = b.GetApp(appID)
			require.Error(t, err)
		})
	}
}

func TestBackend_GetApps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		appNames  []string
		wantCount int
	}{
		{
			name:      "returns_empty_list",
			appNames:  nil,
			wantCount: 0,
		},
		{
			name:      "returns_all_apps",
			appNames:  []string{"app-a", "app-b", "app-c"},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			for _, n := range tt.appNames {
				_, err := b.CreateApp("us-east-1", "123456789012", n, nil)
				require.NoError(t, err)
			}

			apps, err := b.GetApps()

			require.NoError(t, err)
			assert.Len(t, apps, tt.wantCount)
		})
	}
}

// TestParity_CreateApp_ErrorMapping verifies that handleCreateApp maps errors to
// the correct HTTP status codes. Before the fix, any backend error was always
// mapped to 500 regardless of type.
func TestCreateApp_ErrorMapping(t *testing.T) {
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
func TestAppNotFound_Returns404(t *testing.T) {
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
func TestCreateApp_ResponseShape(t *testing.T) {
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
func TestGetApps_ReflectsCreatedApps(t *testing.T) {
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
func TestDeleteApp_RemovesFromList(t *testing.T) {
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

func TestHandlerReset(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "before-reset"})
	require.Equal(t, http.StatusCreated, rec.Code)

	h.Reset()

	rec2 := doPinpointRequest(t, h, http.MethodGet, "/v1/apps", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&resp))
	items, _ := resp["Item"].([]any)
	assert.Empty(t, items)
}

// ──────────────────────────────────────────────────
// Backend lifecycle methods
// ──────────────────────────────────────────────────

func TestCreateAppInvalidJSON(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doRawPinpointRequest(t, h, http.MethodPost, "/v1/apps", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name:       "creates_app",
			body:       map[string]any{"Name": "my-app"},
			wantStatus: http.StatusCreated,
			wantID:     true,
		},
		{
			name:       "creates_app_with_tags",
			body:       map[string]any{"Name": "tagged-app", "tags": map[string]string{"env": "prod"}},
			wantStatus: http.StatusCreated,
			wantID:     true,
		},
		{
			name:       "rejects_empty_name",
			body:       map[string]any{"Name": ""},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "rejects_whitespace_name",
			body:       map[string]any{"Name": "   "},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.NotEmpty(t, resp["Id"])
				assert.NotEmpty(t, resp["Arn"])
			}
		})
	}
}

func TestHandler_GetApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		appName    string
		wantStatus int
	}{
		{
			name:       "gets_existing_app",
			appName:    "existing-app",
			wantStatus: http.StatusOK,
		},
		{
			name:       "returns_404_for_missing",
			appName:    "",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			var appID string

			if tt.appName != "" {
				rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": tt.appName})
				require.Equal(t, http.StatusCreated, rec.Code)

				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

				appID, _ = resp["Id"].(string)
			} else {
				appID = "nonexistent-id"
			}

			rec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteApp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		appName    string
		wantStatus int
	}{
		{
			name:       "deletes_existing_app",
			appName:    "app-to-delete",
			wantStatus: http.StatusOK,
		},
		{
			name:       "returns_404_for_missing",
			appName:    "",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			var appID string

			if tt.appName != "" {
				rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": tt.appName})
				require.Equal(t, http.StatusCreated, rec.Code)

				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

				appID, _ = resp["Id"].(string)
			} else {
				appID = "nonexistent-id"
			}

			rec := doPinpointRequest(t, h, http.MethodDelete, "/v1/apps/"+appID, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetApps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		appNames   []string
		wantCount  int
		wantStatus int
	}{
		{
			name:       "returns_empty",
			appNames:   nil,
			wantCount:  0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "returns_all_apps",
			appNames:   []string{"app-1", "app-2"},
			wantCount:  2,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			for _, n := range tt.appNames {
				rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": n})
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps", nil)

			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

			items, _ := resp["Item"].([]any)
			assert.Len(t, items, tt.wantCount)
		})
	}
}

// TestHandler_GetAppsPagination verifies pageSize and token query parameter support.
func TestHandler_GetAppsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pageSize  string
		appNames  []string
		wantCount int
		wantToken bool
	}{
		{
			name:      "first_page_limited",
			appNames:  []string{"app-a", "app-b", "app-c"},
			pageSize:  "2",
			wantCount: 2,
			wantToken: true,
		},
		{
			name:      "all_results_no_token",
			appNames:  []string{"app-x", "app-y"},
			pageSize:  "10",
			wantCount: 2,
			wantToken: false,
		},
		{
			name:      "zero_page_size_uses_default",
			appNames:  []string{"app-p", "app-q"},
			pageSize:  "",
			wantCount: 2,
			wantToken: false,
		},
		{
			name:      "empty_list_no_token",
			appNames:  nil,
			pageSize:  "5",
			wantCount: 0,
			wantToken: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			for _, n := range tt.appNames {
				rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": n})
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			path := "/v1/apps"
			if tt.pageSize != "" {
				path += "?pageSize=" + url.QueryEscape(tt.pageSize)
			}

			rec := doPinpointRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

			items, ok := resp["Item"].([]any)
			if tt.wantCount > 0 {
				require.True(t, ok, "expected Item array in response")
			}
			assert.Len(t, items, tt.wantCount)

			_, hasToken := resp["NextToken"]
			assert.Equal(t, tt.wantToken, hasToken)
		})
	}
}

// TestHandler_GetAppsContinuation verifies two-page traversal using the NextToken cursor.
func TestHandler_GetAppsContinuation(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	for _, n := range []string{"app-a", "app-b", "app-c"} {
		rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": n})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	// First page: 2 of 3.
	rec1 := doPinpointRequest(t, h, http.MethodGet, "/v1/apps?pageSize=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]any
	require.NoError(t, json.NewDecoder(rec1.Body).Decode(&resp1))

	page1, ok := resp1["Item"].([]any)
	require.True(t, ok, "expected Item array in first-page response")
	assert.Len(t, page1, 2)

	nextToken, hasToken := resp1["NextToken"].(string)
	require.True(t, hasToken, "expected NextToken in first-page response")
	require.NotEmpty(t, nextToken)

	// Second page: remaining 1.
	path2 := "/v1/apps?pageSize=2&token=" + url.QueryEscape(nextToken)
	rec2 := doPinpointRequest(t, h, http.MethodGet, path2, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&resp2))

	page2, ok := resp2["Item"].([]any)
	require.True(t, ok, "expected Item array in second-page response")
	assert.Len(t, page2, 1)

	_, stillHasToken := resp2["NextToken"]
	assert.False(t, stillHasToken, "last page should have no NextToken")

	// All app names should be present across both pages.
	names := make([]string, 0, 3)

	for _, item := range append(page1, page2...) {
		app, isMap := item.(map[string]any)
		require.True(t, isMap)
		names = append(names, app["Name"].(string))
	}

	assert.ElementsMatch(t, []string{"app-a", "app-b", "app-c"}, names)
}

// TestHandler_GetApps_DuplicateNames_NoDropOrDupAcrossPages proves GetApps loses (or
// repeats) apps at a page boundary when several apps share a Name. Pinpoint applications
// have no name-uniqueness constraint (CreateApp never checks for an existing Name), yet
// GetApps sorts solely by Name with no secondary key, over a *store.Table map walk whose
// iteration order varies between calls; handleGetApps then pages that resort with
// pkgs/page's offset-based cursor. When a group of same-named apps straddles a page
// boundary, the tie group's relative order can differ between the call that computed
// page 1 and the resort behind page 2's offset, dropping or duplicating members. Looped
// because (unlike a plain missing-sort bug) this depends on map iteration reshuffling the
// tie group across the two calls, which does not reproduce on every run.
func TestHandler_GetApps_DuplicateNames_NoDropOrDupAcrossPages(t *testing.T) {
	t.Parallel()

	for range 30 {
		h := newHandlerForTest(t)

		const dupCount = 5
		created := make(map[string]bool, dupCount)

		for range dupCount {
			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "dup-app-name"})
			require.Equal(t, http.StatusCreated, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
			created[resp["Id"].(string)] = true
		}

		seen := make(map[string]bool, dupCount)
		path := "/v1/apps?pageSize=2"

		for range dupCount + 1 {
			rec := doPinpointRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

			items, _ := resp["Item"].([]any)
			for _, item := range items {
				app, isMap := item.(map[string]any)
				require.True(t, isMap)
				seen[app["Id"].(string)] = true
			}

			nextToken, hasToken := resp["NextToken"].(string)
			if !hasToken {
				break
			}

			path = "/v1/apps?pageSize=2&token=" + url.QueryEscape(nextToken)
		}

		assert.Equal(t, created, seen, "paged GetApps dropped or duplicated same-named apps across pages")
	}
}
