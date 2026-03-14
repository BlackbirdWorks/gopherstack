package pinpoint_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "123456789012"
)

func newHandlerForTest(t *testing.T) *pinpoint.Handler {
	t.Helper()

	b := pinpoint.NewInMemoryBackend(testRegion, testAccountID)
	h := pinpoint.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h
}

func doPinpointRequest(t *testing.T, h *pinpoint.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error

		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mobiletargeting/aws4_request",
	)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
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

func TestHandler_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		encodeARN bool
	}{
		{
			name:      "tag_list_untag",
			encodeARN: false,
		},
		{
			name:      "tag_list_untag_percent_encoded_arn",
			encodeARN: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "tag-test-app"})
			require.Equal(t, http.StatusCreated, rec.Code)

			var appResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&appResp))

			appARN, _ := appResp["Arn"].(string)
			require.NotEmpty(t, appARN)

			tagPathARN := appARN
			if tt.encodeARN {
				tagPathARN = url.PathEscape(appARN)
			}

			tagRec := doPinpointRequest(t, h, http.MethodPost, "/v1/tags/"+tagPathARN, map[string]any{
				"tags": map[string]string{"owner": "integration"},
			})
			assert.Equal(t, http.StatusNoContent, tagRec.Code)

			listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/tags/"+tagPathARN, nil)
			assert.Equal(t, http.StatusOK, listRec.Code)

			var tagsResp map[string]any
			require.NoError(t, json.NewDecoder(listRec.Body).Decode(&tagsResp))

			tagsMap, _ := tagsResp["tags"].(map[string]any)
			assert.Equal(t, "integration", tagsMap["owner"])

			untagPath := "/v1/tags/" + tagPathARN + "?tagKeys=owner"
			untagRec := doPinpointRequest(t, h, http.MethodDelete, untagPath, nil)
			assert.Equal(t, http.StatusNoContent, untagRec.Code)
		})
	}
}

func TestHandler_ApplicationSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		wantStatus int
	}{
		{
			name:       "get_settings",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
		},
		{
			name:       "put_settings",
			method:     http.MethodPut,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "settings-app"})
			require.Equal(t, http.StatusCreated, rec.Code)

			var appResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&appResp))

			appID, _ := appResp["Id"].(string)
			require.NotEmpty(t, appID)

			settingsPath := "/v1/apps/" + appID + "/settings"

			var body any
			if tt.method == http.MethodPut {
				body = map[string]any{}
			}

			rec2 := doPinpointRequest(t, h, tt.method, settingsPath, body)
			assert.Equal(t, tt.wantStatus, rec2.Code)

			var settingsResp map[string]any
			require.NoError(t, json.NewDecoder(rec2.Body).Decode(&settingsResp))
			assert.Equal(t, appID, settingsResp["ApplicationId"])
			// CampaignHook, Limits, and QuietTime must be non-nil empty objects so
			// the Terraform provider flatten helpers don't panic on nil dereferences.
			assert.NotNil(t, settingsResp["CampaignHook"])
			assert.NotNil(t, settingsResp["Limits"])
			assert.NotNil(t, settingsResp["QuietTime"])
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
