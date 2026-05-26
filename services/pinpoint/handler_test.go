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

// createTestApp is a helper that creates a Pinpoint application and returns its ID.
func createTestApp(t *testing.T, h *pinpoint.Handler, name string) string {
	t.Helper()

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": name})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	id, _ := resp["Id"].(string)
	require.NotEmpty(t, id)

	return id
}

func TestHandler_CreateCampaign(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name:       "creates_campaign",
			body:       map[string]any{"Name": "my-campaign", "SegmentId": "seg-1"},
			wantStatus: http.StatusCreated,
			wantID:     true,
		},
		{
			name:       "creates_campaign_with_tags",
			body:       map[string]any{"Name": "tagged-campaign", "tags": map[string]string{"env": "prod"}},
			wantStatus: http.StatusCreated,
			wantID:     true,
		},
		{
			name:       "rejects_empty_name",
			body:       map[string]any{"Name": ""},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "campaign-test-app")

			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.NotEmpty(t, resp["Id"])
				assert.NotEmpty(t, resp["Arn"])
				assert.Equal(t, appID, resp["ApplicationId"])
			}
		})
	}
}

func TestHandler_CreateEmailTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantARN    bool
	}{
		{
			name:       "creates_email_template",
			body:       map[string]any{"Subject": "Hello", "HtmlPart": "<p>Hi</p>"},
			wantStatus: http.StatusCreated,
			wantARN:    true,
		},
		{
			name:       "creates_email_template_minimal",
			body:       map[string]any{},
			wantStatus: http.StatusCreated,
			wantARN:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/my-email-tpl/email", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantARN {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.NotEmpty(t, resp["Arn"])
				assert.Equal(t, "Created", resp["Message"])
			}
		})
	}
}

func TestHandler_CreateExportJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name:       "creates_export_job",
			body:       map[string]any{"RoleArn": "arn:aws:iam::123:role/export", "S3UrlPrefix": "s3://bucket/prefix"},
			wantStatus: http.StatusCreated,
			wantID:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "export-test-app")

			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/jobs/export", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.NotEmpty(t, resp["Id"])
				assert.Equal(t, appID, resp["ApplicationId"])
				assert.Equal(t, "CREATED", resp["JobStatus"])
				assert.Equal(t, "EXPORT", resp["Type"])
			}
		})
	}
}

func TestHandler_CreateImportJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name: "creates_import_job",
			body: map[string]any{
				"RoleArn": "arn:aws:iam::123:role/import",
				"Format":  "CSV",
				"S3Url":   "s3://bucket/data.csv",
			},
			wantStatus: http.StatusCreated,
			wantID:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "import-test-app")

			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/jobs/import", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.NotEmpty(t, resp["Id"])
				assert.Equal(t, appID, resp["ApplicationId"])
				assert.Equal(t, "CREATED", resp["JobStatus"])
				assert.Equal(t, "IMPORT", resp["Type"])
			}
		})
	}
}

func TestHandler_CreateInAppTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantARN    bool
	}{
		{
			name:       "creates_inapp_template",
			body:       map[string]any{"tags": map[string]string{"env": "test"}},
			wantStatus: http.StatusCreated,
			wantARN:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/my-inapp-tpl/inapp", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantARN {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.NotEmpty(t, resp["Arn"])
				assert.Equal(t, "Created", resp["Message"])
			}
		})
	}
}

func TestHandler_CreateJourney(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name:       "creates_journey",
			body:       map[string]any{"Name": "my-journey"},
			wantStatus: http.StatusCreated,
			wantID:     true,
		},
		{
			name:       "rejects_empty_name",
			body:       map[string]any{"Name": ""},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "journey-test-app")

			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/journeys", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.NotEmpty(t, resp["Id"])
				assert.Equal(t, appID, resp["ApplicationId"])
				assert.Equal(t, "DRAFT", resp["State"])
			}
		})
	}
}

func TestHandler_CreatePushTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantARN    bool
	}{
		{
			name:       "creates_push_template",
			body:       map[string]any{},
			wantStatus: http.StatusCreated,
			wantARN:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/my-push-tpl/push", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantARN {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.NotEmpty(t, resp["Arn"])
				assert.Equal(t, "Created", resp["Message"])
			}
		})
	}
}

func TestHandler_CreateRecommenderConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name: "creates_recommender",
			body: map[string]any{
				"Name":                          "my-recommender",
				"RecommendationProviderRoleArn": "arn:aws:iam::123:role/recommender",
				"RecommendationProviderUri":     "arn:aws:personalize:us-east-1:123:campaign/my-campaign",
			},
			wantStatus: http.StatusCreated,
			wantID:     true,
		},
		{
			name:       "rejects_empty_name",
			body:       map[string]any{"Name": ""},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/recommenders", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.NotEmpty(t, resp["Id"])
				assert.Equal(t, "my-recommender", resp["Name"])
			}
		})
	}
}

func TestHandler_CreateSegment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name:       "creates_segment",
			body:       map[string]any{"Name": "my-segment"},
			wantStatus: http.StatusCreated,
			wantID:     true,
		},
		{
			name:       "rejects_empty_name",
			body:       map[string]any{"Name": ""},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "segment-test-app")

			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.NotEmpty(t, resp["Id"])
				assert.NotEmpty(t, resp["Arn"])
				assert.Equal(t, appID, resp["ApplicationId"])
				assert.Equal(t, "DIMENSIONAL", resp["SegmentType"])
			}
		})
	}
}

func TestHandler_CreateSmsTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantARN    bool
	}{
		{
			name:       "creates_sms_template",
			body:       map[string]any{"Body": "Hello {{User.UserAttributes.FirstName}}"},
			wantStatus: http.StatusCreated,
			wantARN:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/my-sms-tpl/sms", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantARN {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.NotEmpty(t, resp["Arn"])
				assert.Equal(t, "Created", resp["Message"])
			}
		})
	}
}

func TestHandler_ServiceInterface(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "Name",
			run: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, "Pinpoint", h.Name())
			},
		},
		{
			name: "ChaosServiceName",
			run: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, "mobiletargeting", h.ChaosServiceName())
			},
		},
		{
			name: "ChaosOperations",
			run: func(t *testing.T) {
				t.Helper()
				assert.NotEmpty(t, h.ChaosOperations())
			},
		},
		{
			name: "ChaosRegions",
			run: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, []string{testRegion}, h.ChaosRegions())
			},
		},
		{
			name: "RouteMatcher",
			run: func(t *testing.T) {
				t.Helper()
				assert.NotNil(t, h.RouteMatcher())
			},
		},
		{
			name: "MatchPriority",
			run: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, 87, h.MatchPriority())
			},
		},
		{
			name: "GetSupportedOperations",
			run: func(t *testing.T) {
				t.Helper()
				ops := h.GetSupportedOperations()
				assert.Contains(t, ops, "CreateCampaign")
				assert.Contains(t, ops, "CreateEmailTemplate")
				assert.Contains(t, ops, "CreateExportJob")
				assert.Contains(t, ops, "CreateImportJob")
				assert.Contains(t, ops, "CreateInAppTemplate")
				assert.Contains(t, ops, "CreateJourney")
				assert.Contains(t, ops, "CreatePushTemplate")
				assert.Contains(t, ops, "CreateRecommenderConfiguration")
				assert.Contains(t, ops, "CreateSegment")
				assert.Contains(t, ops, "CreateSmsTemplate")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	tests := []struct {
		method string
		path   string
		name   string
		wantOp string
	}{
		{name: "CreateApp", method: http.MethodPost, path: "/v1/apps", wantOp: "CreateApp"},
		{name: "GetApps", method: http.MethodGet, path: "/v1/apps", wantOp: "GetApps"},
		{name: "GetApps_trailing_slash", method: http.MethodGet, path: "/v1/apps/", wantOp: "GetApps"},
		{name: "GetApp", method: http.MethodGet, path: "/v1/apps/abc123", wantOp: "GetApp"},
		{name: "DeleteApp", method: http.MethodDelete, path: "/v1/apps/abc123", wantOp: "DeleteApp"},
		{
			name:   "ListTagsForResource",
			method: http.MethodGet,
			path:   "/v1/tags/arn%3Aaws",
			wantOp: "ListTagsForResource",
		},
		{name: "TagResource", method: http.MethodPost, path: "/v1/tags/arn%3Aaws", wantOp: "TagResource"},
		{name: "UntagResource", method: http.MethodDelete, path: "/v1/tags/arn%3Aaws", wantOp: "UntagResource"},
		{name: "TagResource_unknown_method", method: http.MethodPut, path: "/v1/tags/arn%3Aaws", wantOp: "Unknown"},
		{
			name:   "CreateRecommenderConfiguration",
			method: http.MethodPost,
			path:   "/v1/recommenders",
			wantOp: "CreateRecommenderConfiguration",
		},
		{
			name:   "CreateRecommenderConfiguration_slash",
			method: http.MethodPost,
			path:   "/v1/recommenders/",
			wantOp: "CreateRecommenderConfiguration",
		},
		{
			name:   "recommenders_list_method",
			method: http.MethodGet,
			path:   "/v1/recommenders",
			wantOp: "GetRecommenderConfigurations",
		},
		{
			name:   "CreateEmailTemplate",
			method: http.MethodPost,
			path:   "/v1/templates/my-tpl/email",
			wantOp: "CreateEmailTemplate",
		},
		{
			name:   "CreateInAppTemplate",
			method: http.MethodPost,
			path:   "/v1/templates/my-tpl/inapp",
			wantOp: "CreateInAppTemplate",
		},
		{
			name:   "CreatePushTemplate",
			method: http.MethodPost,
			path:   "/v1/templates/my-tpl/push",
			wantOp: "CreatePushTemplate",
		},
		{
			name:   "CreateSmsTemplate",
			method: http.MethodPost,
			path:   "/v1/templates/my-tpl/sms",
			wantOp: "CreateSmsTemplate",
		},
		{
			name:   "template_get_method",
			method: http.MethodGet,
			path:   "/v1/templates/my-tpl/email",
			wantOp: "GetEmailTemplate",
		},
		{name: "unknown_path", method: http.MethodGet, path: "/v1/unknown", wantOp: "Unknown"},
		{
			name:   "apps_sub_path_campaigns",
			method: http.MethodPost,
			path:   "/v1/apps/abc/campaigns",
			wantOp: "CreateCampaign",
		},
		{
			name:   "apps_sub_path_journeys",
			method: http.MethodPost,
			path:   "/v1/apps/abc/journeys",
			wantOp: "CreateJourney",
		},
		{
			name:   "apps_sub_path_segments",
			method: http.MethodPost,
			path:   "/v1/apps/abc/segments",
			wantOp: "CreateSegment",
		},
		{
			name:   "apps_sub_path_export_jobs",
			method: http.MethodPost,
			path:   "/v1/apps/abc/jobs/export",
			wantOp: "CreateExportJob",
		},
		{
			name:   "apps_sub_path_import_jobs",
			method: http.MethodPost,
			path:   "/v1/apps/abc/jobs/import",
			wantOp: "CreateImportJob",
		},
		{
			name:   "apps_sub_path_settings_get",
			method: http.MethodGet,
			path:   "/v1/apps/abc/settings",
			wantOp: "GetApplicationSettings",
		},
		{
			name:   "apps_sub_path_settings_put",
			method: http.MethodPut,
			path:   "/v1/apps/abc/settings",
			wantOp: "UpdateApplicationSettings",
		},
		{name: "apps_sub_path_unknown", method: http.MethodGet, path: "/v1/apps/abc/unknown", wantOp: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			op := h.ExtractOperation(c)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	tests := []struct {
		path         string
		name         string
		wantResource string
	}{
		{name: "apps_root", path: "/v1/apps", wantResource: ""},
		{name: "app_id", path: "/v1/apps/abc123", wantResource: "abc123"},
		{name: "app_sub_path", path: "/v1/apps/abc/campaigns", wantResource: "abc/campaigns"},
		{
			name:         "tags_arn",
			path:         "/v1/tags/arn%3Aaws%3Amobiletargeting%3Aus-east-1%3A123",
			wantResource: "arn:aws:mobiletargeting:us-east-1:123",
		},
		{name: "recommenders", path: "/v1/recommenders", wantResource: ""},
		{name: "template_path", path: "/v1/templates/my-tpl/email", wantResource: "my-tpl/email"},
		{name: "unknown_path", path: "/v1/unknown", wantResource: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			resource := h.ExtractResource(c)
			assert.Equal(t, tt.wantResource, resource)
		})
	}
}

func TestHandler_ErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		method     string
		path       string
		name       string
		wantStatus int
	}{
		{
			name:       "ServeHTTP_unknown_path",
			method:     http.MethodGet,
			path:       "/v1/unknown/path",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "dispatchApps_method_not_allowed",
			method:     http.MethodPut,
			path:       "/v1/apps",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "dispatchApp_method_not_allowed",
			method:     http.MethodPost,
			path:       "/v1/apps/abc123",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "dispatchAppSubPath_settings_method_not_allowed",
			method:     http.MethodDelete,
			path:       "/v1/apps/abc/settings",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "dispatchAppSubPath_unknown_sub_path",
			method:     http.MethodGet,
			path:       "/v1/apps/abc/unknown-resource",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "createCampaign_bad_json",
			method:     http.MethodPost,
			path:       "/v1/apps/abc/campaigns",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "createExportJob_bad_json",
			method:     http.MethodPost,
			path:       "/v1/apps/abc/jobs/export",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "createImportJob_bad_json",
			method:     http.MethodPost,
			path:       "/v1/apps/abc/jobs/import",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "createJourney_bad_json",
			method:     http.MethodPost,
			path:       "/v1/apps/abc/journeys",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "createSegment_bad_json",
			method:     http.MethodPost,
			path:       "/v1/apps/abc/segments",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "dispatchRecommenders_method_not_allowed",
			method:     http.MethodDelete,
			path:       "/v1/recommenders",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "createRecommenderConfiguration_bad_json",
			method:     http.MethodPost,
			path:       "/v1/recommenders",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "dispatchTemplates_method_not_allowed",
			method:     http.MethodPatch,
			path:       "/v1/templates/my-tpl/email",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "dispatchTemplates_unsupported_type",
			method:     http.MethodPost,
			path:       "/v1/templates/my-tpl/unknown-type",
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "createEmailTemplate_bad_json",
			method:     http.MethodPost,
			path:       "/v1/templates/my-tpl/email",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "createInAppTemplate_bad_json",
			method:     http.MethodPost,
			path:       "/v1/templates/my-tpl/inapp",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "createPushTemplate_bad_json",
			method:     http.MethodPost,
			path:       "/v1/templates/my-tpl/push",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "createSmsTemplate_bad_json",
			method:     http.MethodPost,
			path:       "/v1/templates/my-tpl/sms",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "dispatchTemplates_list",
			method:     http.MethodGet,
			path:       "/v1/templates/",
			wantStatus: http.StatusOK,
		},
		{
			name:       "handleTagResource_bad_json",
			method:     http.MethodPost,
			path:       "/v1/tags/arn%3Aaws%3Amobiletargeting%3Aus-east-1%3A123456789012%3Aapps%2Fabc",
			body:       "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "handleUntagResource_resource_not_found",
			method:     http.MethodDelete,
			path:       "/v1/tags/arn%3Aaws%3Amobiletargeting%3Aus-east-1%3A123456789012%3Aapps%2Fabc?tagKeys=foo",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "handleListTagsForResource_not_found",
			method:     http.MethodGet,
			path:       "/v1/tags/arn%3Aaws%3Amobiletargeting%3Aus-east-1%3A123456789012%3Aapps%2Fnot-found",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)

			var bodyBytes []byte
			if s, ok := tt.body.(string); ok {
				bodyBytes = []byte(s)
			} else if tt.body != nil {
				var err error
				bodyBytes, err = json.Marshal(tt.body)
				require.NoError(t, err)
			}

			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
