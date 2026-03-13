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
		})
	}
}
