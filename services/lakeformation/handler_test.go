package lakeformation_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

const (
	testAccountID = "123456789012"
	testRegion    = "us-east-1"
)

func newTestHandler() *lakeformation.Handler {
	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h
}

func doLFRequest(t *testing.T, h *lakeformation.Handler, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()

	var req *http.Request

	if body != "" {
		req = httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	} else {
		req = httptest.NewRequest(http.MethodPost, path, http.NoBody)
	}

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_GetDataLakeSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "empty_body",
			body:       "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "with_catalog_id",
			body:       `{"CatalogId":"123456789012"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doLFRequest(t, h, "/GetDataLakeSettings", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Contains(t, resp, "DataLakeSettings")
		})
	}
}

func TestHandler_PutDataLakeSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "valid_settings",
			body: `{"DataLakeSettings":{"DataLakeAdmins":[` +
				`{"DataLakePrincipalIdentifier":"arn:aws:iam::123456789012:user/admin"}]}}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_settings",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_json",
			body:       `not-json`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doLFRequest(t, h, "/PutDataLakeSettings", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_RegisterDeregisterDescribeResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		resourceArn     string
		roleArn         string
		wantRegStatus   int
		wantDescStatus  int
		wantDeregStatus int
	}{
		{
			name:            "full_lifecycle",
			resourceArn:     "arn:aws:s3:::my-bucket",
			roleArn:         "arn:aws:iam::123456789012:role/MyRole",
			wantRegStatus:   http.StatusOK,
			wantDescStatus:  http.StatusOK,
			wantDeregStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Register
			body := `{"ResourceArn":"` + tt.resourceArn + `","RoleArn":"` + tt.roleArn + `"}`
			rec := doLFRequest(t, h, "/RegisterResource", body)
			assert.Equal(t, tt.wantRegStatus, rec.Code)

			// Describe
			descBody := `{"ResourceArn":"` + tt.resourceArn + `"}`
			rec = doLFRequest(t, h, "/DescribeResource", descBody)
			assert.Equal(t, tt.wantDescStatus, rec.Code)

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
			ri, ok := descResp["ResourceInfo"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.resourceArn, ri["ResourceArn"])

			// Deregister
			rec = doLFRequest(t, h, "/DeregisterResource", descBody)
			assert.Equal(t, tt.wantDeregStatus, rec.Code)

			// Describe after deregister → 404
			rec = doLFRequest(t, h, "/DescribeResource", descBody)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_RegisterResource_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	body := `{"ResourceArn":"arn:aws:s3:::bucket","RoleArn":"arn:aws:iam::123:role/R"}`
	rec := doLFRequest(t, h, "/RegisterResource", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doLFRequest(t, h, "/RegisterResource", body)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandler_ListResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		setupArns  []string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "empty",
			body:       "{}",
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name:       "with_resources",
			setupArns:  []string{"arn:aws:s3:::a", "arn:aws:s3:::b"},
			body:       "{}",
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			for _, arn := range tt.setupArns {
				require.NoError(t, b.RegisterResource(arn, "arn:aws:iam::123:role/R"))
			}

			rec := doLFRequest(t, h, "/ListResources", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			list, _ := resp["ResourceInfoList"].([]any)
			assert.Len(t, list, tt.wantCount)
		})
	}
}

func TestHandler_GrantRevokeListPermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		principal   string
		resourceArn string
		wantStatus  int
	}{
		{
			name:        "grant_list_revoke",
			principal:   "arn:aws:iam::123456789012:user/alice",
			resourceArn: "arn:aws:s3:::my-bucket",
			wantStatus:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			grantBody := `{"Principal":{"DataLakePrincipalIdentifier":"` + tt.principal +
				`"},"Resource":{"DataLocation":{"ResourceArn":"` + tt.resourceArn +
				`"}},"Permissions":["DATA_LOCATION_ACCESS"]}`
			rec := doLFRequest(t, h, "/GrantPermissions", grantBody)
			assert.Equal(t, tt.wantStatus, rec.Code)

			listBody := `{"ResourceArn":"` + tt.resourceArn + `"}`
			rec = doLFRequest(t, h, "/ListPermissions", listBody)
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			perms, _ := listResp["PrincipalResourcePermissions"].([]any)
			assert.Len(t, perms, 1)

			rec = doLFRequest(t, h, "/RevokePermissions", grantBody)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_LFTagLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		catalogID  string
		tagKey     string
		tagValues  string
		wantStatus int
	}{
		{
			name:       "full_lifecycle",
			catalogID:  "123456789012",
			tagKey:     "env",
			tagValues:  `["dev","prod"]`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Create
			createBody := `{"CatalogId":"` + tt.catalogID + `","TagKey":"` + tt.tagKey + `","TagValues":` + tt.tagValues + `}`
			rec := doLFRequest(t, h, "/CreateLFTag", createBody)
			assert.Equal(t, tt.wantStatus, rec.Code)

			// Get
			getBody := `{"CatalogId":"` + tt.catalogID + `","TagKey":"` + tt.tagKey + `"}`
			rec = doLFRequest(t, h, "/GetLFTag", getBody)
			assert.Equal(t, http.StatusOK, rec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.tagKey, getResp["TagKey"])

			// Update
			updateBody := `{"CatalogId":"` + tt.catalogID + `","TagKey":"` + tt.tagKey +
				`","TagValuesToAdd":["staging"],"TagValuesToDelete":["dev"]}`
			rec = doLFRequest(t, h, "/UpdateLFTag", updateBody)
			assert.Equal(t, http.StatusOK, rec.Code)

			// List
			listBody := `{"CatalogId":"` + tt.catalogID + `"}`
			rec = doLFRequest(t, h, "/ListLFTags", listBody)
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			tags, _ := listResp["LFTags"].([]any)
			assert.Len(t, tags, 1)

			// Delete
			rec = doLFRequest(t, h, "/DeleteLFTag", getBody)
			assert.Equal(t, http.StatusOK, rec.Code)

			// Get after delete → 404
			rec = doLFRequest(t, h, "/GetLFTag", getBody)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestHandler_BatchGrantRevokePermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "batch_grant_success",
			body: `{"Entries":[{"Principal":{"DataLakePrincipalIdentifier":"arn:aws:iam::123:user/a"},` +
				`"Resource":{"DataLocation":{"ResourceArn":"arn:aws:s3:::b"}},` +
				`"Permissions":["DATA_LOCATION_ACCESS"]}]}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doLFRequest(t, h, "/BatchGrantPermissions", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			failures, _ := resp["Failures"].([]any)
			assert.Empty(t, failures)

			rec = doLFRequest(t, h, "/BatchRevokePermissions", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/UnknownOp", http.NoBody)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/GetDataLakeSettings", http.NoBody)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		authHeader string
		want       bool
	}{
		{
			name:       "matching_path_and_service",
			path:       "/GetDataLakeSettings",
			authHeader: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/lakeformation/aws4_request",
			want:       true,
		},
		{
			name:       "wrong_service",
			path:       "/GetDataLakeSettings",
			authHeader: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request",
			want:       false,
		},
		{
			name: "unknown_path",
			path: "/SomeOtherPath",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, http.NoBody)

			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		wantOp string
	}{
		{
			name:   "get_settings",
			path:   "/GetDataLakeSettings",
			wantOp: "GetDataLakeSettings",
		},
		{
			name:   "create_lf_tag",
			path:   "/CreateLFTag",
			wantOp: "CreateLFTag",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, http.NoBody)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "provider name", want: "LakeFormation"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &lakeformation.Provider{}
			assert.Equal(t, tt.want, p.Name())

			ctx := &service.AppContext{Logger: slog.Default()}
			svc, err := p.Init(ctx)
			require.NoError(t, err)
			assert.NotNil(t, svc)
		})
	}
}

func TestHandler_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		body       string
		wantStatus int
	}{
		{
			name:       "RegisterResource missing ResourceArn",
			path:       "/lakeformation/RegisterResource",
			body:       `{"RoleArn":"arn:role"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "DeregisterResource missing ResourceArn",
			path:       "/lakeformation/DeregisterResource",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "GrantPermissions missing Principal",
			path:       "/lakeformation/GrantPermissions",
			body:       `{"Resource":{"Catalog":{}}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "RevokePermissions missing Principal",
			path:       "/lakeformation/RevokePermissions",
			body:       `{"Resource":{"Catalog":{}}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "CreateLFTag missing TagKey",
			path:       "/lakeformation/CreateLFTag",
			body:       `{"TagValues":["v1"]}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "DeleteLFTag missing TagKey",
			path:       "/lakeformation/DeleteLFTag",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "UpdateLFTag missing TagKey",
			path:       "/lakeformation/UpdateLFTag",
			body:       `{"TagValuesToAdd":["v1"]}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "BatchGrantPermissions missing Entries",
			path:       "/lakeformation/BatchGrantPermissions",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "BatchRevokePermissions missing Entries",
			path:       "/lakeformation/BatchRevokePermissions",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			result := doLFRequest(t, h, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns service name", want: "LakeFormation"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			assert.Equal(t, tt.want, h.Name())
		})
	}
}

func TestHandler_ChaosAndPriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		testFunc func(h *lakeformation.Handler) any
		name     string
	}{
		{
			name:     "ChaosServiceName returns non-empty string",
			testFunc: func(h *lakeformation.Handler) any { return h.ChaosServiceName() != "" },
		},
		{
			name:     "ChaosOperations returns non-empty slice",
			testFunc: func(h *lakeformation.Handler) any { return len(h.ChaosOperations()) > 0 },
		},
		{
			name:     "ChaosRegions returns non-empty slice",
			testFunc: func(h *lakeformation.Handler) any { return len(h.ChaosRegions()) > 0 },
		},
		{
			name:     "GetSupportedOperations returns non-empty slice",
			testFunc: func(h *lakeformation.Handler) any { return len(h.GetSupportedOperations()) > 0 },
		},
		{
			name:     "MatchPriority returns positive int",
			testFunc: func(h *lakeformation.Handler) any { return h.MatchPriority() > 0 },
		},
		{
			name: "ExtractResource returns empty string",
			testFunc: func(h *lakeformation.Handler) any {
				e := echo.New()
				req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
				rec := httptest.NewRecorder()
				c := e.NewContext(req, rec)

				return h.ExtractResource(c) == ""
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			result := tt.testFunc(h)
			b, ok := result.(bool)
			require.True(t, ok)
			assert.True(t, b)
		})
	}
}
