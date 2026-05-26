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

func TestHandler_AddLFTagsToResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupFn    func(b *lakeformation.InMemoryBackend)
		body       string
		wantStatus int
		wantFails  int
	}{
		{
			name: "success_with_existing_tag",
			setupFn: func(b *lakeformation.InMemoryBackend) {
				require.NoError(t, b.CreateLFTag("123456789012", "env", []string{"dev", "prod"}))
			},
			body: `{"CatalogId":"123456789012","Resource":{"Database":{"Name":"mydb"}},` +
				`"LFTags":[{"TagKey":"env","TagValues":["dev"]}]}`,
			wantStatus: http.StatusOK,
			wantFails:  0,
		},
		{
			name:    "tag_not_found_returns_failures",
			setupFn: nil,
			body: `{"CatalogId":"123456789012","Resource":{"Database":{"Name":"mydb"}},` +
				`"LFTags":[{"TagKey":"nonexistent","TagValues":["v1"]}]}`,
			wantStatus: http.StatusOK,
			wantFails:  1,
		},
		{
			name:       "missing_resource",
			body:       `{"LFTags":[{"TagKey":"env","TagValues":["dev"]}]}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_lftags",
			body:       `{"Resource":{"Database":{"Name":"mydb"}}}`,
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

			b := lakeformation.NewInMemoryBackend()
			if tt.setupFn != nil {
				tt.setupFn(b)
			}

			h := lakeformation.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doLFRequest(t, h, "/AddLFTagsToResource", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				failures, _ := resp["Failures"].([]any)
				assert.Len(t, failures, tt.wantFails)
			}
		})
	}
}

func TestHandler_AssumeDecoratedRoleWithSAML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantFields []string
		wantStatus int
	}{
		{
			name: "success",
			body: `{"PrincipalArn":"arn:aws:iam::123:saml-provider/MyProvider",` +
				`"RoleArn":"arn:aws:iam::123:role/MyRole",` +
				`"SAMLAssertion":"c2FtbGFzc2VydGlvbg=="}`,
			wantStatus: http.StatusOK,
			wantFields: []string{"AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration"},
		},
		{
			name:       "missing_principal_arn",
			body:       `{"RoleArn":"arn:aws:iam::123:role/R","SAMLAssertion":"abc"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_role_arn",
			body:       `{"PrincipalArn":"arn:aws:iam::123:saml-provider/P","SAMLAssertion":"abc"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_saml_assertion",
			body:       `{"PrincipalArn":"arn:aws:iam::123:saml-provider/P","RoleArn":"arn:aws:iam::123:role/R"}`,
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

			rec := doLFRequest(t, h, "/AssumeDecoratedRoleWithSAML", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				for _, field := range tt.wantFields {
					assert.Contains(t, resp, field)
					assert.NotEmpty(t, resp[field])
				}
			}
		})
	}
}

func TestHandler_CancelTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		transactionID string
		setupFn       func(b *lakeformation.InMemoryBackend)
		body          string
		wantStatus    int
	}{
		{
			name:          "cancel_new_transaction",
			transactionID: "txn-0001",
			body:          `{"TransactionId":"txn-0001"}`,
			wantStatus:    http.StatusOK,
		},
		{
			name:          "cancel_already_committed",
			transactionID: "txn-0002",
			setupFn: func(b *lakeformation.InMemoryBackend) {
				_, err := b.CommitTransaction("txn-0002")
				require.NoError(t, err)
			},
			body:       `{"TransactionId":"txn-0002"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_transaction_id",
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

			b := lakeformation.NewInMemoryBackend()
			if tt.setupFn != nil {
				tt.setupFn(b)
			}

			h := lakeformation.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doLFRequest(t, h, "/CancelTransaction", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CommitTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn     func(b *lakeformation.InMemoryBackend)
		name        string
		body        string
		wantStatus2 string
		wantStatus  int
	}{
		{
			name:        "commit_new_transaction",
			body:        `{"TransactionId":"txn-1001"}`,
			wantStatus:  http.StatusOK,
			wantStatus2: "COMMITTED",
		},
		{
			name: "commit_already_cancelled",
			setupFn: func(b *lakeformation.InMemoryBackend) {
				require.NoError(t, b.CancelTransaction("txn-1002"))
			},
			body:       `{"TransactionId":"txn-1002"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_transaction_id",
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

			b := lakeformation.NewInMemoryBackend()
			if tt.setupFn != nil {
				tt.setupFn(b)
			}

			h := lakeformation.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doLFRequest(t, h, "/CommitTransaction", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK && tt.wantStatus2 != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantStatus2, resp["TransactionStatus"])
			}
		})
	}
}

func TestHandler_CreateDataCellsFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "success",
			body: `{"TableData":{"TableCatalogId":"123456789012","DatabaseName":"db1",` +
				`"TableName":"tbl1","Name":"filter1"}}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_table_data",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_name",
			body:       `{"TableData":{"TableCatalogId":"123456789012","DatabaseName":"db1","TableName":"tbl1"}}`,
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

			rec := doLFRequest(t, h, "/CreateDataCellsFilter", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateDataCellsFilter_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	body := `{"TableData":{"TableCatalogId":"123456789012","DatabaseName":"db1","TableName":"tbl1","Name":"dup"}}`

	rec := doLFRequest(t, h, "/CreateDataCellsFilter", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doLFRequest(t, h, "/CreateDataCellsFilter", body)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandler_CreateLFTagExpression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"Name":"my-expr","Expression":[{"TagKey":"env","TagValues":["prod"]}]}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_name",
			body:       `{"Expression":[{"TagKey":"env","TagValues":["prod"]}]}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_expression",
			body:       `{"Name":"my-expr"}`,
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

			rec := doLFRequest(t, h, "/CreateLFTagExpression", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateLFTagExpression_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	body := `{"Name":"dup-expr","Expression":[{"TagKey":"env","TagValues":["prod"]}]}`

	rec := doLFRequest(t, h, "/CreateLFTagExpression", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doLFRequest(t, h, "/CreateLFTagExpression", body)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandler_CreateLakeFormationIdentityCenterConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "success_with_instance_arn",
			body:       `{"CatalogId":"123456789012","InstanceArn":"arn:aws:sso:::instance/ssoins-123"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "success_no_catalog_id",
			body:       `{"InstanceArn":"arn:aws:sso:::instance/ssoins-456"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty_body",
			body:       `{}`,
			wantStatus: http.StatusOK,
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

			rec := doLFRequest(t, h, "/CreateLakeFormationIdentityCenterConfiguration", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "ApplicationArn")
			}
		})
	}
}

func TestHandler_CreateLakeFormationOptIn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "success",
			body: `{"Principal":{"DataLakePrincipalIdentifier":"arn:aws:iam::123:role/MyRole"},` +
				`"Resource":{"Database":{"Name":"mydb"}}}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_principal",
			body:       `{"Resource":{"Database":{"Name":"mydb"}}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_resource",
			body:       `{"Principal":{"DataLakePrincipalIdentifier":"arn:aws:iam::123:role/R"}}`,
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

			rec := doLFRequest(t, h, "/CreateLakeFormationOptIn", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateLakeFormationOptIn_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	body := `{"Principal":{"DataLakePrincipalIdentifier":"arn:aws:iam::123:role/R"},` +
		`"Resource":{"Database":{"Name":"db1"}}}`

	rec := doLFRequest(t, h, "/CreateLakeFormationOptIn", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doLFRequest(t, h, "/CreateLakeFormationOptIn", body)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandler_DeleteDataCellsFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupFn    func(b *lakeformation.InMemoryBackend)
		body       string
		wantStatus int
	}{
		{
			name: "success",
			setupFn: func(b *lakeformation.InMemoryBackend) {
				err := b.CreateDataCellsFilter(&lakeformation.DataCellsFilter{
					TableCatalogID: "123456789012",
					DatabaseName:   "db1",
					TableName:      "tbl1",
					Name:           "filter1",
				})
				require.NoError(t, err)
			},
			body:       `{"TableCatalogId":"123456789012","DatabaseName":"db1","TableName":"tbl1","Name":"filter1"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			body:       `{"TableCatalogId":"123456789012","DatabaseName":"db1","TableName":"tbl1","Name":"nofilter"}`,
			wantStatus: http.StatusNotFound,
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

			b := lakeformation.NewInMemoryBackend()
			if tt.setupFn != nil {
				tt.setupFn(b)
			}

			h := lakeformation.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doLFRequest(t, h, "/DeleteDataCellsFilter", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteLFTagExpression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupFn    func(b *lakeformation.InMemoryBackend)
		body       string
		wantStatus int
	}{
		{
			name: "success",
			setupFn: func(b *lakeformation.InMemoryBackend) {
				err := b.CreateLFTagExpression("my-expr", "desc", "123456789012", []lakeformation.LFTag{
					{TagKey: "env", TagValues: []string{"prod"}},
				})
				require.NoError(t, err)
			},
			body:       `{"Name":"my-expr","CatalogId":"123456789012"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			body:       `{"Name":"nonexistent"}`,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing_name",
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

			b := lakeformation.NewInMemoryBackend()
			if tt.setupFn != nil {
				tt.setupFn(b)
			}

			h := lakeformation.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doLFRequest(t, h, "/DeleteLFTagExpression", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_TransactionLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("cancel_then_commit_fails", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler()

		rec := doLFRequest(t, h, "/CancelTransaction", `{"TransactionId":"txn-lifecycle-1"}`)
		assert.Equal(t, http.StatusOK, rec.Code)

		rec = doLFRequest(t, h, "/CommitTransaction", `{"TransactionId":"txn-lifecycle-1"}`)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("commit_then_cancel_fails", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler()

		rec := doLFRequest(t, h, "/CommitTransaction", `{"TransactionId":"txn-lifecycle-2"}`)
		assert.Equal(t, http.StatusOK, rec.Code)

		rec = doLFRequest(t, h, "/CancelTransaction", `{"TransactionId":"txn-lifecycle-2"}`)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandler_AddLFTagsToResource_RouteMatches(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	matcher := h.RouteMatcher()

	tests := []struct {
		name       string
		path       string
		authHeader string
		want       bool
	}{
		{
			name:       "add_lf_tags_to_resource",
			path:       "/AddLFTagsToResource",
			authHeader: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/lakeformation/aws4_request",
			want:       true,
		},
		{
			name:       "cancel_transaction",
			path:       "/CancelTransaction",
			authHeader: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/lakeformation/aws4_request",
			want:       true,
		},
		{
			name:       "create_data_cells_filter",
			path:       "/CreateDataCellsFilter",
			authHeader: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/lakeformation/aws4_request",
			want:       true,
		},
		{
			name:       "create_lf_tag_expression",
			path:       "/CreateLFTagExpression",
			authHeader: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/lakeformation/aws4_request",
			want:       true,
		},
		{
			name:       "create_identity_center_config",
			path:       "/CreateLakeFormationIdentityCenterConfiguration",
			authHeader: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/lakeformation/aws4_request",
			want:       true,
		},
	}

	e := echo.New()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

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

func TestBackend_AddLFTagsToResource_UpdateExisting(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	require.NoError(t, b.CreateLFTag("", "env", []string{"dev", "prod"}))

	resource := &lakeformation.Resource{
		Database: &lakeformation.DatabaseResource{Name: "mydb"},
	}
	tags := []lakeformation.LFTagPair{{TagKey: "env", TagValues: []string{"dev"}}}

	failures := b.AddLFTagsToResource("", resource, tags)
	assert.Empty(t, failures)

	// Update same tag with a valid value from the allowed set
	tags2 := []lakeformation.LFTagPair{{TagKey: "env", TagValues: []string{"prod"}}}
	failures2 := b.AddLFTagsToResource("", resource, tags2)
	assert.Empty(t, failures2)
}

func TestBackend_CancelAndCommitTransaction_Idempotent(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()

	// Cancel twice is fine
	require.NoError(t, b.CancelTransaction("txn-idem-1"))
	require.NoError(t, b.CancelTransaction("txn-idem-1"))

	// Commit twice is fine
	status, err := b.CommitTransaction("txn-idem-2")
	require.NoError(t, err)
	assert.Equal(t, "COMMITTED", status)

	status2, err2 := b.CommitTransaction("txn-idem-2")
	require.NoError(t, err2)
	assert.Equal(t, "COMMITTED", status2)
}

func TestBackend_CreateLakeFormationIdentityCenterConfiguration_ReturnsARN(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	appArn := b.CreateLakeFormationIdentityCenterConfiguration("123456789012", "arn:aws:sso:::instance/x")
	assert.NotEmpty(t, appArn)
	assert.Contains(t, appArn, "123456789012")
}

func TestBackend_AssumeDecoratedRoleWithSAML_ReturnsCreds(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	out := b.AssumeDecoratedRoleWithSAML("arn:principal", "arn:role", "assertion", nil)
	assert.NotNil(t, out)
	assert.NotEmpty(t, out.AccessKeyID)
	assert.NotEmpty(t, out.SecretAccessKey)
	assert.NotEmpty(t, out.SessionToken)
	assert.NotEmpty(t, out.Expiration)
}
