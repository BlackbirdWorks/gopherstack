package lakeformation_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// newTestContext builds an echo.Context pointed at the given path with a JSON body.
func newTestContext(t *testing.T, path string, body any) (*echo.Context, *httptest.ResponseRecorder) {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(bodyBytes))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()

	c := e.NewContext(req, rec)

	return c, rec
}

func postJSON(t *testing.T, h *lakeformation.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	c, rec := newTestContext(t, path, body)
	fn := h.Handler()
	err := fn(c)
	require.NoError(t, err)

	return rec
}

// jsonDecode is a helper to decode a response body into a map.
func jsonDecode(r io.Reader, v any) error {
	return json.NewDecoder(r).Decode(v)
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

func TestHandlerReset(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddLFTagInternal("cat", "key", []string{"v"})
	require.Equal(t, 1, b.TagCount())

	h.Reset()

	assert.Equal(t, 0, b.TagCount())
}

func TestHandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	assert.Equal(t, len(h.GetSupportedOperations()), h.HandlerOpsLen())
}

func TestGetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	ops := h.GetSupportedOperations()
	assert.Len(t, ops, 61)

	expected := []string{
		"AddLFTagsToResource",
		"AssumeDecoratedRoleWithSAML",
		"BatchGrantPermissions",
		"BatchRevokePermissions",
		"CancelTransaction",
		"CommitTransaction",
		"CreateDataCellsFilter",
		"CreateLFTag",
		"CreateLFTagExpression",
		"CreateLakeFormationIdentityCenterConfiguration",
		"CreateLakeFormationOptIn",
		"DeleteDataCellsFilter",
		"DeleteLFTag",
		"DeleteLFTagExpression",
		"DeleteLakeFormationIdentityCenterConfiguration",
		"DeleteLakeFormationOptIn",
		"DeleteObjectsOnCancel",
		"DeregisterResource",
		"DescribeLakeFormationIdentityCenterConfiguration",
		"DescribeResource",
		"DescribeTransaction",
		"ExtendTransaction",
		"GetDataCellsFilter",
		"GetDataLakePrincipal",
		"GetDataLakeSettings",
		"GetEffectivePermissionsForPath",
		"GetLFTag",
		"GetLFTagExpression",
		"GetQueryState",
		"GetQueryStatistics",
		"GetResourceLFTags",
		"GetTableObjects",
		"GetTemporaryDataLocationCredentials",
		"GetTemporaryGluePartitionCredentials",
		"GetTemporaryGlueTableCredentials",
		"GetWorkUnitResults",
		"GetWorkUnits",
		"GrantPermissions",
		"ListDataCellsFilter",
		"ListLFTagExpressions",
		"ListLFTags",
		"ListLakeFormationOptIns",
		"ListPermissions",
		"ListResources",
		"ListTableStorageOptimizers",
		"ListTransactions",
		"PutDataLakeSettings",
		"RegisterResource",
		"RemoveLFTagsFromResource",
		"RevokePermissions",
		"SearchDatabasesByLFTags",
		"SearchTablesByLFTags",
		"StartQueryPlanning",
		"StartTransaction",
		"UpdateDataCellsFilter",
		"UpdateLFTag",
		"UpdateLFTagExpression",
		"UpdateLakeFormationIdentityCenterConfiguration",
		"UpdateResource",
		"UpdateTableObjects",
		"UpdateTableStorageOptimizer",
	}

	assert.Equal(t, expected, ops)
}

func TestErrValidationMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body map[string]any
		path string
	}{
		{
			name: "create_lf_tag_empty_key",
			path: "/CreateLFTag",
			body: map[string]any{"CatalogId": "cat", "TagKey": "", "TagValues": []string{"v"}},
		},
		{
			name: "create_lf_tag_empty_values",
			path: "/CreateLFTag",
			body: map[string]any{"CatalogId": "cat", "TagKey": "env"},
		},
		{
			name: "delete_lf_tag_empty_key",
			path: "/DeleteLFTag",
			body: map[string]any{"CatalogId": "cat", "TagKey": ""},
		},
		{
			name: "get_lf_tag_empty_key",
			path: "/GetLFTag",
			body: map[string]any{"CatalogId": "cat", "TagKey": ""},
		},
		{
			name: "update_lf_tag_empty_key",
			path: "/UpdateLFTag",
			body: map[string]any{"CatalogId": "cat", "TagKey": "", "TagValuesToAdd": []string{"v"}},
		},
		{
			name: "register_resource_empty_arn",
			path: "/RegisterResource",
			body: map[string]any{"ResourceArn": "", "RoleArn": "arn:aws:iam::123:role/r"},
		},
		{
			name: "deregister_resource_empty_arn",
			path: "/DeregisterResource",
			body: map[string]any{"ResourceArn": ""},
		},
		{
			name: "describe_resource_empty_arn",
			path: "/DescribeResource",
			body: map[string]any{"ResourceArn": ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			h := lakeformation.NewHandler(b)
			rec := postJSON(t, h, tt.path, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "InvalidInputException", resp["__type"])
		})
	}
}

func TestUnknownOperationReturns400(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	c, rec := newTestContext(t, "/UnknownOp", nil)
	fn := h.Handler()
	require.NoError(t, fn(c))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestNonPostMethodReturns405(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/GetDataLakeSettings", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	fn := h.Handler()
	require.NoError(t, fn(c))
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}
