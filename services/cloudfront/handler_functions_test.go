package cloudfront_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestFunctionStatusDevelopment verifies created/updated functions use DEVELOPMENT status.
func TestFunctionStatusDevelopment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		body     string
		wantIn   string
		wantCode int
	}{
		{
			name:   "create_function_status_development",
			method: http.MethodPost,
			path:   "/2020-05-31/function",
			body: `<CreateFunctionRequest>
				<Name>my-function</Name>
				<FunctionConfig>
					<Runtime>cloudfront-js-2.0</Runtime>
					<Comment>test</Comment>
				</FunctionConfig>
				<FunctionCode>ZnVuY3Rpb24gaGFuZGxlcihldmVudCkge30=</FunctionCode>
			</CreateFunctionRequest>`,
			wantCode: http.StatusCreated,
			wantIn:   "DEVELOPMENT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend(t)
			h := cloudfront.NewHandler(b)
			rec := doReq(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.wantIn)
		})
	}
}

// TestFunctionRuntimeValidation verifies invalid runtimes are rejected.
func TestFunctionRuntimeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name: "invalid_runtime_rejected",
			body: `<CreateFunctionRequest>
				<Name>bad-function</Name>
				<FunctionConfig>
					<Runtime>nodejs18.x</Runtime>
					<Comment>bad runtime</Comment>
				</FunctionConfig>
				<FunctionCode>ZnVuY3Rpb24gaGFuZGxlcihldmVudCkge30=</FunctionCode>
			</CreateFunctionRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "valid_runtime_js1_accepted",
			body: `<CreateFunctionRequest>
				<Name>js1-function</Name>
				<FunctionConfig>
					<Runtime>cloudfront-js-1.0</Runtime>
					<Comment>js1</Comment>
				</FunctionConfig>
				<FunctionCode>ZnVuY3Rpb24gaGFuZGxlcihldmVudCkge30=</FunctionCode>
			</CreateFunctionRequest>`,
			wantCode: http.StatusCreated,
		},
		{
			name: "valid_runtime_js2_accepted",
			body: `<CreateFunctionRequest>
				<Name>js2-function</Name>
				<FunctionConfig>
					<Runtime>cloudfront-js-2.0</Runtime>
					<Comment>js2</Comment>
				</FunctionConfig>
				<FunctionCode>ZnVuY3Rpb24gaGFuZGxlcihldmVudCkge30=</FunctionCode>
			</CreateFunctionRequest>`,
			wantCode: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newAuditBackend(t)
			h := cloudfront.NewHandler(b)
			rec := doReq(t, h, http.MethodPost, "/2020-05-31/function", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}

// TestCloudFrontFunctionCRUD covers function create, get, describe, list, publish, update, delete, test.
func TestCloudFrontFunctionCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *cloudfront.Handler) string
		check      func(*testing.T, *httptest.ResponseRecorder, string)
		headers    func(*testing.T, *cloudfront.Handler, string) map[string]string
		name       string
		method     string
		path       string
		body       []byte
		wantStatus int
	}{
		{
			name:   "create_function",
			method: http.MethodPost,
			path:   "/2020-05-31/function",
			body: []byte(
				`<CreateFunctionRequest>` +
					`<Name>my-fn</Name>` +
					`<FunctionConfig>` +
					`<Comment>test fn</Comment><Runtime>cloudfront-js-2.0</Runtime>` +
					`</FunctionConfig>` +
					`<FunctionCode>ZnVuY3Rpb24gaGFuZGxlcihldmVudCkge3JldHVybiBldmVudC5yZXF1ZXN0O30=</FunctionCode>` +
					`</CreateFunctionRequest>`,
			),
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "FunctionSummary")
				assert.Contains(t, rec.Body.String(), "my-fn")
				assert.NotEmpty(t, rec.Header().Get("Location"))
				// FunctionMetadata.FunctionARN and LastModifiedTime are required members
				// of the real aws-sdk-go-v2 FunctionMetadata shape: without FunctionARN a
				// real SDK caller has no way to attach the function to a distribution's
				// FunctionAssociations, since those require the ARN, not the name.
				assert.Contains(t, rec.Body.String(), "<FunctionARN>")
				assert.Contains(t, rec.Body.String(), "arn:aws:cloudfront")
				assert.Contains(t, rec.Body.String(), "<CreatedTime>")
				assert.Contains(t, rec.Body.String(), "<LastModifiedTime>")
			},
		},
		{
			name:   "get_function",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateFunction("get-fn", "comment", "cloudfront-js-2.0", "code")
				require.NoError(t, err)

				return "/2020-05-31/function/get-fn"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "get-fn")
			},
		},
		{
			name:   "get_function_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/function/DOESNOTEXIST",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "NoSuchFunctionExists")
			},
		},
		{
			name:   "describe_function",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateFunction("desc-fn", "comment", "cloudfront-js-2.0", "code")
				require.NoError(t, err)

				return "/2020-05-31/function/desc-fn/describe"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "desc-fn")
			},
		},
		{
			name:   "list_functions",
			method: http.MethodGet,
			path:   "/2020-05-31/function",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateFunction("list-fn", "comment", "cloudfront-js-2.0", "code")
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "FunctionList")
				assert.Contains(t, rec.Body.String(), "list-fn")
			},
		},
		{
			name:   "publish_function",
			method: http.MethodPost,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateFunction("pub-fn", "comment", "cloudfront-js-2.0", "code")
				require.NoError(t, err)

				return "/2020-05-31/function/pub-fn/publish"
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				name := strings.TrimPrefix(strings.TrimSuffix(path, "/publish"), "/2020-05-31/function/")
				fn, err := h.Backend.GetFunction(name)
				require.NoError(t, err)

				return map[string]string{"If-Match": fn.ETag}
			},
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "LIVE")
			},
		},
		{
			name:   "update_function",
			method: http.MethodPut,
			path:   "",
			body: []byte(
				`<CreateFunctionRequest>` +
					`<Name>upd-fn</Name>` +
					`<FunctionConfig><Comment>updated</Comment><Runtime>cloudfront-js-2.0</Runtime></FunctionConfig>` +
					`</CreateFunctionRequest>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateFunction("upd-fn", "original", "cloudfront-js-2.0", "code")
				require.NoError(t, err)

				return "/2020-05-31/function/upd-fn"
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				name := strings.TrimPrefix(path, "/2020-05-31/function/")
				fn, err := h.Backend.GetFunction(name)
				require.NoError(t, err)

				return map[string]string{"If-Match": fn.ETag}
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "upd-fn")
			},
		},
		{
			name:   "delete_function",
			method: http.MethodDelete,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateFunction("del-fn", "comment", "cloudfront-js-2.0", "code")
				require.NoError(t, err)

				return "/2020-05-31/function/del-fn"
			},
			headers: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				name := strings.TrimPrefix(path, "/2020-05-31/function/")
				fn, err := h.Backend.GetFunction(name)
				require.NoError(t, err)

				return map[string]string{"If-Match": fn.ETag}
			},
			wantStatus: http.StatusNoContent,
			check:      func(t *testing.T, _ *httptest.ResponseRecorder, _ string) { t.Helper() },
		},
		{
			name:   "test_function",
			method: http.MethodPost,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateFunction("test-fn", "comment", "cloudfront-js-2.0", "code")
				require.NoError(t, err)

				return "/2020-05-31/function/test-fn/test"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "TestResult")
				assert.Contains(t, rec.Body.String(), "test-fn")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			path := tt.path

			if tt.setup != nil {
				if p := tt.setup(t, h); p != "" {
					path = p
				}
			}

			var hdrs map[string]string
			if tt.headers != nil {
				hdrs = tt.headers(t, h, path)
			}

			rec := doXMLWithHeaders(t, h, tt.method, path, tt.body, hdrs)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.check != nil {
				tt.check(t, rec, path)
			}
		})
	}
}
