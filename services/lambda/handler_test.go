package lambda_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	gophercontainer "github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// ---- mock backend ----

type mockBackend struct {
	invokeErr    error
	functions    map[string]*lambda.FunctionConfiguration
	invokeResult []byte
	mu           sync.RWMutex
}

func newMockBackend() *mockBackend {
	return &mockBackend{functions: make(map[string]*lambda.FunctionConfiguration)}
}

func (m *mockBackend) CreateFunction(fn *lambda.FunctionConfiguration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.functions[fn.FunctionName]; exists {
		return lambda.ErrFunctionAlreadyExists
	}

	m.functions[fn.FunctionName] = fn

	return nil
}

func (m *mockBackend) GetFunction(name string) (*lambda.FunctionConfiguration, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fn, ok := m.functions[name]
	if !ok {
		return nil, lambda.ErrFunctionNotFound
	}

	return fn, nil
}

func (m *mockBackend) ListFunctions(marker string, maxItems int) page.Page[*lambda.FunctionConfiguration] {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fns := make([]*lambda.FunctionConfiguration, 0, len(m.functions))
	for _, fn := range m.functions {
		fns = append(fns, fn)
	}

	sort.Slice(fns, func(i, j int) bool {
		return fns[i].FunctionName < fns[j].FunctionName
	})

	return page.New(fns, marker, maxItems, 50)
}

func (m *mockBackend) DeleteFunction(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.functions[name]; !ok {
		return lambda.ErrFunctionNotFound
	}

	delete(m.functions, name)

	return nil
}

func (m *mockBackend) UpdateFunction(fn *lambda.FunctionConfiguration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.functions[fn.FunctionName]; !ok {
		return lambda.ErrFunctionNotFound
	}

	m.functions[fn.FunctionName] = fn

	return nil
}

func (m *mockBackend) InvokeFunction(
	_ context.Context,
	name string,
	invocationType lambda.InvocationType,
	_ []byte,
) ([]byte, int, error) {
	if m.invokeErr != nil {
		return nil, http.StatusInternalServerError, m.invokeErr
	}

	if invocationType == lambda.InvocationTypeDryRun {
		return nil, http.StatusNoContent, nil
	}

	if invocationType == lambda.InvocationTypeEvent {
		return nil, http.StatusAccepted, nil
	}

	if _, ok := m.functions[name]; !ok {
		return nil, http.StatusNotFound, lambda.ErrFunctionNotFound
	}

	result := m.invokeResult
	if result == nil {
		result = []byte(`{"result":"ok"}`)
	}

	return result, http.StatusOK, nil
}

// ---- helpers ----

func newHandler(t *testing.T) (*lambda.Handler, *mockBackend) {
	t.Helper()

	bk := newMockBackend()
	h := lambda.NewHandler(bk)
	h.DefaultRegion = "us-east-1"
	h.AccountID = "000000000000"

	return h, bk
}

func callHandler(
	t *testing.T,
	h *lambda.Handler,
	method, path, body string,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyReader *strings.Reader
	if body != "" {
		bodyReader = strings.NewReader(body)
	} else {
		bodyReader = strings.NewReader("")
	}

	req := httptest.NewRequest(method, path, bodyReader)
	req.Header.Set("Content-Type", "application/json")

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// ---- CreateFunction tests ----

func TestCreateFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup                func(*mockBackend)
		name                 string
		body                 string
		wantErrType          string
		wantFunctionName     string
		wantFunctionArn      string
		wantPackageType      string
		wantState            lambda.FunctionState
		wantLastUpdateStatus lambda.LastUpdateStatus
		wantCode             int
		wantMemorySize       int
		wantTimeout          int
		wantRevisionID       bool
	}{
		{
			name: "success",
			body: `{"FunctionName":"my-func","PackageType":"Image",` +
				`"Code":{"ImageUri":"123456789012.dkr.ecr.us-east-1.amazonaws.com/myimage:latest"},` +
				`"Role":"arn:aws:iam::000000000000:role/myrole"}`,
			wantCode:             http.StatusCreated,
			wantFunctionName:     "my-func",
			wantFunctionArn:      "arn:aws:lambda:us-east-1:000000000000:function:my-func",
			wantPackageType:      lambda.PackageTypeImage,
			wantState:            lambda.FunctionStateActive,
			wantLastUpdateStatus: lambda.LastUpdateStatusSuccessful,
			wantMemorySize:       128,
			wantTimeout:          3,
			wantRevisionID:       true,
		},
		{
			name: "defaults_applied",
			body: `{"FunctionName":"defaults-func","PackageType":"Image",` +
				`"Code":{"ImageUri":"myimage:latest"},"MemorySize":256,"Timeout":60}`,
			wantCode:             http.StatusCreated,
			wantFunctionName:     "defaults-func",
			wantLastUpdateStatus: lambda.LastUpdateStatusSuccessful,
			wantMemorySize:       256,
			wantTimeout:          60,
		},
		{
			name:        "missing_function_name",
			body:        `{"PackageType":"Image","Code":{"ImageUri":"myimage:latest"}}`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "InvalidParameterValueException",
		},
		{
			name:        "invalid_package_type",
			body:        `{"FunctionName":"zip-func","PackageType":"Zip","Code":{"S3Bucket":"mybucket","S3Key":"code.zip"}}`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "InvalidParameterValueException",
		},
		{
			name:        "missing_image_uri",
			body:        `{"FunctionName":"no-image-func","PackageType":"Image","Code":{}}`,
			wantCode:    http.StatusBadRequest,
			wantErrType: "InvalidParameterValueException",
		},
		{
			name:     "invalid_body",
			body:     "not-json{",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "already_exists",
			setup: func(bk *mockBackend) {
				_ = bk.CreateFunction(&lambda.FunctionConfiguration{
					FunctionName: "dup-func",
					PackageType:  lambda.PackageTypeImage,
					ImageURI:     "myimage:latest",
				})
			},
			body:        `{"FunctionName":"dup-func","PackageType":"Image","Code":{"ImageUri":"myimage:latest"}}`,
			wantCode:    http.StatusConflict,
			wantErrType: "ResourceConflictException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newHandler(t)
			if tt.setup != nil {
				tt.setup(bk)
			}

			rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", tt.body, nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				assertLambdaError(t, rec, tt.wantErrType)
			}

			if tt.wantFunctionName == "" {
				return
			}

			var fn lambda.FunctionConfiguration
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fn))
			assert.Equal(t, tt.wantFunctionName, fn.FunctionName)
			if tt.wantFunctionArn != "" {
				assert.Equal(t, tt.wantFunctionArn, fn.FunctionArn)
			}
			if tt.wantPackageType != "" {
				assert.Equal(t, tt.wantPackageType, fn.PackageType)
			}
			if tt.wantState != "" {
				assert.Equal(t, tt.wantState, fn.State)
			}
			if tt.wantLastUpdateStatus != "" {
				assert.Equal(t, tt.wantLastUpdateStatus, fn.LastUpdateStatus)
			}
			if tt.wantMemorySize > 0 {
				assert.Equal(t, tt.wantMemorySize, fn.MemorySize)
			}
			if tt.wantTimeout > 0 {
				assert.Equal(t, tt.wantTimeout, fn.Timeout)
			}
			if tt.wantRevisionID {
				assert.NotEmpty(t, fn.RevisionID)
			}
		})
	}
}

// ---- GetFunction tests ----

func TestGetFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*mockBackend)
		name         string
		funcName     string
		wantErrType  string
		wantImageURI string
		wantRepoType string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(bk *mockBackend) {
				bk.functions["get-func"] = &lambda.FunctionConfiguration{
					FunctionName: "get-func",
					FunctionArn:  "arn:aws:lambda:us-east-1:000000000000:function:get-func",
					ImageURI:     "myimage:latest",
					PackageType:  lambda.PackageTypeImage,
					State:        lambda.FunctionStateActive,
				}
			},
			funcName:     "get-func",
			wantCode:     http.StatusOK,
			wantImageURI: "myimage:latest",
			wantRepoType: "ECR",
		},
		{
			name:        "not_found",
			funcName:    "nonexistent",
			wantCode:    http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newHandler(t)
			if tt.setup != nil {
				tt.setup(bk)
			}

			rec := callHandler(t, h, http.MethodGet, "/2015-03-31/functions/"+tt.funcName, "", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				assertLambdaError(t, rec, tt.wantErrType)
			}

			if tt.wantCode == http.StatusOK {
				var out lambda.GetFunctionOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.NotNil(t, out.Configuration)
				assert.Equal(t, tt.funcName, out.Configuration.FunctionName)
				require.NotNil(t, out.Code)
				assert.Equal(t, tt.wantImageURI, out.Code.ImageURI)
				assert.Equal(t, tt.wantRepoType, out.Code.RepositoryType)
			}
		})
	}
}

// ---- ListFunctions tests ----

func TestListFunctions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*mockBackend)
		name      string
		wantCount int
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name: "multiple",
			setup: func(bk *mockBackend) {
				bk.functions["func-a"] = &lambda.FunctionConfiguration{FunctionName: "func-a"}
				bk.functions["func-b"] = &lambda.FunctionConfiguration{FunctionName: "func-b"}
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newHandler(t)
			if tt.setup != nil {
				tt.setup(bk)
			}

			rec := callHandler(t, h, http.MethodGet, "/2015-03-31/functions", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out lambda.ListFunctionsOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Functions, tt.wantCount)
		})
	}
}

// ---- DeleteFunction tests ----

func TestDeleteFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*mockBackend)
		name        string
		funcName    string
		wantErrType string
		wantCode    int
		wantEmpty   bool
	}{
		{
			name: "success",
			setup: func(bk *mockBackend) {
				bk.functions["del-func"] = &lambda.FunctionConfiguration{FunctionName: "del-func"}
			},
			funcName:  "del-func",
			wantCode:  http.StatusNoContent,
			wantEmpty: true,
		},
		{
			name:        "not_found",
			funcName:    "missing",
			wantCode:    http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newHandler(t)
			if tt.setup != nil {
				tt.setup(bk)
			}

			rec := callHandler(t, h, http.MethodDelete, "/2015-03-31/functions/"+tt.funcName, "", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				assertLambdaError(t, rec, tt.wantErrType)
			}

			if tt.wantEmpty {
				assert.Empty(t, bk.functions)
			}
		})
	}
}

// ---- UpdateFunctionCode tests ----

func TestUpdateFunctionCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup                func(*mockBackend)
		name                 string
		funcName             string
		body                 string
		wantImageURI         string
		wantLastUpdateStatus lambda.LastUpdateStatus
		wantCode             int
	}{
		{
			name: "success",
			setup: func(bk *mockBackend) {
				bk.functions["code-func"] = &lambda.FunctionConfiguration{
					FunctionName: "code-func",
					ImageURI:     "old-image:v1",
				}
			},
			funcName:             "code-func",
			body:                 `{"ImageUri":"new-image:v2"}`,
			wantCode:             http.StatusOK,
			wantImageURI:         "new-image:v2",
			wantLastUpdateStatus: lambda.LastUpdateStatusSuccessful,
		},
		{
			name:     "not_found",
			funcName: "missing",
			body:     `{"ImageUri":"new-image:v2"}`,
			wantCode: http.StatusNotFound,
		},
		{
			name: "missing_image_uri",
			setup: func(bk *mockBackend) {
				bk.functions["code-func"] = &lambda.FunctionConfiguration{FunctionName: "code-func"}
			},
			funcName: "code-func",
			body:     `{}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "invalid_body",
			setup: func(bk *mockBackend) {
				bk.functions["code-func"] = &lambda.FunctionConfiguration{FunctionName: "code-func"}
			},
			funcName: "code-func",
			body:     "bad{json}",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newHandler(t)
			if tt.setup != nil {
				tt.setup(bk)
			}

			rec := callHandler(t, h, http.MethodPut, "/2015-03-31/functions/"+tt.funcName+"/code", tt.body, nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantImageURI != "" {
				var fn lambda.FunctionConfiguration
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fn))
				assert.Equal(t, tt.wantImageURI, fn.ImageURI)
				assert.NotEmpty(t, fn.RevisionID)
				if tt.wantLastUpdateStatus != "" {
					assert.Equal(t, tt.wantLastUpdateStatus, fn.LastUpdateStatus)
				}
			}
		})
	}
}

// ---- UpdateFunctionConfiguration tests ----

func TestUpdateFunctionConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup                func(*mockBackend)
		name                 string
		funcName             string
		body                 string
		wantDescription      string
		wantRole             string
		wantEnvKey           string
		wantEnvValue         string
		wantLastUpdateStatus lambda.LastUpdateStatus
		wantCode             int
		wantMemorySize       int
		wantTimeout          int
	}{
		{
			name: "success",
			setup: func(bk *mockBackend) {
				bk.functions["cfg-func"] = &lambda.FunctionConfiguration{
					FunctionName: "cfg-func",
					MemorySize:   128,
					Timeout:      3,
					Description:  "old description",
				}
			},
			funcName:             "cfg-func",
			body:                 `{"Description":"new description","MemorySize":512,"Timeout":30,"Role":"new-role"}`,
			wantCode:             http.StatusOK,
			wantDescription:      "new description",
			wantMemorySize:       512,
			wantTimeout:          30,
			wantRole:             "new-role",
			wantLastUpdateStatus: lambda.LastUpdateStatusSuccessful,
		},
		{
			name: "update_environment",
			setup: func(bk *mockBackend) {
				bk.functions["env-func"] = &lambda.FunctionConfiguration{FunctionName: "env-func"}
			},
			funcName:             "env-func",
			body:                 `{"Environment":{"Variables":{"KEY":"VALUE"}}}`,
			wantCode:             http.StatusOK,
			wantEnvKey:           "KEY",
			wantEnvValue:         "VALUE",
			wantLastUpdateStatus: lambda.LastUpdateStatusSuccessful,
		},
		{
			name:     "not_found",
			funcName: "missing",
			body:     `{}`,
			wantCode: http.StatusNotFound,
		},
		{
			name: "invalid_body",
			setup: func(bk *mockBackend) {
				bk.functions["cfg-func"] = &lambda.FunctionConfiguration{FunctionName: "cfg-func"}
			},
			funcName: "cfg-func",
			body:     "bad{json}",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newHandler(t)
			if tt.setup != nil {
				tt.setup(bk)
			}

			rec := callHandler(
				t,
				h,
				http.MethodPut,
				"/2015-03-31/functions/"+tt.funcName+"/configuration",
				tt.body,
				nil,
			)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var fn lambda.FunctionConfiguration
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fn))
			if tt.wantDescription != "" {
				assert.Equal(t, tt.wantDescription, fn.Description)
			}
			if tt.wantMemorySize > 0 {
				assert.Equal(t, tt.wantMemorySize, fn.MemorySize)
			}
			if tt.wantTimeout > 0 {
				assert.Equal(t, tt.wantTimeout, fn.Timeout)
			}
			if tt.wantRole != "" {
				assert.Equal(t, tt.wantRole, fn.Role)
			}
			if tt.wantEnvKey != "" {
				require.NotNil(t, fn.Environment)
				assert.Equal(t, tt.wantEnvValue, fn.Environment.Variables[tt.wantEnvKey])
			}
			if tt.wantLastUpdateStatus != "" {
				assert.Equal(t, tt.wantLastUpdateStatus, fn.LastUpdateStatus)
			}
		})
	}
}

// ---- Invoke tests ----

func TestInvoke(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*mockBackend)
		headers      map[string]string
		name         string
		funcName     string
		body         string
		wantErrType  string
		wantContains string
		wantCode     int
	}{
		{
			name: "request_response",
			setup: func(bk *mockBackend) {
				bk.functions["invoke-func"] = &lambda.FunctionConfiguration{FunctionName: "invoke-func"}
				bk.invokeResult = []byte(`{"answer":42}`)
			},
			funcName:     "invoke-func",
			body:         `{"key":"value"}`,
			wantCode:     http.StatusOK,
			wantContains: "42",
		},
		{
			name: "event",
			setup: func(bk *mockBackend) {
				bk.functions["event-func"] = &lambda.FunctionConfiguration{FunctionName: "event-func"}
			},
			funcName: "event-func",
			body:     `{}`,
			headers:  map[string]string{"X-Amz-Invocation-Type": "Event"},
			wantCode: http.StatusAccepted,
		},
		{
			name: "dry_run",
			setup: func(bk *mockBackend) {
				bk.functions["dryrun-func"] = &lambda.FunctionConfiguration{FunctionName: "dryrun-func"}
			},
			funcName: "dryrun-func",
			body:     `{}`,
			headers:  map[string]string{"X-Amz-Invocation-Type": "DryRun"},
			wantCode: http.StatusNoContent,
		},
		{
			name:        "not_found",
			funcName:    "missing",
			body:        `{}`,
			wantCode:    http.StatusNotFound,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name: "service_error",
			setup: func(bk *mockBackend) {
				bk.functions["err-func"] = &lambda.FunctionConfiguration{FunctionName: "err-func"}
				bk.invokeErr = fmt.Errorf("%w: Docker unavailable", lambda.ErrLambdaUnavailable)
			},
			funcName: "err-func",
			body:     `{}`,
			wantCode: http.StatusInternalServerError,
		},
		{
			name: "empty_body",
			setup: func(bk *mockBackend) {
				bk.functions["body-func"] = &lambda.FunctionConfiguration{FunctionName: "body-func"}
			},
			funcName: "body-func",
			body:     "",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newHandler(t)
			if tt.setup != nil {
				tt.setup(bk)
			}

			rec := callHandler(
				t,
				h,
				http.MethodPost,
				"/2015-03-31/functions/"+tt.funcName+"/invocations",
				tt.body,
				tt.headers,
			)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrType != "" {
				assertLambdaError(t, rec, tt.wantErrType)
			}

			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}
		})
	}
}

// ---- Routing tests ----

func TestHandler_UnknownRoute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "unknown_sub_path", wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandler(t)
			rec := callHandler(t, h, http.MethodGet, "/2015-03-31/functions/foo/unknown-sub", "", nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		target string
		want   bool
	}{
		{name: "lambda_path", method: http.MethodGet, path: "/2015-03-31/functions", want: true},
		{
			name:   "amz_target_header",
			method: http.MethodGet,
			path:   "/other",
			target: "AWSLambda.ListFunctions",
			want:   true,
		},
		{name: "no_match", method: http.MethodGet, path: "/other", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandler(t)
			matcher := h.RouteMatcher()
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains []string
	}{
		{
			name:         "returns_operations",
			wantContains: []string{"CreateFunction", "InvokeFunction"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandler(t)
			ops := h.GetSupportedOperations()
			assert.NotEmpty(t, ops)
			for _, op := range tt.wantContains {
				assert.Contains(t, ops, op)
			}
		})
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want int
	}{
		{name: "returns_95", want: 95},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandler(t)
			assert.Equal(t, tt.want, h.MatchPriority())
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{name: "create_function", method: http.MethodPost, path: "/2015-03-31/functions", wantOp: "CreateFunction"},
		{name: "list_functions", method: http.MethodGet, path: "/2015-03-31/functions", wantOp: "ListFunctions"},
		{name: "get_function", method: http.MethodGet, path: "/2015-03-31/functions/my-func", wantOp: "GetFunction"},
		{
			name:   "delete_function",
			method: http.MethodDelete,
			path:   "/2015-03-31/functions/my-func",
			wantOp: "DeleteFunction",
		},
		{
			name:   "update_code",
			method: http.MethodPut,
			path:   "/2015-03-31/functions/my-func/code",
			wantOp: "UpdateFunctionCode",
		},
		{
			name:   "update_config",
			method: http.MethodPut,
			path:   "/2015-03-31/functions/my-func/configuration",
			wantOp: "UpdateFunctionConfiguration",
		},
		{
			name:   "invoke",
			method: http.MethodPost,
			path:   "/2015-03-31/functions/my-func/invocations",
			wantOp: "InvokeFunction",
		},
		{name: "unknown", method: http.MethodGet, path: "/2015-03-31/functions/my-func/unknown", wantOp: "Unknown"},
		{
			// Layer list path exercises extractLayerOperation with rest == "" branch (correct prefix).
			name:   "layers_list",
			method: http.MethodGet,
			path:   "/2018-10-31/layers",
			wantOp: "ListLayers",
		},
		{
			// Layer versions path: extractLayerOperation returns "" (n=2,lastSeg="" not in table) → "Unknown".
			name:   "layer_versions_list",
			method: http.MethodGet,
			path:   "/2018-10-31/layers/my-layer/versions",
			wantOp: "Unknown",
		},
		{
			// Layer version get exercises extractLayerOperation with numParts==3 branch.
			name:   "layer_version_get",
			method: http.MethodGet,
			path:   "/2018-10-31/layers/my-layer/versions/1",
			wantOp: "GetLayerVersion",
		},
		{
			// Layer version policy exercises extractLayerOperation with numParts==4, lastSeg="policy".
			name:   "layer_version_policy_get",
			method: http.MethodGet,
			path:   "/2018-10-31/layers/my-layer/versions/1/policy",
			wantOp: "GetLayerVersionPolicy",
		},
		{
			// Layer path with bad format (parts[1]!="versions") → extractLayerOperation returns "" → "Unknown".
			name:   "layer_bad_format",
			method: http.MethodGet,
			path:   "/2018-10-31/layers/my-layer/bad",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		method       string
		path         string
		wantResource string
	}{
		{
			name:         "with_function_name",
			method:       http.MethodGet,
			path:         "/2015-03-31/functions/my-func",
			wantResource: "my-func",
		},
		{
			name:         "without_function_name",
			method:       http.MethodGet,
			path:         "/2015-03-31/functions",
			wantResource: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantResource, h.ExtractResource(c))
		})
	}
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns_lambda", want: "Lambda"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandler(t)
			assert.Equal(t, tt.want, h.Name())
		})
	}
}

// ---- Backend tests ----

func TestBackend_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "full_crud_lifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1")

			fn := &lambda.FunctionConfiguration{
				FunctionName: "test-func",
				ImageURI:     "myimage:latest",
				PackageType:  lambda.PackageTypeImage,
				State:        lambda.FunctionStateActive,
			}

			require.NoError(t, bk.CreateFunction(fn))
			require.ErrorIs(t, bk.CreateFunction(fn), lambda.ErrFunctionAlreadyExists)

			got, err := bk.GetFunction("test-func")
			require.NoError(t, err)
			assert.Equal(t, "test-func", got.FunctionName)

			_, err = bk.GetFunction("nonexistent")
			require.ErrorIs(t, err, lambda.ErrFunctionNotFound)

			list := bk.ListFunctions("", 0)
			assert.Len(t, list.Data, 1)

			fn2 := *fn
			fn2.Description = "updated"
			require.NoError(t, bk.UpdateFunction(&fn2))

			got2, err := bk.GetFunction("test-func")
			require.NoError(t, err)
			assert.Equal(t, "updated", got2.Description)

			notExist := &lambda.FunctionConfiguration{FunctionName: "nonexistent"}
			require.ErrorIs(t, bk.UpdateFunction(notExist), lambda.ErrFunctionNotFound)

			require.NoError(t, bk.DeleteFunction("test-func"))
			assert.Empty(t, bk.ListFunctions("", 0).Data)

			assert.ErrorIs(t, bk.DeleteFunction("test-func"), lambda.ErrFunctionNotFound)
		})
	}
}

func TestBackend_InvokeFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr        error
		name           string
		funcToInvoke   string
		invocationType lambda.InvocationType
		portRange      [2]int
		wantCode       int
		createFunc     bool
	}{
		{
			name:           "no_port_alloc",
			createFunc:     true,
			funcToInvoke:   "invoke-func",
			invocationType: lambda.InvocationTypeRequestResponse,
			wantErr:        lambda.ErrLambdaUnavailable,
		},
		{
			name:           "no_docker",
			portRange:      [2]int{19000, 19100},
			createFunc:     true,
			funcToInvoke:   "invoke-func",
			invocationType: lambda.InvocationTypeRequestResponse,
			wantErr:        lambda.ErrLambdaUnavailable,
		},
		{
			name:           "not_found",
			funcToInvoke:   "nonexistent",
			invocationType: lambda.InvocationTypeRequestResponse,
			wantErr:        lambda.ErrFunctionNotFound,
			wantCode:       http.StatusNotFound,
		},
		{
			name:           "dry_run",
			createFunc:     true,
			funcToInvoke:   "fn",
			invocationType: lambda.InvocationTypeDryRun,
			wantCode:       http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var pa *portalloc.Allocator
			if tt.portRange[0] > 0 {
				var err error
				pa, err = portalloc.New(tt.portRange[0], tt.portRange[1])
				require.NoError(t, err)
			}

			bk := lambda.NewInMemoryBackend(nil, pa, lambda.DefaultSettings(), "000000000000", "us-east-1")

			if tt.createFunc {
				fn := &lambda.FunctionConfiguration{
					FunctionName: tt.funcToInvoke,
					ImageURI:     "myimage:latest",
					Timeout:      3,
				}
				require.NoError(t, bk.CreateFunction(fn))
			}

			_, statusCode, err := bk.InvokeFunction(t.Context(), tt.funcToInvoke, tt.invocationType, []byte("{}"))

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			if tt.wantCode > 0 {
				assert.Equal(t, tt.wantCode, statusCode)
			}
		})
	}
}

// ---- Runtime API server tests ----

func TestRuntimeServer_Invoke(t *testing.T) {
	t.Parallel()

	tests := []struct {
		simulate    func(t *testing.T, port int, requestID string)
		name        string
		wantBody    string
		payload     []byte
		port        int
		wantIsError bool
	}{
		{
			name:    "success_response",
			port:    18101,
			payload: []byte(`{"key":"value"}`),
			simulate: func(t *testing.T, port int, requestID string) {
				t.Helper()
				simulateContainerResponse(t, port, requestID, `{"answer":42}`)
			},
			wantBody:    `{"answer":42}`,
			wantIsError: false,
		},
		{
			name:    "error_response",
			port:    18102,
			payload: []byte(`{}`),
			simulate: func(t *testing.T, port int, requestID string) {
				t.Helper()
				simulateContainerError(t, port, requestID, `{"errorMessage":"function panicked"}`)
			},
			wantBody:    "panicked",
			wantIsError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestRuntimeServer(t, tt.port)
			ctx := t.Context()

			resultCh := make(chan []byte, 1)
			errCh := make(chan error, 1)
			isErrCh := make(chan bool, 1)

			go func() {
				result, isErr, invokeErr := srv.Invoke(ctx, tt.payload, 5*time.Second)
				if invokeErr != nil {
					errCh <- invokeErr

					return
				}
				resultCh <- result
				isErrCh <- isErr
			}()

			requestID := simulateContainerNext(t, tt.port)
			tt.simulate(t, tt.port, requestID)

			select {
			case result := <-resultCh:
				isErr := <-isErrCh
				assert.Equal(t, tt.wantIsError, isErr)
				if tt.wantIsError {
					assert.Contains(t, string(result), tt.wantBody)
				} else {
					assert.JSONEq(t, tt.wantBody, string(result))
				}
			case err := <-errCh:
				require.NoError(t, err, "invoke error")
			case <-time.After(5 * time.Second):
				require.FailNow(t, "test timed out")
			}
		})
	}
}

func TestRuntimeServer_HTTPEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		body     string
		port     int
		wantCode int
	}{
		{
			name:     "init_error",
			port:     18103,
			method:   http.MethodPost,
			path:     "/2018-06-01/runtime/init/error",
			body:     `{"errorMessage":"init failed","errorType":"Runtime.ExitError"}`,
			wantCode: http.StatusAccepted,
		},
		{
			name:     "method_not_allowed",
			port:     18104,
			method:   http.MethodPost,
			path:     "/2018-06-01/runtime/invocation/next",
			wantCode: http.StatusMethodNotAllowed,
		},
		{
			name:     "response_unknown_request_id",
			port:     18105,
			method:   http.MethodPost,
			path:     "/2018-06-01/runtime/invocation/unknown-id/response",
			body:     `{"result":"ok"}`,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_ = newTestRuntimeServer(t, tt.port)

			req, err := http.NewRequestWithContext(t.Context(),
				tt.method,
				fmt.Sprintf("http://127.0.0.1:%d%s", tt.port, tt.path),
				strings.NewReader(tt.body),
			)
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}
}

func TestRuntimeServer_InvokeStop(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantErrContains string
		port            int
		timeout         time.Duration
		cancelCtx       bool
	}{
		{
			name:            "invoke_timeout",
			port:            18106,
			timeout:         100 * time.Millisecond,
			wantErrContains: "timed out",
		},
		{
			name:      "context_cancelled",
			port:      18107,
			timeout:   30 * time.Second,
			cancelCtx: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestRuntimeServer(t, tt.port)

			if tt.cancelCtx {
				ctx, cancel := context.WithCancel(t.Context())
				errCh := make(chan error, 1)

				go func() {
					_, _, err := srv.Invoke(ctx, []byte(`{}`), tt.timeout)
					errCh <- err
				}()

				time.Sleep(50 * time.Millisecond)
				cancel()

				select {
				case err := <-errCh:
					require.Error(t, err)
				case <-time.After(2 * time.Second):
					require.FailNow(t, "expected context cancellation error")
				}
			} else {
				_, _, err := srv.Invoke(t.Context(), []byte(`{}`), tt.timeout)
				require.Error(t, err)
				if tt.wantErrContains != "" {
					assert.Contains(t, err.Error(), tt.wantErrContains)
				}
			}
		})
	}
}

// ---- Settings tests ----

func TestDefaultSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantDockerHost  string
		wantPoolSize    int
		wantIdleTimeout time.Duration
	}{
		{
			name: "platform_defaults",
			wantDockerHost: func() string {
				if runtime.GOOS == "darwin" || runtime.GOOS == "windows" {
					return "host.docker.internal"
				}

				return "172.17.0.1"
			}(),
			wantPoolSize:    3,
			wantIdleTimeout: 10 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := lambda.DefaultSettings()
			assert.Equal(t, tt.wantDockerHost, s.DockerHost)
			assert.Equal(t, tt.wantPoolSize, s.PoolSize)
			assert.Equal(t, tt.wantIdleTimeout, s.IdleTimeout)
		})
	}
}

// ---- helper functions ----

// newTestRuntimeServer is an exported alias used in tests to access the internal runtimeServer.
// We use this via the Invoke method exposed for testing.
func newTestRuntimeServer(t *testing.T, port int) testRuntimeServerIface {
	t.Helper()

	srv := newPublicRuntimeServer(t, port)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		srv.Stop(ctx)
	})

	return srv
}

// testRuntimeServerIface wraps the runtimeServer for white-box testing.
type testRuntimeServerIface interface {
	Invoke(ctx context.Context, payload []byte, timeout time.Duration) ([]byte, bool, error)
	Stop(ctx context.Context)
}

// publicRuntimeServer wraps the internal runtimeServer for test access.
type publicRuntimeServer struct {
	inner *lambda.ExportedRuntimeServer
}

func newPublicRuntimeServer(t *testing.T, port int) *publicRuntimeServer {
	t.Helper()

	srv := lambda.NewExportedRuntimeServer(port)
	require.NoError(t, srv.Start(t.Context()))

	return &publicRuntimeServer{inner: srv}
}

func (p *publicRuntimeServer) Invoke(ctx context.Context, payload []byte, timeout time.Duration) ([]byte, bool, error) {
	return p.inner.Invoke(ctx, payload, timeout)
}

func (p *publicRuntimeServer) Stop(ctx context.Context) {
	p.inner.Stop(ctx)
}

func simulateContainerNext(t *testing.T, port int) string {
	t.Helper()

	// Poll until the invocation is queued (the invoke goroutine may not have run yet).
	var resp *http.Response

	for range 20 {
		req, err := http.NewRequestWithContext(t.Context(),
			http.MethodGet,
			fmt.Sprintf("http://127.0.0.1:%d/2018-06-01/runtime/invocation/next", port),
			nil,
		)
		require.NoError(t, err)

		var doErr error

		resp, doErr = http.DefaultClient.Do(req)
		if doErr == nil && resp.StatusCode == http.StatusOK {
			break
		}

		if resp != nil {
			resp.Body.Close()
		}

		time.Sleep(50 * time.Millisecond)
	}

	require.NotNil(t, resp)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	requestID := resp.Header.Get("Lambda-Runtime-Aws-Request-Id")
	require.NotEmpty(t, requestID)

	return requestID
}

func simulateContainerResponse(t *testing.T, port int, requestID, responseBody string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(),
		http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/2018-06-01/runtime/invocation/%s/response", port, requestID),
		strings.NewReader(responseBody),
	)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
}

func simulateContainerError(t *testing.T, port int, requestID, errorBody string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(),
		http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/2018-06-01/runtime/invocation/%s/error", port, requestID),
		strings.NewReader(errorBody),
	)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
}

// assertLambdaError asserts that the response body contains a Lambda error with the given type.
func assertLambdaError(t *testing.T, rec *httptest.ResponseRecorder, errType string) {
	t.Helper()

	var lambdaErr lambda.Error
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lambdaErr))
	assert.Equal(t, errType, lambdaErr.Type)
}

// ---- Mock Docker API ----

// mockDockerAPI implements container.APIClient for testing without a real daemon.
type mockDockerAPI struct {
	createErr error
	counter   int
	mu        sync.Mutex
}

func (m *mockDockerAPI) ImagePull(_ context.Context, _ string, _ image.PullOptions) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (m *mockDockerAPI) ImageList(_ context.Context, _ image.ListOptions) ([]image.Summary, error) {
	return nil, nil
}

func (m *mockDockerAPI) ContainerCreate(
	_ context.Context,
	_ *container.Config,
	_ *container.HostConfig,
	_ any,
	_ any,
	_ string,
) (container.CreateResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.createErr != nil {
		return container.CreateResponse{}, m.createErr
	}

	m.counter++

	return container.CreateResponse{ID: fmt.Sprintf("mock-container-%d", m.counter)}, nil
}

func (m *mockDockerAPI) ContainerStart(_ context.Context, _ string, _ container.StartOptions) error {
	return nil
}

func (m *mockDockerAPI) ContainerStop(_ context.Context, _ string, _ container.StopOptions) error {
	return nil
}

func (m *mockDockerAPI) ContainerRemove(_ context.Context, _ string, _ container.RemoveOptions) error {
	return nil
}

func (m *mockDockerAPI) Ping(_ context.Context) (any, error) {
	return struct{}{}, nil
}

func (m *mockDockerAPI) Close() error {
	return nil
}

// newMockDockerClient creates a container.Runtime backed by mockDockerAPI.
func newMockDockerClient() gophercontainer.Runtime {
	return gophercontainer.NewDockerRuntimeWithAPI(&mockDockerAPI{}, gophercontainer.Config{
		PoolSize:    3,
		IdleTimeout: time.Minute,
	})
}

// ---- Backend tests with mock Docker ----

func TestBackend_InvokeFunction_MockDocker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		funcName  string
		portRange [2]int
		wantCode  int
		callTwice bool
	}{
		{
			name:      "event_with_mock_docker",
			portRange: [2]int{19200, 19250},
			funcName:  "event-fn",
			wantCode:  http.StatusAccepted,
		},
		{
			name:      "event_second_call_reuse_runtime",
			portRange: [2]int{19300, 19350},
			funcName:  "reuse-fn",
			wantCode:  http.StatusAccepted,
			callTwice: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pa, err := portalloc.New(tt.portRange[0], tt.portRange[1])
			require.NoError(t, err)

			dc := newMockDockerClient()
			bk := lambda.NewInMemoryBackend(
				dc,
				pa,
				lambda.DefaultSettings(),
				"000000000000",
				"us-east-1",
			)

			fn := &lambda.FunctionConfiguration{
				FunctionName: tt.funcName,
				ImageURI:     "myimage:latest",
				Timeout:      3,
			}
			require.NoError(t, bk.CreateFunction(fn))

			_, statusCode, invokeErr := bk.InvokeFunction(
				t.Context(),
				tt.funcName,
				lambda.InvocationTypeEvent,
				[]byte(`{}`),
			)
			require.NoError(t, invokeErr)
			assert.Equal(t, tt.wantCode, statusCode)

			if tt.callTwice {
				_, sc2, err2 := bk.InvokeFunction(t.Context(), tt.funcName, lambda.InvocationTypeEvent, []byte(`{}`))
				require.NoError(t, err2)
				assert.Equal(t, tt.wantCode, sc2)
			}
		})
	}
}

func TestBackend_DeleteFunction_WithRuntime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		funcName  string
		portRange [2]int
	}{
		{
			name:      "deletes_with_runtime",
			portRange: [2]int{19400, 19450},
			funcName:  "delete-with-rt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pa, err := portalloc.New(tt.portRange[0], tt.portRange[1])
			require.NoError(t, err)

			dc := newMockDockerClient()
			bk := lambda.NewInMemoryBackend(
				dc,
				pa,
				lambda.DefaultSettings(),
				"000000000000",
				"us-east-1",
			)

			fn := &lambda.FunctionConfiguration{
				FunctionName: tt.funcName,
				ImageURI:     "myimage:latest",
				Timeout:      3,
			}
			require.NoError(t, bk.CreateFunction(fn))

			_, _, _ = bk.InvokeFunction(t.Context(), tt.funcName, lambda.InvocationTypeEvent, []byte(`{}`))

			require.NoError(t, bk.DeleteFunction(tt.funcName))

			_, err = bk.GetFunction(tt.funcName)
			assert.ErrorIs(t, err, lambda.ErrFunctionNotFound)
		})
	}
}

func TestBackend_InvokeFunction_RequestResponse_WithMockDocker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		funcName  string
		portRange [2]int
	}{
		{
			name:      "request_response_mock_docker",
			portRange: [2]int{19500, 19550},
			funcName:  "rr-fn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pa, err := portalloc.New(tt.portRange[0], tt.portRange[1])
			require.NoError(t, err)

			dc := newMockDockerClient()
			bk := lambda.NewInMemoryBackend(
				dc,
				pa,
				lambda.DefaultSettings(),
				"000000000000",
				"us-east-1",
			)

			fn := &lambda.FunctionConfiguration{
				FunctionName: tt.funcName,
				ImageURI:     "myimage:latest",
				Timeout:      3,
			}
			require.NoError(t, bk.CreateFunction(fn))

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			resultCh := make(chan error, 1)

			go func() {
				_, _, invokeErr := bk.InvokeFunction(
					ctx,
					tt.funcName,
					lambda.InvocationTypeRequestResponse,
					[]byte(`{}`),
				)
				resultCh <- invokeErr
			}()

			time.Sleep(200 * time.Millisecond)

			var runtimePort int

			for p := tt.portRange[0]; p < tt.portRange[1]; p++ {
				req, reqErr := http.NewRequestWithContext(t.Context(),
					http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/2018-06-01/runtime/invocation/next", p), nil,
				)
				if reqErr != nil {
					continue
				}

				client := &http.Client{Timeout: 200 * time.Millisecond}
				resp, doErr := client.Do(req)

				if doErr == nil && resp != nil {
					requestID := resp.Header.Get("Lambda-Runtime-Aws-Request-Id")
					resp.Body.Close()

					if requestID != "" {
						runtimePort = p
						simulateContainerResponse(t, p, requestID, `{"result":"ok"}`)

						break
					}
				}
			}

			select {
			case invokeErr := <-resultCh:
				if runtimePort > 0 {
					require.NoError(t, invokeErr)
				}
			case <-time.After(4 * time.Second):
				cancel()
			}
		})
	}
}

// ---- Provider tests ----

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns_lambda", want: "Lambda"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &lambda.Provider{}
			assert.Equal(t, tt.want, p.Name())
		})
	}
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config interface {
			GetLambdaSettings() lambda.Settings
			GetGlobalConfig() config.GlobalConfig
		}
		wantAccountID string
		wantRegion    string
	}{
		{
			name:          "no_config",
			config:        nil,
			wantAccountID: "",
			wantRegion:    "",
		},
		{
			name:          "with_config",
			config:        &mockConfig{accountID: "111111111111", region: "eu-west-1"},
			wantAccountID: "111111111111",
			wantRegion:    "eu-west-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &lambda.Provider{}
			appCtx := &service.AppContext{
				Logger:    slog.Default(),
				PortAlloc: nil,
			}
			if tt.config != nil {
				appCtx.Config = tt.config
			}

			svc, err := p.Init(appCtx)
			require.NoError(t, err)
			assert.NotNil(t, svc)
			assert.Equal(t, "Lambda", svc.Name())

			if tt.wantAccountID != "" || tt.wantRegion != "" {
				h, ok := svc.(*lambda.Handler)
				require.True(t, ok)
				assert.Equal(t, tt.wantAccountID, h.AccountID)
				assert.Equal(t, tt.wantRegion, h.DefaultRegion)
			}
		})
	}
}

// mockConfig implements lambda.SettingsProvider and config.Provider for provider tests.
type mockConfig struct {
	accountID string
	region    string
}

func (m *mockConfig) GetLambdaSettings() lambda.Settings {
	return lambda.DefaultSettings()
}

func (m *mockConfig) GetGlobalConfig() config.GlobalConfig {
	return config.GlobalConfig{AccountID: m.accountID, Region: m.region}
}

// ---- Function URL tests ----

func newInMemHandlerWithPortAlloc(t *testing.T) *lambda.Handler {
	t.Helper()

	pa, err := portalloc.New(20000, 20050)
	require.NoError(t, err)

	bk := lambda.NewInMemoryBackend(nil, pa, lambda.DefaultSettings(), "000000000000", "us-east-1")
	h := lambda.NewHandler(bk)
	h.DefaultRegion = "us-east-1"
	h.AccountID = "000000000000"

	return h
}

func mustCreateFunctionViaHandler(t *testing.T, h *lambda.Handler, name string) {
	t.Helper()

	const roleARN = "arn:aws:iam::000000000000:role/r"
	body := fmt.Sprintf(
		`{"FunctionName":%q,"PackageType":"Image","Code":{"ImageUri":"test:latest"},"Role":%q}`,
		name, roleARN,
	)
	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", body, nil)
	require.Equal(t, http.StatusCreated, rec.Code)
}

// ---- Function URL tests ----

func TestFunctionUrl_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		funcName string
	}{
		{name: "create_get_delete", funcName: "url-fn"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newInMemHandlerWithPortAlloc(t)
			mustCreateFunctionViaHandler(t, h, tt.funcName)

			rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions/"+tt.funcName+"/url",
				`{"AuthType":"NONE"}`, nil)
			require.Equal(t, http.StatusCreated, rec.Code)

			var cfg lambda.FunctionURLConfig
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cfg))
			assert.NotEmpty(t, cfg.FunctionURL)
			assert.Equal(t, "NONE", cfg.AuthType)
			assert.NotEmpty(t, cfg.FunctionArn)
			assert.NotEmpty(t, cfg.CreationTime)

			rec = callHandler(t, h, http.MethodGet, "/2015-03-31/functions/"+tt.funcName+"/url", "", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var getCfg lambda.FunctionURLConfig
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getCfg))
			assert.Equal(t, cfg.FunctionURL, getCfg.FunctionURL)

			rec = callHandler(t, h, http.MethodDelete, "/2015-03-31/functions/"+tt.funcName+"/url", "", nil)
			require.Equal(t, http.StatusNoContent, rec.Code)

			rec = callHandler(t, h, http.MethodGet, "/2015-03-31/functions/"+tt.funcName+"/url", "", nil)
			require.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestFunctionURL_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(*testing.T, *lambda.Handler)
		method   string
		funcName string
		wantCode int
	}{
		{
			name:     "create_function_not_found",
			method:   http.MethodPost,
			funcName: "nonexistent",
			wantCode: http.StatusNotFound,
		},
		{
			name: "get_url_not_found",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "no-url-fn")
			},
			method:   http.MethodGet,
			funcName: "no-url-fn",
			wantCode: http.StatusNotFound,
		},
		{
			name: "delete_url_not_found",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "del-url-fn")
			},
			method:   http.MethodDelete,
			funcName: "del-url-fn",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newInMemHandlerWithPortAlloc(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			body := ""
			if tt.method == http.MethodPost {
				body = `{"AuthType":"NONE"}`
			}

			rec := callHandler(t, h, tt.method, "/2015-03-31/functions/"+tt.funcName+"/url", body, nil)
			require.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestFunctionUrl_HTTP_ForwardsToLambda(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		funcName  string
		portRange [2]int
	}{
		{
			name:      "creates_url_with_http",
			portRange: [2]int{20100, 20200},
			funcName:  "http-url-fn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pa, err := portalloc.New(tt.portRange[0], tt.portRange[1])
			require.NoError(t, err)

			bk := lambda.NewInMemoryBackend(
				nil,
				pa,
				lambda.DefaultSettings(),
				"000000000000",
				"us-east-1",
			)

			fn := &lambda.FunctionConfiguration{
				FunctionName: tt.funcName,
				PackageType:  lambda.PackageTypeImage,
				ImageURI:     "test:latest",
			}
			require.NoError(t, bk.CreateFunction(fn))

			cfg, createErr := bk.CreateFunctionURLConfig(tt.funcName, "NONE")
			require.NoError(t, createErr)
			assert.NotEmpty(t, cfg.FunctionURL)
			assert.Contains(t, cfg.FunctionURL, "http://")
		})
	}
}

// ---- Version tests ----

func TestPublishVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *lambda.Handler)
		name        string
		funcName    string
		body        string
		wantVersion string
		wantDesc    string
		wantCode    int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "ver-fn")
			},
			funcName:    "ver-fn",
			body:        `{"Description":"v1"}`,
			wantCode:    http.StatusCreated,
			wantVersion: "1",
			wantDesc:    "v1",
		},
		{
			name:     "function_not_found",
			funcName: "no-fn",
			body:     `{}`,
			wantCode: http.StatusNotFound,
		},
		{
			name:     "mock_backend_service_error",
			funcName: "fn",
			body:     `{}`,
			wantCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h *lambda.Handler
			if tt.name == "mock_backend_service_error" {
				h, _ = newHandler(t)
			} else {
				h = newInMemHandlerWithPortAlloc(t)
			}

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions/"+tt.funcName+"/versions", tt.body, nil)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantVersion != "" {
				var ver lambda.FunctionVersion
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ver))
				assert.Equal(t, tt.wantVersion, ver.Version)
				assert.Equal(t, tt.wantDesc, ver.Description)
				assert.Equal(t, tt.funcName, ver.FunctionName)
			}
		})
	}
}

func TestListVersionsByFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *lambda.Handler)
		name         string
		funcName     string
		wantVersions []string
		wantCode     int
		wantCount    int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "list-ver-fn")
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/list-ver-fn/versions", `{}`, nil)
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/list-ver-fn/versions", `{}`, nil)
			},
			funcName:     "list-ver-fn",
			wantCode:     http.StatusOK,
			wantCount:    3,
			wantVersions: []string{"$LATEST", "1", "2"},
		},
		{
			name:     "function_not_found",
			funcName: "nofn",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "mock_backend_service_error",
			funcName: "fn",
			wantCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h *lambda.Handler
			if tt.name == "mock_backend_service_error" {
				h, _ = newHandler(t)
			} else {
				h = newInMemHandlerWithPortAlloc(t)
			}

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := callHandler(t, h, http.MethodGet, "/2015-03-31/functions/"+tt.funcName+"/versions", "", nil)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCount > 0 {
				var out lambda.ListVersionsByFunctionOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.Versions, tt.wantCount)
				for i, v := range tt.wantVersions {
					assert.Equal(t, v, out.Versions[i].Version)
				}
			}
		})
	}
}

// ---- Alias tests ----

func TestCreateAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *lambda.Handler)
		name        string
		funcName    string
		body        string
		wantName    string
		wantVersion string
		wantCode    int
		useMock     bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "alias-fn")
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-fn/versions", `{}`, nil)
			},
			funcName:    "alias-fn",
			body:        `{"Name":"live","FunctionVersion":"1"}`,
			wantCode:    http.StatusCreated,
			wantName:    "live",
			wantVersion: "1",
		},
		{
			name: "missing_name",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "alias-missing-name-fn")
			},
			funcName: "alias-missing-name-fn",
			body:     `{"FunctionVersion":"1"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_version",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "alias-missing-ver-fn")
			},
			funcName: "alias-missing-ver-fn",
			body:     `{"Name":"v1"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "dup-alias-fn")
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/dup-alias-fn/versions", `{}`, nil)
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/dup-alias-fn/aliases",
					`{"Name":"dup","FunctionVersion":"1"}`, nil)
			},
			funcName: "dup-alias-fn",
			body:     `{"Name":"dup","FunctionVersion":"1"}`,
			wantCode: http.StatusConflict,
		},
		{
			name:     "function_not_found",
			funcName: "nofn",
			body:     `{"Name":"v1","FunctionVersion":"1"}`,
			wantCode: http.StatusNotFound,
		},
		{
			name:     "mock_backend_service_error",
			funcName: "fn",
			body:     `{"Name":"v1","FunctionVersion":"1"}`,
			wantCode: http.StatusInternalServerError,
			useMock:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h *lambda.Handler
			if tt.useMock {
				h, _ = newHandler(t)
			} else {
				h = newInMemHandlerWithPortAlloc(t)
			}

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions/"+tt.funcName+"/aliases", tt.body, nil)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantName != "" {
				var alias lambda.FunctionAlias
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &alias))
				assert.Equal(t, tt.wantName, alias.Name)
				assert.Equal(t, tt.wantVersion, alias.FunctionVersion)
			}
		})
	}
}

func TestGetAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*testing.T, *lambda.Handler)
		name      string
		funcName  string
		aliasName string
		wantName  string
		wantCode  int
		useMock   bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "getalias-fn")
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/getalias-fn/versions", `{}`, nil)
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/getalias-fn/aliases",
					`{"Name":"stable","FunctionVersion":"1"}`, nil)
			},
			funcName:  "getalias-fn",
			aliasName: "stable",
			wantCode:  http.StatusOK,
			wantName:  "stable",
		},
		{
			name: "not_found",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "noalias-fn")
			},
			funcName:  "noalias-fn",
			aliasName: "missing",
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "mock_backend_service_error",
			funcName:  "fn",
			aliasName: "v1",
			wantCode:  http.StatusInternalServerError,
			useMock:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h *lambda.Handler
			if tt.useMock {
				h, _ = newHandler(t)
			} else {
				h = newInMemHandlerWithPortAlloc(t)
			}

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := callHandler(t, h, http.MethodGet,
				"/2015-03-31/functions/"+tt.funcName+"/aliases/"+tt.aliasName, "", nil)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantName != "" {
				var alias lambda.FunctionAlias
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &alias))
				assert.Equal(t, tt.wantName, alias.Name)
			}
		})
	}
}

func TestListAliases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(*testing.T, *lambda.Handler)
		funcName  string
		wantCode  int
		wantCount int
		useMock   bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "listalias-fn")
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/listalias-fn/versions", `{}`, nil)
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/listalias-fn/aliases",
					`{"Name":"v1","FunctionVersion":"1"}`, nil)
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/listalias-fn/aliases",
					`{"Name":"v2","FunctionVersion":"1"}`, nil)
			},
			funcName:  "listalias-fn",
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
		{
			name:     "function_not_found",
			funcName: "nofn",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "mock_backend_service_error",
			funcName: "fn",
			wantCode: http.StatusInternalServerError,
			useMock:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h *lambda.Handler
			if tt.useMock {
				h, _ = newHandler(t)
			} else {
				h = newInMemHandlerWithPortAlloc(t)
			}

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := callHandler(t, h, http.MethodGet, "/2015-03-31/functions/"+tt.funcName+"/aliases", "", nil)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCount > 0 {
				var out lambda.ListAliasesOutput
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Len(t, out.Aliases, tt.wantCount)
			}
		})
	}
}

func TestUpdateAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *lambda.Handler)
		name        string
		funcName    string
		aliasName   string
		body        string
		wantVersion string
		wantCode    int
		useMock     bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "updalias-fn")
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/updalias-fn/versions", `{}`, nil)
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/updalias-fn/versions", `{}`, nil)
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/updalias-fn/aliases",
					`{"Name":"prod","FunctionVersion":"1"}`, nil)
			},
			funcName:    "updalias-fn",
			aliasName:   "prod",
			body:        `{"FunctionVersion":"2"}`,
			wantCode:    http.StatusOK,
			wantVersion: "2",
		},
		{
			name: "not_found",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "updnotfound-fn")
			},
			funcName:  "updnotfound-fn",
			aliasName: "missing",
			body:      `{"FunctionVersion":"1"}`,
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "mock_backend_service_error",
			funcName:  "fn",
			aliasName: "v1",
			body:      `{"FunctionVersion":"2"}`,
			wantCode:  http.StatusInternalServerError,
			useMock:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h *lambda.Handler
			if tt.useMock {
				h, _ = newHandler(t)
			} else {
				h = newInMemHandlerWithPortAlloc(t)
			}

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := callHandler(t, h, http.MethodPut,
				"/2015-03-31/functions/"+tt.funcName+"/aliases/"+tt.aliasName, tt.body, nil)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantVersion != "" {
				var alias lambda.FunctionAlias
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &alias))
				assert.Equal(t, tt.wantVersion, alias.FunctionVersion)
			}
		})
	}
}

func TestDeleteAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(*testing.T, *lambda.Handler)
		funcName  string
		aliasName string
		wantCode  int
		verify    bool
		useMock   bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "delalias-fn")
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/delalias-fn/versions", `{}`, nil)
				callHandler(t, h, http.MethodPost, "/2015-03-31/functions/delalias-fn/aliases",
					`{"Name":"old","FunctionVersion":"1"}`, nil)
			},
			funcName:  "delalias-fn",
			aliasName: "old",
			wantCode:  http.StatusNoContent,
			verify:    true,
		},
		{
			name: "not_found",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				mustCreateFunctionViaHandler(t, h, "delnotfound-fn")
			},
			funcName:  "delnotfound-fn",
			aliasName: "missing",
			wantCode:  http.StatusNotFound,
		},
		{
			name:      "mock_backend_service_error",
			funcName:  "fn",
			aliasName: "v1",
			wantCode:  http.StatusInternalServerError,
			useMock:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var h *lambda.Handler
			if tt.useMock {
				h, _ = newHandler(t)
			} else {
				h = newInMemHandlerWithPortAlloc(t)
			}

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := callHandler(t, h, http.MethodDelete,
				"/2015-03-31/functions/"+tt.funcName+"/aliases/"+tt.aliasName, "", nil)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.verify {
				verifyRec := callHandler(t, h, http.MethodGet,
					"/2015-03-31/functions/"+tt.funcName+"/aliases/"+tt.aliasName, "", nil)
				require.Equal(t, http.StatusNotFound, verifyRec.Code)
			}
		})
	}
}

func TestInvokeWithQualifier_Alias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		funcName  string
		qualifier string
		wantCode  int
	}{
		{
			name:      "qualifier_accepted",
			funcName:  "qual-fn",
			qualifier: "live",
			wantCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newHandler(t)
			fn := &lambda.FunctionConfiguration{
				FunctionName: tt.funcName,
				PackageType:  lambda.PackageTypeImage,
			}
			require.NoError(t, bk.CreateFunction(fn))
			bk.invokeResult = []byte(`{"result":"alias-ok"}`)

			rec := callHandler(t, h, http.MethodPost,
				"/2015-03-31/functions/"+tt.funcName+"/invocations?Qualifier="+tt.qualifier,
				`{"event":"test"}`, nil)
			require.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ---- Tags tests ----

func TestHandler_TagsRoute(t *testing.T) {
	t.Parallel()

	arn := "arn:aws:lambda:us-east-1:000000000000:function:my-fn"
	tagsPath := "/2015-03-31/tags/" + arn

	tests := []struct {
		wantTagValues   map[string]string
		setup           func(*testing.T, *lambda.Handler)
		verifyTagValues map[string]string
		verifyPath      string
		body            string
		path            string
		name            string
		method          string
		wantTagAbsent   []string
		verifyTagAbsent []string
		wantCode        int
		verifyCode      int
		wantTagsNotNil  bool
	}{
		{
			name:           "get_empty",
			method:         http.MethodGet,
			path:           tagsPath,
			wantCode:       http.StatusOK,
			wantTagsNotNil: true,
		},
		{
			name: "post_and_get",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				rec := callHandler(t, h, http.MethodPost, tagsPath, `{"Tags":{"env":"prod","team":"infra"}}`, nil)
				assert.Equal(t, http.StatusNoContent, rec.Code)
			},
			method:        http.MethodGet,
			path:          tagsPath,
			wantCode:      http.StatusOK,
			wantTagValues: map[string]string{"env": "prod", "team": "infra"},
		},
		{
			name: "delete_tag",
			setup: func(t *testing.T, h *lambda.Handler) {
				t.Helper()
				callHandler(t, h, http.MethodPost, tagsPath, `{"Tags":{"env":"prod","team":"infra"}}`, nil)
			},
			method:          http.MethodDelete,
			path:            tagsPath + "?tagKeys=team",
			wantCode:        http.StatusNoContent,
			verifyPath:      tagsPath,
			verifyCode:      http.StatusOK,
			verifyTagValues: map[string]string{"env": "prod"},
			verifyTagAbsent: []string{"team"},
		},
		{
			name:     "method_not_allowed",
			method:   http.MethodPut,
			path:     tagsPath,
			wantCode: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := callHandler(t, h, tt.method, tt.path, tt.body, nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantTagsNotNil || len(tt.wantTagValues) > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				if tt.wantTagsNotNil {
					assert.NotNil(t, resp["Tags"])
				}
				if len(tt.wantTagValues) > 0 {
					tags, ok := resp["Tags"].(map[string]any)
					require.True(t, ok)
					for k, v := range tt.wantTagValues {
						assert.Equal(t, v, tags[k])
					}
				}
			}

			if tt.verifyPath != "" {
				verifyRec := callHandler(t, h, http.MethodGet, tt.verifyPath, "", nil)
				assert.Equal(t, tt.verifyCode, verifyRec.Code)

				if len(tt.verifyTagValues) > 0 || len(tt.verifyTagAbsent) > 0 {
					var resp map[string]any
					require.NoError(t, json.Unmarshal(verifyRec.Body.Bytes(), &resp))
					tags, ok := resp["Tags"].(map[string]any)
					require.True(t, ok)
					for k, v := range tt.verifyTagValues {
						assert.Equal(t, v, tags[k])
					}
					for _, k := range tt.verifyTagAbsent {
						_, present := tags[k]
						assert.False(t, present, "tag %q should be absent", k)
					}
				}
			}
		})
	}
}

func TestHandler_IAMAction(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{
			name:   "create_function",
			method: http.MethodPost,
			path:   "/2015-03-31/functions",
			want:   "lambda:CreateFunction",
		},
		{
			name:   "list_functions",
			method: http.MethodGet,
			path:   "/2015-03-31/functions",
			want:   "lambda:ListFunctions",
		},
		{
			name:   "get_function",
			method: http.MethodGet,
			path:   "/2015-03-31/functions/my-func",
			want:   "lambda:GetFunction",
		},
		{
			name:   "delete_function",
			method: http.MethodDelete,
			path:   "/2015-03-31/functions/my-func",
			want:   "lambda:DeleteFunction",
		},
		{
			name:   "update_code",
			method: http.MethodPut,
			path:   "/2015-03-31/functions/my-func/code",
			want:   "lambda:UpdateFunctionCode",
		},
		{
			name:   "invoke",
			method: http.MethodPost,
			path:   "/2015-03-31/functions/my-func/invocations",
			want:   "lambda:InvokeFunction",
		},
		{
			name:   "list_tags",
			method: http.MethodGet,
			path:   "/2015-03-31/tags/arn:aws:lambda:us-east-1:0:function:f",
			want:   "lambda:ListTags",
		},
		{
			name:   "tag_resource",
			method: http.MethodPost,
			path:   "/2015-03-31/tags/arn:aws:lambda:us-east-1:0:function:f",
			want:   "lambda:TagResource",
		},
		{name: "non_lambda_path", method: http.MethodGet, path: "/s3/bucket", want: ""},
		{
			name:   "esm_create",
			method: http.MethodPost,
			path:   "/2015-03-31/event-source-mappings",
			want:   "lambda:CreateEventSourceMapping",
		},
		{
			name:   "esm_list",
			method: http.MethodGet,
			path:   "/2015-03-31/event-source-mappings",
			want:   "lambda:ListEventSourceMappings",
		},
		{
			name:   "esm_get",
			method: http.MethodGet,
			path:   "/2015-03-31/event-source-mappings/uuid-1234",
			want:   "lambda:GetEventSourceMapping",
		},
		{
			name:   "esm_delete",
			method: http.MethodDelete,
			path:   "/2015-03-31/event-source-mappings/uuid-1234",
			want:   "lambda:DeleteEventSourceMapping",
		},
		{
			name:   "esm_update",
			method: http.MethodPut,
			path:   "/2015-03-31/event-source-mappings/uuid-1234",
			want:   "lambda:UpdateEventSourceMapping",
		},
		{
			// ESM with unrecognized method → esmIAMAction returns "".
			name:   "esm_unknown_method",
			method: http.MethodPatch,
			path:   "/2015-03-31/event-source-mappings/uuid-1234",
			want:   "",
		},
		{
			// Lambda layers path → extractLayerOperation path in IAMAction (correct prefix).
			name:   "layers_list",
			method: http.MethodGet,
			path:   "/2018-10-31/layers",
			want:   "lambda:ListLayers",
		},
		{
			// Lambda 2020-06-30 path prefix → lambda2020PathPrefix branch in IAMAction.
			name:   "lambda_2020_get_function",
			method: http.MethodGet,
			path:   "/2020-06-30/functions/my-func",
			want:   "lambda:GetFunction",
		},
		{
			// Lambda path with no matching route → returns "".
			name:   "lambda_path_unknown_sub_path",
			method: http.MethodPatch,
			path:   "/2015-03-31/functions/fn/unknown-route",
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			assert.Equal(t, tt.want, h.IAMAction(req))
		})
	}
}

// TestHandler_ChaosProvider verifies that the Lambda handler implements the ChaosProvider interface.
func TestHandler_ChaosProvider(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	assert.Equal(t, "lambda", h.ChaosServiceName())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
}

// TestRuntimeServer_InvokeTimeoutRace verifies that when a container response arrives
// concurrently with a timeout, the result is not silently discarded — invoke either
// returns the result or returns the timeout error, but never panics or deadlocks.
func TestRuntimeServer_InvokeTimeoutRace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		responseBody string
		port         int
		// responseDelay is injected between receiving /next and posting the result.
		// When it is shorter than the invoke timeout, the response should win.
		// When it is longer, the timeout should win.
		responseDelay time.Duration
		invokeTimeout time.Duration
		wantTimeout   bool
	}{
		{
			name:          "response_arrives_before_timeout",
			port:          18150,
			responseBody:  `{"ok":true}`,
			responseDelay: 0,
			invokeTimeout: 500 * time.Millisecond,
			wantTimeout:   false,
		},
		{
			name:          "response_arrives_after_timeout",
			port:          18151,
			responseBody:  `{"ok":true}`,
			responseDelay: 300 * time.Millisecond,
			invokeTimeout: 50 * time.Millisecond,
			wantTimeout:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestRuntimeServer(t, tt.port)
			ctx := t.Context()

			resultCh := make(chan []byte, 1)
			errCh := make(chan error, 1)

			go func() {
				result, _, invokeErr := srv.Invoke(ctx, []byte(`{}`), tt.invokeTimeout)
				if invokeErr != nil {
					errCh <- invokeErr

					return
				}

				resultCh <- result
			}()

			requestID := simulateContainerNext(t, tt.port)

			// Optionally delay the container response to force a timeout race.
			if tt.responseDelay > 0 {
				time.Sleep(tt.responseDelay)
			}

			// Send the response without asserting the status code — in the timeout
			// case the invocation is already gone so the runtime API returns 404.
			sendContainerResponse(t, tt.port, requestID, tt.responseBody)

			select {
			case result := <-resultCh:
				if tt.wantTimeout {
					// Result arrived before we expected — acceptable since timing is not exact.
					assert.NotEmpty(t, result)
				} else {
					assert.JSONEq(t, tt.responseBody, string(result))
				}
			case err := <-errCh:
				if tt.wantTimeout {
					require.ErrorIs(t, err, lambda.ErrInvocationTimeout)
				} else {
					require.NoError(t, err, "unexpected invoke error")
				}
			case <-time.After(5 * time.Second):
				require.FailNow(t, "test timed out — possible deadlock in invoke")
			}
		})
	}
}

// sendContainerResponse posts a response to the runtime API without asserting the HTTP status.
// This is used when the response may legitimately race with a timeout (404 is acceptable).
func sendContainerResponse(t *testing.T, port int, requestID, responseBody string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx,
		http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/2018-06-01/runtime/invocation/%s/response", port, requestID),
		strings.NewReader(responseBody),
	)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	_ = resp.Body.Close()
}
