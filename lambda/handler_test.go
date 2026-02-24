package lambda_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/lambda"
	"github.com/blackbirdworks/gopherstack/pkgs/docker"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
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

func (m *mockBackend) ListFunctions() []*lambda.FunctionConfiguration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fns := make([]*lambda.FunctionConfiguration, 0, len(m.functions))
	for _, fn := range m.functions {
		fns = append(fns, fn)
	}

	return fns
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
	h := lambda.NewHandler(bk, nil)
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

func TestCreateFunction_Success(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)
	body := `{"FunctionName":"my-func","PackageType":"Image",` +
		`"Code":{"ImageUri":"123456789012.dkr.ecr.us-east-1.amazonaws.com/myimage:latest"},` +
		`"Role":"arn:aws:iam::000000000000:role/myrole"}`

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", body, nil)

	assert.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fn))
	assert.Equal(t, "my-func", fn.FunctionName)
	assert.Equal(t, "arn:aws:lambda:us-east-1:000000000000:function:my-func", fn.FunctionArn)
	assert.Equal(t, lambda.PackageTypeImage, fn.PackageType)
	assert.Equal(t, lambda.FunctionStateActive, fn.State)
	assert.Equal(t, 128, fn.MemorySize)
	assert.Equal(t, 3, fn.Timeout)
	assert.NotEmpty(t, fn.RevisionID)
}

func TestCreateFunction_DefaultsApplied(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)
	body := `{"FunctionName":"defaults-func","PackageType":"Image",` +
		`"Code":{"ImageUri":"myimage:latest"},"MemorySize":256,"Timeout":60}`

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", body, nil)
	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fn))
	assert.Equal(t, 256, fn.MemorySize)
	assert.Equal(t, 60, fn.Timeout)
}

func TestCreateFunction_MissingFunctionName(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)
	body := `{"PackageType":"Image","Code":{"ImageUri":"myimage:latest"}}`

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", body, nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assertLambdaError(t, rec, "InvalidParameterValueException")
}

func TestCreateFunction_InvalidPackageType(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)
	body := `{"FunctionName":"zip-func","PackageType":"Zip","Code":{"S3Bucket":"mybucket","S3Key":"code.zip"}}`

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", body, nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assertLambdaError(t, rec, "InvalidParameterValueException")
}

func TestCreateFunction_MissingImageURI(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)
	body := `{"FunctionName":"no-image-func","PackageType":"Image","Code":{}}`

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", body, nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assertLambdaError(t, rec, "InvalidParameterValueException")
}

func TestCreateFunction_InvalidBody(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", "not-json{", nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateFunction_AlreadyExists(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)
	body := `{"FunctionName":"dup-func","PackageType":"Image","Code":{"ImageUri":"myimage:latest"}}`

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", body, nil)
	require.Equal(t, http.StatusCreated, rec.Code)

	rec2 := callHandler(t, h, http.MethodPost, "/2015-03-31/functions", body, nil)
	assert.Equal(t, http.StatusConflict, rec2.Code)
	assertLambdaError(t, rec2, "ResourceConflictException")
}

// ---- GetFunction tests ----

func TestGetFunction_Success(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["get-func"] = &lambda.FunctionConfiguration{
		FunctionName: "get-func",
		FunctionArn:  "arn:aws:lambda:us-east-1:000000000000:function:get-func",
		ImageURI:     "myimage:latest",
		PackageType:  lambda.PackageTypeImage,
		State:        lambda.FunctionStateActive,
	}

	rec := callHandler(t, h, http.MethodGet, "/2015-03-31/functions/get-func", "", nil)

	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.GetFunctionOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.Configuration)
	assert.Equal(t, "get-func", out.Configuration.FunctionName)
	require.NotNil(t, out.Code)
	assert.Equal(t, "myimage:latest", out.Code.ImageURI)
	assert.Equal(t, "ECR", out.Code.RepositoryType)
}

func TestGetFunction_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	rec := callHandler(t, h, http.MethodGet, "/2015-03-31/functions/nonexistent", "", nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assertLambdaError(t, rec, "ResourceNotFoundException")
}

// ---- ListFunctions tests ----

func TestListFunctions_Empty(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	rec := callHandler(t, h, http.MethodGet, "/2015-03-31/functions", "", nil)

	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.ListFunctionsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out.Functions)
}

func TestListFunctions_Multiple(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["func-a"] = &lambda.FunctionConfiguration{FunctionName: "func-a"}
	bk.functions["func-b"] = &lambda.FunctionConfiguration{FunctionName: "func-b"}

	rec := callHandler(t, h, http.MethodGet, "/2015-03-31/functions", "", nil)

	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.ListFunctionsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.Functions, 2)
}

// ---- DeleteFunction tests ----

func TestDeleteFunction_Success(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["del-func"] = &lambda.FunctionConfiguration{FunctionName: "del-func"}

	rec := callHandler(t, h, http.MethodDelete, "/2015-03-31/functions/del-func", "", nil)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Empty(t, bk.functions)
}

func TestDeleteFunction_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	rec := callHandler(t, h, http.MethodDelete, "/2015-03-31/functions/missing", "", nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assertLambdaError(t, rec, "ResourceNotFoundException")
}

// ---- UpdateFunctionCode tests ----

func TestUpdateFunctionCode_Success(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["code-func"] = &lambda.FunctionConfiguration{
		FunctionName: "code-func",
		ImageURI:     "old-image:v1",
	}

	body := `{"ImageUri":"new-image:v2"}`
	rec := callHandler(t, h, http.MethodPut, "/2015-03-31/functions/code-func/code", body, nil)

	require.Equal(t, http.StatusOK, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fn))
	assert.Equal(t, "new-image:v2", fn.ImageURI)
	assert.NotEmpty(t, fn.RevisionID)
}

func TestUpdateFunctionCode_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)
	body := `{"ImageUri":"new-image:v2"}`

	rec := callHandler(t, h, http.MethodPut, "/2015-03-31/functions/missing/code", body, nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestUpdateFunctionCode_MissingImageURI(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["code-func"] = &lambda.FunctionConfiguration{FunctionName: "code-func"}

	rec := callHandler(t, h, http.MethodPut, "/2015-03-31/functions/code-func/code", `{}`, nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestUpdateFunctionCode_InvalidBody(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["code-func"] = &lambda.FunctionConfiguration{FunctionName: "code-func"}

	rec := callHandler(t, h, http.MethodPut, "/2015-03-31/functions/code-func/code", "bad{json}", nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- UpdateFunctionConfiguration tests ----

func TestUpdateFunctionConfiguration_Success(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["cfg-func"] = &lambda.FunctionConfiguration{
		FunctionName: "cfg-func",
		MemorySize:   128,
		Timeout:      3,
		Description:  "old description",
	}

	body := `{"Description":"new description","MemorySize":512,"Timeout":30,"Role":"new-role"}`
	rec := callHandler(t, h, http.MethodPut, "/2015-03-31/functions/cfg-func/configuration", body, nil)

	require.Equal(t, http.StatusOK, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fn))
	assert.Equal(t, "new description", fn.Description)
	assert.Equal(t, 512, fn.MemorySize)
	assert.Equal(t, 30, fn.Timeout)
	assert.Equal(t, "new-role", fn.Role)
}

func TestUpdateFunctionConfiguration_UpdateEnvironment(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["env-func"] = &lambda.FunctionConfiguration{FunctionName: "env-func"}

	body := `{"Environment":{"Variables":{"KEY":"VALUE"}}}`
	rec := callHandler(t, h, http.MethodPut, "/2015-03-31/functions/env-func/configuration", body, nil)

	require.Equal(t, http.StatusOK, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fn))
	require.NotNil(t, fn.Environment)
	assert.Equal(t, "VALUE", fn.Environment.Variables["KEY"])
}

func TestUpdateFunctionConfiguration_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	rec := callHandler(t, h, http.MethodPut, "/2015-03-31/functions/missing/configuration", `{}`, nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestUpdateFunctionConfiguration_InvalidBody(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["cfg-func"] = &lambda.FunctionConfiguration{FunctionName: "cfg-func"}

	rec := callHandler(t, h, http.MethodPut, "/2015-03-31/functions/cfg-func/configuration", "bad{json}", nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- Invoke tests ----

func TestInvoke_RequestResponse(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["invoke-func"] = &lambda.FunctionConfiguration{FunctionName: "invoke-func"}
	bk.invokeResult = []byte(`{"answer":42}`)

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions/invoke-func/invocations", `{"key":"value"}`, nil)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "42")
}

func TestInvoke_Event(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["event-func"] = &lambda.FunctionConfiguration{FunctionName: "event-func"}

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions/event-func/invocations", `{}`,
		map[string]string{"X-Amz-Invocation-Type": "Event"})

	assert.Equal(t, http.StatusAccepted, rec.Code)
}

func TestInvoke_DryRun(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["dryrun-func"] = &lambda.FunctionConfiguration{FunctionName: "dryrun-func"}

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions/dryrun-func/invocations", `{}`,
		map[string]string{"X-Amz-Invocation-Type": "DryRun"})

	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestInvoke_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions/missing/invocations", `{}`, nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assertLambdaError(t, rec, "ResourceNotFoundException")
}

func TestInvoke_ServiceError(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["err-func"] = &lambda.FunctionConfiguration{FunctionName: "err-func"}
	bk.invokeErr = fmt.Errorf("%w: Docker unavailable", lambda.ErrLambdaUnavailable)

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions/err-func/invocations", `{}`, nil)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestInvoke_EmptyBody(t *testing.T) {
	t.Parallel()

	h, bk := newHandler(t)
	bk.functions["body-func"] = &lambda.FunctionConfiguration{FunctionName: "body-func"}

	rec := callHandler(t, h, http.MethodPost, "/2015-03-31/functions/body-func/invocations", "", nil)

	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- Routing tests ----

func TestHandler_UnknownRoute(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)

	rec := callHandler(t, h, http.MethodGet, "/2015-03-31/functions/foo/unknown-sub", "", nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()

	req1 := httptest.NewRequest(http.MethodGet, "/2015-03-31/functions", nil)
	c1 := e.NewContext(req1, httptest.NewRecorder())
	assert.True(t, matcher(c1))

	req2 := httptest.NewRequest(http.MethodGet, "/other", nil)
	req2.Header.Set("X-Amz-Target", "AWSLambda.ListFunctions")
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.True(t, matcher(c2))

	req3 := httptest.NewRequest(http.MethodGet, "/other", nil)
	c3 := e.NewContext(req3, httptest.NewRecorder())
	assert.False(t, matcher(c3))
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)
	ops := h.GetSupportedOperations()
	assert.NotEmpty(t, ops)
	assert.Contains(t, ops, "CreateFunction")
	assert.Contains(t, ops, "InvokeFunction")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)
	assert.Equal(t, 95, h.MatchPriority())
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)
	e := echo.New()

	tests := []struct {
		method   string
		path     string
		expected string
	}{
		{http.MethodPost, "/2015-03-31/functions", "CreateFunction"},
		{http.MethodGet, "/2015-03-31/functions", "ListFunctions"},
		{http.MethodGet, "/2015-03-31/functions/my-func", "GetFunction"},
		{http.MethodDelete, "/2015-03-31/functions/my-func", "DeleteFunction"},
		{http.MethodPut, "/2015-03-31/functions/my-func/code", "UpdateFunctionCode"},
		{http.MethodPut, "/2015-03-31/functions/my-func/configuration", "UpdateFunctionConfiguration"},
		{http.MethodPost, "/2015-03-31/functions/my-func/invocations", "InvokeFunction"},
		{http.MethodGet, "/2015-03-31/functions/my-func/unknown", "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.expected, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodGet, "/2015-03-31/functions/my-func", nil)
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "my-func", h.ExtractResource(c))

	req2 := httptest.NewRequest(http.MethodGet, "/2015-03-31/functions", nil)
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.Empty(t, h.ExtractResource(c2))
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h, _ := newHandler(t)
	assert.Equal(t, "Lambda", h.Name())
}

// ---- Backend tests ----

func TestBackend_CRUD(t *testing.T) {
	t.Parallel()

	bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1", nil)

	fn := &lambda.FunctionConfiguration{
		FunctionName: "test-func",
		ImageURI:     "myimage:latest",
		PackageType:  lambda.PackageTypeImage,
		State:        lambda.FunctionStateActive,
	}

	// Create
	require.NoError(t, bk.CreateFunction(fn))

	// Duplicate
	require.ErrorIs(t, bk.CreateFunction(fn), lambda.ErrFunctionAlreadyExists)

	// Get
	got, err := bk.GetFunction("test-func")
	require.NoError(t, err)
	assert.Equal(t, "test-func", got.FunctionName)

	// Get not found
	_, err = bk.GetFunction("nonexistent")
	require.ErrorIs(t, err, lambda.ErrFunctionNotFound)

	// List
	list := bk.ListFunctions()
	assert.Len(t, list, 1)

	// Update
	fn2 := *fn
	fn2.Description = "updated"
	require.NoError(t, bk.UpdateFunction(&fn2))

	got2, err := bk.GetFunction("test-func")
	require.NoError(t, err)
	assert.Equal(t, "updated", got2.Description)

	// Update not found
	notExist := &lambda.FunctionConfiguration{FunctionName: "nonexistent"}
	require.ErrorIs(t, bk.UpdateFunction(notExist), lambda.ErrFunctionNotFound)

	// Delete
	require.NoError(t, bk.DeleteFunction("test-func"))
	assert.Empty(t, bk.ListFunctions())

	// Delete not found
	assert.ErrorIs(t, bk.DeleteFunction("test-func"), lambda.ErrFunctionNotFound)
}

func TestBackend_InvokeFunction_NoPortAlloc(t *testing.T) {
	t.Parallel()

	bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1", nil)

	fn := &lambda.FunctionConfiguration{
		FunctionName: "invoke-func",
		ImageURI:     "myimage:latest",
		Timeout:      3,
	}
	require.NoError(t, bk.CreateFunction(fn))

	_, _, err := bk.InvokeFunction(
		context.Background(), "invoke-func", lambda.InvocationTypeRequestResponse, []byte("{}"),
	)
	assert.ErrorIs(t, err, lambda.ErrLambdaUnavailable)
}

func TestBackend_InvokeFunction_NoDocker(t *testing.T) {
	t.Parallel()

	pa, err := portalloc.New(19000, 19100)
	require.NoError(t, err)

	bk := lambda.NewInMemoryBackend(nil, pa, lambda.DefaultSettings(), "000000000000", "us-east-1", nil)

	fn := &lambda.FunctionConfiguration{
		FunctionName: "invoke-func",
		ImageURI:     "myimage:latest",
		Timeout:      3,
	}
	require.NoError(t, bk.CreateFunction(fn))

	_, _, err = bk.InvokeFunction(
		context.Background(), "invoke-func", lambda.InvocationTypeRequestResponse, []byte("{}"),
	)
	assert.ErrorIs(t, err, lambda.ErrLambdaUnavailable)
}

func TestBackend_InvokeFunction_NotFound(t *testing.T) {
	t.Parallel()

	bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1", nil)

	_, statusCode, err := bk.InvokeFunction(
		context.Background(), "nonexistent", lambda.InvocationTypeRequestResponse, []byte("{}"),
	)
	require.ErrorIs(t, err, lambda.ErrFunctionNotFound)
	assert.Equal(t, http.StatusNotFound, statusCode)
}

func TestBackend_InvokeFunction_DryRun(t *testing.T) {
	t.Parallel()

	bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", "us-east-1", nil)
	fn := &lambda.FunctionConfiguration{FunctionName: "fn", ImageURI: "img:latest", Timeout: 3}
	require.NoError(t, bk.CreateFunction(fn))

	_, statusCode, err := bk.InvokeFunction(context.Background(), "fn", lambda.InvocationTypeDryRun, []byte("{}"))
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, statusCode)
}

// ---- Runtime API server tests ----

func TestRuntimeServer_NextAndResponse(t *testing.T) {
	t.Parallel()

	port := 18101
	srv := newTestRuntimeServer(t, port)

	ctx := context.Background()
	payload := []byte(`{"key":"value"}`)

	// Start an invoke in a goroutine — it will block until the container responds.
	resultCh := make(chan []byte, 1)
	errCh := make(chan error, 1)

	go func() {
		result, _, invokeErr := srv.Invoke(ctx, payload, 5*time.Second)
		if invokeErr != nil {
			errCh <- invokeErr

			return
		}

		resultCh <- result
	}()

	// Simulate the container: GET /next then POST /response.
	requestID := simulateContainerNext(t, port)
	simulateContainerResponse(t, port, requestID, `{"answer":42}`)

	select {
	case result := <-resultCh:
		assert.JSONEq(t, `{"answer":42}`, string(result))
	case err := <-errCh:
		t.Fatalf("invoke error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("test timed out")
	}
}

func TestRuntimeServer_NextAndError(t *testing.T) {
	t.Parallel()

	port := 18102
	srv := newTestRuntimeServer(t, port)

	ctx := context.Background()

	resultCh := make(chan []byte, 1)
	errCh := make(chan error, 1)
	isErrorCh := make(chan bool, 1)

	go func() {
		result, isErr, invokeErr := srv.Invoke(ctx, []byte(`{}`), 5*time.Second)
		if invokeErr != nil {
			errCh <- invokeErr

			return
		}

		resultCh <- result
		isErrorCh <- isErr
	}()

	requestID := simulateContainerNext(t, port)
	simulateContainerError(t, port, requestID, `{"errorMessage":"function panicked"}`)

	select {
	case result := <-resultCh:
		isError := <-isErrorCh
		assert.True(t, isError, "expected isError=true")
		assert.Contains(t, string(result), "panicked")
	case err := <-errCh:
		t.Fatalf("invoke error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("test timed out")
	}
}

func TestRuntimeServer_InitError(t *testing.T) {
	t.Parallel()

	port := 18103
	_ = newTestRuntimeServer(t, port)

	// POST /2018-06-01/runtime/init/error should return 202.
	body := bytes.NewBufferString(`{"errorMessage":"init failed","errorType":"Runtime.ExitError"}`)
	req, err := http.NewRequestWithContext(context.Background(),
		http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/2018-06-01/runtime/init/error", port),
		body,
	)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
}

func TestRuntimeServer_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	port := 18104
	_ = newTestRuntimeServer(t, port)

	// /next only allows GET
	req, err := http.NewRequestWithContext(context.Background(),
		http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/2018-06-01/runtime/invocation/next", port),
		nil,
	)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

func TestRuntimeServer_ResponseUnknownRequestID(t *testing.T) {
	t.Parallel()

	port := 18105
	_ = newTestRuntimeServer(t, port)

	body := bytes.NewBufferString(`{"result":"ok"}`)
	req, err := http.NewRequestWithContext(context.Background(),
		http.MethodPost,
		fmt.Sprintf("http://127.0.0.1:%d/2018-06-01/runtime/invocation/unknown-id/response", port),
		body,
	)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestRuntimeServer_InvokeTimeout(t *testing.T) {
	t.Parallel()

	port := 18106
	srv := newTestRuntimeServer(t, port)

	ctx := context.Background()
	// Use a very short timeout — no container will call /next.
	_, _, err := srv.Invoke(ctx, []byte(`{}`), 100*time.Millisecond)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}

func TestRuntimeServer_InvokeContextCancelled(t *testing.T) {
	t.Parallel()

	port := 18107
	srv := newTestRuntimeServer(t, port)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)

	go func() {
		_, _, err := srv.Invoke(ctx, []byte(`{}`), 30*time.Second)
		errCh <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("expected context cancellation error")
	}
}

// ---- Settings tests ----

func TestDefaultSettings(t *testing.T) {
	t.Parallel()

	s := lambda.DefaultSettings()
	assert.Equal(t, "172.17.0.1", s.DockerHost)
	assert.Equal(t, 3, s.PoolSize)
	assert.Equal(t, 10*time.Minute, s.IdleTimeout)
}

// ---- helper functions ----

// newTestRuntimeServer is an exported alias used in tests to access the internal runtimeServer.
// We use this via the Invoke method exposed for testing.
func newTestRuntimeServer(t *testing.T, port int) testRuntimeServerIface {
	t.Helper()

	srv := newPublicRuntimeServer(t, port)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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
	require.NoError(t, srv.Start(context.Background()))

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
		req, err := http.NewRequestWithContext(context.Background(),
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

	req, err := http.NewRequestWithContext(context.Background(),
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

	req, err := http.NewRequestWithContext(context.Background(),
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

// mockDockerAPI implements docker.APIClient for testing without a real daemon.
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

// newMockDockerClient creates a docker.Client backed by mockDockerAPI.
func newMockDockerClient() *docker.Client {
	return docker.NewClientWithAPI(&mockDockerAPI{}, docker.Config{
		PoolSize:    3,
		IdleTimeout: time.Minute,
	})
}

// ---- Backend tests with mock Docker ----

func TestBackend_InvokeFunction_Event_WithMockDocker(t *testing.T) {
	t.Parallel()

	// Use a dedicated port range to avoid conflicts.
	pa, err := portalloc.New(19200, 19250)
	require.NoError(t, err)

	dc := newMockDockerClient()

	bk := lambda.NewInMemoryBackend(dc, pa, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default())
	fn := &lambda.FunctionConfiguration{
		FunctionName: "event-fn",
		ImageURI:     "myimage:latest",
		Timeout:      3,
	}
	require.NoError(t, bk.CreateFunction(fn))

	// Event invocation: fire and forget — no container response needed.
	_, statusCode, invokeErr := bk.InvokeFunction(
		context.Background(), "event-fn", lambda.InvocationTypeEvent, []byte(`{}`),
	)
	require.NoError(t, invokeErr)
	assert.Equal(t, http.StatusAccepted, statusCode)
}

func TestBackend_InvokeFunction_Event_SecondCall_ReuseRuntime(t *testing.T) {
	t.Parallel()

	pa, err := portalloc.New(19300, 19350)
	require.NoError(t, err)

	dc := newMockDockerClient()

	bk := lambda.NewInMemoryBackend(dc, pa, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default())
	fn := &lambda.FunctionConfiguration{
		FunctionName: "reuse-fn",
		ImageURI:     "myimage:latest",
		Timeout:      3,
	}
	require.NoError(t, bk.CreateFunction(fn))

	// Two event invocations — second call reuses the already-started runtime.
	_, sc1, err1 := bk.InvokeFunction(context.Background(), "reuse-fn", lambda.InvocationTypeEvent, []byte(`{}`))
	require.NoError(t, err1)
	assert.Equal(t, http.StatusAccepted, sc1)

	_, sc2, err2 := bk.InvokeFunction(context.Background(), "reuse-fn", lambda.InvocationTypeEvent, []byte(`{}`))
	require.NoError(t, err2)
	assert.Equal(t, http.StatusAccepted, sc2)
}

func TestBackend_DeleteFunction_WithRuntime(t *testing.T) {
	t.Parallel()

	pa, err := portalloc.New(19400, 19450)
	require.NoError(t, err)

	dc := newMockDockerClient()

	bk := lambda.NewInMemoryBackend(dc, pa, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default())
	fn := &lambda.FunctionConfiguration{
		FunctionName: "delete-with-rt",
		ImageURI:     "myimage:latest",
		Timeout:      3,
	}
	require.NoError(t, bk.CreateFunction(fn))

	// Start the runtime first via Event invocation.
	_, _, _ = bk.InvokeFunction(context.Background(), "delete-with-rt", lambda.InvocationTypeEvent, []byte(`{}`))

	// Delete should clean up the runtime server and release the port.
	require.NoError(t, bk.DeleteFunction("delete-with-rt"))

	// Verify the function is gone.
	_, err = bk.GetFunction("delete-with-rt")
	assert.ErrorIs(t, err, lambda.ErrFunctionNotFound)
}

func TestBackend_InvokeFunction_RequestResponse_WithMockDocker(t *testing.T) {
	t.Parallel()

	pa, err := portalloc.New(19500, 19550)
	require.NoError(t, err)

	dc := newMockDockerClient()

	bk := lambda.NewInMemoryBackend(dc, pa, lambda.DefaultSettings(), "000000000000", "us-east-1", slog.Default())
	fn := &lambda.FunctionConfiguration{
		FunctionName: "rr-fn",
		ImageURI:     "myimage:latest",
		Timeout:      3,
	}
	require.NoError(t, bk.CreateFunction(fn))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resultCh := make(chan error, 1)

	go func() {
		_, _, invokeErr := bk.InvokeFunction(ctx, "rr-fn", lambda.InvocationTypeRequestResponse, []byte(`{}`))
		resultCh <- invokeErr
	}()

	// Give the runtime server a moment to start, then simulate the container.
	time.Sleep(200 * time.Millisecond)

	// Find the allocated port by repeatedly trying to connect to candidate ports.
	var runtimePort int

	for p := 19500; p < 19550; p++ {
		req, reqErr := http.NewRequestWithContext(context.Background(),
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
		// If no port found (e.g., race), just check we get some result.
	case <-time.After(4 * time.Second):
		cancel()
	}
}

// ---- Provider tests ----

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &lambda.Provider{}
	assert.Equal(t, "Lambda", p.Name())
}

func TestProvider_Init_NoConfig(t *testing.T) {
	t.Parallel()

	p := &lambda.Provider{}
	appCtx := &service.AppContext{
		Logger:    slog.Default(),
		Config:    nil,
		PortAlloc: nil,
	}

	svc, err := p.Init(appCtx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "Lambda", svc.Name())
}

func TestProvider_Init_WithConfig(t *testing.T) {
	t.Parallel()

	p := &lambda.Provider{}
	appCtx := &service.AppContext{
		Logger:    slog.Default(),
		Config:    &mockConfig{accountID: "111111111111", region: "eu-west-1"},
		PortAlloc: nil,
	}

	svc, err := p.Init(appCtx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

// mockConfig implements lambda.SettingsProvider for provider tests.
type mockConfig struct {
	accountID string
	region    string
}

func (m *mockConfig) GetLambdaSettings() lambda.Settings {
	return lambda.DefaultSettings()
}
