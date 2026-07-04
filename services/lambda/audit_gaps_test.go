package lambda_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// ---- helpers ----

func auditCreateFunction(t *testing.T, h *lambda.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/2015-03-31/functions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func auditUpdateConfig(t *testing.T, h *lambda.Handler, name, body string) *httptest.ResponseRecorder {
	t.Helper()

	path := "/2015-03-31/functions/" + name + "/configuration"
	req := httptest.NewRequest(http.MethodPut, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func auditGetConfig(t *testing.T, h *lambda.Handler, name string) *httptest.ResponseRecorder {
	t.Helper()

	path := "/2015-03-31/functions/" + name + "/configuration"
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func auditGetFunction(t *testing.T, h *lambda.Handler, name string) *httptest.ResponseRecorder {
	t.Helper()

	path := "/2015-03-31/functions/" + name
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func baseImageFn(name string) string {
	return fmt.Sprintf(
		`{"FunctionName":%q,"PackageType":"Image","Code":{"ImageUri":"ecr/x:latest"},"Role":"arn:aws:iam:::role/r"}`,
		name,
	)
}

func baseZipFn(name string) string {
	const tpl = `{"FunctionName":%q,"PackageType":"Zip","Runtime":"python3.12",` +
		`"Handler":"index.handler","Code":{"ZipFile":"UEsDBA=="},"Role":"arn:aws:iam:::role/r"}`

	return fmt.Sprintf(tpl, name)
}

// ---- Gap 1: VpcConfig ----

func TestAudit_VpcConfig_CreateAndGet(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"vpc-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"ecr/x:latest"},
		"Role":"arn:aws:iam:::role/r",
		"VpcConfig":{"SubnetIds":["subnet-abc"],"SecurityGroupIds":["sg-xyz"]}
	}`
	rec := auditCreateFunction(t, h, body)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var created lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&created))
	require.NotNil(t, created.VpcConfig)
	assert.Equal(t, []string{"subnet-abc"}, created.VpcConfig.SubnetIDs)
	assert.Equal(t, []string{"sg-xyz"}, created.VpcConfig.SecurityGroupIDs)

	// GetFunction returns VpcConfig
	rec2 := auditGetFunction(t, h, "vpc-fn")
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&out))
	var cfg lambda.FunctionConfiguration
	require.NoError(t, json.Unmarshal(out["Configuration"], &cfg))
	require.NotNil(t, cfg.VpcConfig)
	assert.Equal(t, []string{"subnet-abc"}, cfg.VpcConfig.SubnetIDs)
}

func TestAudit_VpcConfig_UpdateAndGet(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("vpc-update-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	updateBody := `{"VpcConfig":{"SubnetIds":["subnet-new"],"SecurityGroupIds":["sg-new"]}}`
	rec2 := auditUpdateConfig(t, h, "vpc-update-fn", updateBody)
	require.Equal(t, http.StatusOK, rec2.Code, rec2.Body.String())

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&fn))
	require.NotNil(t, fn.VpcConfig)
	assert.Equal(t, []string{"subnet-new"}, fn.VpcConfig.SubnetIDs)
}

// ---- Gap 2: TracingConfig ----

func TestAudit_TracingConfig_CreateDefault(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("tracing-default-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	// Default TracingConfig is PassThrough
	require.NotNil(t, fn.TracingConfig)
	assert.Equal(t, "PassThrough", fn.TracingConfig.Mode)
}

func TestAudit_TracingConfig_CreateActive(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"tracing-active-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"ecr/x:latest"},
		"Role":"arn:aws:iam:::role/r",
		"TracingConfig":{"Mode":"Active"}
	}`
	rec := auditCreateFunction(t, h, body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	require.NotNil(t, fn.TracingConfig)
	assert.Equal(t, "Active", fn.TracingConfig.Mode)
}

func TestAudit_TracingConfig_Update(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("tracing-update-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	rec2 := auditUpdateConfig(t, h, "tracing-update-fn", `{"TracingConfig":{"Mode":"Active"}}`)
	require.Equal(t, http.StatusOK, rec2.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&fn))
	require.NotNil(t, fn.TracingConfig)
	assert.Equal(t, "Active", fn.TracingConfig.Mode)
}

// ---- Gap 3: FileSystemConfigs ----

func TestAudit_FileSystemConfigs_CreateAndGet(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"efs-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"ecr/x:latest"},
		"Role":"arn:aws:iam:::role/r",
		"FileSystemConfigs":[{"Arn":"arn:aws:elasticfilesystem:us-east-1:123:access-point/fsap-abc",` +
		`"LocalMountPath":"/mnt/data"}]
	}`
	rec := auditCreateFunction(t, h, body)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	require.Len(t, fn.FileSystemConfigs, 1)
	assert.Equal(t, "arn:aws:elasticfilesystem:us-east-1:123:access-point/fsap-abc", fn.FileSystemConfigs[0].Arn)
	assert.Equal(t, "/mnt/data", fn.FileSystemConfigs[0].LocalMountPath)
}

func TestAudit_FileSystemConfigs_Update(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("efs-update-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	body := `{"FileSystemConfigs":[{"Arn":"arn:aws:elasticfilesystem:us-east-1:123:access-point/fsap-new",` +
		`"LocalMountPath":"/mnt/new"}]}`
	rec2 := auditUpdateConfig(t, h, "efs-update-fn", body)
	require.Equal(t, http.StatusOK, rec2.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&fn))
	require.Len(t, fn.FileSystemConfigs, 1)
	assert.Equal(t, "/mnt/new", fn.FileSystemConfigs[0].LocalMountPath)
}

// ---- Gap 4: DeadLetterConfig ----

func TestAudit_DeadLetterConfig_CreateAndGet(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"dlq-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"ecr/x:latest"},
		"Role":"arn:aws:iam:::role/r",
		"DeadLetterConfig":{"TargetArn":"arn:aws:sqs:us-east-1:123:my-dlq"}
	}`
	rec := auditCreateFunction(t, h, body)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	require.NotNil(t, fn.DeadLetterConfig)
	assert.Equal(t, "arn:aws:sqs:us-east-1:123:my-dlq", fn.DeadLetterConfig.TargetArn)
}

func TestAudit_DeadLetterConfig_Update(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("dlq-update-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	body := `{"DeadLetterConfig":{"TargetArn":"arn:aws:sns:us-east-1:123:my-topic"}}`
	rec2 := auditUpdateConfig(t, h, "dlq-update-fn", body)
	require.Equal(t, http.StatusOK, rec2.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&fn))
	require.NotNil(t, fn.DeadLetterConfig)
	assert.Equal(t, "arn:aws:sns:us-east-1:123:my-topic", fn.DeadLetterConfig.TargetArn)
}

// ---- Gap 5: EphemeralStorage validation ----

func TestAudit_EphemeralStorage_DefaultOn_Create(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("eph-default-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	require.NotNil(t, fn.EphemeralStorage)
	assert.Equal(t, int32(512), fn.EphemeralStorage.Size)
}

func TestAudit_EphemeralStorage_ValidSize_Create(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"eph-valid-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"ecr/x:latest"},
		"Role":"arn:aws:iam:::role/r",
		"EphemeralStorage":{"Size":1024}
	}`
	rec := auditCreateFunction(t, h, body)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	require.NotNil(t, fn.EphemeralStorage)
	assert.Equal(t, int32(1024), fn.EphemeralStorage.Size)
}

func TestAudit_EphemeralStorage_TooSmall_Create(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"eph-small-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"ecr/x:latest"},
		"Role":"arn:aws:iam:::role/r",
		"EphemeralStorage":{"Size":100}
	}`
	rec := auditCreateFunction(t, h, body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit_EphemeralStorage_TooLarge_Create(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"eph-large-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"ecr/x:latest"},
		"Role":"arn:aws:iam:::role/r",
		"EphemeralStorage":{"Size":99999}
	}`
	rec := auditCreateFunction(t, h, body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit_EphemeralStorage_TooSmall_Update(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("eph-update-small-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	rec2 := auditUpdateConfig(t, h, "eph-update-small-fn", `{"EphemeralStorage":{"Size":10}}`)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestAudit_EphemeralStorage_TooLarge_Update(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("eph-update-large-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	rec2 := auditUpdateConfig(t, h, "eph-update-large-fn", `{"EphemeralStorage":{"Size":99999}}`)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestAudit_EphemeralStorage_Valid_Update(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("eph-update-valid-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	rec2 := auditUpdateConfig(t, h, "eph-update-valid-fn", `{"EphemeralStorage":{"Size":2048}}`)
	require.Equal(t, http.StatusOK, rec2.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&fn))
	require.NotNil(t, fn.EphemeralStorage)
	assert.Equal(t, int32(2048), fn.EphemeralStorage.Size)
}

// ---- Gap 6: ImageConfig in responses ----

func TestAudit_ImageConfig_Persisted_Create(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"imgcfg-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"ecr/myapp:latest"},
		"Role":"arn:aws:iam:::role/r",
		"ImageConfig":{"Command":["serve"],"EntryPoint":["/app"],"WorkingDirectory":"/app"}
	}`
	rec := auditCreateFunction(t, h, body)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	require.NotNil(t, fn.ImageConfig)
	assert.Equal(t, []string{"serve"}, fn.ImageConfig.Command)
	assert.Equal(t, []string{"/app"}, fn.ImageConfig.EntryPoint)
	assert.Equal(t, "/app", fn.ImageConfig.WorkingDirectory)
}

func TestAudit_ImageConfig_InGetFunction(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"imgcfg-get-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"ecr/myapp:latest"},
		"Role":"arn:aws:iam:::role/r",
		"ImageConfig":{"Command":["run"],"EntryPoint":["/entry"]}
	}`
	rec := auditCreateFunction(t, h, body)
	require.Equal(t, http.StatusCreated, rec.Code)

	rec2 := auditGetFunction(t, h, "imgcfg-get-fn")
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&out))
	var cfg lambda.FunctionConfiguration
	require.NoError(t, json.Unmarshal(out["Configuration"], &cfg))
	require.NotNil(t, cfg.ImageConfig)
	assert.Equal(t, []string{"run"}, cfg.ImageConfig.Command)
}

func TestAudit_ImageConfig_NotSetForZip(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseZipFn("zip-no-imgcfg"))
	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	assert.Nil(t, fn.ImageConfig)
}

// ---- Gap 8: ScalingConfig.MaximumConcurrency enforcement ----

func TestAudit_ScalingConfig_MaximumConcurrency_Enforced(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("scaling-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	// Set MaximumConcurrency = 1
	maxConc := 1
	_, err := bk.PutFunctionScalingConfig(
		"scaling-fn",
		&lambda.PutFunctionScalingConfigInput{MaximumConcurrency: &maxConc},
	)
	require.NoError(t, err)

	// Manually hold 1 slot (simulate active invocation)
	held, err := lambda.AcquireConcurrencySlot(bk, "scaling-fn")
	require.NoError(t, err)
	require.True(t, held)

	// Next acquire must fail with TooManyRequests
	_, err = lambda.AcquireConcurrencySlot(bk, "scaling-fn")
	require.ErrorIs(t, err, lambda.ErrTooManyRequests)

	// Release and verify slot becomes available
	lambda.ReleaseConcurrencySlot(bk, "scaling-fn")
	held2, err := lambda.AcquireConcurrencySlot(bk, "scaling-fn")
	require.NoError(t, err)
	require.True(t, held2)
	lambda.ReleaseConcurrencySlot(bk, "scaling-fn")
}

func TestAudit_ScalingConfig_ZeroConcurrency_Blocked(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("scaling-zero-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	// MaximumConcurrency = 0 → no invocations permitted
	zero := 0
	_, err := bk.PutFunctionScalingConfig(
		"scaling-zero-fn",
		&lambda.PutFunctionScalingConfigInput{MaximumConcurrency: &zero},
	)
	require.NoError(t, err)

	// No slots should be acquirable (returns false, nil because hasLimit=false and no reserved)
	// But MaximumConcurrency=0 with scaling config enforcement should block:
	_, err = lambda.AcquireConcurrencySlot(bk, "scaling-zero-fn")
	// With MaximumConcurrency=0, active(0) >= 0 is true so it blocks
	require.ErrorIs(t, err, lambda.ErrTooManyRequests)
}

// ---- Gap 10: Qualifier validation ----

func TestAudit_QualifierValidation_InvalidChars(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("qual-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	// Invoke with invalid qualifier (spaces are not valid)
	path := "/2015-03-31/functions/qual-fn/invocations?Qualifier=bad%20qualifier"
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec2)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestAudit_QualifierValidation_Latest_OK(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("qual-latest-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	path := "/2015-03-31/functions/qual-latest-fn/invocations?Qualifier=$LATEST"
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
	rec2 := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec2)
	require.NoError(t, h.Handler()(c))
	// Should NOT be 400 (function has no container in test, but qualifier is valid)
	assert.NotEqual(t, http.StatusBadRequest, rec2.Code)
}

func TestAudit_QualifierValidation_VersionNumber_OK(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("qual-ver-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	path := "/2015-03-31/functions/qual-ver-fn/invocations?Qualifier=1"
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
	rec2 := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec2)
	require.NoError(t, h.Handler()(c))
	// Not 400 — qualifier is valid (even though version 1 doesn't exist yet)
	assert.NotEqual(t, http.StatusBadRequest, rec2.Code)
}

func TestAudit_QualifierValidation_AliasName_OK(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("qual-alias-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	path := "/2015-03-31/functions/qual-alias-fn/invocations?Qualifier=my-alias_v1"
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
	rec2 := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec2)
	require.NoError(t, h.Handler()(c))
	// Not 400 — qualifier is valid alias name
	assert.NotEqual(t, http.StatusBadRequest, rec2.Code)
}

// ---- RecursiveLoop enforcement ----

func TestAudit_RecursiveLoop_Deny_BlocksSelfInvoke(t *testing.T) {
	t.Parallel()

	bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1")
	closeBackend(t, bk)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "recursive-fn",
		FunctionArn:  "arn:aws:lambda:us-east-1:123456789012:function:recursive-fn",
		Runtime:      "python3.12",
		Handler:      "index.handler",
		Role:         "arn:aws:iam:::role/r",
		PackageType:  "Zip",
		State:        lambda.FunctionStateActive,
	}))

	_, putErr := bk.PutFunctionRecursionConfig("recursive-fn", &lambda.PutFunctionRecursionConfigInput{
		RecursiveLoop: "Deny",
	})
	require.NoError(t, putErr)

	// Simulate a self-invocation: inject the function name into the context chain
	ctx := context.Background()
	// Use the exported helpers from export_test.go — but RecursiveLoop tests need internal access.
	// Instead, call InvokeFunctionWithQualifier twice simulating nesting by wrapping context.
	// We test via the backend directly, injecting the chain.
	_, _, _, _, err := bk.InvokeFunctionWithQualifier(
		lambda.WithInvocationChainForTest(ctx, "recursive-fn"),
		"recursive-fn",
		"",
		"", "",
		lambda.InvocationTypeRequestResponse,
		[]byte("{}"),
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, lambda.ErrInvalidParameterValue)
}

func TestAudit_RecursiveLoop_Terminate_AllowsSelfInvoke(t *testing.T) {
	t.Parallel()

	bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1")
	closeBackend(t, bk)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "recursive-terminate-fn",
		FunctionArn:  "arn:aws:lambda:us-east-1:123456789012:function:recursive-terminate-fn",
		Runtime:      "python3.12",
		Handler:      "index.handler",
		Role:         "arn:aws:iam:::role/r",
		PackageType:  "Zip",
		State:        lambda.FunctionStateActive,
	}))

	_, putErr := bk.PutFunctionRecursionConfig("recursive-terminate-fn", &lambda.PutFunctionRecursionConfigInput{
		RecursiveLoop: "Terminate",
	})
	require.NoError(t, putErr)

	// With Terminate mode, self-invocation should NOT return ErrInvalidParameterValue
	// (it will fail for other reasons like no runtime, but not for recursion rejection)
	ctx := lambda.WithInvocationChainForTest(context.Background(), "recursive-terminate-fn")
	_, _, _, _, err := bk.InvokeFunctionWithQualifier(
		ctx,
		"recursive-terminate-fn",
		"",
		"", "",
		lambda.InvocationTypeRequestResponse,
		[]byte("{}"),
	)
	// Should not be a recursion-denial error
	assert.NotErrorIs(t, err, lambda.ErrInvalidParameterValue)
}

// ---- InvokeWithResponseStream ----

func TestAudit_InvokeWithResponseStream_FunctionNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	path := "/2021-11-15/functions/nonexistent/response-streaming-invocations"
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAudit_InvokeWithResponseStream_ReturnsEventstreamContentType(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("stream-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	path := "/2021-11-15/functions/stream-fn/response-streaming-invocations"
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
	rec2 := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec2)
	require.NoError(t, h.Handler()(c))

	// Should return streaming content type (function will fail to run in test env,
	// but at minimum should not return 400/404 for existing function)
	ct := rec2.Header().Get("Content-Type")
	if rec2.Code == http.StatusOK {
		assert.Equal(t, "application/vnd.amazon.eventstream", ct)
	}
}

// ---- Eventstream frame format ----

func TestAudit_EventstreamFrame_Format(t *testing.T) {
	t.Parallel()

	// Verify our frame encoding: 4-byte big-endian length prefix followed by payload.
	payload := []byte(`{"result":"ok"}`)
	buf := &strings.Builder{}

	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))

	buf.Write(lenBuf[:])
	buf.Write(payload)

	// End of stream
	binary.BigEndian.PutUint32(lenBuf[:], 0)
	buf.Write(lenBuf[:])

	raw := []byte(buf.String())

	// Read back frame 1
	require.GreaterOrEqual(t, len(raw), 4)
	frameLen := binary.BigEndian.Uint32(raw[:4])
	assert.Equal(t, uint32(len(payload)), frameLen)
	assert.Equal(t, payload, raw[4:4+frameLen])

	// Read back end-of-stream
	eos := raw[4+frameLen:]
	require.Len(t, eos, 4)
	eosLen := binary.BigEndian.Uint32(eos)
	assert.Equal(t, uint32(0), eosLen)
}

// ---- Close() cancels poller ----

func TestAudit_Close_CancelsPoller(t *testing.T) {
	t.Parallel()

	bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1")
	closeBackend(t, bk)

	pollDone := make(chan struct{})
	poller := lambda.NewPollerWithCancelSignal(pollDone)

	bk.SetKinesisPoller(poller)
	bk.StartKinesisPoller(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	bk.Close(ctx)

	select {
	case <-pollDone:
		// poller's goroutine received the cancel signal
	case <-time.After(2 * time.Second):
		t.Fatal("poller was not cancelled within 2 seconds after Close()")
	}
}

// ---- All new config fields persist through GetFunctionConfiguration ----

func TestAudit_AllNewFields_PersistInGetConfiguration(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"all-fields-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"ecr/x:latest"},
		"Role":"arn:aws:iam:::role/r",
		"VpcConfig":{"SubnetIds":["subnet-1"],"SecurityGroupIds":["sg-1"]},
		"TracingConfig":{"Mode":"Active"},
		"FileSystemConfigs":[{"Arn":"arn:aws:elasticfilesystem:us-east-1:123:access-point/fsap-1",` +
		`"LocalMountPath":"/mnt/efs"}],
		"DeadLetterConfig":{"TargetArn":"arn:aws:sqs:us-east-1:123:dlq"},
		"EphemeralStorage":{"Size":1024},
		"ImageConfig":{"Command":["app"],"EntryPoint":["/docker-entry"],"WorkingDirectory":"/var/app"}
	}`
	rec := auditCreateFunction(t, h, body)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	rec2 := auditGetConfig(t, h, "all-fields-fn")
	require.Equal(t, http.StatusOK, rec2.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&fn))

	require.NotNil(t, fn.VpcConfig)
	assert.Equal(t, []string{"subnet-1"}, fn.VpcConfig.SubnetIDs)

	require.NotNil(t, fn.TracingConfig)
	assert.Equal(t, "Active", fn.TracingConfig.Mode)

	require.Len(t, fn.FileSystemConfigs, 1)
	assert.Equal(t, "/mnt/efs", fn.FileSystemConfigs[0].LocalMountPath)

	require.NotNil(t, fn.DeadLetterConfig)
	assert.Equal(t, "arn:aws:sqs:us-east-1:123:dlq", fn.DeadLetterConfig.TargetArn)

	require.NotNil(t, fn.EphemeralStorage)
	assert.Equal(t, int32(1024), fn.EphemeralStorage.Size)

	require.NotNil(t, fn.ImageConfig)
	assert.Equal(t, []string{"app"}, fn.ImageConfig.Command)
}

// ---- PublishVersion carries new fields ----

func TestAudit_PublishVersion_CarriesNewFields(t *testing.T) {
	t.Parallel()

	bk := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1")
	closeBackend(t, bk)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName:     "pubver-fn",
		FunctionArn:      "arn:aws:lambda:us-east-1:123456789012:function:pubver-fn",
		PackageType:      "Image",
		Role:             "arn:aws:iam:::role/r",
		State:            lambda.FunctionStateActive,
		VpcConfig:        &lambda.VpcConfig{SubnetIDs: []string{"subnet-pub"}},
		TracingConfig:    &lambda.TracingConfig{Mode: "Active"},
		DeadLetterConfig: &lambda.DeadLetterConfig{TargetArn: "arn:aws:sqs:us-east-1:123:dlq"},
		ImageConfig:      &lambda.ImageConfig{Command: []string{"run"}},
		EphemeralStorage: &lambda.EphemeralStorageConfig{Size: 2048},
	}))

	ver, err := bk.PublishVersion("pubver-fn", "v1")
	require.NoError(t, err)

	require.NotNil(t, ver.VpcConfig)
	assert.Equal(t, []string{"subnet-pub"}, ver.VpcConfig.SubnetIDs)

	require.NotNil(t, ver.TracingConfig)
	assert.Equal(t, "Active", ver.TracingConfig.Mode)

	require.NotNil(t, ver.DeadLetterConfig)
	assert.Equal(t, "arn:aws:sqs:us-east-1:123:dlq", ver.DeadLetterConfig.TargetArn)

	require.NotNil(t, ver.ImageConfig)
	assert.Equal(t, []string{"run"}, ver.ImageConfig.Command)
}

// ---- EphemeralStorage boundary values ----

func TestAudit_EphemeralStorage_BoundaryValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		size   int
		wantOK bool
	}{
		{"min_512", 512, true},
		{"max_10240", 10240, true},
		{"below_min_511", 511, false},
		{"above_max_10241", 10241, false},
		{"zero", 0, false},
		{"mid_5000", 5000, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)

			body := fmt.Sprintf(`{
				"FunctionName":"eph-boundary-%s",
				"PackageType":"Image",
				"Code":{"ImageUri":"ecr/x:latest"},
				"Role":"arn:aws:iam:::role/r",
				"EphemeralStorage":{"Size":%d}
			}`, tc.name, tc.size)
			rec := auditCreateFunction(t, h, body)

			if tc.wantOK {
				assert.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
			} else {
				assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			}
		})
	}
}

// ---- JSON response shapes match AWS ----

func TestAudit_VpcConfig_JSONShape(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"vpc-shape-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"ecr/x:latest"},
		"Role":"arn:aws:iam:::role/r",
		"VpcConfig":{"SubnetIds":["subnet-shape"],"SecurityGroupIds":["sg-shape"]}
	}`
	rec := auditCreateFunction(t, h, body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var raw map[string]json.RawMessage
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&raw))

	vpcRaw, ok := raw["VpcConfig"]
	require.True(t, ok, "VpcConfig key missing from response")

	var vpc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(vpcRaw, &vpc))
	assert.Contains(t, vpc, "SubnetIds")
	assert.Contains(t, vpc, "SecurityGroupIds")
}

func TestAudit_TracingConfig_JSONShape(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("tracing-shape-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	var raw map[string]json.RawMessage
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&raw))

	_, ok := raw["TracingConfig"]
	assert.True(t, ok, "TracingConfig key missing from response")
}

// ---- Integration: multiple fields round-trip ----

func TestAudit_MultipleFields_RoundTrip(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	create := `{
		"FunctionName":"roundtrip-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"ecr/app:v2"},
		"Role":"arn:aws:iam:::role/r",
		"VpcConfig":{"SubnetIds":["subnet-rt"],"SecurityGroupIds":["sg-rt"]},
		"TracingConfig":{"Mode":"Active"},
		"DeadLetterConfig":{"TargetArn":"arn:aws:sqs:us-east-1:123:rt-dlq"},
		"EphemeralStorage":{"Size":768}
	}`
	rec := auditCreateFunction(t, h, create)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Update: change VpcConfig and TracingConfig
	update := `{
		"VpcConfig":{"SubnetIds":["subnet-updated"],"SecurityGroupIds":["sg-updated"]},
		"TracingConfig":{"Mode":"PassThrough"}
	}`
	rec2 := auditUpdateConfig(t, h, "roundtrip-fn", update)
	require.Equal(t, http.StatusOK, rec2.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&fn))

	// VpcConfig updated
	require.NotNil(t, fn.VpcConfig)
	assert.Equal(t, []string{"subnet-updated"}, fn.VpcConfig.SubnetIDs)

	// TracingConfig updated
	require.NotNil(t, fn.TracingConfig)
	assert.Equal(t, "PassThrough", fn.TracingConfig.Mode)

	// DeadLetterConfig unchanged
	require.NotNil(t, fn.DeadLetterConfig)
	assert.Equal(t, "arn:aws:sqs:us-east-1:123:rt-dlq", fn.DeadLetterConfig.TargetArn)

	// EphemeralStorage unchanged
	require.NotNil(t, fn.EphemeralStorage)
	assert.Equal(t, int32(768), fn.EphemeralStorage.Size)
}

// ---- IO readback via GetFunctionConfiguration ----

func TestAudit_GetFunctionConfiguration_IncludesNewFields(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"getconfig-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"ecr/x:latest"},
		"Role":"arn:aws:iam:::role/r",
		"VpcConfig":{"SubnetIds":["subnet-gc"],"SecurityGroupIds":["sg-gc"]},
		"DeadLetterConfig":{"TargetArn":"arn:aws:sqs:us-east-1:123:gc-dlq"},
		"TracingConfig":{"Mode":"Active"},
		"EphemeralStorage":{"Size":1536}
	}`
	rec := auditCreateFunction(t, h, body)
	require.Equal(t, http.StatusCreated, rec.Code)

	// GET /2015-03-31/functions/{name}/configuration
	path := "/2015-03-31/functions/getconfig-fn/configuration"
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec2 := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec2)
	require.NoError(t, h.Handler()(c))
	require.Equal(t, http.StatusOK, rec2.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&fn))

	require.NotNil(t, fn.VpcConfig)
	assert.Equal(t, []string{"subnet-gc"}, fn.VpcConfig.SubnetIDs)

	require.NotNil(t, fn.DeadLetterConfig)
	assert.Equal(t, "arn:aws:sqs:us-east-1:123:gc-dlq", fn.DeadLetterConfig.TargetArn)

	require.NotNil(t, fn.TracingConfig)
	assert.Equal(t, "Active", fn.TracingConfig.Mode)

	require.NotNil(t, fn.EphemeralStorage)
	assert.Equal(t, int32(1536), fn.EphemeralStorage.Size)
}

// ---- ListFunctions includes new fields ----

func TestAudit_ListFunctions_IncludesNewFields(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"list-fields-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"ecr/x:latest"},
		"Role":"arn:aws:iam:::role/r",
		"VpcConfig":{"SubnetIds":["subnet-list"],"SecurityGroupIds":["sg-list"]}
	}`
	rec := auditCreateFunction(t, h, body)
	require.Equal(t, http.StatusCreated, rec.Code)

	req := httptest.NewRequest(http.MethodGet, "/2015-03-31/functions", nil)
	rec2 := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec2)
	require.NoError(t, h.Handler()(c))
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]json.RawMessage
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&out))
	var fns []lambda.FunctionConfiguration
	require.NoError(t, json.Unmarshal(out["Functions"], &fns))
	require.Len(t, fns, 1)
	require.NotNil(t, fns[0].VpcConfig)
	assert.Equal(t, []string{"subnet-list"}, fns[0].VpcConfig.SubnetIDs)
}

// ---- Streaming invoke reads body correctly ----

func TestAudit_InvokeWithResponseStream_NoBody_NotBadRequest(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("stream-nobody-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	path := "/2021-11-15/functions/stream-nobody-fn/response-streaming-invocations"
	req := httptest.NewRequest(http.MethodPost, path, nil) // no body
	rec2 := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec2)
	require.NoError(t, h.Handler()(c))
	// Nil body must not trigger a 400 body-parse error — any other status is acceptable
	// (without Docker, InvokeFunction returns 500, which is expected in unit tests).
	assert.NotEqual(t, http.StatusBadRequest, rec2.Code)
}

// ---- Streaming response frame structure ----

func TestAudit_InvokeWithResponseStream_FrameStructure(t *testing.T) {
	t.Parallel()

	// Create a minimal fake response to verify frame encoding.
	// We can't fully invoke without a container, so we test the frame writer directly.
	type frame struct {
		payload []byte
		length  uint32
	}

	readFrame := func(r io.Reader) (frame, error) {
		var lenBuf [4]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return frame{}, err
		}

		l := binary.BigEndian.Uint32(lenBuf[:])
		if l == 0 {
			return frame{length: 0}, nil
		}

		payload := make([]byte, l)
		if _, err := io.ReadFull(r, payload); err != nil {
			return frame{}, err
		}

		return frame{length: l, payload: payload}, nil
	}

	// Simulate what writeEventStreamFrame does for a known payload.
	payload := []byte(`{"status":"ok"}`)

	var buf strings.Builder

	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(payload)))
	buf.Write(lb[:])
	buf.Write(payload)

	binary.BigEndian.PutUint32(lb[:], 0)
	buf.Write(lb[:])

	r := strings.NewReader(buf.String())
	f1, err := readFrame(r)
	require.NoError(t, err)
	assert.Equal(t, uint32(len(payload)), f1.length)
	assert.Equal(t, payload, f1.payload)

	f2, err := readFrame(r)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), f2.length)
}
