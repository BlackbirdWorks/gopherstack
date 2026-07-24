package lambda_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lambda"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// VPC config v2 — Ipv6AllowedForDualStack
// ============================================================

func TestVpcConfig_Ipv6AllowedForDualStack_Create(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"vpc6-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"x"},
		"Role":"arn:aws:iam:::role/r",
		"VpcConfig":{
			"SubnetIds":["subnet-1"],
			"SecurityGroupIds":["sg-1"],
			"Ipv6AllowedForDualStack":true
		}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	vpc, _ := out["VpcConfig"].(map[string]any)
	require.NotNil(t, vpc)
	assert.Equal(t, true, vpc["Ipv6AllowedForDualStack"])
}

func TestVpcConfig_Ipv6AllowedForDualStack_Update(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "vpc6-update-fn")

	rec := callInMemoryHandler(
		t, h, http.MethodPut,
		"/2015-03-31/functions/vpc6-update-fn/configuration",
		`{"VpcConfig":{"SubnetIds":["subnet-2"],"Ipv6AllowedForDualStack":true}}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	vpc, _ := out["VpcConfig"].(map[string]any)
	require.NotNil(t, vpc)
	assert.Equal(t, true, vpc["Ipv6AllowedForDualStack"])
}

func TestVpcConfig_Ipv6AllowedForDualStack_NotSet_Omitted(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"vpc6-omit-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"x"},
		"Role":"arn:aws:iam:::role/r",
		"VpcConfig":{
			"SubnetIds":["subnet-1"]
		}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	vpc, _ := out["VpcConfig"].(map[string]any)
	require.NotNil(t, vpc)
	// omitempty: nil pointer → field absent in JSON
	_, hasField := vpc["Ipv6AllowedForDualStack"]
	assert.False(t, hasField)
}

func TestVpcConfig_Ipv6AllowedForDualStack_GetConfiguration(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"vpc6-get-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"x"},
		"Role":"arn:aws:iam:::role/r",
		"VpcConfig":{
			"SubnetIds":["subnet-1"],
			"SecurityGroupIds":["sg-1"],
			"Ipv6AllowedForDualStack":true
		}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	rec2 := callInMemoryHandler(t, h, http.MethodGet, "/2015-03-31/functions/vpc6-get-fn/configuration", "")
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&out))
	vpc, _ := out["VpcConfig"].(map[string]any)
	require.NotNil(t, vpc)
	assert.Equal(t, true, vpc["Ipv6AllowedForDualStack"])
}

// ---- Gap 1: VpcConfig ----

func TestVpcConfig_CreateAndGet(t *testing.T) {
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

func TestVpcConfig_UpdateAndGet(t *testing.T) {
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

func TestTracingConfig_CreateDefault(t *testing.T) {
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

func TestTracingConfig_CreateActive(t *testing.T) {
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

func TestTracingConfig_Update(t *testing.T) {
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

func TestFileSystemConfigs_CreateAndGet(t *testing.T) {
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

func TestFileSystemConfigs_Update(t *testing.T) {
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

func TestDeadLetterConfig_CreateAndGet(t *testing.T) {
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

func TestDeadLetterConfig_Update(t *testing.T) {
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

// ============================================================
// DurableConfig — accept-and-echo (PARITY.md deferred item: the SDK
// v1.94.1->v1.97.0 bump added a customer-managed-KMS-key field to the
// durable-execution config; this locks in that CreateFunction,
// UpdateFunctionConfiguration, and PublishVersion all round-trip the whole
// DurableConfig shape (ExecutionTimeout, KMSKeyArn, RetentionPeriodInDays).
// ============================================================

func TestDurableConfig_CreateAndGet(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"durable-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"ecr/x:latest"},
		"Role":"arn:aws:iam:::role/r",
		"DurableConfig":{"ExecutionTimeout":3600,"KMSKeyArn":"arn:aws:kms:us-east-1:123:key/abc","RetentionPeriodInDays":30}
	}`
	rec := auditCreateFunction(t, h, body)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	require.NotNil(t, fn.DurableConfig)
	require.NotNil(t, fn.DurableConfig.ExecutionTimeout)
	assert.Equal(t, int32(3600), *fn.DurableConfig.ExecutionTimeout)
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/abc", fn.DurableConfig.KMSKeyArn)
	require.NotNil(t, fn.DurableConfig.RetentionPeriodInDays)
	assert.Equal(t, int32(30), *fn.DurableConfig.RetentionPeriodInDays)
}

func TestDurableConfig_Update(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("durable-update-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	body := `{"DurableConfig":{"ExecutionTimeout":7200,"RetentionPeriodInDays":90}}`
	rec2 := auditUpdateConfig(t, h, "durable-update-fn", body)
	require.Equal(t, http.StatusOK, rec2.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&fn))
	require.NotNil(t, fn.DurableConfig)
	require.NotNil(t, fn.DurableConfig.ExecutionTimeout)
	assert.Equal(t, int32(7200), *fn.DurableConfig.ExecutionTimeout)
}

func TestDurableConfig_PublishedVersionCarriesConfig(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"durable-pub-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"ecr/x:latest"},
		"Role":"arn:aws:iam:::role/r",
		"DurableConfig":{"ExecutionTimeout":1800}
	}`
	rec := auditCreateFunction(t, h, body)
	require.Equal(t, http.StatusCreated, rec.Code)

	pubRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/durable-pub-fn/versions", "{}")
	require.Equal(t, http.StatusCreated, pubRec.Code)

	var ver lambda.FunctionVersion
	require.NoError(t, json.NewDecoder(pubRec.Body).Decode(&ver))
	require.NotNil(t, ver.DurableConfig)
	require.NotNil(t, ver.DurableConfig.ExecutionTimeout)
	assert.Equal(t, int32(1800), *ver.DurableConfig.ExecutionTimeout)
}

// ============================================================
// RevisionId optimistic concurrency on UpdateFunctionConfiguration /
// UpdateFunctionCode (PARITY.md deferred item, extended from AddPermission's
// RevisionId to the function-config update path): a stale RevisionId is
// rejected with PreconditionFailedException (412) without applying the
// update; the current RevisionId succeeds.
// ============================================================

func TestUpdateFunctionConfiguration_RevisionID_Precondition(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("cfg-precond-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	fn, err := bk.GetFunction("cfg-precond-fn")
	require.NoError(t, err)
	currentRevision := fn.RevisionID

	staleRec := auditUpdateConfig(t, h, "cfg-precond-fn",
		`{"Description":"nope","RevisionId":"not-the-real-revision"}`)
	assert.Equal(t, http.StatusPreconditionFailed, staleRec.Code)

	fnAfter, err := bk.GetFunction("cfg-precond-fn")
	require.NoError(t, err)
	assert.Empty(t, fnAfter.Description, "rejected update must not change the function")

	okRec := auditUpdateConfig(t, h, "cfg-precond-fn",
		fmt.Sprintf(`{"Description":"yep","RevisionId":%q}`, currentRevision))
	assert.Equal(t, http.StatusOK, okRec.Code)
}

func TestUpdateFunctionCode_RevisionID_Precondition(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("code-precond-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	fn, err := bk.GetFunction("code-precond-fn")
	require.NoError(t, err)
	currentRevision := fn.RevisionID

	staleRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/code-precond-fn/code",
		`{"ImageUri":"ecr/new:latest","RevisionId":"not-the-real-revision"}`)
	assert.Equal(t, http.StatusPreconditionFailed, staleRec.Code)

	fnAfter, err := bk.GetFunction("code-precond-fn")
	require.NoError(t, err)
	assert.Equal(t, "ecr/x:latest", fnAfter.ImageURI, "rejected update must not change the function's code")

	okRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/code-precond-fn/code",
		fmt.Sprintf(`{"ImageUri":"ecr/new:latest","RevisionId":%q}`, currentRevision))
	assert.Equal(t, http.StatusOK, okRec.Code)
}

// ---- Gap 5: EphemeralStorage validation ----

func TestEphemeralStorage_DefaultOn_Create(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseImageFn("eph-default-fn"))
	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	require.NotNil(t, fn.EphemeralStorage)
	assert.Equal(t, int32(512), fn.EphemeralStorage.Size)
}

// TestEphemeralStorage_Create covers CreateFunction with an explicit
// EphemeralStorage.Size: a valid size is accepted and echoed back, and
// out-of-range sizes are rejected with 400. Table-driven: each case shares
// the same request/response shape and differs only in the size and the
// expected outcome.
func TestEphemeralStorage_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fnName   string
		size     int
		wantCode int
	}{
		{name: "valid size accepted and echoed back", fnName: "eph-valid-fn", size: 1024, wantCode: http.StatusCreated},
		{name: "too small rejected", fnName: "eph-small-fn", size: 100, wantCode: http.StatusBadRequest},
		{name: "too large rejected", fnName: "eph-large-fn", size: 99999, wantCode: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)

			body := fmt.Sprintf(`{
				"FunctionName":"%s",
				"PackageType":"Image",
				"Code":{"ImageUri":"ecr/x:latest"},
				"Role":"arn:aws:iam:::role/r",
				"EphemeralStorage":{"Size":%d}
			}`, tc.fnName, tc.size)
			rec := auditCreateFunction(t, h, body)
			require.Equal(t, tc.wantCode, rec.Code, rec.Body.String())

			if tc.wantCode == http.StatusCreated {
				var fn lambda.FunctionConfiguration
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
				require.NotNil(t, fn.EphemeralStorage)
				assert.Equal(t, int32(tc.size), fn.EphemeralStorage.Size)
			}
		})
	}
}

// TestEphemeralStorage_Update covers UpdateFunctionConfiguration with
// an EphemeralStorage.Size change: a valid size is accepted and echoed back,
// and out-of-range sizes are rejected with 400. Table-driven: each case
// shares the same create-then-update shape and differs only in the update
// size and the expected outcome.
func TestEphemeralStorage_Update(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fnName   string
		size     int
		wantCode int
	}{
		{name: "too small rejected", fnName: "eph-update-small-fn", size: 10, wantCode: http.StatusBadRequest},
		{name: "too large rejected", fnName: "eph-update-large-fn", size: 99999, wantCode: http.StatusBadRequest},
		{
			name: "valid size accepted and echoed back", fnName: "eph-update-valid-fn",
			size: 2048, wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			rec := auditCreateFunction(t, h, baseImageFn(tc.fnName))
			require.Equal(t, http.StatusCreated, rec.Code)

			rec2 := auditUpdateConfig(t, h, tc.fnName, fmt.Sprintf(`{"EphemeralStorage":{"Size":%d}}`, tc.size))
			require.Equal(t, tc.wantCode, rec2.Code)

			if tc.wantCode == http.StatusOK {
				var fn lambda.FunctionConfiguration
				require.NoError(t, json.NewDecoder(rec2.Body).Decode(&fn))
				require.NotNil(t, fn.EphemeralStorage)
				assert.Equal(t, int32(tc.size), fn.EphemeralStorage.Size)
			}
		})
	}
}

// ---- Gap 6: ImageConfig in responses ----

func TestImageConfig_Persisted_Create(t *testing.T) {
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

func TestImageConfig_InGetFunction(t *testing.T) {
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

func TestImageConfig_NotSetForZip(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	rec := auditCreateFunction(t, h, baseZipFn("zip-no-imgcfg"))
	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	assert.Nil(t, fn.ImageConfig)
}

// ---- Gap 10: Qualifier validation ----

func TestQualifierValidation_InvalidChars(t *testing.T) {
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

func TestQualifierValidation_Latest_OK(t *testing.T) {
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

func TestQualifierValidation_VersionNumber_OK(t *testing.T) {
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

func TestQualifierValidation_AliasName_OK(t *testing.T) {
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

// ---- All new config fields persist through GetFunctionConfiguration ----

func TestAllNewFields_PersistInGetConfiguration(t *testing.T) {
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

func TestPublishVersion_CarriesNewFields(t *testing.T) {
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

func TestEphemeralStorage_BoundaryValues(t *testing.T) {
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

func TestVpcConfig_JSONShape(t *testing.T) {
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

func TestTracingConfig_JSONShape(t *testing.T) {
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

func TestMultipleFields_RoundTrip(t *testing.T) {
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

func TestGetFunctionConfiguration_IncludesNewFields(t *testing.T) {
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

func TestListFunctions_IncludesNewFields(t *testing.T) {
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
