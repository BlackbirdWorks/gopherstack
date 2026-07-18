package lambda_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- FunctionConfig state tests (waiter compatibility) ---

func TestFunctionConfig_StateActiveAfterCreate(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "state-test-fn"
	createFunctionForTest(t, h, fnName)

	// GetFunctionConfiguration should return State: Active
	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/"+fnName+"/configuration", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"State":"Active"`)
	assert.Contains(t, rec.Body.String(), `"LastUpdateStatus":"Successful"`)
}

func TestFunctionConfig_StateAfterUpdateCode(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "update-state-fn"
	createFunctionForTest(t, h, fnName)

	// Update function code
	rec := callInMemoryHandler(
		t, h, http.MethodPut,
		"/2015-03-31/functions/"+fnName+"/code",
		`{"ImageUri":"ecr.example.com/my-image:v2"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"LastUpdateStatus":"Successful"`)
}

func TestFunctionConfig_StateAfterUpdateConfiguration(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "update-config-fn"
	createFunctionForTest(t, h, fnName)

	// Update function configuration
	rec := callInMemoryHandler(
		t, h, http.MethodPut,
		"/2015-03-31/functions/"+fnName+"/configuration",
		`{"Description":"updated description"}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"LastUpdateStatus":"Successful"`)
}

// ============================================================
// FunctionConfiguration: env vars + timeout + memory + layers + vpc + dlq + tracing + filesystem
// ============================================================

func TestUpdateConfig_EnvVars(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "env-fn")

	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/env-fn/configuration",
		`{"Environment":{"Variables":{"KEY":"val","FOO":"bar"}}}`)
	require.Equal(t, http.StatusOK, updRec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&fn))
	require.NotNil(t, fn.Environment)
	assert.Equal(t, "val", fn.Environment.Variables["KEY"])
	assert.Equal(t, "bar", fn.Environment.Variables["FOO"])
}

func TestUpdateConfig_TimeoutAndMemory(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "tm-fn")

	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/tm-fn/configuration",
		`{"Timeout":30,"MemorySize":512}`)
	require.Equal(t, http.StatusOK, updRec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&fn))
	assert.Equal(t, 30, fn.Timeout)
	assert.Equal(t, 512, fn.MemorySize)
}

func TestUpdateConfig_VPC(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "vpc-fn")

	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/vpc-fn/configuration",
		`{"VpcConfig":{"SubnetIds":["subnet-abc"],"SecurityGroupIds":["sg-xyz"]}}`)
	require.Equal(t, http.StatusOK, updRec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&fn))
	require.NotNil(t, fn.VpcConfig)
	assert.Equal(t, []string{"subnet-abc"}, fn.VpcConfig.SubnetIDs)
	assert.Equal(t, []string{"sg-xyz"}, fn.VpcConfig.SecurityGroupIDs)
}

func TestUpdateConfig_DLQ(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "dlq-fn")

	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/dlq-fn/configuration",
		`{"DeadLetterConfig":{"TargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq"}}`)
	require.Equal(t, http.StatusOK, updRec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&fn))
	require.NotNil(t, fn.DeadLetterConfig)
	assert.Equal(t, "arn:aws:sqs:us-east-1:000000000000:dlq", fn.DeadLetterConfig.TargetArn)
}

func TestUpdateConfig_Tracing(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "trace-fn")

	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/trace-fn/configuration",
		`{"TracingConfig":{"Mode":"Active"}}`)
	require.Equal(t, http.StatusOK, updRec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&fn))
	require.NotNil(t, fn.TracingConfig)
	assert.Equal(t, "Active", fn.TracingConfig.Mode)
}

func TestUpdateConfig_FileSystem(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "fs-fn")

	fsCfgBody := `{"FileSystemConfigs":[{"Arn":"arn:aws:elasticfilesystem:us-east-1:000000000000:access-point/fsap-abc",` +
		`"LocalMountPath":"/mnt/data"}]}`
	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/fs-fn/configuration", fsCfgBody)
	require.Equal(t, http.StatusOK, updRec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&fn))
	require.Len(t, fn.FileSystemConfigs, 1)
	assert.Equal(t, "/mnt/data", fn.FileSystemConfigs[0].LocalMountPath)
}

func TestUpdateConfig_Layers(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "layer-cfg-fn")

	// Publish a layer first
	pubRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2018-10-31/layers/my-layer/versions",
		`{"Content":{"ZipFile":"UEsDBAA="},"CompatibleRuntimes":["python3.12"]}`)
	require.Equal(t, http.StatusCreated, pubRec.Code)

	var layerOut lambda.PublishLayerVersionOutput
	require.NoError(t, json.NewDecoder(pubRec.Body).Decode(&layerOut))
	layerARN := layerOut.LayerVersionArn

	// Update function to use layer
	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/layer-cfg-fn/configuration",
		fmt.Sprintf(`{"Layers":[%q]}`, layerARN))
	require.Equal(t, http.StatusOK, updRec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&fn))
	require.Len(t, fn.Layers, 1)
	assert.Contains(t, fn.Layers[0].Arn, "my-layer")
}

func TestUpdateConfig_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/nonexistent/configuration",
		`{"Timeout":10}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestUpdateConfig_InvalidTimeout(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "bad-timeout-fn")

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/bad-timeout-fn/configuration",
		`{"Timeout":901}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetFunctionConfiguration(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "get-cfg-fn")

	callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/get-cfg-fn/configuration",
		`{"Timeout":15,"MemorySize":256}`)

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/get-cfg-fn/configuration", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	assert.Equal(t, 15, fn.Timeout)
	assert.Equal(t, 256, fn.MemorySize)
}

// ============================================================
// FunctionArchitectures: arm64 + x86_64
// ============================================================

func TestArchitectures_CreateArm64(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{"FunctionName":"arm-fn","PackageType":"Image","Code":{"ImageUri":"x"},` +
		`"Role":"arn:aws:iam:::role/r","Architectures":["arm64"]}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	require.Equal(t, []string{"arm64"}, fn.Architectures)
}

func TestArchitectures_CreateX86(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{"FunctionName":"x86-fn","PackageType":"Image","Code":{"ImageUri":"x"},` +
		`"Role":"arn:aws:iam:::role/r","Architectures":["x86_64"]}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	assert.Equal(t, []string{"x86_64"}, fn.Architectures)
}

func TestArchitectures_UpdateCode(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "arch-upd-fn")

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/arch-upd-fn/code",
		`{"ImageUri":"new-image","Architectures":["arm64"]}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	assert.Equal(t, []string{"arm64"}, fn.Architectures)
}

func TestArchitectures_DefaultIsX86(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "default-arch-fn")

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/default-arch-fn", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.GetFunctionOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	// may be empty or x86_64 — just confirm it doesn't return arm64
	for _, arch := range out.Configuration.Architectures {
		assert.NotEqual(t, "arm64", arch)
	}
}

// ============================================================
// SnapStart
// ============================================================

func TestSnapStart_CreateWithPublishedVersions(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{"FunctionName":"snap-fn","PackageType":"Image","Code":{"ImageUri":"x"},` +
		`"Role":"arn:aws:iam:::role/r","Runtime":"java21",` +
		`"SnapStart":{"ApplyOn":"PublishedVersions"}}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	require.NotNil(t, fn.SnapStart)
	assert.Equal(t, "PublishedVersions", fn.SnapStart.ApplyOn)
	assert.Equal(t, "On", fn.SnapStart.OptimizationStatus)
}

func TestSnapStart_CreateWithNone(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{"FunctionName":"snap-none-fn","PackageType":"Image","Code":{"ImageUri":"x"},` +
		`"Role":"arn:aws:iam:::role/r","SnapStart":{"ApplyOn":"None"}}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	require.NotNil(t, fn.SnapStart)
	assert.Equal(t, "None", fn.SnapStart.ApplyOn)
	assert.Equal(t, "Off", fn.SnapStart.OptimizationStatus)
}

func TestSnapStart_UpdateFunctionConfiguration(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "snap-upd-fn")

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/snap-upd-fn/configuration",
		`{"SnapStart":{"ApplyOn":"PublishedVersions"}}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	require.NotNil(t, fn.SnapStart)
	assert.Equal(t, "PublishedVersions", fn.SnapStart.ApplyOn)
	assert.Equal(t, "On", fn.SnapStart.OptimizationStatus)
}

func TestSnapStart_DisableViaUpdate(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{"FunctionName":"snap-dis-fn","PackageType":"Image","Code":{"ImageUri":"x"},` +
		`"Role":"arn:aws:iam:::role/r","SnapStart":{"ApplyOn":"PublishedVersions"}}`
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/snap-dis-fn/configuration",
		`{"SnapStart":{"ApplyOn":"None"}}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	require.NotNil(t, fn.SnapStart)
	assert.Equal(t, "None", fn.SnapStart.ApplyOn)
	assert.Equal(t, "Off", fn.SnapStart.OptimizationStatus)
}

func TestSnapStart_GetFunctionReflectsState(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{"FunctionName":"snap-get-fn","PackageType":"Image","Code":{"ImageUri":"x"},` +
		`"Role":"arn:aws:iam:::role/r","SnapStart":{"ApplyOn":"PublishedVersions"}}`
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)

	rec := callInMemoryHandler(t, h, http.MethodGet, "/2015-03-31/functions/snap-get-fn", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.GetFunctionOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.NotNil(t, out.Configuration.SnapStart)
	assert.Equal(t, "PublishedVersions", out.Configuration.SnapStart.ApplyOn)
}

// ============================================================
// Function State: Active / Pending / Inactive lifecycle
// ============================================================

func TestFunctionState_ActiveOnCreate(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "state-fn")

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/state-fn", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.GetFunctionOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Equal(t, lambda.FunctionStateActive, out.Configuration.State)
}

func TestFunctionState_InactiveConstant(t *testing.T) {
	t.Parallel()

	// Verify the constant exists and has the correct value
	assert.Equal(t, lambda.FunctionStateInactive, lambda.FunctionState("Inactive"))
}

func TestFunctionState_ListedFunctionsHaveState(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "state-list-fn-a")
	createFunctionForTest(t, h, "state-list-fn-b")

	rec := callInMemoryHandler(t, h, http.MethodGet, "/2015-03-31/functions", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.ListFunctionsOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	for _, fn := range out.Functions {
		assert.NotEmpty(t, string(fn.State), "function %s has empty State", fn.FunctionName)
	}
}

func TestFunctionState_VersionHasState(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "state-ver-fn")

	pubRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/state-ver-fn/versions", `{}`)
	require.Equal(t, http.StatusCreated, pubRec.Code)

	var v lambda.FunctionVersion
	require.NoError(t, json.NewDecoder(pubRec.Body).Decode(&v))
	assert.NotEmpty(t, string(v.State))
}

// ============================================================
// CreateFunction: all input field roundtrips
// ============================================================

func TestCreateFunction_AllFields(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions",
		`{"FunctionName":"full-fn","PackageType":"Image","Code":{"ImageUri":"my-image:latest"},`+
			`"Role":"arn:aws:iam::000000000000:role/exec","Description":"full function",`+
			`"MemorySize":1024,"Timeout":60,"Architectures":["arm64"],`+
			`"Environment":{"Variables":{"DB_HOST":"localhost"}},`+
			`"VpcConfig":{"SubnetIds":["subnet-1"],"SecurityGroupIds":["sg-1"]},`+
			`"TracingConfig":{"Mode":"PassThrough"},`+
			`"DeadLetterConfig":{"TargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq"},`+
			`"EphemeralStorage":{"Size":1024},`+
			`"SnapStart":{"ApplyOn":"PublishedVersions"}}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	assert.Equal(t, "full-fn", fn.FunctionName)
	assert.Equal(t, 1024, fn.MemorySize)
	assert.Equal(t, 60, fn.Timeout)
	assert.Equal(t, []string{"arm64"}, fn.Architectures)
	require.NotNil(t, fn.Environment)
	assert.Equal(t, "localhost", fn.Environment.Variables["DB_HOST"])
	require.NotNil(t, fn.VpcConfig)
	require.NotNil(t, fn.TracingConfig)
	require.NotNil(t, fn.DeadLetterConfig)
	require.NotNil(t, fn.EphemeralStorage)
	require.NotNil(t, fn.SnapStart)
	assert.Equal(t, "PublishedVersions", fn.SnapStart.ApplyOn)
}

// ============================================================
// Misc: function URL + version + alias qualifier invokes
// ============================================================

func TestGetFunction_WithQualifierAlias(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "qual-fn")
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/qual-fn/versions", `{}`)
	callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/qual-fn/aliases",
		`{"Name":"prod","FunctionVersion":"1"}`)

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/qual-fn?Qualifier=prod", "")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestGetFunction_WithVersionQualifier(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "qual-ver-fn")
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/qual-ver-fn/versions", `{}`)

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/qual-ver-fn?Qualifier=1", "")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestGetFunction_WithLatestQualifier(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "qual-latest-fn")

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/qual-latest-fn?Qualifier=$LATEST", "")
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestUpdateFunctionCode_Publish verifies that UpdateFunctionCode with
// Publish=true publishes a new numbered version after updating the code, matching
// AWS Lambda behaviour. Previously the Publish field was absent from the input
// struct and the version was never published.
func TestUpdateFunctionCode_Publish(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantVersion string
		publish     bool
	}{
		{
			name:        "publish_true_returns_version_number",
			publish:     true,
			wantVersion: "1",
		},
		{
			name:        "publish_false_returns_latest",
			publish:     false,
			wantVersion: "$LATEST",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, "upd-code-pub-fn")

			publishStr := "false"
			if tt.publish {
				publishStr = "true"
			}

			body := fmt.Sprintf(
				`{"ImageUri":"ecr/new:v2","Publish":%s}`,
				publishStr,
			)
			rec := callInMemoryHandler(t, h, http.MethodPut,
				"/2015-03-31/functions/upd-code-pub-fn/code", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var fn lambda.FunctionConfiguration
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))

			assert.Equal(t, tt.wantVersion, fn.Version,
				"UpdateFunctionCode Publish=%v: Version must be %q", tt.publish, tt.wantVersion)
		})
	}
}

// TestCreateFunction_Tags verifies that tags supplied in CreateFunction
// are returned in the function configuration, matching AWS Lambda behaviour.
// Previously CreateFunctionInput lacked a Tags field and any tags were silently dropped.
func TestCreateFunction_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		wantTags map[string]string
		name     string
	}{
		{
			name:     "tags_set_at_creation",
			tags:     map[string]string{"env": "prod", "team": "platform"},
			wantTags: map[string]string{"env": "prod", "team": "platform"},
		},
		{
			name:     "no_tags_returns_empty_map",
			tags:     nil,
			wantTags: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)

			var tagsJSON string
			if tt.tags != nil {
				tagsJSON = `,"Tags":{"env":"prod","team":"platform"}`
			}

			const tagFnBase = `{"FunctionName":"tag-create-fn","PackageType":"Image",` +
				`"Code":{"ImageUri":"ecr/x:latest"},"Role":"arn:aws:iam:::role/r"`
			body := fmt.Sprintf("%s%s}", tagFnBase, tagsJSON)
			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var fn lambda.FunctionConfiguration
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))

			for k, v := range tt.wantTags {
				assert.Equal(t, v, fn.Tags[k],
					"tag %q must be present with value %q", k, v)
			}
		})
	}
}

// TestCreateFunction_MemorySizeNotDivisibleBy64 verifies that CreateFunction
// returns HTTP 400 InvalidParameterValueException when MemorySize is not divisible by 64,
// matching AWS Lambda behaviour. Previously the backend returned ErrInvalidParameterValue
// but the handler mapped it to HTTP 500 ServiceException.
func TestCreateFunction_MemorySizeNotDivisibleBy64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		memorySize int
		wantStatus int
	}{
		{
			name:       "130_not_divisible_by_64_returns_400",
			memorySize: 130,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "192_divisible_by_64_returns_201",
			memorySize: 192,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "256_divisible_by_64_returns_201",
			memorySize: 256,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "500_not_divisible_by_64_returns_400",
			memorySize: 500,
			wantStatus: http.StatusBadRequest,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)

			const memFnBase = `{"FunctionName":"mem-div-fn-%d","PackageType":"Image",` +
				`"Code":{"ImageUri":"ecr/x:latest"},"Role":"arn:aws:iam:::role/r","MemorySize":%d}`
			body := fmt.Sprintf(memFnBase, i, tt.memorySize)
			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
			assert.Equal(t, tt.wantStatus, rec.Code,
				"MemorySize=%d must return HTTP %d", tt.memorySize, tt.wantStatus)

			if tt.wantStatus == http.StatusBadRequest {
				var errResp map[string]string
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
				assert.Equal(t, "InvalidParameterValueException", errResp["__type"],
					"error type must be InvalidParameterValueException, not ServiceException")
			}
		})
	}
}

// ============================================================
// SnapStart — OptimizationStatus in GetFunctionConfiguration
// ============================================================

func TestSnapStart_GetConfiguration_OptimizationStatus(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{
		"FunctionName":"snap-cfg-fn",
		"PackageType":"Image",
		"Code":{"ImageUri":"x"},
		"Role":"arn:aws:iam:::role/r",
		"SnapStart":{"ApplyOn":"PublishedVersions"}
	}`
	createRec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, createRec.Code)

	cfgRec := callInMemoryHandler(t, h, http.MethodGet, "/2015-03-31/functions/snap-cfg-fn/configuration", "")
	require.Equal(t, http.StatusOK, cfgRec.Code)

	var cfg map[string]any
	require.NoError(t, json.NewDecoder(cfgRec.Body).Decode(&cfg))
	snap, _ := cfg["SnapStart"].(map[string]any)
	require.NotNil(t, snap)
	assert.Equal(t, "PublishedVersions", snap["ApplyOn"])
	assert.Equal(t, "On", snap["OptimizationStatus"])
}

// TestLoggingConfig_DefaultOnCreate verifies GetFunction returns
// LoggingConfig with format=Text and the correct log group.
func TestLoggingConfig_DefaultOnCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		fnName       string
		wantLogGroup string
		wantFormat   string
	}{
		{
			name:         "basic function gets default logging config",
			fnName:       "log-fn",
			wantLogGroup: "/aws/lambda/log-fn",
			wantFormat:   "Text",
		},
		{
			name:         "different function name reflects in log group",
			fnName:       "my-other-fn",
			wantLogGroup: "/aws/lambda/my-other-fn",
			wantFormat:   "Text",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, tc.fnName)

			rec := callInMemoryHandler(t, h, http.MethodGet,
				"/2015-03-31/functions/"+tc.fnName, "")
			require.Equal(t, http.StatusOK, rec.Code)

			body := lambdaParseBody(t, rec)
			cfg, ok := body["Configuration"].(map[string]any)
			require.True(t, ok, "response must have Configuration key")

			logCfg, ok := cfg["LoggingConfig"].(map[string]any)
			require.True(t, ok, "LoggingConfig must be present in Configuration")
			assert.Equal(t, tc.wantFormat, logCfg["LogFormat"], "LogFormat must be Text")
			assert.Equal(t, tc.wantLogGroup, logCfg["LogGroup"], "LogGroup must be /aws/lambda/{name}")
		})
	}
}

// TestListFunctions_FunctionVersionAll verifies that ListFunctions?FunctionVersion=ALL
// returns published versions alongside $LATEST.
func TestListFunctions_FunctionVersionAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		fnName       string
		publishCount int
		wantMinCount int // at minimum: $LATEST + published versions
	}{
		{
			name:         "no published versions — only $LATEST",
			fnName:       "fn-no-ver",
			publishCount: 0,
			wantMinCount: 1,
		},
		{
			name:         "two published versions — $LATEST + 2",
			fnName:       "fn-two-ver",
			publishCount: 2,
			wantMinCount: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)
			createFunctionForTest(t, h, tc.fnName)

			for range tc.publishCount {
				_, pubErr := bk.PublishVersion(tc.fnName, "")
				require.NoError(t, pubErr)
			}

			rec := callInMemoryHandler(t, h, http.MethodGet,
				"/2015-03-31/functions?FunctionVersion=ALL", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			fns, _ := out["Functions"].([]any)
			assert.GreaterOrEqual(t, len(fns), tc.wantMinCount,
				"FunctionVersion=ALL must include $LATEST and all published versions")

			// Verify versions present in response.
			versions := make(map[string]bool)
			for _, f := range fns {
				fm := f.(map[string]any)
				v, _ := fm["Version"].(string)
				versions[v] = true
			}
			assert.True(t, versions["$LATEST"], "$LATEST must appear in FunctionVersion=ALL")

			for i := 1; i <= tc.publishCount; i++ {
				assert.True(t, versions[strconv.Itoa(i)],
					"version %d must appear in FunctionVersion=ALL", i)
			}
		})
	}
}

// TestUpdateFunctionCodeArchitectures verifies that Architectures is stored and returned.
func TestUpdateFunctionCodeArchitectures(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "arch-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:v1",
		State:        lambda.FunctionStateActive,
	}))

	// UpdateFunctionCode with arm64.
	body := `{"ImageUri":"test:v2","Architectures":["arm64"]}`
	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/arch-fn/code", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fn))
	require.Equal(t, []string{"arm64"}, fn.Architectures)

	// GetFunctionConfiguration returns the stored architectures.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/arch-fn/configuration", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var fn2 lambda.FunctionConfiguration
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fn2))
	assert.Equal(t, []string{"arm64"}, fn2.Architectures)
}

// TestMasterArn verifies that MasterArn is returned when set on a function.
func TestMasterArn(t *testing.T) {
	t.Parallel()

	_, bk := newInMemoryHandler(t)

	masterARN := "arn:aws:lambda:us-east-1:000000000000:function:edge-origin"

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "edge-replica-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
		State:        lambda.FunctionStateActive,
		MasterArn:    masterARN,
	}))

	fn, err := bk.GetFunction("edge-replica-fn")
	require.NoError(t, err)
	assert.Equal(t, masterARN, fn.MasterArn)
}
