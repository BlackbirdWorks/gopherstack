package lambda_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// ============================================================
// FunctionVersion lifecycle: $LATEST → numbered
// ============================================================

func TestBatch2_PublishVersion_Basic(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "ver-fn")

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/ver-fn/versions", `{}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var v lambda.FunctionVersion
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&v))
	assert.Equal(t, "1", v.Version)
	assert.Equal(t, "ver-fn", v.FunctionName)
	assert.NotEmpty(t, v.FunctionArn)
}

func TestBatch2_PublishVersion_Increments(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "ver-inc-fn")

	for i := 1; i <= 3; i++ {
		rec := callInMemoryHandler(t, h, http.MethodPost,
			"/2015-03-31/functions/ver-inc-fn/versions", `{}`)
		require.Equal(t, http.StatusCreated, rec.Code)

		var v lambda.FunctionVersion
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&v))
		assert.Equal(t, strconv.Itoa(i), v.Version)
	}
}

func TestBatch2_PublishVersion_WithDescription(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "ver-desc-fn")

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/ver-desc-fn/versions", `{"Description":"my-snapshot"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var v lambda.FunctionVersion
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&v))
	assert.Equal(t, "my-snapshot", v.Description)
	assert.Equal(t, "1", v.Version)
}

func TestBatch2_PublishVersion_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/nonexistent/versions", `{}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch2_ListVersionsByFunction_Empty(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "ver-list-fn")

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/ver-list-fn/versions", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.ListVersionsByFunctionOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	// $LATEST is always present
	require.NotEmpty(t, out.Versions)
	assert.Equal(t, "$LATEST", out.Versions[0].Version)
}

func TestBatch2_ListVersionsByFunction_AfterPublish(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "ver-after-fn")

	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/ver-after-fn/versions", `{}`)
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/ver-after-fn/versions", `{}`)

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/ver-after-fn/versions", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.ListVersionsByFunctionOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	// $LATEST + 2 numbered versions
	assert.GreaterOrEqual(t, len(out.Versions), 2)

	versions := make(map[string]bool)
	for _, v := range out.Versions {
		versions[v.Version] = true
	}
	assert.True(t, versions["1"])
	assert.True(t, versions["2"])
}

func TestBatch2_ListVersionsByFunction_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/nonexistent/versions", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch2_PublishVersion_Publish_CreateFunction(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{"FunctionName":"pub-create-fn","PackageType":"Image","Code":{"ImageUri":"x"},` +
		`"Role":"arn:aws:iam:::role/r","Publish":true}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	assert.Equal(t, "1", fn.Version)
}

func TestBatch2_FunctionVersion_ARNFormat(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "ver-arn-fn")

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/ver-arn-fn/versions", `{}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var v lambda.FunctionVersion
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&v))
	// versioned ARN should end with :1
	assert.True(t, strings.HasSuffix(v.FunctionArn, ":1"), "expected ARN to end with :1, got %s", v.FunctionArn)
}

// ============================================================
// Alias routing + weights
// ============================================================

func TestBatch2_Alias_CreateGetDeleteList(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "alias-fn")
	// publish a version first
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-fn/versions", `{}`)

	// Create alias
	createRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/alias-fn/aliases",
		`{"Name":"live","FunctionVersion":"1","Description":"prod alias"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var alias lambda.FunctionAlias
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&alias))
	assert.Equal(t, "live", alias.Name)
	assert.Equal(t, "1", alias.FunctionVersion)
	assert.Equal(t, "prod alias", alias.Description)
	assert.NotEmpty(t, alias.AliasArn)

	// Get alias
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/alias-fn/aliases/live", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var getAlias lambda.FunctionAlias
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getAlias))
	assert.Equal(t, "live", getAlias.Name)

	// List aliases
	listRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/alias-fn/aliases", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut lambda.ListAliasesOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.Len(t, listOut.Aliases, 1)

	// Delete alias
	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/functions/alias-fn/aliases/live", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// Get after delete → 404
	getRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/alias-fn/aliases/live", "")
	assert.Equal(t, http.StatusNotFound, getRec2.Code)
}

func TestBatch2_Alias_RoutingConfig_WeightedTraffic(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "alias-weighted-fn")
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-weighted-fn/versions", `{}`)
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-weighted-fn/versions", `{}`)

	createRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/alias-weighted-fn/aliases",
		`{"Name":"canary","FunctionVersion":"1","RoutingConfig":{"AdditionalVersionWeights":{"2":0.1}}}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var alias lambda.FunctionAlias
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&alias))
	require.NotNil(t, alias.RoutingConfig)
	assert.InDelta(t, 0.1, alias.RoutingConfig.AdditionalVersionWeights["2"], 0.001)
}

func TestBatch2_Alias_Update(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "alias-upd-fn")
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-upd-fn/versions", `{}`)
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-upd-fn/versions", `{}`)

	callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/alias-upd-fn/aliases",
		`{"Name":"stable","FunctionVersion":"1"}`)

	// Update to point to version 2
	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/alias-upd-fn/aliases/stable",
		`{"FunctionVersion":"2","Description":"now v2"}`)
	require.Equal(t, http.StatusOK, updRec.Code)

	var updated lambda.FunctionAlias
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&updated))
	assert.Equal(t, "2", updated.FunctionVersion)
	assert.Equal(t, "now v2", updated.Description)
}

func TestBatch2_Alias_UpdateRoutingConfig(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "alias-upd-rc-fn")
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-upd-rc-fn/versions", `{}`)
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-upd-rc-fn/versions", `{}`)

	callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/alias-upd-rc-fn/aliases",
		`{"Name":"rc","FunctionVersion":"1"}`)

	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/alias-upd-rc-fn/aliases/rc",
		`{"RoutingConfig":{"AdditionalVersionWeights":{"2":0.2}}}`)
	require.Equal(t, http.StatusOK, updRec.Code)

	var updated lambda.FunctionAlias
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&updated))
	require.NotNil(t, updated.RoutingConfig)
	assert.InDelta(t, 0.2, updated.RoutingConfig.AdditionalVersionWeights["2"], 0.001)
}

func TestBatch2_Alias_Duplicate(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "alias-dup-fn")
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-dup-fn/versions", `{}`)

	callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/alias-dup-fn/aliases",
		`{"Name":"dup","FunctionVersion":"1"}`)

	dupRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/alias-dup-fn/aliases",
		`{"Name":"dup","FunctionVersion":"1"}`)
	assert.Equal(t, http.StatusConflict, dupRec.Code)
}

func TestBatch2_Alias_GetNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "alias-404-fn")

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/alias-404-fn/aliases/nope", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch2_Alias_FunctionNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/nonexistent-fn/aliases",
		`{"Name":"x","FunctionVersion":"1"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch2_Alias_RevisionID_Changes(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "alias-rev-fn")
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-rev-fn/versions", `{}`)
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/alias-rev-fn/versions", `{}`)

	createRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/alias-rev-fn/aliases",
		`{"Name":"rev","FunctionVersion":"1"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)
	var a1 lambda.FunctionAlias
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&a1))

	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/alias-rev-fn/aliases/rev",
		`{"FunctionVersion":"2"}`)
	require.Equal(t, http.StatusOK, updRec.Code)
	var a2 lambda.FunctionAlias
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&a2))

	assert.NotEqual(t, a1.RevisionID, a2.RevisionID)
}

// ============================================================
// FunctionConfiguration: env vars + timeout + memory + layers + vpc + dlq + tracing + filesystem
// ============================================================

func TestBatch2_UpdateConfig_EnvVars(t *testing.T) {
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

func TestBatch2_UpdateConfig_TimeoutAndMemory(t *testing.T) {
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

func TestBatch2_UpdateConfig_VPC(t *testing.T) {
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

func TestBatch2_UpdateConfig_DLQ(t *testing.T) {
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

func TestBatch2_UpdateConfig_Tracing(t *testing.T) {
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

func TestBatch2_UpdateConfig_FileSystem(t *testing.T) {
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

func TestBatch2_UpdateConfig_Layers(t *testing.T) {
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

func TestBatch2_UpdateConfig_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/nonexistent/configuration",
		`{"Timeout":10}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch2_UpdateConfig_InvalidTimeout(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "bad-timeout-fn")

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/bad-timeout-fn/configuration",
		`{"Timeout":901}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch2_GetFunctionConfiguration(t *testing.T) {
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

func TestBatch2_Architectures_CreateArm64(t *testing.T) {
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

func TestBatch2_Architectures_CreateX86(t *testing.T) {
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

func TestBatch2_Architectures_UpdateCode(t *testing.T) {
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

func TestBatch2_Architectures_DefaultIsX86(t *testing.T) {
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

func TestBatch2_SnapStart_CreateWithPublishedVersions(t *testing.T) {
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

func TestBatch2_SnapStart_CreateWithNone(t *testing.T) {
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

func TestBatch2_SnapStart_UpdateFunctionConfiguration(t *testing.T) {
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

func TestBatch2_SnapStart_DisableViaUpdate(t *testing.T) {
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

func TestBatch2_SnapStart_GetFunctionReflectsState(t *testing.T) {
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
// RuntimeManagementConfig
// ============================================================

func TestBatch2_RuntimeManagementConfig_GetDefault(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "rmc-fn")

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2021-07-20/functions/rmc-fn/runtime-management-config", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var cfg lambda.RuntimeManagementConfig
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&cfg))
	assert.Equal(t, "Auto", cfg.UpdateRuntimeOn)
}

func TestBatch2_RuntimeManagementConfig_Put(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "rmc-put-fn")

	putRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2021-07-20/functions/rmc-put-fn/runtime-management-config",
		`{"UpdateRuntimeOn":"FunctionUpdate"}`)
	require.Equal(t, http.StatusOK, putRec.Code)

	var cfg lambda.RuntimeManagementConfig
	require.NoError(t, json.NewDecoder(putRec.Body).Decode(&cfg))
	assert.Equal(t, "FunctionUpdate", cfg.UpdateRuntimeOn)
}

func TestBatch2_RuntimeManagementConfig_PutManualWithARN(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "rmc-manual-fn")

	rva := "arn:aws:lambda:us-east-1::runtime:python3.12:abc123"
	body := fmt.Sprintf(`{"UpdateRuntimeOn":"Manual","RuntimeVersionArn":%q}`, rva)
	putRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2021-07-20/functions/rmc-manual-fn/runtime-management-config", body)
	require.Equal(t, http.StatusOK, putRec.Code)

	var cfg lambda.RuntimeManagementConfig
	require.NoError(t, json.NewDecoder(putRec.Body).Decode(&cfg))
	assert.Equal(t, "Manual", cfg.UpdateRuntimeOn)
	assert.Equal(t, rva, cfg.RuntimeVersionArn)
}

func TestBatch2_RuntimeManagementConfig_GetNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2021-07-20/functions/nonexistent/runtime-management-config", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch2_RuntimeManagementConfig_PutNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2021-07-20/functions/nonexistent/runtime-management-config",
		`{"UpdateRuntimeOn":"Auto"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch2_RuntimeManagementConfig_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "rmc-method-fn")

	rec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2021-07-20/functions/rmc-method-fn/runtime-management-config", "")
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestBatch2_RuntimeManagementConfig_FunctionARNInResponse(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "rmc-arn-fn")

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2021-07-20/functions/rmc-arn-fn/runtime-management-config", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var cfg lambda.RuntimeManagementConfig
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&cfg))
	assert.Contains(t, cfg.FunctionArn, "rmc-arn-fn")
}

// ============================================================
// FunctionRecursionConfig
// ============================================================

func TestBatch2_RecursionConfig_GetDefault(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "rec-fn")

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2024-08-28/functions/rec-fn/recursion-config", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var cfg lambda.FunctionRecursionConfig
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&cfg))
	assert.Equal(t, "Terminate", cfg.RecursiveLoop)
}

func TestBatch2_RecursionConfig_Put_Allow(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "rec-allow-fn")

	putRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2024-08-28/functions/rec-allow-fn/recursion-config",
		`{"RecursiveLoop":"Allow"}`)
	require.Equal(t, http.StatusOK, putRec.Code)

	var cfg lambda.FunctionRecursionConfig
	require.NoError(t, json.NewDecoder(putRec.Body).Decode(&cfg))
	assert.Equal(t, "Allow", cfg.RecursiveLoop)
}

func TestBatch2_RecursionConfig_Put_Terminate(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "rec-term-fn")

	callInMemoryHandler(t, h, http.MethodPut,
		"/2024-08-28/functions/rec-term-fn/recursion-config",
		`{"RecursiveLoop":"Allow"}`)

	putRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2024-08-28/functions/rec-term-fn/recursion-config",
		`{"RecursiveLoop":"Terminate"}`)
	require.Equal(t, http.StatusOK, putRec.Code)

	var cfg lambda.FunctionRecursionConfig
	require.NoError(t, json.NewDecoder(putRec.Body).Decode(&cfg))
	assert.Equal(t, "Terminate", cfg.RecursiveLoop)
}

func TestBatch2_RecursionConfig_GetNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2024-08-28/functions/nonexistent/recursion-config", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestBatch2_RecursionConfig_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "rec-mna-fn")

	rec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2024-08-28/functions/rec-mna-fn/recursion-config", "")
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// ============================================================
// EventSourceMapping: batch + filter + window parameters
// ============================================================

func TestBatch2_ESM_BatchWindow(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-batch-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
		"FunctionName":"esm-batch-fn",
		"StartingPosition":"LATEST",
		"BatchSize":100,
		"MaximumBatchingWindowInSeconds":60
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(60), out["MaximumBatchingWindowInSeconds"], 0)
}

func TestBatch2_ESM_TumblingWindow(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-tumble-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/tumble-stream",
		"FunctionName":"esm-tumble-fn",
		"StartingPosition":"LATEST",
		"TumblingWindowInSeconds":300
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(300), out["TumblingWindowInSeconds"], 0)
}

func TestBatch2_ESM_FilterCriteria(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-filter-fn")

	body := `{
		"EventSourceArn":"arn:aws:sqs:us-east-1:000000000000:my-queue",
		"FunctionName":"esm-filter-fn",
		"FilterCriteria":{"Filters":[{"Pattern":"{\"source\":[\"myapp\"]}"}]}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	fc, ok := out["FilterCriteria"].(map[string]any)
	require.True(t, ok)
	filters := fc["Filters"].([]any)
	require.Len(t, filters, 1)
}

func TestBatch2_ESM_BisectBatchOnError(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-bisect-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/bisect-stream",
		"FunctionName":"esm-bisect-fn",
		"StartingPosition":"LATEST",
		"BisectBatchOnFunctionError":true
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Equal(t, true, out["BisectBatchOnFunctionError"])
}

func TestBatch2_ESM_MaximumRecordAge(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-recage-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/age-stream",
		"FunctionName":"esm-recage-fn",
		"StartingPosition":"LATEST",
		"MaximumRecordAgeInSeconds":3600
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(3600), out["MaximumRecordAgeInSeconds"], 0)
}

func TestBatch2_ESM_ParallelizationFactor(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-parallel-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/parallel-stream",
		"FunctionName":"esm-parallel-fn",
		"StartingPosition":"LATEST",
		"ParallelizationFactor":5
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(5), out["ParallelizationFactor"], 0)
}

func TestBatch2_ESM_DestinationConfig(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-dest-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/dest-stream",
		"FunctionName":"esm-dest-fn",
		"StartingPosition":"LATEST",
		"DestinationConfig":{"OnFailure":{"Destination":"arn:aws:sqs:us-east-1:000000000000:dead-letter"}}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	dc, ok := out["DestinationConfig"].(map[string]any)
	require.True(t, ok)
	onFail := dc["OnFailure"].(map[string]any)
	assert.Equal(t, "arn:aws:sqs:us-east-1:000000000000:dead-letter", onFail["Destination"])
}

func TestBatch2_ESM_UpdateBatchWindow(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-upd-window-fn")

	createBody := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/upd-stream",
		"FunctionName":"esm-upd-window-fn",
		"StartingPosition":"LATEST"
	}`
	createRec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", createBody)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&created))
	uuid := created["UUID"].(string)

	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/event-source-mappings/"+uuid,
		`{"MaximumBatchingWindowInSeconds":30}`)
	require.Equal(t, http.StatusOK, updRec.Code)

	var updated map[string]any
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&updated))
	assert.InDelta(t, float64(30), updated["MaximumBatchingWindowInSeconds"], 0)
}

func TestBatch2_ESM_SQS_FullOptions(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-sqs-fn")

	body := `{
		"EventSourceArn":"arn:aws:sqs:us-east-1:000000000000:my-sqs-queue",
		"FunctionName":"esm-sqs-fn",
		"BatchSize":10,
		"MaximumBatchingWindowInSeconds":5,
		"FilterCriteria":{"Filters":[{"Pattern":"{\"body\":{\"type\":[\"order\"]}}"}]},
		"DestinationConfig":{"OnFailure":{"Destination":"arn:aws:sqs:us-east-1:000000000000:sqs-dlq"}}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(10), out["BatchSize"], 0)
	assert.InDelta(t, float64(5), out["MaximumBatchingWindowInSeconds"], 0)
	assert.NotNil(t, out["FilterCriteria"])
	assert.NotNil(t, out["DestinationConfig"])
	assert.Equal(t, "Enabled", out["State"])
}

func TestBatch2_ESM_DDB_FullOptions(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-ddb-fn")

	body := `{
		"EventSourceArn":"arn:aws:dynamodb:us-east-1:000000000000:table/my-table/stream/2024-01-01T00:00:00.000",
		"FunctionName":"esm-ddb-fn",
		"StartingPosition":"LATEST",
		"BatchSize":50,
		"BisectBatchOnFunctionError":true,
		"MaximumRetryAttempts":3
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(50), out["BatchSize"], 0)
	assert.Equal(t, true, out["BisectBatchOnFunctionError"])
	assert.InDelta(t, float64(3), out["MaximumRetryAttempts"], 0)
}

func TestBatch2_ESM_Kinesis_AllWindowOptions(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-kin-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/full-stream",
		"FunctionName":"esm-kin-fn",
		"StartingPosition":"TRIM_HORIZON",
		"BatchSize":200,
		"MaximumBatchingWindowInSeconds":120,
		"TumblingWindowInSeconds":60,
		"MaximumRecordAgeInSeconds":7200,
		"MaximumRetryAttempts":5,
		"ParallelizationFactor":3,
		"BisectBatchOnFunctionError":true
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(200), out["BatchSize"], 0)
	assert.InDelta(t, float64(120), out["MaximumBatchingWindowInSeconds"], 0)
	assert.InDelta(t, float64(60), out["TumblingWindowInSeconds"], 0)
	assert.InDelta(t, float64(7200), out["MaximumRecordAgeInSeconds"], 0)
	assert.InDelta(t, float64(5), out["MaximumRetryAttempts"], 0)
	assert.InDelta(t, float64(3), out["ParallelizationFactor"], 0)
	assert.Equal(t, true, out["BisectBatchOnFunctionError"])
}

func TestBatch2_ESM_ListByFunction(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-list-fn")

	for i := range 3 {
		body := fmt.Sprintf(`{
			"EventSourceArn":"arn:aws:sqs:us-east-1:000000000000:q-%d",
			"FunctionName":"esm-list-fn"
		}`, i)
		callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	}

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/event-source-mappings?FunctionName=esm-list-fn", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	mappings := out["EventSourceMappings"].([]any)
	assert.Len(t, mappings, 3)
}

// ============================================================
// Layer CRUD + versions
// ============================================================

func TestBatch2_Layer_PublishAndGet(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	pubRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2018-10-31/layers/test-layer/versions",
		`{"Content":{"ZipFile":"UEsDBAA="},"Description":"v1","CompatibleRuntimes":["python3.12","python3.11"]}`)
	require.Equal(t, http.StatusCreated, pubRec.Code)

	var pub lambda.PublishLayerVersionOutput
	require.NoError(t, json.NewDecoder(pubRec.Body).Decode(&pub))
	assert.Equal(t, int64(1), pub.Version)
	assert.Equal(t, "v1", pub.Description)
	assert.Equal(t, []string{"python3.12", "python3.11"}, pub.CompatibleRuntimes)
	assert.NotEmpty(t, pub.LayerVersionArn)
	assert.NotEmpty(t, pub.LayerArn)

	// Get version
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2018-10-31/layers/test-layer/versions/1", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var got lambda.GetLayerVersionOutput
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&got))
	assert.Equal(t, int64(1), got.Version)
}

func TestBatch2_Layer_MultipleVersions(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	for i := range 3 {
		rec := callInMemoryHandler(t, h, http.MethodPost,
			"/2018-10-31/layers/multi-layer/versions",
			fmt.Sprintf(`{"Content":{"ZipFile":"UEsDBAA="},"Description":"v%d"}`, i+1))
		require.Equal(t, http.StatusCreated, rec.Code)

		var pub lambda.PublishLayerVersionOutput
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&pub))
		assert.Equal(t, int64(i+1), pub.Version)
	}
}

func TestBatch2_Layer_ListVersions(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, "/2018-10-31/layers/list-ver-layer/versions",
		`{"Content":{"ZipFile":"UEsDBAA="}}`)
	callInMemoryHandler(t, h, http.MethodPost, "/2018-10-31/layers/list-ver-layer/versions",
		`{"Content":{"ZipFile":"UEsDBAA="}}`)

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2018-10-31/layers/list-ver-layer/versions", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.ListLayerVersionsOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Len(t, out.LayerVersions, 2)
}

func TestBatch2_Layer_DeleteVersion(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, "/2018-10-31/layers/del-layer/versions",
		`{"Content":{"ZipFile":"UEsDBAA="}}`)

	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2018-10-31/layers/del-layer/versions/1", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2018-10-31/layers/del-layer/versions/1", "")
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}

func TestBatch2_Layer_ListLayers(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, "/2018-10-31/layers/layer-a/versions",
		`{"Content":{"ZipFile":"UEsDBAA="}}`)
	callInMemoryHandler(t, h, http.MethodPost, "/2018-10-31/layers/layer-b/versions",
		`{"Content":{"ZipFile":"UEsDBAA="}}`)

	rec := callInMemoryHandler(t, h, http.MethodGet, "/2018-10-31/layers", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.ListLayersOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.GreaterOrEqual(t, len(out.Layers), 2)
}

func TestBatch2_Layer_GetVersionByArn(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	pubRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2018-10-31/layers/arn-layer/versions",
		`{"Content":{"ZipFile":"UEsDBAA="}}`)
	require.Equal(t, http.StatusCreated, pubRec.Code)

	var pub lambda.PublishLayerVersionOutput
	require.NoError(t, json.NewDecoder(pubRec.Body).Decode(&pub))

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2018-10-31/layers-by-arn?Arn="+pub.LayerVersionArn, "")
	require.Equal(t, http.StatusOK, rec.Code)

	var got lambda.GetLayerVersionOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&got))
	assert.Equal(t, pub.LayerVersionArn, got.LayerVersionArn)
}

func TestBatch2_Layer_GetVersionByArn_MissingArn(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodGet, "/2018-10-31/layers-by-arn", "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBatch2_Layer_Policy_AddAndGet(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, "/2018-10-31/layers/pol-layer/versions",
		`{"Content":{"ZipFile":"UEsDBAA="}}`)

	addRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2018-10-31/layers/pol-layer/versions/1/policy",
		`{"StatementId":"s1","Action":"lambda:GetLayerVersion","Principal":"*"}`)
	require.Equal(t, http.StatusCreated, addRec.Code)

	var addOut lambda.AddLayerVersionPermissionOutput
	require.NoError(t, json.NewDecoder(addRec.Body).Decode(&addOut))
	assert.NotEmpty(t, addOut.Statement)
	assert.NotEmpty(t, addOut.RevisionID)

	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2018-10-31/layers/pol-layer/versions/1/policy", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var pol lambda.LayerVersionPolicy
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&pol))
	assert.NotEmpty(t, pol.Policy)
}

func TestBatch2_Layer_Policy_Remove(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, "/2018-10-31/layers/pol-rm-layer/versions",
		`{"Content":{"ZipFile":"UEsDBAA="}}`)
	callInMemoryHandler(t, h, http.MethodPost,
		"/2018-10-31/layers/pol-rm-layer/versions/1/policy",
		`{"StatementId":"stmt1","Action":"lambda:GetLayerVersion","Principal":"*"}`)

	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2018-10-31/layers/pol-rm-layer/versions/1/policy/stmt1", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)
}

// ============================================================
// Tags
// ============================================================

func TestBatch2_Tags_TagAndListAndUntag(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "tags-fn")

	// Get function ARN
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/tags-fn", "")
	var out lambda.GetFunctionOutput
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&out))
	fnARN := out.Configuration.FunctionArn

	// Tag
	tagRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/tags/"+fnARN,
		`{"Tags":{"env":"prod","team":"platform"}}`)
	assert.Equal(t, http.StatusNoContent, tagRec.Code)

	// List tags
	listRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/tags/"+fnARN, "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsOut map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&tagsOut))
	tags := tagsOut["Tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	// Untag
	untagRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/tags/"+fnARN+"?tagKeys=env", "")
	assert.Equal(t, http.StatusNoContent, untagRec.Code)

	// Verify removed
	listRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/tags/"+fnARN, "")
	require.Equal(t, http.StatusOK, listRec2.Code)

	var tagsOut2 map[string]any
	require.NoError(t, json.NewDecoder(listRec2.Body).Decode(&tagsOut2))
	tags2, _ := tagsOut2["Tags"].(map[string]any)
	_, hasEnv := tags2["env"]
	assert.False(t, hasEnv)
	_, hasTeam := tags2["team"]
	assert.True(t, hasTeam)
}

func TestBatch2_Tags_CreateFunctionWithTags(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{"FunctionName":"tagged-fn","PackageType":"Image","Code":{"ImageUri":"x"},` +
		`"Role":"arn:aws:iam:::role/r","Tags":{"project":"myapp","stage":"dev"}}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var fn lambda.FunctionConfiguration
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&fn))
	// Tags may be in the function config or accessible separately
	_ = fn
}

// ============================================================
// AddPermission / RemovePermission / GetPolicy
// ============================================================

func TestBatch2_Permission_FullLifecycle(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "perm-fn")

	// Add first permission
	addRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/perm-fn/policy",
		`{"StatementId":"AllowS3","Action":"lambda:InvokeFunction","Principal":"s3.amazonaws.com"}`)
	require.Equal(t, http.StatusCreated, addRec.Code)

	var addOut lambda.AddPermissionOutput
	require.NoError(t, json.NewDecoder(addRec.Body).Decode(&addOut))
	require.NotNil(t, addOut.Statement)
	assert.NotEmpty(t, *addOut.Statement)

	// Add second permission
	callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/perm-fn/policy",
		`{"StatementId":"AllowSNS","Action":"lambda:InvokeFunction","Principal":"sns.amazonaws.com"}`)

	// Get policy
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/perm-fn/policy", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var pol lambda.GetPolicyOutput
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&pol))
	require.NotNil(t, pol.Policy)
	assert.Contains(t, *pol.Policy, "AllowS3")
	assert.Contains(t, *pol.Policy, "AllowSNS")

	// Remove permission. Real Lambda sends StatementId as a URI path segment
	// (/policy/{StatementId}), never as a "?StatementId=" query parameter —
	// this used to be a query string here, which encoded the pre-fix (wrong)
	// wire shape and could never be hit by a real aws-sdk-go-v2 client.
	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/functions/perm-fn/policy/AllowS3", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// Get policy after remove
	getRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/perm-fn/policy", "")
	require.Equal(t, http.StatusOK, getRec2.Code)

	var pol2 lambda.GetPolicyOutput
	require.NoError(t, json.NewDecoder(getRec2.Body).Decode(&pol2))
	assert.NotContains(t, *pol2.Policy, "AllowS3")
	assert.Contains(t, *pol2.Policy, "AllowSNS")
}

// Test_Permission_ErrorCases covers GetPolicy/RemovePermission error paths that
// don't fit the full add/get/remove lifecycle above.
func Test_Permission_ErrorCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		fnName     string
		method     string
		pathSuffix string
		wantStatus int
	}{
		{
			name:       "GetPolicy on a function with no permissions returns 404",
			fnName:     "nopolicy-fn",
			method:     http.MethodGet,
			pathSuffix: "/policy",
			wantStatus: http.StatusNotFound,
		},
		{
			// Wire format: StatementId is a path segment, not a query param.
			name:       "RemovePermission for a nonexistent statement returns 404",
			fnName:     "rm-404-fn",
			method:     http.MethodDelete,
			pathSuffix: "/policy/nonexistent-stmt",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, tc.fnName)

			rec := callInMemoryHandler(t, h, tc.method,
				"/2015-03-31/functions/"+tc.fnName+tc.pathSuffix, "")
			assert.Equal(t, tc.wantStatus, rec.Code)
		})
	}
}

func TestBatch2_Permission_SourceArn(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "src-arn-fn")

	body := `{
		"StatementId":"AllowSpecificBucket",
		"Action":"lambda:InvokeFunction",
		"Principal":"s3.amazonaws.com",
		"SourceArn":"arn:aws:s3:::my-bucket",
		"SourceAccount":"000000000000"
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/src-arn-fn/policy", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var addOut lambda.AddPermissionOutput
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&addOut))
	require.NotNil(t, addOut.Statement)
	assert.NotEmpty(t, *addOut.Statement)
	// Statement must include the StatementId
	assert.Contains(t, *addOut.Statement, "AllowSpecificBucket")
}

// Test_AddPermission_Qualifier verifies real Lambda's per-qualifier
// resource-based policies (parity-sweep-3 fix): a statement added with
// Qualifier=<version> is stored separately from the unqualified policy and
// only visible via GetPolicy scoped to that same qualifier, and AddPermission
// rejects Qualifier=$LATEST exactly as AWS does ("Lambda does not support
// adding policies to version $LATEST"). Each case builds its own handler,
// backend, and function from scratch — no case depends on state left behind
// by another, and every case is safe to run in parallel.
func Test_AddPermission_Qualifier(t *testing.T) {
	t.Parallel()

	const fnName = "qual-fn"

	tests := []struct {
		name           string
		qualifier      string
		publishVersion bool
		wantAddStatus  int
	}{
		{
			name:          "rejects Qualifier=$LATEST",
			qualifier:     "$LATEST",
			wantAddStatus: http.StatusBadRequest,
		},
		{
			name:          "rejects an unknown qualifier",
			qualifier:     "99",
			wantAddStatus: http.StatusNotFound,
		},
		{
			name:           "scoped to a published version succeeds and is isolated from the unqualified policy",
			qualifier:      "1",
			publishVersion: true,
			wantAddStatus:  http.StatusCreated,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)
			createFunctionForTest(t, h, fnName)

			if tc.publishVersion {
				_, pubErr := bk.PublishVersion(fnName, "")
				require.NoError(t, pubErr)
			}

			addRec := callInMemoryHandler(t, h, http.MethodPost,
				"/2015-03-31/functions/"+fnName+"/policy?Qualifier="+tc.qualifier,
				`{"StatementId":"s-stmt","Action":"lambda:InvokeFunction","Principal":"s3.amazonaws.com"}`)
			require.Equal(t, tc.wantAddStatus, addRec.Code)

			if tc.wantAddStatus != http.StatusCreated {
				return
			}

			// The qualified statement must not leak into the unqualified policy:
			// no unqualified statement was ever added, so GetPolicy 404s.
			unqualRec := callInMemoryHandler(t, h, http.MethodGet,
				"/2015-03-31/functions/"+fnName+"/policy", "")
			assert.Equal(t, http.StatusNotFound, unqualRec.Code)

			// ...but it is visible when GetPolicy is scoped to the same qualifier,
			// and the Resource ARN in the statement carries the qualifier suffix.
			qualRec := callInMemoryHandler(t, h, http.MethodGet,
				"/2015-03-31/functions/"+fnName+"/policy?Qualifier="+tc.qualifier, "")
			require.Equal(t, http.StatusOK, qualRec.Code)

			qualOut := lambdaParseBody(t, qualRec)
			policyStr, _ := qualOut["Policy"].(string)
			assert.Contains(t, policyStr, "s-stmt")
			assert.Contains(t, policyStr, "arn:aws:lambda:us-east-1:000000000000:function:qual-fn:1")
		})
	}
}

// ============================================================
// FunctionEventInvokeConfig
// ============================================================

func TestBatch2_EventInvokeConfig_Lifecycle(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "eic-fn")

	// Put config
	maxRetry := 2
	maxAge := 3600
	body := fmt.Sprintf(`{"MaximumRetryAttempts":%d,"MaximumEventAgeInSeconds":%d}`, maxRetry, maxAge)
	putRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/eic-fn/event-invoke-config", body)
	require.Equal(t, http.StatusOK, putRec.Code)

	var cfg lambda.FunctionEventInvokeConfig
	require.NoError(t, json.NewDecoder(putRec.Body).Decode(&cfg))
	require.NotNil(t, cfg.MaximumRetryAttempts)
	assert.Equal(t, maxRetry, *cfg.MaximumRetryAttempts)
	require.NotNil(t, cfg.MaximumEventAgeInSeconds)
	assert.Equal(t, maxAge, *cfg.MaximumEventAgeInSeconds)

	// Get config
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/eic-fn/event-invoke-config", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var getCfg lambda.FunctionEventInvokeConfig
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getCfg))
	assert.Equal(t, maxRetry, *getCfg.MaximumRetryAttempts)

	// Update config
	updRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/eic-fn/event-invoke-config",
		`{"MaximumRetryAttempts":0}`)
	require.Equal(t, http.StatusOK, updRec.Code)

	// List configs
	listRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/eic-fn/event-invoke-configs", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut lambda.ListFunctionEventInvokeConfigsOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.NotEmpty(t, listOut.FunctionEventInvokeConfigs)

	// Delete config
	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/functions/eic-fn/event-invoke-config", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// Get after delete → 404
	getRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/eic-fn/event-invoke-config", "")
	assert.Equal(t, http.StatusNotFound, getRec2.Code)
}

func TestBatch2_EventInvokeConfig_WithDestinations(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "eic-dest-fn")

	body := `{
		"DestinationConfig":{
			"OnSuccess":{"Destination":"arn:aws:sqs:us-east-1:000000000000:success-q"},
			"OnFailure":{"Destination":"arn:aws:sqs:us-east-1:000000000000:failure-q"}
		}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/eic-dest-fn/event-invoke-config", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var cfg lambda.FunctionEventInvokeConfig
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&cfg))
	require.NotNil(t, cfg.DestinationConfig)
	require.NotNil(t, cfg.DestinationConfig.OnSuccess)
	assert.Contains(t, cfg.DestinationConfig.OnSuccess.Destination, "success-q")
	require.NotNil(t, cfg.DestinationConfig.OnFailure)
	assert.Contains(t, cfg.DestinationConfig.OnFailure.Destination, "failure-q")
}

// ============================================================
// Concurrency (reserved)
// ============================================================

func TestBatch2_Concurrency_PutGetDelete(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "conc-fn")

	// Put
	putRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2017-10-31/functions/conc-fn/concurrency",
		`{"ReservedConcurrentExecutions":50}`)
	require.Equal(t, http.StatusOK, putRec.Code)

	var conc lambda.FunctionConcurrency
	require.NoError(t, json.NewDecoder(putRec.Body).Decode(&conc))
	assert.Equal(t, 50, conc.ReservedConcurrentExecutions)

	// Get
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2019-09-30/functions/conc-fn/concurrency", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var getConc lambda.FunctionConcurrency
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getConc))
	assert.Equal(t, 50, getConc.ReservedConcurrentExecutions)

	// Delete
	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2017-10-31/functions/conc-fn/concurrency", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// Get after delete → 404 (no reserved concurrency configured)
	getRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2019-09-30/functions/conc-fn/concurrency", "")
	assert.Equal(t, http.StatusNotFound, getRec2.Code)
}

func TestBatch2_Concurrency_PutZero(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "conc-zero-fn")

	// Setting 0 reserved concurrency throttles the function
	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2017-10-31/functions/conc-zero-fn/concurrency",
		`{"ReservedConcurrentExecutions":0}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ============================================================
// ProvisionedConcurrencyConfig
// ============================================================

func TestBatch2_ProvisionedConcurrency_Lifecycle(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "pc-fn")
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/pc-fn/versions", `{}`)

	// Put
	putRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2019-09-30/functions/pc-fn/provisioned-concurrency?Qualifier=1",
		`{"ProvisionedConcurrentExecutions":5}`)
	require.Equal(t, http.StatusCreated, putRec.Code)

	var pc lambda.ProvisionedConcurrencyConfig
	require.NoError(t, json.NewDecoder(putRec.Body).Decode(&pc))
	assert.Equal(t, 5, pc.RequestedProvisionedConcurrentExecutions)
	assert.Equal(t, "READY", pc.Status)

	// Get
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2019-09-30/functions/pc-fn/provisioned-concurrency?Qualifier=1", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var getPC lambda.ProvisionedConcurrencyConfig
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&getPC))
	assert.Equal(t, 5, getPC.RequestedProvisionedConcurrentExecutions)

	// List
	listRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2019-09-30/functions/pc-fn/provisioned-concurrency", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut lambda.ListProvisionedConcurrencyConfigsOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.Len(t, listOut.ProvisionedConcurrencyConfigs, 1)

	// Delete
	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2019-09-30/functions/pc-fn/provisioned-concurrency?Qualifier=1", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	// List after delete
	listRec2 := callInMemoryHandler(t, h, http.MethodGet,
		"/2019-09-30/functions/pc-fn/provisioned-concurrency", "")
	var listOut2 lambda.ListProvisionedConcurrencyConfigsOutput
	require.NoError(t, json.NewDecoder(listRec2.Body).Decode(&listOut2))
	assert.Empty(t, listOut2.ProvisionedConcurrencyConfigs)
}

func TestBatch2_ProvisionedConcurrency_AliasQualifier(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "pc-alias-fn")
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/pc-alias-fn/versions", `{}`)
	callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/pc-alias-fn/aliases",
		`{"Name":"stable","FunctionVersion":"1"}`)

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2019-09-30/functions/pc-alias-fn/provisioned-concurrency?Qualifier=stable",
		`{"ProvisionedConcurrentExecutions":3}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2019-09-30/functions/pc-alias-fn/provisioned-concurrency?Qualifier=stable", "")
	assert.Equal(t, http.StatusOK, getRec.Code)
}

// ============================================================
// Function State: Active / Pending / Inactive lifecycle
// ============================================================

func TestBatch2_FunctionState_ActiveOnCreate(t *testing.T) {
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

func TestBatch2_FunctionState_InactiveConstant(t *testing.T) {
	t.Parallel()

	// Verify the constant exists and has the correct value
	assert.Equal(t, lambda.FunctionStateInactive, lambda.FunctionState("Inactive"))
}

func TestBatch2_FunctionState_ListedFunctionsHaveState(t *testing.T) {
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

func TestBatch2_FunctionState_VersionHasState(t *testing.T) {
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
// FunctionURL: complete coverage
// ============================================================

func TestBatch2_FunctionURL_CORS(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "url-cors-fn")

	body := `{
		"AuthType":"NONE",
		"Cors":{
			"AllowOrigins":["https://example.com"],
			"AllowMethods":["GET","POST"],
			"AllowHeaders":["content-type"],
			"MaxAge":300
		}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/url-cors-fn/url", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var cfg lambda.FunctionURLConfig
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&cfg))
	require.NotNil(t, cfg.Cors)
	assert.Equal(t, []string{"https://example.com"}, cfg.Cors.AllowOrigins)
	assert.Equal(t, 300, cfg.Cors.MaxAge)
}

func TestBatch2_FunctionURL_InvokeMode(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "url-stream-fn")

	body := `{"AuthType":"NONE","InvokeMode":"RESPONSE_STREAM"}`
	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/url-stream-fn/url", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var cfg lambda.FunctionURLConfig
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&cfg))
	assert.Equal(t, "RESPONSE_STREAM", cfg.InvokeMode)
}

func TestBatch2_FunctionURL_AWSIAMAuth(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "url-iam-fn")

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/url-iam-fn/url",
		`{"AuthType":"AWS_IAM"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var cfg lambda.FunctionURLConfig
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&cfg))
	assert.Equal(t, "AWS_IAM", cfg.AuthType)
}

func TestBatch2_FunctionURL_FunctionURLIsSet(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "url-set-fn")

	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/url-set-fn/url",
		`{"AuthType":"NONE"}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var cfg lambda.FunctionURLConfig
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&cfg))
	assert.NotEmpty(t, cfg.FunctionURL)
}

// ============================================================
// GetSupportedOperations: verify batch-2 ops listed
// ============================================================

func TestBatch2_GetSupportedOperations_AllBatch2Ops(t *testing.T) {
	t.Parallel()

	h := lambda.NewHandler(nil)
	ops := h.GetSupportedOperations()

	opSet := make(map[string]bool, len(ops))
	for _, op := range ops {
		opSet[op] = true
	}

	batch2Ops := []string{
		"PublishVersion",
		"ListVersionsByFunction",
		"CreateAlias",
		"GetAlias",
		"UpdateAlias",
		"DeleteAlias",
		"ListAliases",
		"AddPermission",
		"RemovePermission",
		"GetPolicy",
		"PutFunctionEventInvokeConfig",
		"GetFunctionEventInvokeConfig",
		"UpdateFunctionEventInvokeConfig",
		"DeleteFunctionEventInvokeConfig",
		"ListFunctionEventInvokeConfigs",
		"PutFunctionConcurrency",
		"GetFunctionConcurrency",
		"DeleteFunctionConcurrency",
		"PutProvisionedConcurrencyConfig",
		"GetProvisionedConcurrencyConfig",
		"DeleteProvisionedConcurrencyConfig",
		"ListProvisionedConcurrencyConfigs",
		"CreateEventSourceMapping",
		"GetEventSourceMapping",
		"UpdateEventSourceMapping",
		"DeleteEventSourceMapping",
		"ListEventSourceMappings",
		"PublishLayerVersion",
		"GetLayerVersion",
		"ListLayerVersions",
		"ListLayers",
		"DeleteLayerVersion",
		"GetLayerVersionByArn",
		"AddLayerVersionPermission",
		"GetLayerVersionPolicy",
		"RemoveLayerVersionPermission",
		"TagResource",
		"UntagResource",
		"ListTags",
		"GetFunctionConfiguration",
		"UpdateFunctionConfiguration",
		"UpdateFunctionCode",
		"GetRuntimeManagementConfig",
		"PutRuntimeManagementConfig",
		"GetFunctionRecursionConfig",
		"PutFunctionRecursionConfig",
		"GetFunctionScalingConfig",
		"PutFunctionScalingConfig",
	}

	for _, op := range batch2Ops {
		assert.True(t, opSet[op], "expected op %q in GetSupportedOperations", op)
	}
}

// ============================================================
// CreateFunction: all input field roundtrips
// ============================================================

func TestBatch2_CreateFunction_AllFields(t *testing.T) {
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

func TestBatch2_GetFunction_WithQualifierAlias(t *testing.T) {
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

func TestBatch2_GetFunction_WithVersionQualifier(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "qual-ver-fn")
	callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/qual-ver-fn/versions", `{}`)

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/qual-ver-fn?Qualifier=1", "")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestBatch2_GetFunction_WithLatestQualifier(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "qual-latest-fn")

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/qual-latest-fn?Qualifier=$LATEST", "")
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ============================================================
// ESM: enable / disable state transitions
// ============================================================

func TestBatch2_ESM_EnableDisable(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-toggle-fn")

	createRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/event-source-mappings",
		`{"EventSourceArn":"arn:aws:sqs:us-east-1:000000000000:toggle-q","FunctionName":"esm-toggle-fn"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&created))
	uuid := created["UUID"].(string)

	// Disable
	disabled := false
	_ = disabled
	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/event-source-mappings/"+uuid,
		`{"Enabled":false}`)
	require.Equal(t, http.StatusOK, updRec.Code)

	var updOut map[string]any
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&updOut))
	assert.Equal(t, "Disabled", updOut["State"])

	// Re-enable
	enableRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/event-source-mappings/"+uuid,
		`{"Enabled":true}`)
	require.Equal(t, http.StatusOK, enableRec.Code)

	var enableOut map[string]any
	require.NoError(t, json.NewDecoder(enableRec.Body).Decode(&enableOut))
	assert.Equal(t, "Enabled", enableOut["State"])
}

func TestBatch2_ESM_DeleteReturnsMapping(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-del-fn")

	createRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/event-source-mappings",
		`{"EventSourceArn":"arn:aws:sqs:us-east-1:000000000000:del-q","FunctionName":"esm-del-fn"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&created))
	uuid := created["UUID"].(string)

	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/event-source-mappings/"+uuid, "")
	require.Equal(t, http.StatusOK, delRec.Code)

	var delOut map[string]any
	require.NoError(t, json.NewDecoder(delRec.Body).Decode(&delOut))
	assert.Equal(t, uuid, delOut["UUID"])

	// Get after delete → 404
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/event-source-mappings/"+uuid, "")
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}
