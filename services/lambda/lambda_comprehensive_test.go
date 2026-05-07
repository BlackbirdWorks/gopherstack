package lambda_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// TestComprehensive_AliasRouting verifies that alias CRUD and weighted routing work end-to-end.
func TestComprehensive_AliasRouting(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)

	// Create a function.
	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "alias-routing-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
		State:        lambda.FunctionStateActive,
	}))

	// Publish two versions.
	v1, err := bk.PublishVersion("alias-routing-fn", "v1 description")
	require.NoError(t, err)
	require.Equal(t, "1", v1.Version)

	v2, err := bk.PublishVersion("alias-routing-fn", "v2 description")
	require.NoError(t, err)
	require.Equal(t, "2", v2.Version)

	// CreateAlias pointing to v1.
	body := fmt.Sprintf(`{"Name":"prod","FunctionVersion":%q}`, v1.Version)
	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/functions/alias-routing-fn/aliases", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var alias lambda.FunctionAlias
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &alias))
	assert.Equal(t, "prod", alias.Name)
	assert.Equal(t, v1.Version, alias.FunctionVersion)

	// GetAlias.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/alias-routing-fn/aliases/prod", "")
	require.Equal(t, http.StatusOK, rec.Code)

	// ListAliases.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/alias-routing-fn/aliases", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var listOut lambda.ListAliasesOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	require.Len(t, listOut.Aliases, 1)

	// UpdateAlias — point to v2 with weighted routing 10% to v1.
	updateBody := fmt.Sprintf(
		`{"FunctionVersion":%q,"RoutingConfig":{"AdditionalVersionWeights":{%q:0.1}}}`,
		v2.Version, v1.Version,
	)
	rec = callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/functions/alias-routing-fn/aliases/prod", updateBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var updated lambda.FunctionAlias
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, v2.Version, updated.FunctionVersion)
	require.NotNil(t, updated.RoutingConfig)
	assert.InDelta(t, 0.1, updated.RoutingConfig.AdditionalVersionWeights[v1.Version], 0.001)

	// DeleteAlias.
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/functions/alias-routing-fn/aliases/prod", "")
	require.Equal(t, http.StatusNoContent, rec.Code)

	// Confirm it's gone.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/functions/alias-routing-fn/aliases/prod", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestComprehensive_CodeSigning verifies full code signing config lifecycle and function association.
func TestComprehensive_CodeSigning(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)

	// CreateCodeSigningConfig.
	createBody := `{
		"AllowedPublishers":{"SigningProfileVersionArns":["arn:aws:signer:us-east-1:000000000000:/signing-profiles/p/v1"]},
		"Description":"test-csc"
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost,
		"/2020-04-22/code-signing-configs", createBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var cscOut lambda.CreateCodeSigningConfigOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &cscOut))
	require.NotNil(t, cscOut.CodeSigningConfig)
	cscArn := cscOut.CodeSigningConfig.CodeSigningConfigArn
	assert.NotEmpty(t, cscArn)
	assert.Equal(t, "test-csc", cscOut.CodeSigningConfig.Description)

	// GetCodeSigningConfig — key is the ARN.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2020-04-22/code-signing-configs/"+cscArn, "")
	require.Equal(t, http.StatusOK, rec.Code)

	// ListCodeSigningConfigs.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2020-04-22/code-signing-configs", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var listOut lambda.ListCodeSigningConfigsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	require.Len(t, listOut.CodeSigningConfigs, 1)

	// UpdateCodeSigningConfig.
	updateBody := `{"Description":"updated"}`
	rec = callInMemoryHandler(t, h, http.MethodPut,
		"/2020-04-22/code-signing-configs/"+cscArn, updateBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// Create a function to associate.
	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "csc-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
		State:        lambda.FunctionStateActive,
	}))

	// PutFunctionCodeSigningConfig.
	putBody := fmt.Sprintf(`{"CodeSigningConfigArn":%q}`, cscArn)
	rec = callInMemoryHandler(t, h, http.MethodPut,
		"/2020-06-30/functions/csc-fn/code-signing-config", putBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// GetFunctionCodeSigningConfig.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2020-06-30/functions/csc-fn/code-signing-config", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var getCSCOut lambda.GetFunctionCodeSigningConfigOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getCSCOut))
	assert.Equal(t, cscArn, getCSCOut.CodeSigningConfigArn)
	assert.Equal(t, "csc-fn", getCSCOut.FunctionName)

	// DeleteFunctionCodeSigningConfig.
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2020-06-30/functions/csc-fn/code-signing-config", "")
	require.Equal(t, http.StatusNoContent, rec.Code)

	// DeleteCodeSigningConfig.
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2020-04-22/code-signing-configs/"+cscArn, "")
	require.Equal(t, http.StatusNoContent, rec.Code)
}

// TestComprehensive_ProvisionedConcurrency verifies provisioned concurrency CRUD.
func TestComprehensive_ProvisionedConcurrency(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "prov-conc-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
		State:        lambda.FunctionStateActive,
	}))

	// Publish a version so we can set provisioned concurrency on it.
	v, err := bk.PublishVersion("prov-conc-fn", "")
	require.NoError(t, err)

	// PutProvisionedConcurrencyConfig.
	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2019-09-30/functions/prov-conc-fn/provisioned-concurrency?Qualifier="+v.Version,
		`{"ProvisionedConcurrentExecutions":5}`)
	require.Equal(t, http.StatusCreated, rec.Code)

	var pcOut lambda.ProvisionedConcurrencyConfig
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &pcOut))
	assert.Equal(t, 5, pcOut.RequestedProvisionedConcurrentExecutions)
	assert.Equal(t, "READY", pcOut.Status)

	// GetProvisionedConcurrencyConfig.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2019-09-30/functions/prov-conc-fn/provisioned-concurrency?Qualifier="+v.Version, "")
	require.Equal(t, http.StatusOK, rec.Code)

	// ListProvisionedConcurrencyConfigs.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2019-09-30/functions/prov-conc-fn/provisioned-concurrency", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var listOut lambda.ListProvisionedConcurrencyConfigsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listOut))
	require.Len(t, listOut.ProvisionedConcurrencyConfigs, 1)

	// DeleteProvisionedConcurrencyConfig.
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2019-09-30/functions/prov-conc-fn/provisioned-concurrency?Qualifier="+v.Version, "")
	require.Equal(t, http.StatusNoContent, rec.Code)
}

// TestComprehensive_FunctionDestinations verifies PutFunctionEventInvokeConfig with DestinationConfig.
func TestComprehensive_FunctionDestinations(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "dest-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
		State:        lambda.FunctionStateActive,
	}))

	body := `{
		"MaximumRetryAttempts":2,
		"MaximumEventAgeInSeconds":300,
		"DestinationConfig":{
			"OnSuccess":{"Destination":"arn:aws:sqs:us-east-1:000000000000:success-q"},
			"OnFailure":{"Destination":"arn:aws:sqs:us-east-1:000000000000:failure-q"}
		}
	}`

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2019-09-30/functions/dest-fn/event-invoke-config", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var out lambda.FunctionEventInvokeConfig
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotNil(t, out.DestinationConfig)
	require.NotNil(t, out.DestinationConfig.OnSuccess)
	require.NotNil(t, out.DestinationConfig.OnFailure)
	assert.Equal(t, "arn:aws:sqs:us-east-1:000000000000:success-q", out.DestinationConfig.OnSuccess.Destination)
	assert.Equal(t, "arn:aws:sqs:us-east-1:000000000000:failure-q", out.DestinationConfig.OnFailure.Destination)

	// GetFunctionEventInvokeConfig.
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2019-09-30/functions/dest-fn/event-invoke-config", "")
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestComprehensive_UpdateFunctionCodeArchitectures verifies that Architectures is stored and returned.
func TestComprehensive_UpdateFunctionCodeArchitectures(t *testing.T) {
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

// TestComprehensive_MasterArn verifies that MasterArn is returned when set on a function.
func TestComprehensive_MasterArn(t *testing.T) {
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
