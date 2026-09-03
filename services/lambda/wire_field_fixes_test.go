package lambda_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// TestUpdateAlias_UnknownVersionSurfacesResourceNotFoundException guards
// gopherstack-huyl: UpdateAlias set FunctionVersion unconditionally, unlike
// CreateAlias, which validates the target version against the function's
// known versions. lambda@v1.101.2 deserializers.go's
// deserializeOpErrorUpdateAlias models ResourceNotFoundException (the same
// code this package's ErrVersionNotFound already maps to for CreateAlias),
// so an alias could be pointed at a version that never existed.
func TestUpdateAlias_UnknownVersionSurfacesResourceNotFoundException(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	client := newWireTestLambdaClient(t, h)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "updalias-wire-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
		State:        lambda.FunctionStateActive,
	}))

	v1, err := bk.PublishVersion("updalias-wire-fn", "v1")
	require.NoError(t, err)

	_, err = client.CreateAlias(t.Context(), &lambdasdk.CreateAliasInput{
		FunctionName:    aws.String("updalias-wire-fn"),
		Name:            aws.String("prod"),
		FunctionVersion: aws.String(v1.Version),
	})
	require.NoError(t, err)

	_, err = client.UpdateAlias(t.Context(), &lambdasdk.UpdateAliasInput{
		FunctionName:    aws.String("updalias-wire-fn"),
		Name:            aws.String("prod"),
		FunctionVersion: aws.String("99"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())

	got, getErr := client.GetAlias(t.Context(), &lambdasdk.GetAliasInput{
		FunctionName: aws.String("updalias-wire-fn"),
		Name:         aws.String("prod"),
	})
	require.NoError(t, getErr)
	assert.Equal(t, v1.Version, aws.ToString(got.FunctionVersion),
		"a rejected UpdateAlias must not leave the alias pointed at the invalid version")
}

// TestUpdateAlias_LatestVersionSucceeds confirms the new version check
// doesn't regress the $LATEST special case, which CreateAlias also exempts
// from the known-versions lookup.
func TestUpdateAlias_LatestVersionSucceeds(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	client := newWireTestLambdaClient(t, h)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "updalias-latest-fn",
		PackageType:  lambda.PackageTypeImage,
		ImageURI:     "test:latest",
		State:        lambda.FunctionStateActive,
	}))

	v1, err := bk.PublishVersion("updalias-latest-fn", "v1")
	require.NoError(t, err)

	_, err = client.CreateAlias(t.Context(), &lambdasdk.CreateAliasInput{
		FunctionName:    aws.String("updalias-latest-fn"),
		Name:            aws.String("dev"),
		FunctionVersion: aws.String(v1.Version),
	})
	require.NoError(t, err)

	out, err := client.UpdateAlias(t.Context(), &lambdasdk.UpdateAliasInput{
		FunctionName:    aws.String("updalias-latest-fn"),
		Name:            aws.String("dev"),
		FunctionVersion: aws.String("$LATEST"),
	})
	require.NoError(t, err)
	assert.Equal(t, "$LATEST", aws.ToString(out.FunctionVersion))
}

// TestPutFunctionScalingConfig_MinMaxExecutionEnvironments covers
// gopherstack-wksweep-lambda-1: the real FunctionScalingConfig
// (lambda@v1.101.2 types/types.go:1614, nested under
// PutFunctionScalingConfigInput.FunctionScalingConfig) has
// MinExecutionEnvironments/MaxExecutionEnvironments -- an unrelated concept
// to a flat MaximumConcurrency field a prior version invented. This proves
// the real nested shape round-trips through GetFunctionScalingConfig's
// AppliedFunctionScalingConfig/RequestedFunctionScalingConfig/FunctionArn,
// none of which a prior version emitted either.
func TestPutFunctionScalingConfig_MinMaxExecutionEnvironments(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newWireTestLambdaClient(t, h)
	ctx := t.Context()

	createFunctionForTest(t, h, "scaling-wire-fn")

	putOut, err := client.PutFunctionScalingConfig(ctx, &lambdasdk.PutFunctionScalingConfigInput{
		FunctionName: aws.String("scaling-wire-fn"),
		Qualifier:    aws.String("$LATEST"),
		FunctionScalingConfig: &lambdatypes.FunctionScalingConfig{
			MinExecutionEnvironments: aws.Int32(2),
			MaxExecutionEnvironments: aws.Int32(10),
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, putOut.FunctionState)

	getOut, err := client.GetFunctionScalingConfig(ctx, &lambdasdk.GetFunctionScalingConfigInput{
		FunctionName: aws.String("scaling-wire-fn"),
		Qualifier:    aws.String("$LATEST"),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(getOut.FunctionArn), "scaling-wire-fn")

	require.NotNil(t, getOut.AppliedFunctionScalingConfig)
	assert.Equal(t, int32(2), aws.ToInt32(getOut.AppliedFunctionScalingConfig.MinExecutionEnvironments))
	assert.Equal(t, int32(10), aws.ToInt32(getOut.AppliedFunctionScalingConfig.MaxExecutionEnvironments))

	require.NotNil(t, getOut.RequestedFunctionScalingConfig)
	assert.Equal(t, int32(2), aws.ToInt32(getOut.RequestedFunctionScalingConfig.MinExecutionEnvironments))
	assert.Equal(t, int32(10), aws.ToInt32(getOut.RequestedFunctionScalingConfig.MaxExecutionEnvironments))
}

// TestUpdateFunctionUrlConfig_InvokeMode guards gopherstack-id70's re-audit
// finding: UpdateFunctionUrlConfigInput.InvokeMode (lambda@v1.101.2
// api_op_UpdateFunctionUrlConfig.go:68) was never declared on this
// package's UpdateFunctionURLConfigInput struct, so the handler could never
// read it and the backend method took no parameter to apply it. A function
// URL created BUFFERED could never be switched to RESPONSE_STREAM.
func TestUpdateFunctionUrlConfig_InvokeMode(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newWireTestLambdaClient(t, h)
	ctx := t.Context()

	createFunctionForTest(t, h, "invokemode-wire-fn")

	createOut, err := client.CreateFunctionUrlConfig(ctx, &lambdasdk.CreateFunctionUrlConfigInput{
		FunctionName: aws.String("invokemode-wire-fn"),
		AuthType:     lambdatypes.FunctionUrlAuthTypeNone,
		InvokeMode:   lambdatypes.InvokeModeBuffered,
	})
	require.NoError(t, err)
	assert.Equal(t, lambdatypes.InvokeModeBuffered, createOut.InvokeMode)

	updateOut, err := client.UpdateFunctionUrlConfig(ctx, &lambdasdk.UpdateFunctionUrlConfigInput{
		FunctionName: aws.String("invokemode-wire-fn"),
		InvokeMode:   lambdatypes.InvokeModeResponseStream,
	})
	require.NoError(t, err)
	assert.Equal(t, lambdatypes.InvokeModeResponseStream, updateOut.InvokeMode,
		"UpdateFunctionUrlConfig must apply InvokeMode, not silently drop it")

	getOut, err := client.GetFunctionUrlConfig(ctx, &lambdasdk.GetFunctionUrlConfigInput{
		FunctionName: aws.String("invokemode-wire-fn"),
	})
	require.NoError(t, err)
	assert.Equal(t, lambdatypes.InvokeModeResponseStream, getOut.InvokeMode,
		"the updated InvokeMode must persist and round-trip on Get")
}

// TestListEventSourceMappings_LastModified_RealClient covers a "state tracked, never
// surfaced" gap (gopherstack-6flj/21my): real types.EventSourceMappingConfiguration
// (lambda@v1.101.2 types/types.go) declares LastModified, and gopherstack's own
// EventSourceMapping model already tracks it (event_source_mapping.go's LastModified
// time.Time field, set at creation), but toJSONESMResponse -- shared by
// CreateEventSourceMapping/GetEventSourceMapping/ListEventSourceMappings -- never copied
// it into the wire response, so a real client's LastModified was always the zero time
// regardless of when the mapping was created.
func TestListEventSourceMappings_LastModified_RealClient(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newWireTestLambdaClient(t, h)
	ctx := t.Context()

	createFunctionForTest(t, h, "esm-lastmod-fn")

	before := time.Now().Add(-time.Second)

	created, err := client.CreateEventSourceMapping(ctx, &lambdasdk.CreateEventSourceMappingInput{
		EventSourceArn: aws.String("arn:aws:sqs:us-east-1:123456789012:esm-lastmod-queue"),
		FunctionName:   aws.String("esm-lastmod-fn"),
		Enabled:        aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotNil(t, created.LastModified, "LastModified must round-trip on Create, not decode nil")
	assert.True(t, created.LastModified.After(before))

	out, err := client.ListEventSourceMappings(ctx, &lambdasdk.ListEventSourceMappingsInput{
		FunctionName: aws.String("esm-lastmod-fn"),
	})
	require.NoError(t, err)
	require.Len(t, out.EventSourceMappings, 1)
	require.NotNil(t, out.EventSourceMappings[0].LastModified,
		"LastModified must round-trip on List, not decode nil")
	assert.True(t, out.EventSourceMappings[0].LastModified.After(before))
}

// TestListVersionsByFunction_SiblingFields_RealClient covers a "state tracked, never
// surfaced" gap (gopherstack-6flj/21my): real types.FunctionConfiguration
// (lambda@v1.101.2 types/types.go), the item type ListVersionsByFunctionOutput.Versions
// declares, has Architectures/EphemeralStorage/LoggingConfig/MasterArn/StateReason/
// StateReasonCode/LastUpdateStatus/LastUpdateStatusReason. gopherstack's own
// FunctionConfiguration model tracks every one of these (GetFunctionConfiguration
// already emits them correctly) but fnToVersion/publishVersion
// (versions_aliases.go), which build the sibling FunctionVersion type shared by
// ListVersionsByFunction/PublishVersion/GetFunction-by-version, silently dropped all
// eight -- the source struct passed in had the data, the conversion never copied it.
func TestListVersionsByFunction_SiblingFields_RealClient(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)
	client := newWireTestLambdaClient(t, h)
	ctx := t.Context()

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName:     "listversions-sibling-fn",
		PackageType:      lambda.PackageTypeZip,
		Runtime:          "python3.13",
		Handler:          "index.handler",
		Role:             "arn:aws:iam::123456789012:role/lambda-role",
		State:            lambda.FunctionStateActive,
		Architectures:    []string{"arm64"},
		EphemeralStorage: &lambda.EphemeralStorageConfig{Size: 1024},
		LoggingConfig: &lambda.LoggingConfig{
			LogFormat: "JSON",
			LogGroup:  "/aws/lambda/custom-group",
		},
		MasterArn:              "arn:aws:lambda:us-east-1:123456789012:function:edge-owner:1",
		StateReason:            "The function is idle.",
		StateReasonCode:        "Idle",
		LastUpdateStatus:       lambda.LastUpdateStatusSuccessful,
		LastUpdateStatusReason: "The function is ready.",
	}))

	_, err := bk.PublishVersion("listversions-sibling-fn", "v1")
	require.NoError(t, err)
	_, err = bk.PublishVersion("listversions-sibling-fn", "v2")
	require.NoError(t, err)

	out, err := client.ListVersionsByFunction(ctx, &lambdasdk.ListVersionsByFunctionInput{
		FunctionName: aws.String("listversions-sibling-fn"),
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(out.Versions), 3, "must exercise $LATEST plus at least two published versions")

	for _, v := range out.Versions {
		assert.Equal(t, []lambdatypes.Architecture{lambdatypes.ArchitectureArm64}, v.Architectures,
			"Architectures must round-trip, not decode empty")
		require.NotNil(t, v.EphemeralStorage, "EphemeralStorage must round-trip, not decode nil")
		assert.Equal(t, int32(1024), aws.ToInt32(v.EphemeralStorage.Size))
		require.NotNil(t, v.LoggingConfig, "LoggingConfig must round-trip, not decode nil")
		assert.Equal(t, "JSON", string(v.LoggingConfig.LogFormat))
		assert.Equal(t, "/aws/lambda/custom-group", aws.ToString(v.LoggingConfig.LogGroup))
		assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:function:edge-owner:1", aws.ToString(v.MasterArn),
			"MasterArn must round-trip, not decode empty")
		assert.Equal(t, "The function is idle.", aws.ToString(v.StateReason))
		assert.Equal(t, lambdatypes.StateReasonCodeIdle, v.StateReasonCode)
		assert.Equal(t, lambdatypes.LastUpdateStatusSuccessful, v.LastUpdateStatus)
		assert.Equal(t, "The function is ready.", aws.ToString(v.LastUpdateStatusReason))
	}
}
