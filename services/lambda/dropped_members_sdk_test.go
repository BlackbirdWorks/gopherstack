package lambda_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const droppedMembersZip = "PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"

func createDroppedMembersFunction(t *testing.T, client *lambdasdk.Client, name string) {
	t.Helper()

	_, err := client.CreateFunction(t.Context(), &lambdasdk.CreateFunctionInput{
		FunctionName: aws.String(name),
		PackageType:  lambdatypes.PackageTypeZip,
		Runtime:      lambdatypes.RuntimeNodejs20x,
		Handler:      aws.String("index.handler"),
		Role:         aws.String("arn:aws:iam:::role/r"),
		Code:         &lambdatypes.FunctionCode{ZipFile: []byte(droppedMembersZip)},
	})
	require.NoError(t, err)
}

func TestSDK_CreateFunctionKMSLoggingAndCodeSigning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cscArn  func(t *testing.T, client *lambdasdk.Client) string
		wantErr string
	}{
		{name: "existing_config", cscArn: func(t *testing.T, client *lambdasdk.Client) string {
			t.Helper()

			out, err := client.CreateCodeSigningConfig(t.Context(), &lambdasdk.CreateCodeSigningConfigInput{
				AllowedPublishers: &lambdatypes.AllowedPublishers{
					SigningProfileVersionArns: []string{"arn:aws:signer:us-east-1:000000000000:/signing-profiles/p/1"},
				},
				Tags: map[string]string{"owner": "sec"},
			})
			require.NoError(t, err)

			tagsOut, err := client.ListTags(t.Context(), &lambdasdk.ListTagsInput{
				Resource: out.CodeSigningConfig.CodeSigningConfigArn,
			})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"owner": "sec"}, tagsOut.Tags)

			return aws.ToString(out.CodeSigningConfig.CodeSigningConfigArn)
		}},
		{name: "missing_config", cscArn: func(*testing.T, *lambdasdk.Client) string {
			return "arn:aws:lambda:us-east-1:000000000000:code-signing-config:csc-missing"
		}, wantErr: "CodeSigningConfigNotFoundException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			client := newTestLambdaClient(t, h)
			ctx := t.Context()

			cscArn := tt.cscArn(t, client)

			out, err := client.CreateFunction(ctx, &lambdasdk.CreateFunctionInput{
				FunctionName:         aws.String("fn-members"),
				PackageType:          lambdatypes.PackageTypeZip,
				Runtime:              lambdatypes.RuntimeNodejs20x,
				Handler:              aws.String("index.handler"),
				Role:                 aws.String("arn:aws:iam:::role/r"),
				Code:                 &lambdatypes.FunctionCode{ZipFile: []byte(droppedMembersZip)},
				KMSKeyArn:            aws.String("arn:aws:kms:us-east-1:000000000000:key/env-key"),
				CodeSigningConfigArn: aws.String(cscArn),
				LoggingConfig: &lambdatypes.LoggingConfig{
					LogFormat:           lambdatypes.LogFormatJson,
					ApplicationLogLevel: lambdatypes.ApplicationLogLevelWarn,
				},
			})

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/env-key", aws.ToString(out.KMSKeyArn))
			require.NotNil(t, out.LoggingConfig)
			assert.Equal(t, lambdatypes.LogFormatJson, out.LoggingConfig.LogFormat)
			assert.Equal(t, lambdatypes.ApplicationLogLevelWarn, out.LoggingConfig.ApplicationLogLevel)
			assert.Equal(t, "/aws/lambda/fn-members", aws.ToString(out.LoggingConfig.LogGroup))

			csc, err := client.GetFunctionCodeSigningConfig(ctx, &lambdasdk.GetFunctionCodeSigningConfigInput{
				FunctionName: aws.String("fn-members"),
			})
			require.NoError(t, err)
			assert.Equal(t, cscArn, aws.ToString(csc.CodeSigningConfigArn))

			upd, err := client.UpdateFunctionConfiguration(ctx, &lambdasdk.UpdateFunctionConfigurationInput{
				FunctionName: aws.String("fn-members"),
				KMSKeyArn:    aws.String("arn:aws:kms:us-east-1:000000000000:key/other"),
				LoggingConfig: &lambdatypes.LoggingConfig{
					LogFormat: lambdatypes.LogFormatText, LogGroup: aws.String("/custom/group"),
				},
			})
			require.NoError(t, err)
			assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/other", aws.ToString(upd.KMSKeyArn))
			assert.Equal(t, "/custom/group", aws.ToString(upd.LoggingConfig.LogGroup))
		})
	}
}

func TestSDK_PublishVersionCodeSha256(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sha     func(current string) *string
		name    string
		wantErr bool
	}{
		{name: "matching", sha: aws.String},
		{name: "mismatch", sha: func(string) *string { return aws.String("bm90LXRoZS1oYXNo") }, wantErr: true},
		{name: "omitted", sha: func(string) *string { return nil }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			client := newTestLambdaClient(t, h)
			ctx := t.Context()

			createDroppedMembersFunction(t, client, "fn-sha")

			cfg, err := client.GetFunctionConfiguration(ctx, &lambdasdk.GetFunctionConfigurationInput{
				FunctionName: aws.String("fn-sha"),
			})
			require.NoError(t, err)

			_, err = client.PublishVersion(ctx, &lambdasdk.PublishVersionInput{
				FunctionName: aws.String("fn-sha"),
				CodeSha256:   tt.sha(aws.ToString(cfg.CodeSha256)),
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "PreconditionFailedException")

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSDK_EventInvokeConfigIsScopedByQualifier(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)
	ctx := t.Context()

	createDroppedMembersFunction(t, client, "fn-eic")

	ver, err := client.PublishVersion(ctx, &lambdasdk.PublishVersionInput{FunctionName: aws.String("fn-eic")})
	require.NoError(t, err)

	_, err = client.CreateAlias(ctx, &lambdasdk.CreateAliasInput{
		FunctionName: aws.String("fn-eic"), Name: aws.String("live"), FunctionVersion: ver.Version,
	})
	require.NoError(t, err)

	_, err = client.PutFunctionEventInvokeConfig(ctx, &lambdasdk.PutFunctionEventInvokeConfigInput{
		FunctionName: aws.String("fn-eic"), MaximumRetryAttempts: aws.Int32(0),
	})
	require.NoError(t, err)

	aliasCfg, err := client.PutFunctionEventInvokeConfig(ctx, &lambdasdk.PutFunctionEventInvokeConfigInput{
		FunctionName: aws.String("fn-eic"), Qualifier: aws.String("live"), MaximumRetryAttempts: aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(aliasCfg.FunctionArn), ":live")

	unq, err := client.GetFunctionEventInvokeConfig(ctx, &lambdasdk.GetFunctionEventInvokeConfigInput{
		FunctionName: aws.String("fn-eic"),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 0, aws.ToInt32(unq.MaximumRetryAttempts))

	qual, err := client.GetFunctionEventInvokeConfig(ctx, &lambdasdk.GetFunctionEventInvokeConfigInput{
		FunctionName: aws.String("fn-eic"), Qualifier: aws.String("live"),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, aws.ToInt32(qual.MaximumRetryAttempts))

	list, err := client.ListFunctionEventInvokeConfigs(ctx, &lambdasdk.ListFunctionEventInvokeConfigsInput{
		FunctionName: aws.String("fn-eic"),
	})
	require.NoError(t, err)
	assert.Len(t, list.FunctionEventInvokeConfigs, 2)

	_, err = client.PutFunctionEventInvokeConfig(ctx, &lambdasdk.PutFunctionEventInvokeConfigInput{
		FunctionName: aws.String("fn-eic"), Qualifier: aws.String("no-such-alias"),
	})
	require.Error(t, err)

	_, err = client.DeleteFunctionEventInvokeConfig(ctx, &lambdasdk.DeleteFunctionEventInvokeConfigInput{
		FunctionName: aws.String("fn-eic"), Qualifier: aws.String("live"),
	})
	require.NoError(t, err)

	_, err = client.GetFunctionEventInvokeConfig(ctx, &lambdasdk.GetFunctionEventInvokeConfigInput{
		FunctionName: aws.String("fn-eic"), Qualifier: aws.String("live"),
	})
	require.Error(t, err)

	stillThere, err := client.GetFunctionEventInvokeConfig(ctx, &lambdasdk.GetFunctionEventInvokeConfigInput{
		FunctionName: aws.String("fn-eic"),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 0, aws.ToInt32(stillThere.MaximumRetryAttempts))
}

func TestSDK_EventSourceMappingConfigMembers(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)
	ctx := t.Context()

	createDroppedMembersFunction(t, client, "fn-esm")
	createDroppedMembersFunction(t, client, "fn-esm-2")

	created, err := client.CreateEventSourceMapping(ctx, &lambdasdk.CreateEventSourceMappingInput{
		FunctionName:   aws.String("fn-esm"),
		EventSourceArn: aws.String("arn:aws:sqs:us-east-1:000000000000:q"),
		Tags:           map[string]string{"team": "data"},
		ScalingConfig:  &lambdatypes.ScalingConfig{MaximumConcurrency: aws.Int32(5)},
		LoggingConfig: &lambdatypes.EventSourceMappingLoggingConfig{
			SystemLogLevel: lambdatypes.EventSourceMappingSystemLogLevelInfo,
		},
		MetricsConfig: &lambdatypes.EventSourceMappingMetricsConfig{
			Metrics: []lambdatypes.EventSourceMappingMetric{lambdatypes.EventSourceMappingMetricEventCount},
		},
		ProvisionedPollerConfig: &lambdatypes.ProvisionedPollerConfig{
			MinimumPollers: aws.Int32(1), MaximumPollers: aws.Int32(3),
		},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 5, aws.ToInt32(created.ScalingConfig.MaximumConcurrency))
	assert.Equal(t, lambdatypes.EventSourceMappingSystemLogLevelInfo, created.LoggingConfig.SystemLogLevel)
	assert.Equal(
		t,
		[]lambdatypes.EventSourceMappingMetric{lambdatypes.EventSourceMappingMetricEventCount},
		created.MetricsConfig.Metrics,
	)
	assert.EqualValues(t, 3, aws.ToInt32(created.ProvisionedPollerConfig.MaximumPollers))
	assert.Contains(t, aws.ToString(created.EventSourceMappingArn), ":event-source-mapping:"+aws.ToString(created.UUID))

	tagsOut, err := client.ListTags(ctx, &lambdasdk.ListTagsInput{Resource: created.EventSourceMappingArn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "data"}, tagsOut.Tags)

	updated, err := client.UpdateEventSourceMapping(ctx, &lambdasdk.UpdateEventSourceMappingInput{
		UUID:          created.UUID,
		FunctionName:  aws.String("fn-esm-2"),
		ScalingConfig: &lambdatypes.ScalingConfig{MaximumConcurrency: aws.Int32(9)},
		LoggingConfig: &lambdatypes.EventSourceMappingLoggingConfig{
			SystemLogLevel: lambdatypes.EventSourceMappingSystemLogLevelDebug,
		},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 9, aws.ToInt32(updated.ScalingConfig.MaximumConcurrency))
	assert.Equal(t, lambdatypes.EventSourceMappingSystemLogLevelDebug, updated.LoggingConfig.SystemLogLevel)
	assert.Contains(t, aws.ToString(updated.FunctionArn), "function:fn-esm-2")
	assert.EqualValues(t, 3, aws.ToInt32(updated.ProvisionedPollerConfig.MaximumPollers))

	byFn, err := client.ListEventSourceMappings(ctx, &lambdasdk.ListEventSourceMappingsInput{
		FunctionName: aws.String("fn-esm-2"),
	})
	require.NoError(t, err)
	assert.Len(t, byFn.EventSourceMappings, 1)
}

func TestSDK_LayerCompatibleArchitectures(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)
	ctx := t.Context()

	publish := func(name string, archs ...lambdatypes.Architecture) {
		_, err := client.PublishLayerVersion(ctx, &lambdasdk.PublishLayerVersionInput{
			LayerName:               aws.String(name),
			Content:                 &lambdatypes.LayerVersionContentInput{ZipFile: []byte(droppedMembersZip)},
			CompatibleArchitectures: archs,
		})
		require.NoError(t, err)
	}

	publish("layer-arm", lambdatypes.ArchitectureArm64)
	publish("layer-x86", lambdatypes.ArchitectureX8664)
	publish("layer-x86", lambdatypes.ArchitectureArm64)

	tests := []struct {
		wantLatests map[string]int64
		name        string
		arch        lambdatypes.Architecture
	}{
		{
			name:        "arm64",
			arch:        lambdatypes.ArchitectureArm64,
			wantLatests: map[string]int64{"layer-arm": 1, "layer-x86": 2},
		},
		{name: "x86_64", arch: lambdatypes.ArchitectureX8664, wantLatests: map[string]int64{"layer-x86": 1}},
	}

	for _, tt := range tests {
		layers, err := client.ListLayers(ctx, &lambdasdk.ListLayersInput{CompatibleArchitecture: tt.arch})
		require.NoError(t, err, tt.name)

		got := make(map[string]int64, len(layers.Layers))
		for _, l := range layers.Layers {
			got[aws.ToString(l.LayerName)] = l.LatestMatchingVersion.Version
		}

		assert.Equal(t, tt.wantLatests, got, tt.name)
	}

	versions, err := client.ListLayerVersions(ctx, &lambdasdk.ListLayerVersionsInput{
		LayerName: aws.String("layer-x86"), CompatibleArchitecture: lambdatypes.ArchitectureX8664,
	})
	require.NoError(t, err)
	require.Len(t, versions.LayerVersions, 1)
	assert.EqualValues(t, 1, versions.LayerVersions[0].Version)
	assert.Equal(
		t,
		[]lambdatypes.Architecture{lambdatypes.ArchitectureX8664},
		versions.LayerVersions[0].CompatibleArchitectures,
	)
}
