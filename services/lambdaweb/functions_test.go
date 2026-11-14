package lambdaweb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lwsdk "github.com/aws/aws-sdk-go-v2/service/lambdaweb"
	"github.com/aws/aws-sdk-go-v2/service/lambdaweb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateWebFunction_Shape(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	out, err := env.client.CreateWebFunction(t.Context(), &lwsdk.CreateWebFunctionInput{
		FunctionName: aws.String("my-app"), RevisionConfig: revisionConfig(), EndpointConfig: endpointConfig(),
		Tags: map[string]string{"team": "web"},
	})
	require.NoError(t, err)

	assert.Equal(t, "arn:aws:lambda:us-east-1:123456789012:web-function/my-app", aws.ToString(out.FunctionArn))
	assert.Equal(t, types.FunctionStatePending, out.State)
	assert.Equal(t, map[string]string{"team": "web"}, out.Tags)
	require.NotNil(t, out.Revision)
	assert.Equal(t, types.RevisionStatePending, out.Revision.State)
	assert.Contains(t, aws.ToString(out.Revision.RevisionArn), ":web-function/my-app/revision/rev-")
	require.NotNil(t, out.Endpoint)
	assert.Equal(t, types.EndpointStatePending, out.Endpoint.State)
	assert.Equal(t, types.AutoDeploymentModeDisabled, out.Endpoint.AutoDeploymentMode)
	assert.Contains(t, aws.ToString(out.Endpoint.DomainName), ".lambda-web.us-east-1.on.aws")
	assert.Equal(t, aws.ToString(out.Revision.RevisionId), aws.ToString(out.Endpoint.RevisionWeights[0].RevisionId))
	assert.Equal(t, int32(100), aws.ToInt32(out.Endpoint.RevisionWeights[0].Weight))
}

func TestWebFunction_AsyncActivation(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	created := env.createFunction(t, "my-app")
	revID := aws.ToString(created.Revision.RevisionId)

	fn, err := env.client.GetWebFunction(t.Context(), &lwsdk.GetWebFunctionInput{FunctionName: aws.String("my-app")})
	require.NoError(t, err)
	assert.Equal(t, types.FunctionStatePending, fn.State)

	env.settle()

	fn, err = env.client.GetWebFunction(t.Context(), &lwsdk.GetWebFunctionInput{FunctionName: aws.String("my-app")})
	require.NoError(t, err)
	assert.Equal(t, types.FunctionStateActive, fn.State)

	rev, err := env.client.GetWebFunctionRevision(t.Context(), &lwsdk.GetWebFunctionRevisionInput{
		FunctionName: aws.String("my-app"), RevisionId: aws.String(revID),
	})
	require.NoError(t, err)
	assert.Equal(t, types.RevisionStateActive, rev.State)
	assert.Equal(
		t,
		"v1",
		aws.ToString(rev.BuildConfig.CodeConfig.S3Object.VersionId),
		"revision pins the object version",
	)
	assert.Equal(t, int32(30), aws.ToInt32(rev.ServiceConfig.TimeoutSeconds))
	assert.Equal(t, int32(64), aws.ToInt32(rev.ServiceConfig.MaxConcurrencyPerEnvironment))

	ep, err := env.client.GetWebFunctionEndpoint(t.Context(), &lwsdk.GetWebFunctionEndpointInput{
		FunctionName: aws.String("my-app"), EndpointName: aws.String("prod"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.EndpointStateActive, ep.State)
	assert.Contains(t, ep.RegionalEndpoints, testRegion)
}

func TestCreateWebFunction_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(in *lwsdk.CreateWebFunctionInput)
		name   string
		code   string
	}{
		{
			name:   "bad name",
			mutate: func(in *lwsdk.CreateWebFunctionInput) { in.FunctionName = aws.String("-bad") },
			code:   "ValidationException",
		},
		{name: "long name", mutate: func(in *lwsdk.CreateWebFunctionInput) {
			in.FunctionName = aws.String("a123456789012345678901234567890123456789012345678901234567890123456")
		}, code: "ValidationException"},
		{
			name:   "endpoint without revision",
			mutate: func(in *lwsdk.CreateWebFunctionInput) { in.RevisionConfig = nil },
			code:   "ValidationException",
		},
		{name: "bad runtime", mutate: func(in *lwsdk.CreateWebFunctionInput) {
			in.RevisionConfig.BuildConfig.RuntimeConfig.Runtime = aws.String("nodejs18.x")
		}, code: "ValidationException"},
		{name: "unversioned bucket", mutate: func(in *lwsdk.CreateWebFunctionInput) {
			in.RevisionConfig.BuildConfig.CodeConfig.S3Object.Bucket = aws.String("unversioned")
		}, code: "ValidationException"},
		{name: "missing bucket", mutate: func(in *lwsdk.CreateWebFunctionInput) {
			in.RevisionConfig.BuildConfig.CodeConfig.S3Object.Bucket = aws.String("nope")
		}, code: "ValidationException"},
		{name: "missing object", mutate: func(in *lwsdk.CreateWebFunctionInput) {
			in.RevisionConfig.BuildConfig.CodeConfig.S3Object.Key = aws.String("other.zip")
		}, code: "ValidationException"},
		{name: "bad role", mutate: func(in *lwsdk.CreateWebFunctionInput) {
			in.RevisionConfig.ServiceConfig.ExecutionRoleArn = aws.String("not-an-arn")
		}, code: "ValidationException"},
		{name: "timeout range", mutate: func(in *lwsdk.CreateWebFunctionInput) {
			in.RevisionConfig.ServiceConfig.TimeoutSeconds = aws.Int32(1)
		}, code: "ValidationException"},
		{name: "reserved log group", mutate: func(in *lwsdk.CreateWebFunctionInput) {
			in.RevisionConfig.ServiceConfig.TelemetryConfig = &types.TelemetryConfig{
				LoggingConfig: &types.LoggingConfig{LogGroup: aws.String("aws/x")},
			}
		}, code: "ValidationException"},
		{name: "multi region needs regions", mutate: func(in *lwsdk.CreateWebFunctionInput) {
			in.EndpointConfig.EndpointType = types.EndpointTypeMultiRegion
		}, code: "ValidationException"},
		{name: "reserved tag", mutate: func(in *lwsdk.CreateWebFunctionInput) {
			in.Tags = map[string]string{"aws:x": "y"}
		}, code: "ValidationException"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newTestEnv(t)
			in := &lwsdk.CreateWebFunctionInput{
				FunctionName: aws.String(
					"fn",
				),
				RevisionConfig: revisionConfig(),
				EndpointConfig: endpointConfig(),
			}
			tc.mutate(in)

			_, err := env.client.CreateWebFunction(t.Context(), in)
			require.Error(t, err)
			assertAPIErrorCode(t, err, tc.code)
		})
	}
}

func TestCreateWebFunction_Duplicate(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	env.createFunction(t, "dup")

	_, err := env.client.CreateWebFunction(t.Context(), &lwsdk.CreateWebFunctionInput{
		FunctionName: aws.String("dup"), RevisionConfig: revisionConfig(), EndpointConfig: endpointConfig(),
	})
	assertAPIErrorCode(t, err, "ConflictException")
}

func TestGetWebFunction_NotFound(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)

	_, err := env.client.GetWebFunction(t.Context(), &lwsdk.GetWebFunctionInput{FunctionName: aws.String("ghost")})
	assertAPIErrorCode(t, err, "ResourceNotFoundException")

	var nf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)
	assert.Equal(t, "ghost", aws.ToString(nf.ResourceId))
	assert.Equal(t, "web-function", aws.ToString(nf.ResourceType))
}

func TestGetWebFunction_ByARN(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	created := env.createFunction(t, "by-arn")

	out, err := env.client.GetWebFunction(t.Context(), &lwsdk.GetWebFunctionInput{FunctionName: created.FunctionArn})
	require.NoError(t, err)
	assert.Equal(t, "by-arn", aws.ToString(out.FunctionName))
}

func TestListWebFunctions(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	for _, n := range []string{"c", "a", "b"} {
		env.createFunction(t, n)
	}

	env.settle()
	env.createFunction(t, "d")

	tests := []struct {
		name    string
		input   lwsdk.ListWebFunctionsInput
		want    []string
		wantNxt bool
	}{
		{name: "all sorted", input: lwsdk.ListWebFunctionsInput{}, want: []string{"a", "b", "c", "d"}},
		{
			name:    "page",
			input:   lwsdk.ListWebFunctionsInput{MaxResults: aws.Int32(2)},
			want:    []string{"a", "b"},
			wantNxt: true,
		},
		{name: "state filter", input: lwsdk.ListWebFunctionsInput{
			Filters: []types.Filter{{Name: aws.String("state"), Values: []string{"Pending"}}},
		}, want: []string{"d"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := env.client.ListWebFunctions(t.Context(), &tc.input)
			require.NoError(t, err)

			var names []string
			for _, f := range out.Functions {
				names = append(names, aws.ToString(f.FunctionName))
			}

			assert.Equal(t, tc.want, names)
			assert.Equal(t, tc.wantNxt, out.NextToken != nil)
		})
	}
}

func TestListWebFunctions_Validation(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)

	tests := []struct {
		input lwsdk.ListWebFunctionsInput
		name  string
	}{
		{
			name:  "bad filter",
			input: lwsdk.ListWebFunctionsInput{Filters: []types.Filter{{Name: aws.String("x"), Values: []string{"y"}}}},
		},
		{name: "max results", input: lwsdk.ListWebFunctionsInput{MaxResults: aws.Int32(51)}},
		{name: "bad token", input: lwsdk.ListWebFunctionsInput{NextToken: aws.String("!!")}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := env.client.ListWebFunctions(t.Context(), &tc.input)
			assertAPIErrorCode(t, err, "ValidationException")
		})
	}
}

func TestDeleteWebFunction(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	env.createFunction(t, "gone")
	env.settle()

	_, err := env.client.DeleteWebFunction(t.Context(), &lwsdk.DeleteWebFunctionInput{FunctionName: aws.String("gone")})
	require.NoError(t, err)

	got, err := env.client.GetWebFunction(t.Context(), &lwsdk.GetWebFunctionInput{FunctionName: aws.String("gone")})
	require.NoError(t, err)
	assert.Equal(t, types.FunctionStateDeleting, got.State)

	_, err = env.client.DeleteWebFunction(t.Context(), &lwsdk.DeleteWebFunctionInput{FunctionName: aws.String("gone")})
	assertAPIErrorCode(t, err, "ResourceNotFoundException")

	settings, err := env.client.GetWebAccountSettings(t.Context(), &lwsdk.GetWebAccountSettingsInput{})
	require.NoError(t, err)
	assert.Equal(
		t,
		int32(1),
		aws.ToInt32(settings.AccountUsage.FunctionCount),
		"deleting function still counts until removed",
	)

	env.settle()

	_, err = env.client.GetWebFunction(t.Context(), &lwsdk.GetWebFunctionInput{FunctionName: aws.String("gone")})
	assertAPIErrorCode(t, err, "ResourceNotFoundException")

	settings, err = env.client.GetWebAccountSettings(t.Context(), &lwsdk.GetWebAccountSettingsInput{})
	require.NoError(t, err)
	assert.Equal(t, int32(0), aws.ToInt32(settings.AccountUsage.FunctionCount))
}

func TestGetWebAccountSettings(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)

	out, err := env.client.GetWebAccountSettings(t.Context(), &lwsdk.GetWebAccountSettingsInput{})
	require.NoError(t, err)
	assert.Equal(t, int32(10), aws.ToInt32(out.AccountQuotas.MaxEndpointsPerFunction))
	assert.Equal(t, int32(50), aws.ToInt32(out.AccountQuotas.MaxRevisionsPerFunction))
	assert.Equal(t, int32(2000), aws.ToInt32(out.AccountQuotas.MaxTotalArmVCpus))
	assert.Equal(t, int32(100000), aws.ToInt32(out.AccountQuotas.MaxTotalRateLimit))
}
