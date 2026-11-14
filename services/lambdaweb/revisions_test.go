package lambdaweb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lwsdk "github.com/aws/aws-sdk-go-v2/service/lambdaweb"
	"github.com/aws/aws-sdk-go-v2/service/lambdaweb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateWebFunctionRevision_Config(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	env.createFunction(t, "app")

	rc := revisionConfig()
	rc.ServiceConfig.TimeoutSeconds = aws.Int32(60)
	rc.ServiceConfig.MaxConcurrencyPerEnvironment = aws.Int32(32)
	rc.ServiceConfig.EnvironmentVariables = map[string]string{"LOG_LEVEL": "info"}
	rc.ServiceConfig.TelemetryConfig = &types.TelemetryConfig{LoggingConfig: &types.LoggingConfig{}}

	out, err := env.client.CreateWebFunctionRevision(t.Context(), &lwsdk.CreateWebFunctionRevisionInput{
		FunctionName: aws.String("app"), BuildConfig: rc.BuildConfig, ServiceConfig: rc.ServiceConfig,
		Description: aws.String("canary"), KmsKeyArn: aws.String("arn:aws:kms:us-east-1:123456789012:key/k"),
	})
	require.NoError(t, err)

	assert.Equal(t, types.RevisionStatePending, out.State)
	assert.Equal(t, int32(60), aws.ToInt32(out.ServiceConfig.TimeoutSeconds))
	assert.Equal(t, map[string]string{"LOG_LEVEL": "info"}, out.ServiceConfig.EnvironmentVariables)
	assert.Equal(t, "canary", aws.ToString(out.Description))
	lc := out.ServiceConfig.TelemetryConfig.LoggingConfig
	assert.Equal(t, "/aws/lambda/web/app", aws.ToString(lc.LogGroup), "default log group is returned")
	assert.Equal(t, types.ApplicationLogLevelInfo, lc.ApplicationLogLevel)
	assert.Equal(t, types.SystemLogLevelInfo, lc.SystemLogLevel)

	env.settle()

	got, err := env.client.GetWebFunctionRevision(t.Context(), &lwsdk.GetWebFunctionRevisionInput{
		FunctionName: aws.String("app"), RevisionId: out.RevisionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, types.RevisionStateActive, got.State)
}

func TestCreateWebFunctionRevision_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(in *lwsdk.CreateWebFunctionRevisionInput)
		name   string
		code   string
	}{
		{
			name:   "unknown function",
			mutate: func(in *lwsdk.CreateWebFunctionRevisionInput) { in.FunctionName = aws.String("ghost") },
			code:   "ResourceNotFoundException",
		},
		{name: "concurrency", mutate: func(in *lwsdk.CreateWebFunctionRevisionInput) {
			in.ServiceConfig.MaxConcurrencyPerEnvironment = aws.Int32(129)
		}, code: "ValidationException"},
		{name: "log level", mutate: func(in *lwsdk.CreateWebFunctionRevisionInput) {
			in.ServiceConfig.TelemetryConfig = &types.TelemetryConfig{
				LoggingConfig: &types.LoggingConfig{SystemLogLevel: types.SystemLogLevel("ERROR")},
			}
		}, code: "ValidationException"},
		{name: "unversioned bucket", mutate: func(in *lwsdk.CreateWebFunctionRevisionInput) {
			in.BuildConfig.CodeConfig.S3Object.Bucket = aws.String("unversioned")
		}, code: "ValidationException"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newTestEnv(t)
			env.createFunction(t, "app")

			rc := revisionConfig()
			in := &lwsdk.CreateWebFunctionRevisionInput{
				FunctionName: aws.String("app"), BuildConfig: rc.BuildConfig, ServiceConfig: rc.ServiceConfig,
			}
			tc.mutate(in)

			_, err := env.client.CreateWebFunctionRevision(t.Context(), in)
			assertAPIErrorCode(t, err, tc.code)
		})
	}
}

func TestListWebFunctionRevisions(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	env.createFunction(t, "app")
	env.settle()

	second := env.createRevision(t)

	out, err := env.client.ListWebFunctionRevisions(
		t.Context(),
		&lwsdk.ListWebFunctionRevisionsInput{FunctionName: aws.String("app")},
	)
	require.NoError(t, err)
	require.Len(t, out.Revisions, 2)
	assert.Equal(t, aws.ToString(second.RevisionId), aws.ToString(out.Revisions[0].RevisionId), "newest first")

	active, err := env.client.ListWebFunctionRevisions(t.Context(), &lwsdk.ListWebFunctionRevisionsInput{
		FunctionName: aws.String(
			"app",
		),
		Filters: []types.Filter{{Name: aws.String("state"), Values: []string{"Active"}}},
	})
	require.NoError(t, err)
	assert.Len(t, active.Revisions, 1)

	page, err := env.client.ListWebFunctionRevisions(t.Context(), &lwsdk.ListWebFunctionRevisionsInput{
		FunctionName: aws.String("app"), MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	assert.NotNil(t, page.NextToken)
}

func TestDeleteWebFunctionRevision(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	created := env.createFunction(t, "app")
	env.settle()

	spare := env.createRevision(t)

	_, err := env.client.DeleteWebFunctionRevision(t.Context(), &lwsdk.DeleteWebFunctionRevisionInput{
		FunctionName: aws.String("app"), RevisionId: created.Revision.RevisionId,
	})
	assertAPIErrorCode(t, err, "ConflictException")

	_, err = env.client.DeleteWebFunctionRevision(t.Context(), &lwsdk.DeleteWebFunctionRevisionInput{
		FunctionName: aws.String("app"), RevisionId: spare.RevisionArn,
	})
	require.NoError(t, err)

	_, err = env.client.GetWebFunctionRevision(t.Context(), &lwsdk.GetWebFunctionRevisionInput{
		FunctionName: aws.String("app"), RevisionId: spare.RevisionId,
	})
	assertAPIErrorCode(t, err, "ResourceNotFoundException")
}

func TestRevisionCleanup(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	created := env.createFunction(t, "app")
	env.settle()

	for range 52 {
		env.createRevision(t)
	}

	env.settle()

	out, err := env.client.ListWebFunctionRevisions(t.Context(), &lwsdk.ListWebFunctionRevisionsInput{
		FunctionName: aws.String("app"), MaxResults: aws.Int32(50),
	})
	require.NoError(t, err)

	ids := map[string]bool{}
	for _, r := range out.Revisions {
		ids[aws.ToString(r.RevisionId)] = true
	}

	assert.Len(t, ids, 50, "revisions beyond the cleanup target are removed")
	assert.True(t, ids[aws.ToString(created.Revision.RevisionId)], "a revision serving traffic is never cleaned up")
}
