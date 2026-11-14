package lambdaweb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lwsdk "github.com/aws/aws-sdk-go-v2/service/lambdaweb"
	"github.com/aws/aws-sdk-go-v2/service/lambdaweb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newTestEnv(t)
	created := src.createFunction(t, "app")
	src.settle()

	_, err := src.client.TagResource(
		t.Context(),
		&lwsdk.TagResourceInput{Resource: created.FunctionArn, Tags: map[string]string{"k": "v"}},
	)
	require.NoError(t, err)

	put, err := src.client.PutResourcePolicy(
		t.Context(),
		&lwsdk.PutResourcePolicyInput{ResourceArn: created.FunctionArn, Policy: aws.String(samplePolicy)},
	)
	require.NoError(t, err)

	_, err = src.client.UpdateWebFunctionEndpoint(t.Context(), &lwsdk.UpdateWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("prod"),
		ThrottleConfig: &types.ThrottleConfig{RateLimit: aws.Int32(300)},
	})
	require.NoError(t, err)

	snap := src.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newTestEnv(t)
	require.NoError(t, dst.backend.Restore(t.Context(), snap))

	fn, err := dst.client.GetWebFunction(t.Context(), &lwsdk.GetWebFunctionInput{FunctionName: aws.String("app")})
	require.NoError(t, err)
	assert.Equal(t, types.FunctionStateActive, fn.State)

	tags, err := dst.client.ListTags(t.Context(), &lwsdk.ListTagsInput{Resource: created.FunctionArn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "v"}, tags.Tags)

	pol, err := dst.client.GetResourcePolicy(
		t.Context(),
		&lwsdk.GetResourcePolicyInput{ResourceArn: created.FunctionArn},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(put.RevisionId), aws.ToString(pol.RevisionId))

	ep, err := dst.client.GetWebFunctionEndpoint(t.Context(), &lwsdk.GetWebFunctionEndpointInput{
		FunctionName: aws.String("app"), EndpointName: aws.String("prod"),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(300), aws.ToInt32(ep.ThrottleConfig.RateLimit))
	assert.Equal(t, aws.ToString(created.Endpoint.DomainName), aws.ToString(ep.DomainName))

	rev, err := dst.client.GetWebFunctionRevision(t.Context(), &lwsdk.GetWebFunctionRevisionInput{
		FunctionName: aws.String("app"), RevisionId: created.Revision.RevisionId,
	})
	require.NoError(t, err)
	assert.Equal(t, "v1", aws.ToString(rev.BuildConfig.CodeConfig.S3Object.VersionId))
}

func TestRestore_IncompatibleVersionStartsEmpty(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	env.createFunction(t, "app")

	h := lambdaweb.NewHandler(env.backend)
	require.NoError(t, h.Restore(t.Context(), []byte(`{"service":"lambdaweb","data":{"version":99,"tables":{}}}`)))

	out, err := env.client.ListWebFunctions(t.Context(), &lwsdk.ListWebFunctionsInput{})
	require.NoError(t, err)
	assert.Empty(t, out.Functions)
}

func TestHandlerReset(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	env.createFunction(t, "app")
	env.handler.Reset()

	out, err := env.client.ListWebFunctions(t.Context(), &lwsdk.ListWebFunctionsInput{})
	require.NoError(t, err)
	assert.Empty(t, out.Functions)
}
