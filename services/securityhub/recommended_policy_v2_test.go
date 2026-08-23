package securityhub_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	"github.com/aws/aws-sdk-go-v2/service/securityhub/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

// TestGenerateGetRecommendedPolicyV2_RealClient covers gopherstack-tp8x:
// both ops returned the fabricated shape {MetadataUid,Policy,GenerationTime},
// which belongs to neither op's real output.
// GenerateRecommendedPolicyV2Output (securityhub@v1.75.4
// api_op_GenerateRecommendedPolicyV2.go) has NO members at all -- it only
// starts async generation. GetRecommendedPolicyV2Output
// (api_op_GetRecommendedPolicyV2.go) is an async-retrieval-status shape
// (Status/RecommendationType/ResourceArn/RecommendationSteps/Error/
// NextToken), not a returned policy document; its RecommendationSteps are a
// union tagged "UnusedPermissions"
// (types.RecommendationStepMemberUnusedPermissions), each carrying
// RecommendedAction/RecommendedPolicy/ExistingPolicy/ExistingPolicyId/
// PolicyUpdatedAt (types.UnusedPermissionsRecommendationStep). Driven
// through the real SDK client since a raw-body assertion for a field-family
// swap this large is easy to get subtly wrong (e.g. right keys, wrong
// nesting) in a way only a typed decode catches.
func TestGenerateGetRecommendedPolicyV2_RealClient(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))
	ctx := t.Context()

	genOut, err := client.GenerateRecommendedPolicyV2(ctx, &securityhubsdk.GenerateRecommendedPolicyV2Input{
		MetadataUid: aws.String("finding-metadata-uid-tp8x"),
	})
	require.NoError(t, err)
	require.NotNil(t, genOut,
		"GenerateRecommendedPolicyV2Output has no members, but the pointer itself must not be nil")

	getOut, err := client.GetRecommendedPolicyV2(ctx, &securityhubsdk.GetRecommendedPolicyV2Input{
		MetadataUid: aws.String("finding-metadata-uid-tp8x"),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut)

	assert.Equal(t, types.RecommendationStatusSucceeded, getOut.Status)
	assert.Equal(t, types.RecommendationTypeUnusedPermissionRecommendation, getOut.RecommendationType)
	assert.Nil(t, getOut.Error, "this backend generates synchronously and never fails, so Error must be unset")

	require.Len(t, getOut.RecommendationSteps, 1)

	step, ok := getOut.RecommendationSteps[0].(*types.RecommendationStepMemberUnusedPermissions)
	require.True(t, ok, "recommendation step must decode as the UnusedPermissions union member")
	assert.Equal(t, "CREATE_POLICY", aws.ToString(step.Value.RecommendedAction))
	assert.NotEmpty(t, aws.ToString(step.Value.RecommendedPolicy))
	assert.NotNil(t, step.Value.PolicyUpdatedAt)
}

// TestGetRecommendedPolicyV2_NotFound_RealClient guards the error path still
// works with the rewritten response shape.
func TestGetRecommendedPolicyV2_NotFound_RealClient(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

	_, err := client.GetRecommendedPolicyV2(t.Context(), &securityhubsdk.GetRecommendedPolicyV2Input{
		MetadataUid: aws.String("does-not-exist"),
	})
	require.Error(t, err)
}
