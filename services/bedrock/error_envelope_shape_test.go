package bedrock_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	bedrocksdk "github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/aws/aws-sdk-go-v2/service/bedrock/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// This file covers an error-envelope-shape sweep: four core-bedrock
// operations reported ErrAlreadyExists/ErrNotFound through the shared
// writeError sentinel (services/bedrock/handler.go), which maps those to
// ConflictException/ResourceNotFoundException regardless of the calling
// operation. None of the four operations below declares that type in its own
// awsRestjson1_deserializeOpError<Op> switch (bedrock@v1.66.4
// deserializers.go), so a real client's errors.As into the type it should
// see never matched -- it fell through to an untyped smithy.GenericAPIError.
// Each is now a per-call-site override to ValidationException, the closest
// type the operation's own deserializer does declare.

// TestCreateCustomModelDeployment_DuplicateName_TypesAsValidationException
// covers CreateCustomModelDeployment: its deserializer declares
// AccessDeniedException, InternalServerException, ResourceNotFoundException,
// ServiceQuotaExceededException, ThrottlingException, TooManyTagsException,
// ValidationException -- no ConflictException at all.
func TestCreateCustomModelDeployment_DuplicateName_TypesAsValidationException(t *testing.T) {
	t.Parallel()

	client := newTestBedrockClient(
		t, bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
	)

	in := &bedrocksdk.CreateCustomModelDeploymentInput{
		ModelArn:            aws.String("arn:aws:bedrock:us-east-1:123456789012:custom-model/cm-0000001"),
		ModelDeploymentName: aws.String("dup-deployment"),
	}

	_, err := client.CreateCustomModelDeployment(t.Context(), in)
	require.NoError(t, err)

	_, err = client.CreateCustomModelDeployment(t.Context(), in)
	require.Error(t, err)

	var typed *types.ValidationException

	require.ErrorAs(t, err, &typed)
}

// TestCreateProvisionedModelThroughput_DuplicateName_TypesAsValidationException
// covers CreateProvisionedModelThroughput: its deserializer declares
// AccessDeniedException, InternalServerException, ResourceNotFoundException,
// ServiceQuotaExceededException, ThrottlingException, TooManyTagsException,
// ValidationException -- no ConflictException.
func TestCreateProvisionedModelThroughput_DuplicateName_TypesAsValidationException(t *testing.T) {
	t.Parallel()

	client := newTestBedrockClient(
		t, bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
	)

	in := &bedrocksdk.CreateProvisionedModelThroughputInput{
		ModelId:              aws.String("anthropic.claude-v2"),
		ModelUnits:           aws.Int32(1),
		ProvisionedModelName: aws.String("dup-pmt"),
	}

	_, err := client.CreateProvisionedModelThroughput(t.Context(), in)
	require.NoError(t, err)

	_, err = client.CreateProvisionedModelThroughput(t.Context(), in)
	require.Error(t, err)

	var typed *types.ValidationException

	require.ErrorAs(t, err, &typed)
}

// TestUpdateProvisionedModelThroughput_DuplicateName_TypesAsValidationException
// covers UpdateProvisionedModelThroughput: its deserializer declares
// AccessDeniedException, InternalServerException, ResourceNotFoundException,
// ThrottlingException, ValidationException -- no ConflictException.
func TestUpdateProvisionedModelThroughput_DuplicateName_TypesAsValidationException(t *testing.T) {
	t.Parallel()

	client := newTestBedrockClient(
		t, bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
	)

	first, err := client.CreateProvisionedModelThroughput(
		t.Context(),
		&bedrocksdk.CreateProvisionedModelThroughputInput{
			ModelId:              aws.String("anthropic.claude-v2"),
			ModelUnits:           aws.Int32(1),
			ProvisionedModelName: aws.String("pmt-one"),
		},
	)
	require.NoError(t, err)

	_, err = client.CreateProvisionedModelThroughput(t.Context(), &bedrocksdk.CreateProvisionedModelThroughputInput{
		ModelId:              aws.String("anthropic.claude-v2"),
		ModelUnits:           aws.Int32(1),
		ProvisionedModelName: aws.String("pmt-two"),
	})
	require.NoError(t, err)

	_, err = client.UpdateProvisionedModelThroughput(t.Context(), &bedrocksdk.UpdateProvisionedModelThroughputInput{
		ProvisionedModelId:          first.ProvisionedModelArn,
		DesiredProvisionedModelName: aws.String("pmt-two"),
	})
	require.Error(t, err)

	var typed *types.ValidationException

	require.ErrorAs(t, err, &typed)
}

// TestPutResourcePolicy_UnknownTarget_TypesAsValidationException covers core
// bedrock's PutResourcePolicy (distinct from bedrock-agent's own
// PutResourcePolicy, which DOES declare ConflictException -- see
// resource_policy.go's package doc comment). Its deserializer declares
// AccessDeniedException, ConflictException, InternalServerException,
// ThrottlingException, ValidationException -- no ResourceNotFoundException.
func TestPutResourcePolicy_UnknownTarget_TypesAsValidationException(t *testing.T) {
	t.Parallel()

	client := newTestBedrockClient(
		t, bedrock.NewHandler(bedrock.NewInMemoryBackend("123456789012", "us-east-1")),
	)

	_, err := client.PutResourcePolicy(t.Context(), &bedrocksdk.PutResourcePolicyInput{
		ResourceArn:    aws.String("arn:aws:bedrock:us-east-1:123456789012:guardrail/nonexistent"),
		ResourcePolicy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.Error(t, err)

	var typed *types.ValidationException

	require.ErrorAs(t, err, &typed)
}
