package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// TestAddAssociation_EchoesSourceAndDestinationArn drives AddAssociation
// through the real aws-sdk-go-v2 sagemaker client. AddAssociationOutput has
// no AssociationArn member at all -- it echoes SourceArn and DestinationArn
// (api_op_AddAssociation.go). gopherstack previously wrote only
// "AssociationArn", a key the real deserializer's case list doesn't have, so
// both real members decoded nil on every call.
func TestAddAssociation_EchoesSourceAndDestinationArn(t *testing.T) {
	t.Parallel()

	b := sagemaker.NewInMemoryBackend("123456789012", "us-east-1")
	h := sagemaker.NewHandler(b)
	client := newTestSageMakerClient(t, h)

	sourceArn := "arn:aws:sagemaker:us-east-1:123456789012:experiment-trial/trial-1"
	destArn := "arn:aws:sagemaker:us-east-1:123456789012:artifact/artifact-1"

	out, err := client.AddAssociation(t.Context(), &sagemakersdk.AddAssociationInput{
		SourceArn:      aws.String(sourceArn),
		DestinationArn: aws.String(destArn),
	})
	require.NoError(t, err, "real SDK client must decode AddAssociation without error")
	assert.Equal(t, sourceArn, aws.ToString(out.SourceArn))
	assert.Equal(t, destArn, aws.ToString(out.DestinationArn))
}

// TestDescribeNotebookInstance_SecurityGroupsDecodesNonEmpty drives
// CreateNotebookInstance/DescribeNotebookInstance through the real
// aws-sdk-go-v2 sagemaker client. DescribeNotebookInstanceOutput's member is
// "SecurityGroups" (api_op_DescribeNotebookInstance.go); gopherstack
// previously wrote "SecurityGroupIds" (only ever a
// CreateNotebookInstanceInput field name), so a real client's
// out.SecurityGroups was always empty regardless of what was configured.
func TestDescribeNotebookInstance_SecurityGroupsDecodesNonEmpty(t *testing.T) {
	t.Parallel()

	b := sagemaker.NewInMemoryBackend("123456789012", "us-east-1")
	h := sagemaker.NewHandler(b)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateNotebookInstance(t.Context(), &sagemakersdk.CreateNotebookInstanceInput{
		NotebookInstanceName: aws.String("my-notebook"),
		InstanceType:         "ml.t2.medium",
		RoleArn:              aws.String("arn:aws:iam::123456789012:role/notebook-role"),
		SecurityGroupIds:     []string{"sg-1", "sg-2"},
	})
	require.NoError(t, err)

	out, err := client.DescribeNotebookInstance(t.Context(), &sagemakersdk.DescribeNotebookInstanceInput{
		NotebookInstanceName: aws.String("my-notebook"),
	})
	require.NoError(t, err, "real SDK client must decode DescribeNotebookInstance without error")
	assert.ElementsMatch(t, []string{"sg-1", "sg-2"}, out.SecurityGroups)
}
