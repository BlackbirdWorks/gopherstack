package directoryservice_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	directoryservicesdk "github.com/aws/aws-sdk-go-v2/service/directoryservice"
	"github.com/aws/aws-sdk-go-v2/service/directoryservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

// TestCAEnrollmentPolicy_RoundTrip drives Enable/Describe/DisableCAEnrollmentPolicy
// through a real SDK client and proves PcaConnectorArn is actually persisted
// and echoed back, instead of the pre-fix behavior where it was silently
// dropped and DescribeCAEnrollmentPolicy returned a fabricated
// {"CAEnrollmentPolicy":{"EnrollmentStatus":...}} shape that doesn't exist on
// the real API at all (gopherstack-h910).
func TestCAEnrollmentPolicy_RoundTrip(t *testing.T) {
	t.Parallel()

	h := directoryservice.NewHandler(directoryservice.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestDirectoryServiceClient(t, h)

	created, err := client.CreateDirectory(t.Context(), &directoryservicesdk.CreateDirectoryInput{
		Name:     aws.String("corp.example.com"),
		Password: aws.String("Admin1234!"),
		Size:     types.DirectorySizeSmall,
	})
	require.NoError(t, err)
	dirID := aws.ToString(created.DirectoryId)

	connectorArn := "arn:aws:pca-connector-ad:us-east-1:123456789012:connector/conn-1"

	_, err = client.EnableCAEnrollmentPolicy(t.Context(), &directoryservicesdk.EnableCAEnrollmentPolicyInput{
		DirectoryId:     aws.String(dirID),
		PcaConnectorArn: aws.String(connectorArn),
	})
	require.NoError(t, err)

	described, err := client.DescribeCAEnrollmentPolicy(
		t.Context(),
		&directoryservicesdk.DescribeCAEnrollmentPolicyInput{
			DirectoryId: aws.String(dirID),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.CaEnrollmentPolicyStatusSuccess, described.CaEnrollmentPolicyStatus)
	assert.Equal(t, connectorArn, aws.ToString(described.PcaConnectorArn))
	assert.Equal(t, dirID, aws.ToString(described.DirectoryId))

	_, err = client.DisableCAEnrollmentPolicy(t.Context(), &directoryservicesdk.DisableCAEnrollmentPolicyInput{
		DirectoryId: aws.String(dirID),
	})
	require.NoError(t, err)

	afterDisable, err := client.DescribeCAEnrollmentPolicy(
		t.Context(),
		&directoryservicesdk.DescribeCAEnrollmentPolicyInput{
			DirectoryId: aws.String(dirID),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, types.CaEnrollmentPolicyStatusDisabled, afterDisable.CaEnrollmentPolicyStatus)
	assert.Equal(t, connectorArn, aws.ToString(afterDisable.PcaConnectorArn), "PcaConnectorArn survives disable")
}

// TestEnableCAEnrollmentPolicy_ClientRequiresPcaConnectorArn proves the real
// SDK client itself refuses to send EnableCAEnrollmentPolicy without
// PcaConnectorArn, confirming it is genuinely a required member on the pinned
// SDK, not an assumption (gopherstack-h910).
func TestEnableCAEnrollmentPolicy_ClientRequiresPcaConnectorArn(t *testing.T) {
	t.Parallel()

	h := directoryservice.NewHandler(directoryservice.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestDirectoryServiceClient(t, h)

	_, err := client.EnableCAEnrollmentPolicy(t.Context(), &directoryservicesdk.EnableCAEnrollmentPolicyInput{
		DirectoryId: aws.String("d-1234567890"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PcaConnectorArn")
}
