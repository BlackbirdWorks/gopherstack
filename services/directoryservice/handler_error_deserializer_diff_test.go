package directoryservice_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	directoryservicesdk "github.com/aws/aws-sdk-go-v2/service/directoryservice"
	"github.com/aws/aws-sdk-go-v2/service/directoryservice/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

// These prove the gopherstack-wlo1 per-op error deserializer diffs found in
// directoryservice: several ops returned a wire error code their own
// deserializeOpError switch (directoryservice@v1.41.4 deserializers.go)
// cannot type, so errors.As into the intended concrete types.*Exception
// failed and the real client only ever saw a smithy.GenericAPIError.

// TestAddRegion_AlreadyInRegion_TypesDirectoryAlreadyInRegionException proves
// AddRegion, called twice for the same Region, surfaces
// DirectoryAlreadyInRegionException -- not EntityDoesNotExistException's
// sibling EntityAlreadyExistsException, which AddRegion's own error switch
// does not model at all.
func TestAddRegion_AlreadyInRegion_TypesDirectoryAlreadyInRegionException(t *testing.T) {
	t.Parallel()

	h := directoryservice.NewHandler(directoryservice.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestDirectoryServiceClient(t, h)

	created, err := client.CreateMicrosoftAD(t.Context(), &directoryservicesdk.CreateMicrosoftADInput{
		Name:     aws.String("corp.example.com"),
		Password: aws.String("Admin1234!"),
		VpcSettings: &types.DirectoryVpcSettings{
			VpcId:     aws.String("vpc-0123456789abcdef0"),
			SubnetIds: []string{"subnet-0123456789abcdef0", "subnet-0123456789abcdef1"},
		},
	})
	require.NoError(t, err)
	dirID := aws.ToString(created.DirectoryId)

	vpcSettings := &types.DirectoryVpcSettings{
		VpcId:     aws.String("vpc-0123456789abcdef0"),
		SubnetIds: []string{"subnet-0123456789abcdef0", "subnet-0123456789abcdef1"},
	}

	_, err = client.AddRegion(t.Context(), &directoryservicesdk.AddRegionInput{
		DirectoryId: aws.String(dirID),
		RegionName:  aws.String("us-west-2"),
		VPCSettings: vpcSettings,
	})
	require.NoError(t, err)

	_, err = client.AddRegion(t.Context(), &directoryservicesdk.AddRegionInput{
		DirectoryId: aws.String(dirID),
		RegionName:  aws.String("us-west-2"),
		VPCSettings: vpcSettings,
	})
	require.Error(t, err)

	var alreadyInRegion *types.DirectoryAlreadyInRegionException
	require.ErrorAs(t, err, &alreadyInRegion)
}

// TestDescribeClientAuthenticationSettings_DirectoryNotFound_TypesDirectoryDoesNotExistException
// proves DescribeClientAuthenticationSettings surfaces
// DirectoryDoesNotExistException for an unknown directory, not the generic
// EntityDoesNotExistException -- DescribeClientAuthenticationSettings's own
// error switch types DirectoryDoesNotExistException only.
func TestDescribeClientAuthenticationSettings_DirectoryNotFound_TypesDirectoryDoesNotExistException(t *testing.T) {
	t.Parallel()

	h := directoryservice.NewHandler(directoryservice.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestDirectoryServiceClient(t, h)

	_, err := client.DescribeClientAuthenticationSettings(
		t.Context(),
		&directoryservicesdk.DescribeClientAuthenticationSettingsInput{
			DirectoryId: aws.String("d-0000000000"),
		},
	)
	require.Error(t, err)

	var notExist *types.DirectoryDoesNotExistException
	require.ErrorAs(t, err, &notExist)
}

// TestDescribeCertificate_NotFound_TypesCertificateDoesNotExistException
// proves DescribeCertificate surfaces CertificateDoesNotExistException for an
// unknown certificate, not the generic EntityDoesNotExistException --
// DescribeCertificate's own error switch types CertificateDoesNotExistException
// only, never EntityDoesNotExistException.
func TestDescribeCertificate_NotFound_TypesCertificateDoesNotExistException(t *testing.T) {
	t.Parallel()

	h := directoryservice.NewHandler(directoryservice.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestDirectoryServiceClient(t, h)

	created, err := client.CreateMicrosoftAD(t.Context(), &directoryservicesdk.CreateMicrosoftADInput{
		Name:     aws.String("corp.example.com"),
		Password: aws.String("Admin1234!"),
		VpcSettings: &types.DirectoryVpcSettings{
			VpcId:     aws.String("vpc-0123456789abcdef0"),
			SubnetIds: []string{"subnet-0123456789abcdef0", "subnet-0123456789abcdef1"},
		},
	})
	require.NoError(t, err)
	dirID := aws.ToString(created.DirectoryId)

	_, err = client.DescribeCertificate(t.Context(), &directoryservicesdk.DescribeCertificateInput{
		DirectoryId:   aws.String(dirID),
		CertificateId: aws.String("c-0000000000"),
	})
	require.Error(t, err)

	var notExist *types.CertificateDoesNotExistException
	require.ErrorAs(t, err, &notExist)
}

// TestRegisterEventTopic_Reregister_Succeeds proves re-registering the same
// event topic succeeds instead of erroring: RegisterEventTopic's own error
// switch types no already-exists exception at all, so the previous
// EntityAlreadyExistsException response was untypeable by the real client and
// unreachable via errors.As into any modeled exception.
func TestRegisterEventTopic_Reregister_Succeeds(t *testing.T) {
	t.Parallel()

	h := directoryservice.NewHandler(directoryservice.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestDirectoryServiceClient(t, h)

	created, err := client.CreateMicrosoftAD(t.Context(), &directoryservicesdk.CreateMicrosoftADInput{
		Name:     aws.String("corp.example.com"),
		Password: aws.String("Admin1234!"),
		VpcSettings: &types.DirectoryVpcSettings{
			VpcId:     aws.String("vpc-0123456789abcdef0"),
			SubnetIds: []string{"subnet-0123456789abcdef0", "subnet-0123456789abcdef1"},
		},
	})
	require.NoError(t, err)
	dirID := aws.ToString(created.DirectoryId)

	_, err = client.RegisterEventTopic(t.Context(), &directoryservicesdk.RegisterEventTopicInput{
		DirectoryId: aws.String(dirID),
		TopicName:   aws.String("my-topic"),
	})
	require.NoError(t, err)

	_, err = client.RegisterEventTopic(t.Context(), &directoryservicesdk.RegisterEventTopicInput{
		DirectoryId: aws.String(dirID),
		TopicName:   aws.String("my-topic"),
	})
	require.NoError(t, err)

	described, err := client.DescribeEventTopics(t.Context(), &directoryservicesdk.DescribeEventTopicsInput{
		DirectoryId: aws.String(dirID),
	})
	require.NoError(t, err)
	require.Len(t, described.EventTopics, 1, "re-registration must refresh, not duplicate, the topic entry")
}
