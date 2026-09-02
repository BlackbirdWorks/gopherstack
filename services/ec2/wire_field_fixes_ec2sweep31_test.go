package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestAssociateIamInstanceProfile_StateEnum_RealClient covers
// AssociateIamInstanceProfile/DescribeIamInstanceProfileAssociations. Pre-fix,
// the backend set State to the shared "available" constant (stateAvailable),
// which is not a member of IamInstanceProfileAssociationState at all
// (ec2@v1.319.1 types/enums.go:3556 only defines associating/associated/
// disassociating/disassociated). A client parsing the typed
// types.IamInstanceProfileAssociationState enum got a value AWS never sends.
func TestAssociateIamInstanceProfile_StateEnum_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	h.AccountID = "000000000000"
	client := newTestEC2Client(t, h)

	instOut, err := client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId:      aws.String("ami-sweep31c"),
		InstanceType: types.InstanceTypeT3Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, instOut.Instances, 1)
	instanceID := aws.ToString(instOut.Instances[0].InstanceId)

	assocOut, err := client.AssociateIamInstanceProfile(t.Context(), &ec2sdk.AssociateIamInstanceProfileInput{
		InstanceId: aws.String(instanceID),
		IamInstanceProfile: &types.IamInstanceProfileSpecification{
			Arn: aws.String("arn:aws:iam::000000000000:instance-profile/sweep31-profile"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, assocOut.IamInstanceProfileAssociation)
	assert.Equal(
		t, types.IamInstanceProfileAssociationStateAssociated, assocOut.IamInstanceProfileAssociation.State,
		"State was the invalid value \"available\" pre-fix, not a real IamInstanceProfileAssociationState member",
	)

	descOut, err := client.DescribeIamInstanceProfileAssociations(
		t.Context(), &ec2sdk.DescribeIamInstanceProfileAssociationsInput{},
	)
	require.NoError(t, err)
	require.Len(t, descOut.IamInstanceProfileAssociations, 1, "empty collection is the bug")
	assert.Equal(t, types.IamInstanceProfileAssociationStateAssociated, descOut.IamInstanceProfileAssociations[0].State)
}

// TestDescribeFastLaunchImages_ConfigEcho_RealClient covers
// handleDescribeFastLaunchImages. Pre-fix, EnableFastLaunch's backend method
// stored only a bool (b.fastLaunchImages[imageID] = true), discarding every
// configuration field the request carried. DescribeFastLaunchImages then
// rendered a fastLaunchImageItem with just imageId/state -- resourceType,
// ownerId, maxParallelLaunches, launchTemplate, and snapshotConfiguration
// were always empty on every entry, even though the enabling request supplied
// all of them (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeDocumentDescribeFastLaunchImagesSuccessItem expects
// all of these members).
func TestDescribeFastLaunchImages_ConfigEcho_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	h.AccountID = "000000000000"
	client := newTestEC2Client(t, h)

	_, err := client.EnableFastLaunch(t.Context(), &ec2sdk.EnableFastLaunchInput{
		ImageId:             aws.String("ami-sweep31"),
		ResourceType:        aws.String("snapshot"),
		MaxParallelLaunches: aws.Int32(12),
		LaunchTemplate: &types.FastLaunchLaunchTemplateSpecificationRequest{
			LaunchTemplateId: aws.String("lt-sweep31"),
			Version:          aws.String("3"),
		},
		SnapshotConfiguration: &types.FastLaunchSnapshotConfigurationRequest{
			TargetResourceCount: aws.Int32(4),
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeFastLaunchImages(t.Context(), &ec2sdk.DescribeFastLaunchImagesInput{
		ImageIds: []string{"ami-sweep31"},
	})
	require.NoError(t, err)
	require.Len(t, out.FastLaunchImages, 1, "empty collection is the bug")

	item := out.FastLaunchImages[0]
	assert.Equal(t, "ami-sweep31", aws.ToString(item.ImageId))
	assert.Equal(t, "enabled", string(item.State))
	assert.Equal(
		t, "snapshot", string(item.ResourceType),
		"ResourceType empty - EnableFastLaunch's config was discarded pre-fix",
	)
	assert.Equal(
		t, int32(12), aws.ToInt32(item.MaxParallelLaunches),
		"MaxParallelLaunches empty - EnableFastLaunch's config was discarded pre-fix",
	)
	assert.Equal(t, "000000000000", aws.ToString(item.OwnerId))
	require.NotNil(t, item.LaunchTemplate, "LaunchTemplate nil - discarded pre-fix")
	assert.Equal(t, "lt-sweep31", aws.ToString(item.LaunchTemplate.LaunchTemplateId))
	assert.Equal(t, "3", aws.ToString(item.LaunchTemplate.Version))
	require.NotNil(t, item.SnapshotConfiguration, "SnapshotConfiguration nil - discarded pre-fix")
	assert.Equal(t, int32(4), aws.ToInt32(item.SnapshotConfiguration.TargetResourceCount))
}

// TestDisableFastLaunch_ConfigEcho_RealClient covers handleDisableFastLaunch.
// Pre-fix, the backend discarded the enabling configuration entirely, so
// DisableFastLaunchOutput could never echo back the resourceType/
// maxParallelLaunches/launchTemplate/snapshotConfiguration that were in
// effect (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentDisableFastLaunchOutput).
func TestDisableFastLaunch_ConfigEcho_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	h.AccountID = "000000000000"
	client := newTestEC2Client(t, h)

	_, err := client.EnableFastLaunch(t.Context(), &ec2sdk.EnableFastLaunchInput{
		ImageId:             aws.String("ami-sweep31b"),
		ResourceType:        aws.String("snapshot"),
		MaxParallelLaunches: aws.Int32(9),
	})
	require.NoError(t, err)

	out, err := client.DisableFastLaunch(t.Context(), &ec2sdk.DisableFastLaunchInput{
		ImageId: aws.String("ami-sweep31b"),
	})
	require.NoError(t, err)
	assert.Equal(
		t, "snapshot", string(out.ResourceType),
		"ResourceType empty - prior EnableFastLaunch config was discarded pre-fix",
	)
	assert.Equal(t, int32(9), aws.ToInt32(out.MaxParallelLaunches))
}
