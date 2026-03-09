package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_EC2_CreateDeleteTags verifies that CreateTags and DeleteTags
// write to and remove from the EC2 tag store, and that DescribeTags reflects the changes.
func TestIntegration_EC2_CreateDeleteTags(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEC2Client(t)
	ctx := t.Context()

	// Create a VPC to tag.
	vpcOut, err := client.CreateVpc(ctx, &ec2sdk.CreateVpcInput{
		CidrBlock: aws.String("10.99.0.0/16"),
	})
	require.NoError(t, err)
	vpcID := aws.ToString(vpcOut.Vpc.VpcId)
	require.NotEmpty(t, vpcID)

	t.Cleanup(func() {
		_, _ = client.DeleteVpc(ctx, &ec2sdk.DeleteVpcInput{VpcId: aws.String(vpcID)})
	})

	// CreateTags: add two tags.
	_, err = client.CreateTags(ctx, &ec2sdk.CreateTagsInput{
		Resources: []string{vpcID},
		Tags: []ec2types.Tag{
			{Key: aws.String("Name"), Value: aws.String("integration-vpc")},
			{Key: aws.String("Env"), Value: aws.String("test")},
		},
	})
	require.NoError(t, err)

	// DescribeTags: verify both tags are visible.
	descOut, err := client.DescribeTags(ctx, &ec2sdk.DescribeTagsInput{})
	require.NoError(t, err)

	foundName := false
	foundEnv := false

	for _, tag := range descOut.Tags {
		if aws.ToString(tag.ResourceId) == vpcID {
			switch aws.ToString(tag.Key) {
			case "Name":
				assert.Equal(t, "integration-vpc", aws.ToString(tag.Value))
				foundName = true
			case "Env":
				assert.Equal(t, "test", aws.ToString(tag.Value))
				foundEnv = true
			}
		}
	}

	assert.True(t, foundName, "Name tag not found after CreateTags")
	assert.True(t, foundEnv, "Env tag not found after CreateTags")

	// DeleteTags: remove the Name tag.
	_, err = client.DeleteTags(ctx, &ec2sdk.DeleteTagsInput{
		Resources: []string{vpcID},
		Tags: []ec2types.Tag{
			{Key: aws.String("Name")},
		},
	})
	require.NoError(t, err)

	// DescribeTags: Name tag should be gone, Env tag should remain.
	descOut2, err := client.DescribeTags(ctx, &ec2sdk.DescribeTagsInput{})
	require.NoError(t, err)

	nameGone := true
	foundEnvAfter := false

	for _, tag := range descOut2.Tags {
		if aws.ToString(tag.ResourceId) == vpcID {
			if aws.ToString(tag.Key) == "Name" {
				nameGone = false
			}

			if aws.ToString(tag.Key) == "Env" {
				foundEnvAfter = true
			}
		}
	}

	assert.True(t, nameGone, "Name tag should be removed after DeleteTags")
	assert.True(t, foundEnvAfter, "Env tag should still be present after DeleteTags")
}

// TestIntegration_EC2_CreateTags_MultipleResources verifies that CreateTags applies
// tags to multiple resources at once.
func TestIntegration_EC2_CreateTags_MultipleResources(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEC2Client(t)
	ctx := t.Context()

	// Create two VPCs.
	vpc1Out, err := client.CreateVpc(ctx, &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.100.0.0/16")})
	require.NoError(t, err)
	vpc1ID := aws.ToString(vpc1Out.Vpc.VpcId)

	vpc2Out, err := client.CreateVpc(ctx, &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.101.0.0/16")})
	require.NoError(t, err)
	vpc2ID := aws.ToString(vpc2Out.Vpc.VpcId)

	t.Cleanup(func() {
		_, _ = client.DeleteVpc(ctx, &ec2sdk.DeleteVpcInput{VpcId: aws.String(vpc1ID)})
		_, _ = client.DeleteVpc(ctx, &ec2sdk.DeleteVpcInput{VpcId: aws.String(vpc2ID)})
	})

	// Tag both VPCs at once.
	_, err = client.CreateTags(ctx, &ec2sdk.CreateTagsInput{
		Resources: []string{vpc1ID, vpc2ID},
		Tags:      []ec2types.Tag{{Key: aws.String("Project"), Value: aws.String("multi-tag-test")}},
	})
	require.NoError(t, err)

	// Both VPCs should have the Project tag.
	descOut, err := client.DescribeTags(ctx, &ec2sdk.DescribeTagsInput{})
	require.NoError(t, err)

	found1, found2 := false, false

	for _, tag := range descOut.Tags {
		if aws.ToString(tag.Key) == "Project" && aws.ToString(tag.Value) == "multi-tag-test" {
			switch aws.ToString(tag.ResourceId) {
			case vpc1ID:
				found1 = true
			case vpc2ID:
				found2 = true
			}
		}
	}

	assert.True(t, found1, "VPC 1 should have Project tag")
	assert.True(t, found2, "VPC 2 should have Project tag")
}
