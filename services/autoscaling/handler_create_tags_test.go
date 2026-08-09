package autoscaling_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	assdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateAutoScalingGroup_TagsRoundTrip drives CreateAutoScalingGroup, the
// only autoscaling Create* op whose real Input struct accepts Tags
// (autoscaling@v1.70.4: api_op_CreateAutoScalingGroup.go, `Tags
// []types.Tag`), through the real SDK client and asserts DescribeTags sees
// what was supplied at creation. Real ASG has no TagResource/
// ListTagsForResource API -- DescribeTags is the read path (gopherstack-2mwl).
func TestCreateAutoScalingGroup_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.CreateAutoScalingGroup(t.Context(), &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String("tagged-asg"),
		MinSize:              aws.Int32(0),
		MaxSize:              aws.Int32(1),
		AvailabilityZones:    []string{"us-east-1a"},
		Tags: []types.Tag{
			{
				Key:               aws.String("env"),
				Value:             aws.String("prod"),
				ResourceId:        aws.String("tagged-asg"),
				ResourceType:      aws.String("auto-scaling-group"),
				PropagateAtLaunch: aws.Bool(true),
			},
		},
	})
	require.NoError(t, err)

	got, err := client.DescribeTags(t.Context(), &assdk.DescribeTagsInput{})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
	assert.Equal(t, "prod", aws.ToString(got.Tags[0].Value))
	assert.Equal(t, "tagged-asg", aws.ToString(got.Tags[0].ResourceId))
}
