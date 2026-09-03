package autoscaling_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	assdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/stretchr/testify/require"
)

// TestStartInstanceRefresh_AlreadyInProgress_InstanceRefreshInProgress proves
// StartInstanceRefresh rejects a second concurrent refresh with the real
// typed InstanceRefreshInProgressFault. autoscaling@v1.70.4 deserializers.go's
// awsAwsquery_deserializeOpErrorStartInstanceRefresh switch models
// InstanceRefreshInProgress; the backend previously accepted a second
// StartInstanceRefresh call unconditionally, silently starting a concurrent
// refresh AWS itself rejects.
func TestStartInstanceRefresh_AlreadyInProgress_InstanceRefreshInProgress(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.CreateAutoScalingGroup(ctx, &assdk.CreateAutoScalingGroupInput{
		AutoScalingGroupName: aws.String("refresh-group"),
		MinSize:              aws.Int32(0),
		MaxSize:              aws.Int32(1),
		AvailabilityZones:    []string{"us-east-1a"},
	})
	require.NoError(t, err)

	_, err = client.StartInstanceRefresh(ctx, &assdk.StartInstanceRefreshInput{
		AutoScalingGroupName: aws.String("refresh-group"),
	})
	require.NoError(t, err)

	_, err = client.StartInstanceRefresh(ctx, &assdk.StartInstanceRefreshInput{
		AutoScalingGroupName: aws.String("refresh-group"),
	})
	require.Error(t, err)

	var irip *types.InstanceRefreshInProgressFault
	require.ErrorAsf(
		t, err, &irip,
		"expected a real InstanceRefreshInProgressFault from the SDK deserializer, got %v", err,
	)
}
