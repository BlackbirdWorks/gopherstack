package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_EC2_InstanceRunningWaiter verifies that InstanceRunningWaiter
// succeeds immediately after RunInstances because the instance starts in running state.
func TestIntegration_EC2_InstanceRunningWaiter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createEC2Client(t)
	ctx := t.Context()

	out, err := client.RunInstances(ctx, &ec2sdk.RunInstancesInput{
		ImageId:      aws.String("ami-12345678"),
		InstanceType: ec2types.InstanceTypeT2Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, out.Instances, 1)

	instanceID := aws.ToString(out.Instances[0].InstanceId)
	require.NotEmpty(t, instanceID)

	t.Cleanup(func() {
		_, _ = client.TerminateInstances(ctx, &ec2sdk.TerminateInstancesInput{
			InstanceIds: []string{instanceID},
		})
	})

	// Verify the instance state is running
	assert.Equal(t, ec2types.InstanceStateNameRunning, out.Instances[0].State.Name)

	waiter := ec2sdk.NewInstanceRunningWaiter(client)
	start := time.Now()
	err = waiter.Wait(ctx, &ec2sdk.DescribeInstancesInput{
		InstanceIds: []string{instanceID},
	}, 10*time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err, "InstanceRunningWaiter should succeed immediately after RunInstances")
	assert.Less(t, elapsed, 2*time.Second, "InstanceRunningWaiter should complete quickly, took %v", elapsed)
}

// TestIntegration_EC2_InstanceStoppedWaiter verifies that InstanceStoppedWaiter
// succeeds after StopInstances.
func TestIntegration_EC2_InstanceStoppedWaiter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createEC2Client(t)
	ctx := t.Context()

	out, err := client.RunInstances(ctx, &ec2sdk.RunInstancesInput{
		ImageId:      aws.String("ami-12345678"),
		InstanceType: ec2types.InstanceTypeT2Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, out.Instances, 1)

	instanceID := aws.ToString(out.Instances[0].InstanceId)

	t.Cleanup(func() {
		_, _ = client.TerminateInstances(ctx, &ec2sdk.TerminateInstancesInput{
			InstanceIds: []string{instanceID},
		})
	})

	// Stop the instance
	_, err = client.StopInstances(ctx, &ec2sdk.StopInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)

	waiter := ec2sdk.NewInstanceStoppedWaiter(client)
	start := time.Now()
	err = waiter.Wait(ctx, &ec2sdk.DescribeInstancesInput{
		InstanceIds: []string{instanceID},
	}, 10*time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err, "InstanceStoppedWaiter should succeed after StopInstances")
	assert.Less(t, elapsed, 2*time.Second, "InstanceStoppedWaiter should complete quickly, took %v", elapsed)
}

// TestIntegration_EC2_InstanceTerminatedWaiter verifies that InstanceTerminatedWaiter
// succeeds after TerminateInstances.
func TestIntegration_EC2_InstanceTerminatedWaiter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createEC2Client(t)
	ctx := t.Context()

	out, err := client.RunInstances(ctx, &ec2sdk.RunInstancesInput{
		ImageId:      aws.String("ami-12345678"),
		InstanceType: ec2types.InstanceTypeT2Micro,
		MinCount:     aws.Int32(1),
		MaxCount:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, out.Instances, 1)

	instanceID := aws.ToString(out.Instances[0].InstanceId)

	// Terminate the instance
	_, err = client.TerminateInstances(ctx, &ec2sdk.TerminateInstancesInput{
		InstanceIds: []string{instanceID},
	})
	require.NoError(t, err)

	waiter := ec2sdk.NewInstanceTerminatedWaiter(client)
	start := time.Now()
	err = waiter.Wait(ctx, &ec2sdk.DescribeInstancesInput{
		InstanceIds: []string{instanceID},
	}, 10*time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err, "InstanceTerminatedWaiter should succeed after TerminateInstances")
	assert.Less(t, elapsed, 2*time.Second, "InstanceTerminatedWaiter should complete quickly, took %v", elapsed)
}
