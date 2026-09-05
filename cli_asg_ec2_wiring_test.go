package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestWireAutoScalingEC2_ScaleOutCreatesRealEC2Instance exercises the exact
// composition-root wiring cli.go's setup uses (wireAutoScalingEC2) against
// REAL Auto Scaling and EC2 backends — not test doubles — to prove an
// Auto Scaling group's launched instances actually appear in EC2
// DescribeInstances, and that scaling the group back in actually terminates
// them there too, instead of Auto Scaling fabricating instance IDs with no
// EC2-side record.
func TestWireAutoScalingEC2_ScaleOutCreatesRealEC2Instance(t *testing.T) {
	t.Parallel()

	asgBk := autoscalingbackend.NewInMemoryBackend()
	t.Cleanup(asgBk.Close)
	asgH := autoscalingbackend.NewHandler(asgBk)

	ec2Bk := ec2backend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	ec2H := ec2backend.NewHandler(ec2Bk)

	// This is the exact call cli.go's setupServices makes.
	wireAutoScalingEC2(asgH, ec2H)

	_, err := asgBk.CreateLaunchConfiguration(autoscalingbackend.CreateLaunchConfigurationInput{
		LaunchConfigurationName: "wiring-test-lc",
		ImageID:                 "ami-0123456789abcdef0",
		InstanceType:            "t3.micro",
	})
	require.NoError(t, err)

	group, err := asgBk.CreateAutoScalingGroup(autoscalingbackend.CreateAutoScalingGroupInput{
		AutoScalingGroupName:    "wiring-test-asg",
		LaunchConfigurationName: "wiring-test-lc",
		MinSize:                 0,
		MaxSize:                 5,
		DesiredCapacity:         2,
		AvailabilityZones:       []string{"us-east-1a"},
	})
	require.NoError(t, err)
	require.Len(t, group.Instances, 2)

	// --- Scale-out: every ASG-reported instance ID must be a real EC2 record. ---
	asgInstanceIDs := make([]string, len(group.Instances))
	for i, inst := range group.Instances {
		asgInstanceIDs[i] = inst.InstanceID
	}

	ec2Instances := ec2Bk.DescribeInstances(asgInstanceIDs, "")
	require.Len(t, ec2Instances, 2,
		"ASG-launched instances must be visible in EC2 DescribeInstances via cli.go's wireAutoScalingEC2")

	for _, inst := range ec2Instances {
		assert.Equal(t, "ami-0123456789abcdef0", inst.ImageID)
		assert.Equal(t, "t3.micro", inst.InstanceType)
	}

	// --- Scale-in: reducing DesiredCapacity must terminate the removed instance in EC2 too. ---
	require.NoError(t, asgBk.SetDesiredCapacity("wiring-test-asg", 1))

	groups, err := asgBk.DescribeAutoScalingGroups([]string{"wiring-test-asg"}, nil)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.Len(t, groups[0].Instances, 1)

	remainingID := groups[0].Instances[0].InstanceID

	var removedID string

	for _, id := range asgInstanceIDs {
		if id != remainingID {
			removedID = id
		}
	}

	require.NotEmpty(t, removedID)

	removedInEC2 := ec2Bk.DescribeInstances([]string{removedID}, "")
	require.Len(t, removedInEC2, 1)
	assert.Contains(t, []ec2backend.InstanceState{ec2backend.StateShuttingDown, ec2backend.StateTerminated},
		removedInEC2[0].State,
		"scaled-in instance must have been terminated in the real EC2 backend via cli.go's wireAutoScalingEC2")
}
