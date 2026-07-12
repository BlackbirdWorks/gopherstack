package autoscaling_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// fakeEC2Launcher is a minimal autoscaling.EC2Launcher fake for backend
// tests. It hands out distinguishable "i-ec2fake..." IDs so tests can tell a
// launcher-issued instance apart from a fabricated one, and records every
// call so tests can assert on what was launched/terminated.
type fakeEC2Launcher struct {
	launchErr  error
	launches   []launchCall
	terminated [][]string
	nextID     int
}

type launchCall struct {
	spec  autoscaling.InstanceLaunchSpec
	count int
}

func (f *fakeEC2Launcher) LaunchInstances(
	_ context.Context, spec autoscaling.InstanceLaunchSpec, count int,
) ([]string, error) {
	f.launches = append(f.launches, launchCall{spec: spec, count: count})

	if f.launchErr != nil {
		return nil, f.launchErr
	}

	ids := make([]string, count)
	for i := range ids {
		f.nextID++
		ids[i] = fmt.Sprintf("i-ec2fake%05d", f.nextID)
	}

	return ids, nil
}

func (f *fakeEC2Launcher) TerminateInstances(_ context.Context, ids []string) error {
	cp := make([]string, len(ids))
	copy(cp, ids)
	f.terminated = append(f.terminated, cp)

	return nil
}

// newLaunchConfigGroup creates a group backed by a LaunchConfiguration (the
// only launch source EC2Launcher currently resolves — see the EC2Launcher doc
// comment) with the given desired capacity.
func newLaunchConfigGroup(
	t *testing.T, b *autoscaling.InMemoryBackend, name string, desired int32,
) *autoscaling.AutoScalingGroup {
	t.Helper()

	_, err := b.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
		LaunchConfigurationName: name + "-lc",
		ImageID:                 "ami-12345678",
		InstanceType:            "t3.micro",
		KeyName:                 "my-key",
		SecurityGroups:          []string{"sg-abc"},
	})
	require.NoError(t, err)

	g, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName:    name,
		LaunchConfigurationName: name + "-lc",
		MinSize:                 0,
		MaxSize:                 10,
		DesiredCapacity:         desired,
		AvailabilityZones:       []string{"us-east-1a"},
		VPCZoneIdentifier:       "subnet-abc123,subnet-def456",
		Tags: []autoscaling.Tag{
			{Key: "propagated", Value: "yes", PropagateAtLaunch: true},
			{Key: "not-propagated", Value: "no", PropagateAtLaunch: false},
		},
	})
	require.NoError(t, err)

	return g
}

func TestInMemoryBackend_EC2Launcher_NoLauncher_FabricatesAsBefore(t *testing.T) {
	t.Parallel()

	b := autoscaling.NewInMemoryBackend()
	t.Cleanup(b.Close)

	g := newLaunchConfigGroup(t, b, "asg-no-launcher", 2)
	require.Len(t, g.Instances, 2)

	for _, inst := range g.Instances {
		assert.NotContains(t, inst.InstanceID, "ec2fake")
	}
}

func TestInMemoryBackend_EC2Launcher_ScaleOut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *autoscaling.InMemoryBackend, launcher *fakeEC2Launcher)
		name string
	}{
		{
			name: "create_group_launches_real_instances",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend, launcher *fakeEC2Launcher) {
				t.Helper()

				g := newLaunchConfigGroup(t, b, "asg-create", 2)
				require.Len(t, g.Instances, 2)

				for _, inst := range g.Instances {
					assert.Contains(t, inst.InstanceID, "ec2fake")
				}

				require.Len(t, launcher.launches, 1)
				spec := launcher.launches[0].spec
				assert.Equal(t, "ami-12345678", spec.ImageID)
				assert.Equal(t, "t3.micro", spec.InstanceType)
				assert.Equal(t, "my-key", spec.KeyName)
				assert.Equal(t, []string{"sg-abc"}, spec.SecurityGroups)
				assert.Equal(t, "subnet-abc123", spec.SubnetID)
				assert.Equal(t, "us-east-1a", spec.AvailabilityZone)
				assert.Equal(t, "yes", spec.Tags["propagated"])
				assert.NotContains(t, spec.Tags, "not-propagated")
				assert.Equal(t, "asg-create", spec.Tags["aws:autoscaling:groupName"])
				assert.Equal(t, 2, launcher.launches[0].count)
			},
		},
		{
			name: "set_desired_capacity_increase_launches_delta",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend, launcher *fakeEC2Launcher) {
				t.Helper()

				g := newLaunchConfigGroup(t, b, "asg-scale-out", 1)
				require.Len(t, g.Instances, 1)
				launcher.launches = nil // reset after initial create

				require.NoError(t, b.SetDesiredCapacity("asg-scale-out", 3))

				groups, err := b.DescribeAutoScalingGroups([]string{"asg-scale-out"})
				require.NoError(t, err)
				require.Len(t, groups, 1)
				assert.Len(t, groups[0].Instances, 3)

				require.Len(t, launcher.launches, 1)
				assert.Equal(t, 2, launcher.launches[0].count)
			},
		},
		{
			name: "launcher_error_falls_back_to_fabrication",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend, launcher *fakeEC2Launcher) {
				t.Helper()

				launcher.launchErr = assert.AnError

				g := newLaunchConfigGroup(t, b, "asg-launch-err", 2)
				require.Len(t, g.Instances, 2)

				for _, inst := range g.Instances {
					assert.NotContains(t, inst.InstanceID, "ec2fake")
				}
			},
		},
		{
			name: "no_launch_configuration_falls_back_to_fabrication",
			run: func(t *testing.T, b *autoscaling.InMemoryBackend, _ *fakeEC2Launcher) {
				t.Helper()

				// A group with a LaunchTemplate (not a LaunchConfiguration) has no
				// resolvable EC2 launch spec (see the EC2Launcher doc comment), so
				// it must still fabricate instances even with a launcher wired.
				g, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "asg-launch-template",
					LaunchTemplate:       &autoscaling.LaunchTemplateSpecification{LaunchTemplateID: "lt-123"},
					MinSize:              0,
					MaxSize:              5,
					DesiredCapacity:      2,
					AvailabilityZones:    []string{"us-east-1a"},
				})
				require.NoError(t, err)
				require.Len(t, g.Instances, 2)

				for _, inst := range g.Instances {
					assert.NotContains(t, inst.InstanceID, "ec2fake")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			t.Cleanup(b.Close)

			launcher := &fakeEC2Launcher{}
			b.SetEC2Launcher(launcher)

			tt.run(t, b, launcher)
		})
	}
}

func TestInMemoryBackend_EC2Launcher_ScaleIn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *autoscaling.InMemoryBackend, launcher *fakeEC2Launcher, g *autoscaling.AutoScalingGroup)
		name string
	}{
		{
			name: "set_desired_capacity_decrease_terminates_removed",
			run: func(
				t *testing.T, b *autoscaling.InMemoryBackend, launcher *fakeEC2Launcher, g *autoscaling.AutoScalingGroup,
			) {
				t.Helper()

				require.Len(t, g.Instances, 3)
				launcher.launches = nil

				require.NoError(t, b.SetDesiredCapacity(g.AutoScalingGroupName, 1))

				groups, err := b.DescribeAutoScalingGroups([]string{g.AutoScalingGroupName})
				require.NoError(t, err)
				require.Len(t, groups, 1)
				assert.Len(t, groups[0].Instances, 1)

				require.Len(t, launcher.terminated, 1)
				assert.Len(t, launcher.terminated[0], 2)
			},
		},
		{
			name: "terminate_instance_with_decrement_terminates_in_ec2_no_replacement",
			run: func(
				t *testing.T, b *autoscaling.InMemoryBackend, launcher *fakeEC2Launcher, g *autoscaling.AutoScalingGroup,
			) {
				t.Helper()

				target := g.Instances[0].InstanceID
				_, err := b.TerminateInstanceInAutoScalingGroup(target, true)
				require.NoError(t, err)

				require.Len(t, launcher.terminated, 1)
				assert.Equal(t, []string{target}, launcher.terminated[0])

				groups, err := b.DescribeAutoScalingGroups([]string{g.AutoScalingGroupName})
				require.NoError(t, err)
				assert.Len(t, groups[0].Instances, 2)
			},
		},
		{
			name: "terminate_instance_without_decrement_replaces_via_launcher",
			run: func(
				t *testing.T, b *autoscaling.InMemoryBackend, launcher *fakeEC2Launcher, g *autoscaling.AutoScalingGroup,
			) {
				t.Helper()

				target := g.Instances[0].InstanceID
				launcher.launches = nil

				_, err := b.TerminateInstanceInAutoScalingGroup(target, false)
				require.NoError(t, err)

				require.Len(t, launcher.terminated, 1)
				assert.Equal(t, []string{target}, launcher.terminated[0])

				require.Len(t, launcher.launches, 1)
				assert.Equal(t, 1, launcher.launches[0].count)

				groups, err := b.DescribeAutoScalingGroups([]string{g.AutoScalingGroupName})
				require.NoError(t, err)
				assert.Len(t, groups[0].Instances, 3)

				for _, inst := range groups[0].Instances {
					assert.NotEqual(t, target, inst.InstanceID)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			t.Cleanup(b.Close)

			launcher := &fakeEC2Launcher{}
			b.SetEC2Launcher(launcher)

			g := newLaunchConfigGroup(t, b, "asg-scale-in-"+tt.name, 3)

			tt.run(t, b, launcher, g)
		})
	}
}
