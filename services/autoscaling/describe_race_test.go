package autoscaling_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

// TestInMemoryBackend_DescribeAutoScalingGroups_RacesWithMutation guards cloneGroupMutableSlices.
// Each case pairs a field with the in-place mutator that used to share its backing array with a live group.
func TestInMemoryBackend_DescribeAutoScalingGroups_RacesWithMutation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(b *autoscaling.InMemoryBackend, instanceIDs []string, i int)
		read   func(g autoscaling.AutoScalingGroup)
		name   string
	}{
		{
			name: "instances",
			mutate: func(b *autoscaling.InMemoryBackend, instanceIDs []string, i int) {
				_ = b.SetInstanceProtection("race-group", instanceIDs, i%2 == 0)
			},
			read: func(g autoscaling.AutoScalingGroup) {
				for _, inst := range g.Instances {
					_ = inst.ProtectedFromScaleIn
				}
			},
		},
		{
			name: "tags",
			mutate: func(b *autoscaling.InMemoryBackend, _ []string, _ int) {
				_ = b.CreateOrUpdateTags([]autoscaling.ResourceTag{
					{ResourceID: "race-group", ResourceType: "auto-scaling-group", Key: "env", Value: "prod"},
				})
			},
			read: func(g autoscaling.AutoScalingGroup) {
				for _, tag := range g.Tags {
					_ = tag.Value
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()

			_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
				AutoScalingGroupName: "race-group",
				MinSize:              0,
				MaxSize:              10,
				DesiredCapacity:      3,
				Tags:                 []autoscaling.Tag{{Key: "env", Value: "dev"}},
			})
			require.NoError(t, err)

			groups, err := b.DescribeAutoScalingGroups(nil, nil)
			require.NoError(t, err)
			require.Len(t, groups, 1)
			require.NotEmpty(t, groups[0].Instances)

			instanceIDs := make([]string, len(groups[0].Instances))
			for i, inst := range groups[0].Instances {
				instanceIDs[i] = inst.InstanceID
			}

			const iterations = 300

			var wg sync.WaitGroup

			wg.Add(2)

			go func() {
				defer wg.Done()

				for range iterations {
					gs, describeErr := b.DescribeAutoScalingGroups(nil, nil)
					if describeErr != nil {
						continue
					}

					for _, g := range gs {
						tt.read(g)
					}
				}
			}()

			go func() {
				defer wg.Done()

				for i := range iterations {
					tt.mutate(b, instanceIDs, i)
				}
			}()

			wg.Wait()
		})
	}
}
