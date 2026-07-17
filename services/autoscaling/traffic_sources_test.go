package autoscaling_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

func TestInMemoryBackend_AttachTrafficSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		tss     []autoscaling.TrafficSource
		wantErr bool
		wantLen int
	}{
		{
			name: "attach_traffic_sources",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "ts-g",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			group: "ts-g",
			tss: []autoscaling.TrafficSource{
				{Identifier: "arn:aws:vpc-lattice:us-east-1:123:tg/abc", Type: "vpc-lattice"},
			},
			wantLen: 1,
		},
		{
			name:    "group_not_found",
			group:   "no-such",
			tss:     []autoscaling.TrafficSource{{Identifier: "x", Type: "y"}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.AttachTrafficSources(tt.group, tt.tss)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			groups, err := b.DescribeAutoScalingGroups([]string{tt.group})
			require.NoError(t, err)
			assert.Len(t, groups[0].TrafficSources, tt.wantLen)
		})
	}
}

func TestInMemoryBackend_DetachTrafficSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		sources []autoscaling.TrafficSource
		wantErr bool
	}{
		{
			name:  "detach_existing_traffic_source",
			group: "dts-asg",
			sources: []autoscaling.TrafficSource{
				{Identifier: "arn:ts-1", Type: "vpc-lattice"},
			},
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dts-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.AttachTrafficSources("dts-asg", []autoscaling.TrafficSource{
					{Identifier: "arn:ts-1", Type: "vpc-lattice"},
				})
			},
		},
		{
			name:    "detach_traffic_sources_group_not_found",
			group:   "no-such",
			sources: []autoscaling.TrafficSource{{Identifier: "x", Type: "y"}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.DetachTrafficSources(tt.group, tt.sources)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_DescribeTrafficSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		wantErr bool
		wantLen int
	}{
		{
			name:  "describe_traffic_sources_with_data",
			group: "dts2-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dts2-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.AttachTrafficSources("dts2-asg", []autoscaling.TrafficSource{
					{Identifier: "arn:ts-x", Type: "vpc-lattice"},
				})
			},
			wantLen: 1,
		},
		{
			name:    "describe_traffic_sources_group_not_found",
			group:   "no-such",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			tss, err := b.DescribeTrafficSources(tt.group)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, tss, tt.wantLen)
		})
	}
}
