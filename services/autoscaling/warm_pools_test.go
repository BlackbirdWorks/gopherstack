package autoscaling_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

func TestInMemoryBackend_WarmPoolInstanceReusePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *autoscaling.InMemoryBackend)
		run   func(t *testing.T, b *autoscaling.InMemoryBackend)
		name  string
	}{
		{
			name: "reuse_on_scale_in_stored",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "wp-reuse-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				err := b.PutWarmPool(autoscaling.WarmPoolInput{
					AutoScalingGroupName:     "wp-reuse-asg",
					MinSize:                  1,
					MaxGroupPreparedCapacity: 3,
					InstanceReusePolicy:      autoscaling.InstanceReusePolicy{ReuseOnScaleIn: true},
				})
				require.NoError(t, err)

				wp, err := b.DescribeWarmPool("wp-reuse-asg")
				require.NoError(t, err)
				assert.True(t, wp.InstanceReusePolicy.ReuseOnScaleIn)
				assert.Equal(t, int32(3), wp.MaxGroupPreparedCapacity)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			tt.run(t, b)
		})
	}
}

func TestInMemoryBackend_DeleteWarmPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		wantErr bool
	}{
		{
			name:  "delete_warm_pool_success",
			group: "dwp-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dwp-asg",
					MinSize:              0,
					MaxSize:              10,
				})
				_ = b.PutWarmPool(autoscaling.WarmPoolInput{
					AutoScalingGroupName: "dwp-asg",
				})
			},
		},
		{
			name:    "delete_warm_pool_group_not_found",
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

			err := b.DeleteWarmPool(tt.group)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_WarmPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		input   autoscaling.WarmPoolInput
		wantErr bool
	}{
		{
			name: "put_warm_pool_default_state",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "wp-def-asg",
					MinSize:              0,
					MaxSize:              10,
				})
			},
			input: autoscaling.WarmPoolInput{
				AutoScalingGroupName: "wp-def-asg",
			},
		},
		{
			name: "put_warm_pool_running_state",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "wp-run-asg",
					MinSize:              0,
					MaxSize:              10,
				})
			},
			input: autoscaling.WarmPoolInput{
				AutoScalingGroupName: "wp-run-asg",
				PoolState:            "Running",
			},
		},
		{
			name: "put_warm_pool_invalid_state",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "wp-bad-asg",
					MinSize:              0,
					MaxSize:              10,
				})
			},
			input: autoscaling.WarmPoolInput{
				AutoScalingGroupName: "wp-bad-asg",
				PoolState:            "InvalidState",
			},
			wantErr: true,
		},
		{
			name:    "put_warm_pool_group_not_found",
			wantErr: true,
			input: autoscaling.WarmPoolInput{
				AutoScalingGroupName: "no-such",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.PutWarmPool(tt.input)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			wp, err := b.DescribeWarmPool(tt.input.AutoScalingGroupName)
			require.NoError(t, err)
			assert.NotNil(t, wp)
		})
	}
}

func TestInMemoryBackend_DescribeWarmPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		wantErr bool
	}{
		{
			name:    "describe_warm_pool_group_not_found",
			group:   "no-such",
			wantErr: true,
		},
		{
			name:  "describe_warm_pool_not_configured",
			group: "no-wp-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "no-wp-asg",
					MinSize:              0,
					MaxSize:              5,
				})
			},
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

			_, err := b.DescribeWarmPool(tt.group)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
