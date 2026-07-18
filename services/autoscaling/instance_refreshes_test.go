package autoscaling_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

func TestInMemoryBackend_CancelInstanceRefresh(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		wantID  string
		wantErr bool
	}{
		{
			name: "cancel_active_refresh",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "refresh-g",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.AddInstanceRefresh(autoscaling.InstanceRefresh{
					InstanceRefreshID:    "irs-abc",
					AutoScalingGroupName: "refresh-g",
					Status:               "InProgress",
				})
			},
			group:  "refresh-g",
			wantID: "irs-abc",
		},
		{
			name: "no_active_refresh",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "no-refresh-g",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			group:   "no-refresh-g",
			wantErr: true,
		},
		{
			name:    "group_not_found",
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

			id, err := b.CancelInstanceRefresh(tt.group)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantID, id)
		})
	}
}

func TestInMemoryBackend_InstanceRefreshFullPreferences(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *autoscaling.InMemoryBackend)
		run   func(t *testing.T, b *autoscaling.InMemoryBackend)
		name  string
	}{
		{
			name: "full_preferences_stored",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "ir-full-asg",
					MinSize:              2,
					MaxSize:              5,
					DesiredCapacity:      2,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				refresh, err := b.StartInstanceRefreshWithInput(autoscaling.StartInstanceRefreshInput{
					AutoScalingGroupName: "ir-full-asg",
					Strategy:             "Rolling",
					Preferences: autoscaling.InstanceRefreshPreferences{
						MinHealthyPercentage: 80,
						MaxHealthyPercentage: 110,
						InstanceWarmup:       300,
						SkipMatching:         true,
						AutoRollback:         true,
					},
				})
				require.NoError(t, err)
				assert.Equal(t, int32(80), refresh.Preferences.MinHealthyPercentage)
				assert.Equal(t, int32(110), refresh.Preferences.MaxHealthyPercentage)
				assert.Equal(t, int32(300), refresh.Preferences.InstanceWarmup)
				assert.True(t, refresh.Preferences.SkipMatching)
				assert.True(t, refresh.Preferences.AutoRollback)
			},
		},
		{
			name: "default_min_healthy_percentage",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "ir-default-asg",
					MinSize:              1,
					MaxSize:              3,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				refresh, err := b.StartInstanceRefresh("ir-default-asg")
				require.NoError(t, err)
				assert.Equal(t, int32(90), refresh.Preferences.MinHealthyPercentage)
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

func TestInMemoryBackend_DescribeInstanceRefreshes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *autoscaling.InMemoryBackend)
		name      string
		group     string
		wantErr   bool
		wantCount int
	}{
		{
			name:  "describe_with_active_refresh",
			group: "dir-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dir-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_, _ = b.StartInstanceRefresh("dir-asg")
			},
			wantCount: 1,
		},
		{
			name:    "describe_group_not_found",
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

			refreshes, err := b.DescribeInstanceRefreshes(tt.group, nil)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, refreshes, tt.wantCount)
		})
	}
}

func TestInMemoryBackend_RollbackInstanceRefresh(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		wantErr bool
	}{
		{
			name:  "rollback_active_refresh",
			group: "rib-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "rib-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.AddInstanceRefresh(autoscaling.InstanceRefresh{
					InstanceRefreshID:    "ir-rollback-123",
					AutoScalingGroupName: "rib-asg",
					Status:               "InProgress",
				})
			},
		},
		{
			name:    "rollback_group_not_found",
			group:   "no-such",
			wantErr: true,
		},
		{
			name:  "rollback_no_active_refresh",
			group: "rib-noreresh-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "rib-noreresh-asg",
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

			_, err := b.RollbackInstanceRefresh(tt.group)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
