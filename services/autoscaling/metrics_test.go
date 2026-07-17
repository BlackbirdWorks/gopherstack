package autoscaling_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

func TestInMemoryBackend_EnableDisableMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		metrics []string
		wantErr bool
		disable bool
	}{
		{
			name:    "enable_valid_metrics",
			group:   "met-asg",
			metrics: []string{"GroupMinSize", "GroupMaxSize"},
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "met-asg",
					MinSize:              0,
					MaxSize:              5,
				})
			},
		},
		{
			name:    "enable_unknown_metric_error",
			group:   "met2-asg",
			metrics: []string{"FakeMetric"},
			wantErr: true,
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "met2-asg",
					MinSize:              0,
					MaxSize:              5,
				})
			},
		},
		{
			name:    "enable_group_not_found",
			group:   "no-such",
			metrics: []string{"GroupMinSize"},
			wantErr: true,
		},
		{
			name:    "disable_metrics",
			group:   "met3-asg",
			metrics: []string{"GroupMinSize"},
			disable: true,
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "met3-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.EnableMetricsCollection("met3-asg", []string{"GroupMinSize", "GroupMaxSize"}, "1Minute")
			},
		},
		{
			name:    "disable_metrics_group_not_found",
			group:   "no-such",
			metrics: []string{"GroupMinSize"},
			disable: true,
			wantErr: true,
		},
		{
			name:    "enable_all_metrics_when_empty",
			group:   "met-all-asg",
			metrics: []string{},
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "met-all-asg",
					MinSize:              0,
					MaxSize:              5,
				})
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

			var err error
			if tt.disable {
				err = b.DisableMetricsCollection(tt.group, tt.metrics)
			} else {
				err = b.EnableMetricsCollection(tt.group, tt.metrics, "1Minute")
			}

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
