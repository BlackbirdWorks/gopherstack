package autoscaling_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

func TestInMemoryBackend_DeleteNotificationConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *autoscaling.InMemoryBackend)
		name     string
		group    string
		topicARN string
		wantErr  bool
	}{
		{
			name:     "delete_existing_notification",
			group:    "notif-del-asg",
			topicARN: "arn:aws:sns:us-east-1:000000000000:my-topic",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "notif-del-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.PutNotificationConfiguration(
					"notif-del-asg",
					"arn:aws:sns:us-east-1:000000000000:my-topic",
					[]string{"autoscaling:EC2_INSTANCE_LAUNCH"},
				)
			},
		},
		{
			name:     "delete_notification_group_not_found",
			group:    "no-such",
			topicARN: "arn:aws:sns:us-east-1:000000000000:t",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.DeleteNotificationConfiguration(tt.group, tt.topicARN)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			configs, _ := b.DescribeNotificationConfigurations([]string{tt.group})
			for _, c := range configs {
				assert.NotEqual(t, tt.topicARN, c.TopicARN)
			}
		})
	}
}

func TestInMemoryBackend_DescribeNotificationConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(b *autoscaling.InMemoryBackend)
		name       string
		groupNames []string
		wantCount  int
	}{
		{
			name: "describe_all_notification_configs",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dnc-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.PutNotificationConfiguration(
					"dnc-asg",
					"arn:aws:sns:us-east-1:000000000000:topic-a",
					[]string{"autoscaling:EC2_INSTANCE_LAUNCH", "autoscaling:EC2_INSTANCE_TERMINATE"},
				)
			},
			groupNames: nil, // fetch all
			wantCount:  2,
		},
		{
			name: "describe_notification_configs_filtered",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dnc-filter-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.PutNotificationConfiguration(
					"dnc-filter-asg",
					"arn:aws:sns:us-east-1:000000000000:topic-b",
					[]string{"autoscaling:EC2_INSTANCE_LAUNCH"},
				)
			},
			groupNames: []string{"dnc-filter-asg"},
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			configs, err := b.DescribeNotificationConfigurations(tt.groupNames)
			require.NoError(t, err)
			assert.Len(t, configs, tt.wantCount)
		})
	}
}
