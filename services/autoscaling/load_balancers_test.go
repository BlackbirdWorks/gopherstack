package autoscaling_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

func TestInMemoryBackend_AttachLoadBalancerTargetGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *autoscaling.InMemoryBackend)
		name        string
		group       string
		arns        []string
		wantErr     bool
		wantARNsLen int
	}{
		{
			name: "attach_new_tgs",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "tg-g",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			group:       "tg-g",
			arns:        []string{"arn:aws:tg/one", "arn:aws:tg/two"},
			wantARNsLen: 2,
		},
		{
			name:    "group_not_found",
			group:   "no-such",
			arns:    []string{"arn:aws:tg/one"},
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

			err := b.AttachLoadBalancerTargetGroups(tt.group, tt.arns)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			groups, err := b.DescribeAutoScalingGroups([]string{tt.group})
			require.NoError(t, err)
			assert.Len(t, groups[0].TargetGroupARNs, tt.wantARNsLen)
		})
	}
}

func TestInMemoryBackend_AttachLoadBalancers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		lbNames []string
		wantErr bool
		wantLen int
	}{
		{
			name: "attach_new_lbs",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "lb-g",
					MinSize:              0,
					MaxSize:              5,
				})
			},
			group:   "lb-g",
			lbNames: []string{"elb-1", "elb-2"},
			wantLen: 2,
		},
		{
			name:    "group_not_found",
			group:   "no-such",
			lbNames: []string{"elb-1"},
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

			err := b.AttachLoadBalancers(tt.group, tt.lbNames)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			groups, err := b.DescribeAutoScalingGroups([]string{tt.group})
			require.NoError(t, err)
			assert.Len(t, groups[0].LoadBalancerNames, tt.wantLen)
		})
	}
}

func TestInMemoryBackend_DetachLoadBalancers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		lbs     []string
		wantErr bool
	}{
		{
			name:  "detach_existing_lb",
			group: "dlb-asg",
			lbs:   []string{"my-elb"},
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dlb-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.AttachLoadBalancers("dlb-asg", []string{"my-elb"})
			},
		},
		{
			name:    "detach_lb_group_not_found",
			group:   "no-such",
			lbs:     []string{"elb-x"},
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

			err := b.DetachLoadBalancers(tt.group, tt.lbs)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			groups, _ := b.DescribeAutoScalingGroups([]string{tt.group})
			for _, lb := range tt.lbs {
				assert.NotContains(t, groups[0].LoadBalancerNames, lb)
			}
		})
	}
}

func TestInMemoryBackend_DetachLoadBalancerTargetGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		arns    []string
		wantErr bool
	}{
		{
			name:  "detach_existing_tg",
			group: "dtg-asg",
			arns:  []string{"arn:aws:tg/one"},
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dtg-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.AttachLoadBalancerTargetGroups("dtg-asg", []string{"arn:aws:tg/one"})
			},
		},
		{
			name:    "detach_tg_group_not_found",
			group:   "no-such",
			arns:    []string{"arn:aws:tg/one"},
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

			err := b.DetachLoadBalancerTargetGroups(tt.group, tt.arns)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			groups, _ := b.DescribeAutoScalingGroups([]string{tt.group})
			for _, arn := range tt.arns {
				assert.NotContains(t, groups[0].TargetGroupARNs, arn)
			}
		})
	}
}

func TestInMemoryBackend_DescribeLoadBalancers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		wantErr bool
		wantLen int
	}{
		{
			name:  "describe_load_balancers_with_data",
			group: "dlbs-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dlbs-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.AttachLoadBalancers("dlbs-asg", []string{"elb-a", "elb-b"})
			},
			wantLen: 2,
		},
		{
			name:    "describe_load_balancers_group_not_found",
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

			lbs, err := b.DescribeLoadBalancers(tt.group)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, lbs, tt.wantLen)
		})
	}
}

func TestInMemoryBackend_DescribeLoadBalancerTargetGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		wantErr bool
		wantLen int
	}{
		{
			name:  "describe_target_groups_with_data",
			group: "dltg-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dltg-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_ = b.AttachLoadBalancerTargetGroups("dltg-asg", []string{
					"arn:aws:elasticloadbalancing:us-east-1:123:targetgroup/tg/abc",
				})
			},
			wantLen: 1,
		},
		{
			name:    "describe_target_groups_group_not_found",
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

			tgs, err := b.DescribeLoadBalancerTargetGroups(tt.group)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, tgs, tt.wantLen)
		})
	}
}
