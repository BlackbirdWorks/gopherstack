package autoscaling_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

func TestInMemoryBackend_ScalingPolicyStepAdjustments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *autoscaling.InMemoryBackend)
		run   func(t *testing.T, b *autoscaling.InMemoryBackend)
		name  string
	}{
		{
			name: "step_adjustments_stored_and_retrieved",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "step-asg",
					MinSize:              1,
					MaxSize:              10,
				})
			},
			run: func(t *testing.T, b *autoscaling.InMemoryBackend) {
				t.Helper()

				lower := float64(0)
				upper := float64(10)

				policy, err := b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:            "step-policy",
					AutoScalingGroupName:  "step-asg",
					PolicyType:            "StepScaling",
					AdjustmentType:        "ChangeInCapacity",
					MetricAggregationType: "Average",
					StepAdjustments: []autoscaling.StepAdjustment{
						{MetricIntervalLowerBound: &lower, MetricIntervalUpperBound: &upper, ScalingAdjustment: 2},
						{MetricIntervalLowerBound: &upper, ScalingAdjustment: 4},
					},
				})
				require.NoError(t, err)
				require.Len(t, policy.StepAdjustments, 2)
				assert.Equal(t, int32(2), policy.StepAdjustments[0].ScalingAdjustment)
				assert.Equal(t, int32(4), policy.StepAdjustments[1].ScalingAdjustment)
				assert.Equal(t, "Average", policy.MetricAggregationType)
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

func TestInMemoryBackend_ExecutePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		input   autoscaling.ExecutePolicyInput
		wantErr bool
	}{
		{
			name: "execute_change_in_capacity",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "ep-asg",
					MinSize:              1,
					MaxSize:              10,
					DesiredCapacity:      2,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "scale-up",
					AutoScalingGroupName: "ep-asg",
					PolicyType:           "SimpleScaling",
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    2,
				})
			},
			input: autoscaling.ExecutePolicyInput{
				AutoScalingGroupName: "ep-asg",
				PolicyName:           "scale-up",
			},
		},
		{
			name: "execute_exact_capacity",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "ep-exact-asg",
					MinSize:              1,
					MaxSize:              10,
					DesiredCapacity:      2,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "set-to-5",
					AutoScalingGroupName: "ep-exact-asg",
					AdjustmentType:       "ExactCapacity",
					ScalingAdjustment:    5,
				})
			},
			input: autoscaling.ExecutePolicyInput{
				AutoScalingGroupName: "ep-exact-asg",
				PolicyName:           "set-to-5",
			},
		},
		{
			name: "execute_percent_change",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "ep-pct-asg",
					MinSize:              1,
					MaxSize:              10,
					DesiredCapacity:      4,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "scale-by-pct",
					AutoScalingGroupName: "ep-pct-asg",
					AdjustmentType:       "PercentChangeInCapacity",
					ScalingAdjustment:    50,
				})
			},
			input: autoscaling.ExecutePolicyInput{
				AutoScalingGroupName: "ep-pct-asg",
				PolicyName:           "scale-by-pct",
			},
		},
		{
			name:    "group_not_found",
			wantErr: true,
			input: autoscaling.ExecutePolicyInput{
				AutoScalingGroupName: "no-such",
				PolicyName:           "my-policy",
			},
		},
		{
			name: "policy_not_found",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "ep-nopol-asg",
					MinSize:              1,
					MaxSize:              10,
					DesiredCapacity:      2,
				})
			},
			wantErr: true,
			input: autoscaling.ExecutePolicyInput{
				AutoScalingGroupName: "ep-nopol-asg",
				PolicyName:           "ghost-policy",
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

			err := b.ExecutePolicy(tt.input)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_DeletePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *autoscaling.InMemoryBackend)
		name    string
		group   string
		policy  string
		wantErr bool
	}{
		{
			name:   "delete_by_name",
			group:  "dp-asg",
			policy: "my-policy",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dp-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "my-policy",
					AutoScalingGroupName: "dp-asg",
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    1,
				})
			},
		},
		{
			name:   "delete_by_arn",
			group:  "dp-arn-asg",
			policy: "", // will be set from created policy ARN
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dp-arn-asg",
					MinSize:              0,
					MaxSize:              5,
				})
			},
		},
		{
			name:    "delete_policy_not_found",
			group:   "no-such",
			policy:  "ghost-policy",
			wantErr: true,
		},
		{
			name:   "delete_by_name_empty_group_searches_all",
			group:  "",
			policy: "cross-group-policy",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "cg-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "cross-group-policy",
					AutoScalingGroupName: "cg-asg",
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    1,
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

			policyToDelete := tt.policy

			// For ARN-based test, create the policy and grab its ARN
			if tt.name == "delete_by_arn" {
				p, err := b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "arn-test-policy",
					AutoScalingGroupName: tt.group,
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    1,
				})
				require.NoError(t, err)
				policyToDelete = p.PolicyARN
			}

			err := b.DeletePolicy(tt.group, policyToDelete)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestInMemoryBackend_DescribePolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *autoscaling.InMemoryBackend)
		name        string
		group       string
		policyNames []string
		wantCount   int
	}{
		{
			name:  "describe_all_policies",
			group: "dpol-asg",
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dpol-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "pol-a",
					AutoScalingGroupName: "dpol-asg",
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    1,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "pol-b",
					AutoScalingGroupName: "dpol-asg",
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    -1,
				})
			},
			wantCount: 2,
		},
		{
			name:  "describe_policies_across_all_groups",
			group: "", // empty = describe all
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dpol-cross-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "cross-pol",
					AutoScalingGroupName: "dpol-cross-asg",
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    1,
				})
			},
			wantCount: 1,
		},
		{
			name:        "describe_policies_filter_by_name",
			group:       "dpol-filter-asg",
			policyNames: []string{"pol-x"},
			setup: func(b *autoscaling.InMemoryBackend) {
				_, _ = b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "dpol-filter-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "pol-x",
					AutoScalingGroupName: "dpol-filter-asg",
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    1,
				})
				_, _ = b.PutScalingPolicy(autoscaling.ScalingPolicyInput{
					PolicyName:           "pol-y",
					AutoScalingGroupName: "dpol-filter-asg",
					AdjustmentType:       "ChangeInCapacity",
					ScalingAdjustment:    -1,
				})
			},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			policies, err := b.DescribePolicies(tt.group, tt.policyNames, nil)
			require.NoError(t, err)
			assert.Len(t, policies, tt.wantCount)
		})
	}
}
