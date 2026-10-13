package ecs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func TestCreateCapacityProvider_ManagedSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		asg     *ecs.AutoScalingGroupProvider
		name    string
		wantErr bool
	}{
		{name: "defaults", asg: &ecs.AutoScalingGroupProvider{
			AutoScalingGroupArn: "asg",
			ManagedScaling:      &ecs.ManagedScaling{Status: "ENABLED", InstanceWarmupPeriod: 300},
		}},
		{name: "bad_status", wantErr: true, asg: &ecs.AutoScalingGroupProvider{
			AutoScalingGroupArn: "asg", ManagedScaling: &ecs.ManagedScaling{Status: "MAYBE"},
		}},
		{name: "bad_protection", wantErr: true, asg: &ecs.AutoScalingGroupProvider{
			AutoScalingGroupArn: "asg", ManagedTerminationProtection: "ON",
		}},
		{name: "target_too_high", wantErr: true, asg: &ecs.AutoScalingGroupProvider{
			AutoScalingGroupArn: "asg", ManagedScaling: &ecs.ManagedScaling{TargetCapacityPercent: 101},
		}},
		{name: "step_too_high", wantErr: true, asg: &ecs.AutoScalingGroupProvider{
			AutoScalingGroupArn: "asg", ManagedScaling: &ecs.ManagedScaling{MinimumScalingStepSize: 10001},
		}},
		{name: "warmup_too_high", wantErr: true, asg: &ecs.AutoScalingGroupProvider{
			AutoScalingGroupArn: "asg", ManagedScaling: &ecs.ManagedScaling{InstanceWarmupPeriod: 10001},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
			in := ecs.CreateCapacityProviderInput{Name: "cp", AutoScalingGroupProvider: tt.asg}
			cp, err := b.CreateCapacityProvider(in)

			if tt.wantErr {
				require.ErrorIs(t, err, ecs.ErrInvalidParameter)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "DISABLED", cp.AutoScalingGroupProvider.ManagedTerminationProtection)
			assert.Equal(t, 100, cp.AutoScalingGroupProvider.ManagedScaling.TargetCapacityPercent)
			assert.Equal(t, 1, cp.AutoScalingGroupProvider.ManagedScaling.MinimumScalingStepSize)
			assert.Equal(t, 10000, cp.AutoScalingGroupProvider.ManagedScaling.MaximumScalingStepSize)
			assert.Equal(t, 300, cp.AutoScalingGroupProvider.ManagedScaling.InstanceWarmupPeriod)
		})
	}
}
