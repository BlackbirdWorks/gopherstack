package applicationautoscaling_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	aassdk "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling"
	aastypes "github.com/aws/aws-sdk-go-v2/service/applicationautoscaling/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
)

func TestRegisterScalableTarget_SuspendedStateSemantics(t *testing.T) {
	t.Parallel()

	cases := []struct {
		first   *aastypes.SuspendedState
		second  *aastypes.SuspendedState
		name    string
		wantIn  bool
		wantOut bool
		wantSch bool
	}{
		{name: "omitted state defaults to false"},
		{
			name: "partial update keeps unsent members",
			first: &aastypes.SuspendedState{
				DynamicScalingInSuspended:  aws.Bool(true),
				DynamicScalingOutSuspended: aws.Bool(true),
				ScheduledScalingSuspended:  aws.Bool(true),
			},
			second:  &aastypes.SuspendedState{DynamicScalingInSuspended: aws.Bool(false)},
			wantOut: true, wantSch: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newTestAASSDKClient(t, applicationautoscaling.NewHandler(
				applicationautoscaling.NewInMemoryBackend("123456789012", "us-east-1")))
			in := &aassdk.RegisterScalableTargetInput{
				ServiceNamespace:  aastypes.ServiceNamespaceEcs,
				ResourceId:        aws.String("service/c/s"),
				ScalableDimension: aastypes.ScalableDimensionECSServiceDesiredCount,
				MinCapacity:       aws.Int32(1),
				MaxCapacity:       aws.Int32(5),
				SuspendedState:    tc.first,
			}
			_, err := client.RegisterScalableTarget(ctx, in)
			require.NoError(t, err)

			if tc.second != nil {
				in.SuspendedState, in.MinCapacity, in.MaxCapacity = tc.second, nil, nil
				_, err = client.RegisterScalableTarget(ctx, in)
				require.NoError(t, err)
			}

			got, err := client.DescribeScalableTargets(ctx, &aassdk.DescribeScalableTargetsInput{
				ServiceNamespace: aastypes.ServiceNamespaceEcs,
			})
			require.NoError(t, err)
			require.Len(t, got.ScalableTargets, 1)

			ss := got.ScalableTargets[0].SuspendedState
			require.NotNil(t, ss)
			assert.Equal(t, tc.wantIn, aws.ToBool(ss.DynamicScalingInSuspended))
			assert.Equal(t, tc.wantOut, aws.ToBool(ss.DynamicScalingOutSuspended))
			assert.Equal(t, tc.wantSch, aws.ToBool(ss.ScheduledScalingSuspended))
		})
	}
}
