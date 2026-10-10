package main

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
)

func TestECSELBHealthReaderAdapter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		state     string
		wantState string
		wantOK    bool
		register  bool
	}{
		{name: "unhealthy", state: "unhealthy", wantState: "unhealthy", wantOK: true, register: true},
		{name: "healthy", state: "healthy", wantState: "healthy", wantOK: true, register: true},
		{name: "unregistered", register: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := elbv2backend.NewInMemoryBackend("000000000000", "us-east-1")
			h := elbv2backend.NewHandler(bk)
			adapter := &ecsELBv2RegistrarAdapter{elbv2TargetRegistrarAdapter{handler: h, home: bk}}

			tg, err := bk.CreateTargetGroup(elbv2backend.CreateTargetGroupInput{
				Name: "tg", Protocol: "HTTP", Port: 80, VpcID: "vpc-1", TargetType: "ip",
			})
			require.NoError(t, err)

			target := ecsbackend.ELBTarget{ID: "10.0.0.5", Port: 8080}

			if tt.register {
				require.NoError(
					t,
					adapter.RegisterTargets(t.Context(), tg.TargetGroupArn, []ecsbackend.ELBTarget{target}),
				)
				require.NoError(
					t,
					bk.SetTargetHealthState(tg.TargetGroupArn, target.ID, 8080, tt.state, "Target.FailedHealthChecks"),
				)
			}

			state, ok := adapter.TargetHealthState(context.Background(), tg.TargetGroupArn, target)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantState, state)
		})
	}
}

func TestECSCapacityProviderASGWiring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		asgArn  string
		wantErr bool
	}{
		{name: "existing", asgArn: "ok"},
		{
			name:    "missing",
			asgArn:  "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:x:autoScalingGroupName/nope",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			asg := autoscaling.NewFromConfig(fx.cfg)

			_, err := asg.CreateLaunchConfiguration(t.Context(), &autoscaling.CreateLaunchConfigurationInput{
				LaunchConfigurationName: aws.String("lc"), ImageId: aws.String("ami-12345678"),
				InstanceType: aws.String("t2.micro"),
			})
			require.NoError(t, err)

			_, err = asg.CreateAutoScalingGroup(t.Context(), &autoscaling.CreateAutoScalingGroupInput{
				AutoScalingGroupName: aws.String("ok"), LaunchConfigurationName: aws.String("lc"),
				MinSize: aws.Int32(0), MaxSize: aws.Int32(1), AvailabilityZones: []string{"us-east-1a"},
			})
			require.NoError(t, err)

			_, err = ecs.NewFromConfig(fx.cfg).CreateCapacityProvider(t.Context(), &ecs.CreateCapacityProviderInput{
				Name: aws.String("cp"),
				AutoScalingGroupProvider: &ecstypes.AutoScalingGroupProvider{
					AutoScalingGroupArn: aws.String(tt.asgArn),
				},
			})

			if tt.wantErr {
				require.ErrorContains(t, err, "InvalidParameterException")

				return
			}

			require.NoError(t, err)
		})
	}
}
