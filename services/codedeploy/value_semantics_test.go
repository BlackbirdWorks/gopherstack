package codedeploy_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codedeploysdk "github.com/aws/aws-sdk-go-v2/service/codedeploy"
	"github.com/aws/aws-sdk-go-v2/service/codedeploy/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateDeploymentGroup_OmittedMembersKept(t *testing.T) {
	t.Parallel()

	cases := []struct {
		update     *codedeploysdk.UpdateDeploymentGroupInput
		name       string
		wantASGs   []string
		wantHook   bool
		wantAlarms bool
	}{
		{
			name: "role only keeps everything",
			update: &codedeploysdk.UpdateDeploymentGroupInput{
				ServiceRoleArn: aws.String("arn:aws:iam::000000000000:role/new"),
			},
			wantASGs:   []string{"asg-1"},
			wantHook:   true,
			wantAlarms: true,
		},
		{
			name:     "empty asg list removes groups",
			update:   &codedeploysdk.UpdateDeploymentGroupInput{AutoScalingGroups: []string{}},
			wantASGs: []string{}, wantHook: true, wantAlarms: true,
		},
		{
			name:     "termination hook set false",
			update:   &codedeploysdk.UpdateDeploymentGroupInput{TerminationHookEnabled: aws.Bool(false)},
			wantASGs: []string{"asg-1"}, wantHook: false, wantAlarms: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestCodeDeployClient(t, h)
			ctx := t.Context()

			_, err := client.CreateApplication(
				ctx,
				&codedeploysdk.CreateApplicationInput{ApplicationName: aws.String("app")},
			)
			require.NoError(t, err)

			_, err = client.CreateDeploymentGroup(ctx, &codedeploysdk.CreateDeploymentGroupInput{
				ApplicationName:     aws.String("app"),
				DeploymentGroupName: aws.String("dg"),
				ServiceRoleArn:      aws.String("arn:aws:iam::000000000000:role/old"),
				AutoScalingGroups:   []string{"asg-1"},
				AlarmConfiguration: &types.AlarmConfiguration{
					Enabled: true, Alarms: []types.Alarm{{Name: aws.String("cpu")}},
				},
				DeploymentStyle: &types.DeploymentStyle{
					DeploymentOption: types.DeploymentOptionWithTrafficControl,
					DeploymentType:   types.DeploymentTypeBlueGreen,
				},
				TerminationHookEnabled: aws.Bool(true),
			})
			require.NoError(t, err)

			tc.update.ApplicationName = aws.String("app")
			tc.update.CurrentDeploymentGroupName = aws.String("dg")
			_, err = client.UpdateDeploymentGroup(ctx, tc.update)
			require.NoError(t, err)

			got, err := client.GetDeploymentGroup(ctx, &codedeploysdk.GetDeploymentGroupInput{
				ApplicationName: aws.String("app"), DeploymentGroupName: aws.String("dg"),
			})
			require.NoError(t, err)

			dg := got.DeploymentGroupInfo
			names := make([]string, 0, len(dg.AutoScalingGroups))
			for _, a := range dg.AutoScalingGroups {
				names = append(names, aws.ToString(a.Name))
			}

			assert.Equal(t, tc.wantASGs, names)
			assert.Equal(t, tc.wantHook, dg.TerminationHookEnabled)
			require.NotNil(t, dg.AlarmConfiguration)
			assert.Equal(t, tc.wantAlarms, dg.AlarmConfiguration.Enabled)
			require.NotNil(t, dg.DeploymentStyle)
			assert.Equal(t, types.DeploymentTypeBlueGreen, dg.DeploymentStyle.DeploymentType)
		})
	}
}
