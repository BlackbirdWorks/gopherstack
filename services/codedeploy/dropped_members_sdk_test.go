package codedeploy_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codedeploysdk "github.com/aws/aws-sdk-go-v2/service/codedeploy"
	"github.com/aws/aws-sdk-go-v2/service/codedeploy/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

func newDeployFixture(t *testing.T, platform types.ComputePlatform) *codedeploysdk.Client {
	t.Helper()

	h := codedeploy.NewHandler(codedeploy.NewInMemoryBackend("000000000000", "us-east-1"))
	c := newTestCodeDeployClient(t, h)

	_, err := c.CreateApplication(t.Context(), &codedeploysdk.CreateApplicationInput{
		ApplicationName: aws.String("app"), ComputePlatform: platform,
	})
	require.NoError(t, err)

	_, err = c.CreateDeploymentGroup(t.Context(), &codedeploysdk.CreateDeploymentGroupInput{
		ApplicationName:     aws.String("app"),
		DeploymentGroupName: aws.String("dg"),
		ServiceRoleArn:      aws.String("arn:aws:iam::000000000000:role/role"),
		DeploymentStyle: &types.DeploymentStyle{
			DeploymentType:   types.DeploymentTypeInPlace,
			DeploymentOption: types.DeploymentOptionWithoutTrafficControl,
		},
	})
	require.NoError(t, err)

	return c
}

func TestSDK_CreateDeploymentKeepsOverridesAndContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, info *types.DeploymentInfo)
		name  string
		in    codedeploysdk.CreateDeploymentInput
	}{
		{
			name: "auto rollback override",
			in: codedeploysdk.CreateDeploymentInput{
				AutoRollbackConfiguration: &types.AutoRollbackConfiguration{
					Enabled: true,
					Events:  []types.AutoRollbackEvent{types.AutoRollbackEventDeploymentFailure},
				},
			},
			check: func(t *testing.T, info *types.DeploymentInfo) {
				t.Helper()

				require.NotNil(t, info.AutoRollbackConfiguration)
				assert.True(t, info.AutoRollbackConfiguration.Enabled)
				assert.Equal(t,
					[]types.AutoRollbackEvent{types.AutoRollbackEventDeploymentFailure},
					info.AutoRollbackConfiguration.Events)
			},
		},
		{
			name: "alarm override",
			in: codedeploysdk.CreateDeploymentInput{
				OverrideAlarmConfiguration: &types.AlarmConfiguration{
					Enabled:                true,
					IgnorePollAlarmFailure: true,
					Alarms:                 []types.Alarm{{Name: aws.String("cpu-high")}},
				},
			},
			check: func(t *testing.T, info *types.DeploymentInfo) {
				t.Helper()

				require.NotNil(t, info.OverrideAlarmConfiguration)
				assert.True(t, info.OverrideAlarmConfiguration.Enabled)
				assert.True(t, info.OverrideAlarmConfiguration.IgnorePollAlarmFailure)
				require.Len(t, info.OverrideAlarmConfiguration.Alarms, 1)
				assert.Equal(t, "cpu-high", aws.ToString(info.OverrideAlarmConfiguration.Alarms[0].Name))
			},
		},
		{
			name: "target instances",
			in: codedeploysdk.CreateDeploymentInput{
				TargetInstances: &types.TargetInstances{
					AutoScalingGroups: []string{"asg-green"},
					TagFilters: []types.EC2TagFilter{
						{Key: aws.String("env"), Value: aws.String("green"), Type: types.EC2TagFilterTypeKeyAndValue},
					},
				},
			},
			check: func(t *testing.T, info *types.DeploymentInfo) {
				t.Helper()

				require.NotNil(t, info.TargetInstances)
				assert.Equal(t, []string{"asg-green"}, info.TargetInstances.AutoScalingGroups)
				require.Len(t, info.TargetInstances.TagFilters, 1)
				assert.Equal(t, "env", aws.ToString(info.TargetInstances.TagFilters[0].Key))
			},
		},
		{
			name: "platform and style from application and group",
			in:   codedeploysdk.CreateDeploymentInput{},
			check: func(t *testing.T, info *types.DeploymentInfo) {
				t.Helper()

				assert.Equal(t, types.ComputePlatformServer, info.ComputePlatform)
				require.NotNil(t, info.DeploymentStyle)
				assert.Equal(t, types.DeploymentTypeInPlace, info.DeploymentStyle.DeploymentType)
				assert.Nil(t, info.AutoRollbackConfiguration)
				assert.Nil(t, info.TargetInstances)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newDeployFixture(t, types.ComputePlatformServer)
			tt.in.ApplicationName = aws.String("app")
			tt.in.DeploymentGroupName = aws.String("dg")

			created, err := c.CreateDeployment(t.Context(), &tt.in)
			require.NoError(t, err)

			got, err := c.GetDeployment(t.Context(), &codedeploysdk.GetDeploymentInput{
				DeploymentId: created.DeploymentId,
			})
			require.NoError(t, err)
			tt.check(t, got.DeploymentInfo)

			batch, err := c.BatchGetDeployments(t.Context(), &codedeploysdk.BatchGetDeploymentsInput{
				DeploymentIds: []string{aws.ToString(created.DeploymentId)},
			})
			require.NoError(t, err)
			require.Len(t, batch.DeploymentsInfo, 1)
			tt.check(t, &batch.DeploymentsInfo[0])
		})
	}
}

func TestSDK_CreateDeploymentRejectsConflictingTargetInstances(t *testing.T) {
	t.Parallel()

	c := newDeployFixture(t, types.ComputePlatformServer)

	_, err := c.CreateDeployment(t.Context(), &codedeploysdk.CreateDeploymentInput{
		ApplicationName:     aws.String("app"),
		DeploymentGroupName: aws.String("dg"),
		TargetInstances: &types.TargetInstances{
			Ec2TagSet: &types.EC2TagSet{Ec2TagSetList: [][]types.EC2TagFilter{
				{{Key: aws.String("k"), Type: types.EC2TagFilterTypeKeyOnly}},
			}},
			TagFilters: []types.EC2TagFilter{{Key: aws.String("k"), Type: types.EC2TagFilterTypeKeyOnly}},
		},
	})

	var ex *types.InvalidTargetInstancesException

	require.ErrorAs(t, err, &ex)
}
