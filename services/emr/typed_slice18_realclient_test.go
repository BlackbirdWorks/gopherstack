package emr_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	emrsdk "github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emr"
)

// newSlice18EMRClient stands up a fresh backend/handler/client triple for
// gopherstack-n3zi typed slice 18.
func newSlice18EMRClient(t *testing.T) *emrsdk.Client {
	t.Helper()

	backend := emr.NewInMemoryBackend(testAccountID, testRegion)
	h := emr.NewHandler(backend)

	return newTestEMRClient(t, h)
}

// TestTypedSlice18EMR_RealClient drives every op the census still listed as
// uncovered before this pass (gopherstack-n3zi typed-client coverage slice
// 18).
func TestTypedSlice18EMR_RealClient(t *testing.T) {
	t.Parallel()

	t.Run("instance_groups", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18EMRClient(t)

		runOut, err := client.RunJobFlow(ctx, &emrsdk.RunJobFlowInput{
			Name: aws.String("slice18-ig-cluster"),
			Instances: &emrtypes.JobFlowInstancesConfig{
				InstanceGroups: []emrtypes.InstanceGroupConfig{
					{
						Name:          aws.String("master"),
						InstanceRole:  emrtypes.InstanceRoleTypeMaster,
						InstanceType:  aws.String("m5.xlarge"),
						InstanceCount: aws.Int32(1),
					},
				},
				KeepJobFlowAliveWhenNoSteps: aws.Bool(true),
			},
		})
		require.NoError(t, err)
		clusterID := aws.ToString(runOut.JobFlowId)

		listed, err := client.ListInstanceGroups(ctx, &emrsdk.ListInstanceGroupsInput{ClusterId: aws.String(clusterID)})
		require.NoError(t, err)
		require.Len(t, listed.InstanceGroups, 1)
		assert.Equal(t, "master", aws.ToString(listed.InstanceGroups[0].Name))

		added, err := client.AddInstanceGroups(ctx, &emrsdk.AddInstanceGroupsInput{
			JobFlowId: aws.String(clusterID),
			InstanceGroups: []emrtypes.InstanceGroupConfig{
				{
					Name:          aws.String("task"),
					InstanceRole:  emrtypes.InstanceRoleTypeTask,
					InstanceType:  aws.String("m5.xlarge"),
					InstanceCount: aws.Int32(2),
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, added.InstanceGroupIds, 1)
		taskGroupID := added.InstanceGroupIds[0]

		_, err = client.ModifyInstanceGroups(ctx, &emrsdk.ModifyInstanceGroupsInput{
			ClusterId: aws.String(clusterID),
			InstanceGroups: []emrtypes.InstanceGroupModifyConfig{
				{InstanceGroupId: aws.String(taskGroupID), InstanceCount: aws.Int32(3)},
			},
		})
		require.NoError(t, err)

		afterModify, err := client.ListInstanceGroups(
			ctx,
			&emrsdk.ListInstanceGroupsInput{ClusterId: aws.String(clusterID)},
		)
		require.NoError(t, err)
		require.Len(t, afterModify.InstanceGroups, 2)

		var sawUpdatedCount bool

		for _, g := range afterModify.InstanceGroups {
			if aws.ToString(g.Id) == taskGroupID {
				sawUpdatedCount = aws.ToInt32(g.RequestedInstanceCount) == 3
			}
		}

		assert.True(t, sawUpdatedCount, "expected task instance group's RequestedInstanceCount to update to 3")
	})

	t.Run("instance_fleets", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18EMRClient(t)

		runOut, err := client.RunJobFlow(ctx, &emrsdk.RunJobFlowInput{
			Name: aws.String("slice18-fleet-cluster"),
			Instances: &emrtypes.JobFlowInstancesConfig{
				InstanceFleets: []emrtypes.InstanceFleetConfig{
					{
						Name:                   aws.String("master-fleet"),
						InstanceFleetType:      emrtypes.InstanceFleetTypeMaster,
						TargetOnDemandCapacity: aws.Int32(1),
					},
				},
				KeepJobFlowAliveWhenNoSteps: aws.Bool(true),
			},
		})
		require.NoError(t, err)
		clusterID := aws.ToString(runOut.JobFlowId)

		listed, err := client.ListInstanceFleets(ctx, &emrsdk.ListInstanceFleetsInput{ClusterId: aws.String(clusterID)})
		require.NoError(t, err)
		require.Len(t, listed.InstanceFleets, 1)
		fleetID := aws.ToString(listed.InstanceFleets[0].Id)

		_, err = client.ModifyInstanceFleet(ctx, &emrsdk.ModifyInstanceFleetInput{
			ClusterId: aws.String(clusterID),
			InstanceFleet: &emrtypes.InstanceFleetModifyConfig{
				InstanceFleetId:        aws.String(fleetID),
				TargetOnDemandCapacity: aws.Int32(2),
			},
		})
		require.NoError(t, err)

		afterModify, err := client.ListInstanceFleets(
			ctx,
			&emrsdk.ListInstanceFleetsInput{ClusterId: aws.String(clusterID)},
		)
		require.NoError(t, err)
		require.Len(t, afterModify.InstanceFleets, 1)
		assert.Equal(t, int32(2), aws.ToInt32(afterModify.InstanceFleets[0].TargetOnDemandCapacity))
	})

	t.Run("security_configurations", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18EMRClient(t)

		const secConfigJSON = `{"EncryptionConfiguration":{"EnableInTransitEncryption":false}}`

		created, err := client.CreateSecurityConfiguration(ctx, &emrsdk.CreateSecurityConfigurationInput{
			Name:                  aws.String("slice18-secconfig"),
			SecurityConfiguration: aws.String(secConfigJSON),
		})
		require.NoError(t, err)
		assert.Equal(t, "slice18-secconfig", aws.ToString(created.Name))

		desc, err := client.DescribeSecurityConfiguration(ctx, &emrsdk.DescribeSecurityConfigurationInput{
			Name: aws.String("slice18-secconfig"),
		})
		require.NoError(t, err)
		assert.JSONEq(t, secConfigJSON, aws.ToString(desc.SecurityConfiguration))

		listed, err := client.ListSecurityConfigurations(ctx, &emrsdk.ListSecurityConfigurationsInput{})
		require.NoError(t, err)

		var found bool

		for _, sc := range listed.SecurityConfigurations {
			if aws.ToString(sc.Name) == "slice18-secconfig" {
				found = true
			}
		}

		assert.True(t, found, "expected slice18-secconfig to appear in ListSecurityConfigurations")

		_, err = client.DeleteSecurityConfiguration(ctx, &emrsdk.DeleteSecurityConfigurationInput{
			Name: aws.String("slice18-secconfig"),
		})
		require.NoError(t, err)

		_, err = client.DescribeSecurityConfiguration(ctx, &emrsdk.DescribeSecurityConfigurationInput{
			Name: aws.String("slice18-secconfig"),
		})
		assert.Error(t, err, "expected deleted security configuration to no longer be describable")
	})

	t.Run("release_labels", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18EMRClient(t)

		listed, err := client.ListReleaseLabels(ctx, &emrsdk.ListReleaseLabelsInput{})
		require.NoError(t, err)
		require.NotEmpty(t, listed.ReleaseLabels)
		releaseLabel := listed.ReleaseLabels[0]

		desc, err := client.DescribeReleaseLabel(ctx, &emrsdk.DescribeReleaseLabelInput{
			ReleaseLabel: aws.String(releaseLabel),
		})
		require.NoError(t, err)
		assert.Equal(t, releaseLabel, aws.ToString(desc.ReleaseLabel))

		instanceTypes, err := client.ListSupportedInstanceTypes(ctx, &emrsdk.ListSupportedInstanceTypesInput{
			ReleaseLabel: aws.String(releaseLabel),
		})
		require.NoError(t, err)
		assert.NotNil(t, instanceTypes.SupportedInstanceTypes)
	})

	t.Run("cluster_settings", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18EMRClient(t)

		runOut, err := client.RunJobFlow(ctx, &emrsdk.RunJobFlowInput{
			Name:      aws.String("slice18-settings-cluster"),
			Instances: &emrtypes.JobFlowInstancesConfig{KeepJobFlowAliveWhenNoSteps: aws.Bool(true)},
		})
		require.NoError(t, err)
		clusterID := aws.ToString(runOut.JobFlowId)

		modOut, err := client.ModifyCluster(ctx, &emrsdk.ModifyClusterInput{
			ClusterId:            aws.String(clusterID),
			StepConcurrencyLevel: aws.Int32(5),
		})
		require.NoError(t, err)
		assert.Equal(t, int32(5), aws.ToInt32(modOut.StepConcurrencyLevel))

		_, err = client.SetTerminationProtection(ctx, &emrsdk.SetTerminationProtectionInput{
			JobFlowIds:           []string{clusterID},
			TerminationProtected: aws.Bool(true),
		})
		require.NoError(t, err)

		_, err = client.SetTerminationProtection(ctx, &emrsdk.SetTerminationProtectionInput{
			JobFlowIds:           []string{clusterID},
			TerminationProtected: aws.Bool(false),
		})
		require.NoError(t, err)

		_, err = client.SetKeepJobFlowAliveWhenNoSteps(ctx, &emrsdk.SetKeepJobFlowAliveWhenNoStepsInput{
			JobFlowIds:                  []string{clusterID},
			KeepJobFlowAliveWhenNoSteps: aws.Bool(true),
		})
		require.NoError(t, err)

		_, err = client.SetVisibleToAllUsers(ctx, &emrsdk.SetVisibleToAllUsersInput{
			JobFlowIds:        []string{clusterID},
			VisibleToAllUsers: aws.Bool(true),
		})
		require.NoError(t, err)

		_, err = client.SetUnhealthyNodeReplacement(ctx, &emrsdk.SetUnhealthyNodeReplacementInput{
			JobFlowIds:               []string{clusterID},
			UnhealthyNodeReplacement: aws.Bool(true),
		})
		require.NoError(t, err)

		desc, err := client.DescribeCluster(ctx, &emrsdk.DescribeClusterInput{ClusterId: aws.String(clusterID)})
		require.NoError(t, err)
		require.NotNil(t, desc.Cluster)
		assert.True(t, aws.ToBool(desc.Cluster.VisibleToAllUsers))
	})

	t.Run("policies", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18EMRClient(t)

		runOut, err := client.RunJobFlow(ctx, &emrsdk.RunJobFlowInput{
			Name: aws.String("slice18-policies-cluster"),
			Instances: &emrtypes.JobFlowInstancesConfig{
				InstanceGroups: []emrtypes.InstanceGroupConfig{
					{
						Name:          aws.String("master"),
						InstanceRole:  emrtypes.InstanceRoleTypeCore,
						InstanceType:  aws.String("m5.xlarge"),
						InstanceCount: aws.Int32(1),
					},
				},
				KeepJobFlowAliveWhenNoSteps: aws.Bool(true),
			},
		})
		require.NoError(t, err)
		clusterID := aws.ToString(runOut.JobFlowId)

		groups, err := client.ListInstanceGroups(ctx, &emrsdk.ListInstanceGroupsInput{ClusterId: aws.String(clusterID)})
		require.NoError(t, err)
		require.Len(t, groups.InstanceGroups, 1)
		instanceGroupID := aws.ToString(groups.InstanceGroups[0].Id)

		none, err := client.GetAutoTerminationPolicy(ctx, &emrsdk.GetAutoTerminationPolicyInput{
			ClusterId: aws.String(clusterID),
		})
		require.NoError(t, err)
		assert.Nil(t, none.AutoTerminationPolicy)

		_, err = client.PutAutoTerminationPolicy(ctx, &emrsdk.PutAutoTerminationPolicyInput{
			ClusterId:             aws.String(clusterID),
			AutoTerminationPolicy: &emrtypes.AutoTerminationPolicy{IdleTimeout: aws.Int64(3600)},
		})
		require.NoError(t, err)

		got, err := client.GetAutoTerminationPolicy(ctx, &emrsdk.GetAutoTerminationPolicyInput{
			ClusterId: aws.String(clusterID),
		})
		require.NoError(t, err)
		require.NotNil(t, got.AutoTerminationPolicy)
		assert.Equal(t, int64(3600), aws.ToInt64(got.AutoTerminationPolicy.IdleTimeout))

		_, err = client.RemoveAutoTerminationPolicy(ctx, &emrsdk.RemoveAutoTerminationPolicyInput{
			ClusterId: aws.String(clusterID),
		})
		require.NoError(t, err)

		afterRemove, err := client.GetAutoTerminationPolicy(ctx, &emrsdk.GetAutoTerminationPolicyInput{
			ClusterId: aws.String(clusterID),
		})
		require.NoError(t, err)
		assert.Nil(t, afterRemove.AutoTerminationPolicy)

		noneMS, err := client.GetManagedScalingPolicy(ctx, &emrsdk.GetManagedScalingPolicyInput{
			ClusterId: aws.String(clusterID),
		})
		require.NoError(t, err)
		assert.Nil(t, noneMS.ManagedScalingPolicy)

		_, err = client.PutManagedScalingPolicy(ctx, &emrsdk.PutManagedScalingPolicyInput{
			ClusterId: aws.String(clusterID),
			ManagedScalingPolicy: &emrtypes.ManagedScalingPolicy{
				ComputeLimits: &emrtypes.ComputeLimits{
					UnitType:             emrtypes.ComputeLimitsUnitTypeInstances,
					MinimumCapacityUnits: aws.Int32(1),
					MaximumCapacityUnits: aws.Int32(10),
				},
			},
		})
		require.NoError(t, err)

		gotMS, err := client.GetManagedScalingPolicy(ctx, &emrsdk.GetManagedScalingPolicyInput{
			ClusterId: aws.String(clusterID),
		})
		require.NoError(t, err)
		require.NotNil(t, gotMS.ManagedScalingPolicy)
		assert.Equal(t, int32(10), aws.ToInt32(gotMS.ManagedScalingPolicy.ComputeLimits.MaximumCapacityUnits))

		_, err = client.RemoveManagedScalingPolicy(ctx, &emrsdk.RemoveManagedScalingPolicyInput{
			ClusterId: aws.String(clusterID),
		})
		require.NoError(t, err)

		putAS, err := client.PutAutoScalingPolicy(ctx, &emrsdk.PutAutoScalingPolicyInput{
			ClusterId:       aws.String(clusterID),
			InstanceGroupId: aws.String(instanceGroupID),
			AutoScalingPolicy: &emrtypes.AutoScalingPolicy{
				Constraints: &emrtypes.ScalingConstraints{
					MinCapacity: aws.Int32(1),
					MaxCapacity: aws.Int32(5),
				},
				Rules: []emrtypes.ScalingRule{
					{
						Name: aws.String("scale-out-on-cpu"),
						Action: &emrtypes.ScalingAction{
							SimpleScalingPolicyConfiguration: &emrtypes.SimpleScalingPolicyConfiguration{
								AdjustmentType:    emrtypes.AdjustmentTypeChangeInCapacity,
								ScalingAdjustment: aws.Int32(1),
							},
						},
						Trigger: &emrtypes.ScalingTrigger{
							CloudWatchAlarmDefinition: &emrtypes.CloudWatchAlarmDefinition{
								ComparisonOperator: emrtypes.ComparisonOperatorGreaterThanOrEqual,
								MetricName:         aws.String("YARNMemoryAvailablePercentage"),
								Period:             aws.Int32(300),
								Threshold:          aws.Float64(75),
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, putAS.AutoScalingPolicy)
		assert.Equal(t, int32(5), aws.ToInt32(putAS.AutoScalingPolicy.Constraints.MaxCapacity))

		_, err = client.RemoveAutoScalingPolicy(ctx, &emrsdk.RemoveAutoScalingPolicyInput{
			ClusterId:       aws.String(clusterID),
			InstanceGroupId: aws.String(instanceGroupID),
		})
		require.NoError(t, err)

		_, err = client.PutBlockPublicAccessConfiguration(ctx, &emrsdk.PutBlockPublicAccessConfigurationInput{
			BlockPublicAccessConfiguration: &emrtypes.BlockPublicAccessConfiguration{
				BlockPublicSecurityGroupRules: aws.Bool(true),
			},
		})
		require.NoError(t, err)
	})

	t.Run("tags", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18EMRClient(t)

		runOut, err := client.RunJobFlow(ctx, &emrsdk.RunJobFlowInput{
			Name:      aws.String("slice18-tags-cluster"),
			Instances: &emrtypes.JobFlowInstancesConfig{KeepJobFlowAliveWhenNoSteps: aws.Bool(true)},
		})
		require.NoError(t, err)
		clusterID := aws.ToString(runOut.JobFlowId)
		clusterARN := aws.ToString(runOut.ClusterArn)

		_, err = client.AddTags(ctx, &emrsdk.AddTagsInput{
			ResourceId: aws.String(clusterARN),
			Tags:       []emrtypes.Tag{{Key: aws.String("owner"), Value: aws.String("slice18")}},
		})
		require.NoError(t, err)

		// EMR has no real ListTagsForResource operation (confirmed: no
		// api_op_ListTagsForResource.go in the pinned emr@v1.64.4 module) --
		// gopherstack's ListTagsForResource handler is a non-wire extension,
		// so tags are verified via DescribeCluster.Cluster.Tags instead, the
		// real read path.
		desc, err := client.DescribeCluster(ctx, &emrsdk.DescribeClusterInput{ClusterId: aws.String(clusterID)})
		require.NoError(t, err)
		require.NotNil(t, desc.Cluster)
		require.Len(t, desc.Cluster.Tags, 1)
		assert.Equal(t, "owner", aws.ToString(desc.Cluster.Tags[0].Key))

		_, err = client.RemoveTags(ctx, &emrsdk.RemoveTagsInput{
			ResourceId: aws.String(clusterARN),
			TagKeys:    []string{"owner"},
		})
		require.NoError(t, err)

		afterRemove, err := client.DescribeCluster(ctx, &emrsdk.DescribeClusterInput{ClusterId: aws.String(clusterID)})
		require.NoError(t, err)
		require.NotNil(t, afterRemove.Cluster)
		assert.Empty(t, afterRemove.Cluster.Tags)
	})

	t.Run("bootstrap_actions_and_cancel_steps", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18EMRClient(t)

		runOut, err := client.RunJobFlow(ctx, &emrsdk.RunJobFlowInput{
			Name: aws.String("slice18-steps-cluster"),
			Instances: &emrtypes.JobFlowInstancesConfig{
				KeepJobFlowAliveWhenNoSteps: aws.Bool(true),
			},
			BootstrapActions: []emrtypes.BootstrapActionConfig{
				{
					Name: aws.String("bootstrap-1"),
					ScriptBootstrapAction: &emrtypes.ScriptBootstrapActionConfig{
						Path: aws.String("s3://bucket/bootstrap.sh"),
						Args: []string{"--flag"},
					},
				},
			},
		})
		require.NoError(t, err)
		clusterID := aws.ToString(runOut.JobFlowId)

		listedActions, err := client.ListBootstrapActions(ctx, &emrsdk.ListBootstrapActionsInput{
			ClusterId: aws.String(clusterID),
		})
		require.NoError(t, err)
		require.Len(t, listedActions.BootstrapActions, 1)
		assert.Equal(t, "bootstrap-1", aws.ToString(listedActions.BootstrapActions[0].Name))

		stepsOut, err := client.AddJobFlowSteps(ctx, &emrsdk.AddJobFlowStepsInput{
			JobFlowId: aws.String(clusterID),
			Steps: []emrtypes.StepConfig{
				{
					Name: aws.String("cancel-me"),
					HadoopJarStep: &emrtypes.HadoopJarStepConfig{
						Jar: aws.String("s3://bucket/job.jar"),
					},
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, stepsOut.StepIds, 1)

		cancelled, err := client.CancelSteps(ctx, &emrsdk.CancelStepsInput{
			ClusterId: aws.String(clusterID),
			StepIds:   stepsOut.StepIds,
		})
		require.NoError(t, err)
		require.Len(t, cancelled.CancelStepsInfoList, 1)
		assert.Equal(t, stepsOut.StepIds[0], aws.ToString(cancelled.CancelStepsInfoList[0].StepId))
	})

	t.Run("sessions", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18EMRClient(t)

		runOut, err := client.RunJobFlow(ctx, &emrsdk.RunJobFlowInput{
			Name: aws.String("slice18-session-cluster"),
			Instances: &emrtypes.JobFlowInstancesConfig{
				KeepJobFlowAliveWhenNoSteps: aws.Bool(true),
			},
			SessionEnabled: aws.Bool(true),
		})
		require.NoError(t, err)
		clusterID := aws.ToString(runOut.JobFlowId)

		started, err := client.StartSession(ctx, &emrsdk.StartSessionInput{
			ClusterId:        aws.String(clusterID),
			ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/session-role"),
		})
		require.NoError(t, err)
		sessionID := aws.ToString(started.Id)

		got, err := client.GetSession(ctx, &emrsdk.GetSessionInput{
			ClusterId: aws.String(clusterID),
			SessionId: aws.String(sessionID),
		})
		require.NoError(t, err)
		require.NotNil(t, got.Session)
		assert.Equal(t, sessionID, aws.ToString(got.Session.Id))

		endpoint, err := client.GetSessionEndpoint(ctx, &emrsdk.GetSessionEndpointInput{
			ClusterId: aws.String(clusterID),
			SessionId: aws.String(sessionID),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(endpoint.Endpoint))

		terminated, err := client.TerminateSession(ctx, &emrsdk.TerminateSessionInput{
			ClusterId: aws.String(clusterID),
			SessionId: aws.String(sessionID),
		})
		require.NoError(t, err)
		assert.Equal(t, sessionID, aws.ToString(terminated.SessionId))
	})

	t.Run("persistent_app_ui_and_cluster_credentials", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18EMRClient(t)

		runOut, err := client.RunJobFlow(ctx, &emrsdk.RunJobFlowInput{
			Name:      aws.String("slice18-appui-cluster"),
			Instances: &emrtypes.JobFlowInstancesConfig{KeepJobFlowAliveWhenNoSteps: aws.Bool(true)},
		})
		require.NoError(t, err)
		clusterID := aws.ToString(runOut.JobFlowId)
		clusterARN := aws.ToString(runOut.ClusterArn)

		created, err := client.CreatePersistentAppUI(ctx, &emrsdk.CreatePersistentAppUIInput{
			TargetResourceArn: aws.String(clusterARN),
		})
		require.NoError(t, err)
		uiID := aws.ToString(created.PersistentAppUIId)

		desc, err := client.DescribePersistentAppUI(ctx, &emrsdk.DescribePersistentAppUIInput{
			PersistentAppUIId: aws.String(uiID),
		})
		require.NoError(t, err)
		require.NotNil(t, desc.PersistentAppUI)
		assert.Equal(t, uiID, aws.ToString(desc.PersistentAppUI.PersistentAppUIId))

		onCluster, err := client.GetOnClusterAppUIPresignedURL(ctx, &emrsdk.GetOnClusterAppUIPresignedURLInput{
			ClusterId: aws.String(clusterID),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(onCluster.PresignedURL))

		persistentURL, err := client.GetPersistentAppUIPresignedURL(ctx, &emrsdk.GetPersistentAppUIPresignedURLInput{
			PersistentAppUIId: aws.String(uiID),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, aws.ToString(persistentURL.PresignedURL))

		creds, err := client.GetClusterSessionCredentials(ctx, &emrsdk.GetClusterSessionCredentialsInput{
			ClusterId:        aws.String(clusterID),
			ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/session-role"),
		})
		require.NoError(t, err)
		assert.NotNil(t, creds.Credentials)
	})

	t.Run("studios", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18EMRClient(t)

		created, err := client.CreateStudio(ctx, &emrsdk.CreateStudioInput{
			Name:                     aws.String("slice18-studio"),
			AuthMode:                 emrtypes.AuthModeIam,
			DefaultS3Location:        aws.String("s3://bucket/studio"),
			EngineSecurityGroupId:    aws.String("sg-engine"),
			ServiceRole:              aws.String("arn:aws:iam::000000000000:role/studio-service"),
			VpcId:                    aws.String("vpc-1"),
			WorkspaceSecurityGroupId: aws.String("sg-workspace"),
			SubnetIds:                []string{"subnet-1"},
		})
		require.NoError(t, err)
		studioID := aws.ToString(created.StudioId)

		_, err = client.UpdateStudio(ctx, &emrsdk.UpdateStudioInput{
			StudioId:    aws.String(studioID),
			Name:        aws.String("slice18-studio-updated"),
			Description: aws.String("updated description"),
		})
		require.NoError(t, err)

		listed, err := client.ListStudios(ctx, &emrsdk.ListStudiosInput{})
		require.NoError(t, err)

		var found bool

		for _, s := range listed.Studios {
			if aws.ToString(s.StudioId) == studioID {
				found = true
			}
		}

		assert.True(t, found, "expected created studio to appear in ListStudios")

		_, err = client.CreateStudioSessionMapping(ctx, &emrsdk.CreateStudioSessionMappingInput{
			StudioId:         aws.String(studioID),
			IdentityType:     emrtypes.IdentityTypeUser,
			IdentityName:     aws.String("alice"),
			SessionPolicyArn: aws.String("arn:aws:iam::000000000000:policy/session-policy"),
		})
		require.NoError(t, err)

		gotMapping, err := client.GetStudioSessionMapping(ctx, &emrsdk.GetStudioSessionMappingInput{
			StudioId:     aws.String(studioID),
			IdentityType: emrtypes.IdentityTypeUser,
			IdentityName: aws.String("alice"),
		})
		require.NoError(t, err)
		require.NotNil(t, gotMapping.SessionMapping)
		assert.Equal(t, "alice", aws.ToString(gotMapping.SessionMapping.IdentityName))

		_, err = client.UpdateStudioSessionMapping(ctx, &emrsdk.UpdateStudioSessionMappingInput{
			StudioId:         aws.String(studioID),
			IdentityType:     emrtypes.IdentityTypeUser,
			IdentityName:     aws.String("alice"),
			SessionPolicyArn: aws.String("arn:aws:iam::000000000000:policy/session-policy-v2"),
		})
		require.NoError(t, err)

		afterUpdate, err := client.GetStudioSessionMapping(ctx, &emrsdk.GetStudioSessionMappingInput{
			StudioId:     aws.String(studioID),
			IdentityType: emrtypes.IdentityTypeUser,
			IdentityName: aws.String("alice"),
		})
		require.NoError(t, err)
		require.NotNil(t, afterUpdate.SessionMapping)
		assert.Equal(
			t,
			"arn:aws:iam::000000000000:policy/session-policy-v2",
			aws.ToString(afterUpdate.SessionMapping.SessionPolicyArn),
		)

		_, err = client.DeleteStudioSessionMapping(ctx, &emrsdk.DeleteStudioSessionMappingInput{
			StudioId:     aws.String(studioID),
			IdentityType: emrtypes.IdentityTypeUser,
			IdentityName: aws.String("alice"),
		})
		require.NoError(t, err)

		_, err = client.DeleteStudio(ctx, &emrsdk.DeleteStudioInput{StudioId: aws.String(studioID)})
		require.NoError(t, err)
	})

	t.Run("notebook_execution", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		client := newSlice18EMRClient(t)

		runOut, err := client.RunJobFlow(ctx, &emrsdk.RunJobFlowInput{
			Name:      aws.String("slice18-notebook-cluster"),
			Instances: &emrtypes.JobFlowInstancesConfig{KeepJobFlowAliveWhenNoSteps: aws.Bool(true)},
		})
		require.NoError(t, err)
		clusterID := aws.ToString(runOut.JobFlowId)

		started, err := client.StartNotebookExecution(ctx, &emrsdk.StartNotebookExecutionInput{
			EditorId:              aws.String("editor-1"),
			NotebookExecutionName: aws.String("slice18-notebook"),
			ExecutionEngine:       &emrtypes.ExecutionEngineConfig{Id: aws.String(clusterID)},
			ServiceRole:           aws.String("arn:aws:iam::000000000000:role/notebook-service"),
		})
		require.NoError(t, err)
		execID := aws.ToString(started.NotebookExecutionId)

		_, err = client.StopNotebookExecution(ctx, &emrsdk.StopNotebookExecutionInput{
			NotebookExecutionId: aws.String(execID),
		})
		require.NoError(t, err)

		desc, err := client.DescribeNotebookExecution(ctx, &emrsdk.DescribeNotebookExecutionInput{
			NotebookExecutionId: aws.String(execID),
		})
		require.NoError(t, err)
		require.NotNil(t, desc.NotebookExecution)
		assert.Equal(t, emrtypes.NotebookExecutionStatusStopped, desc.NotebookExecution.Status)
	})
}
