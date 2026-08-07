package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_EMR_ClusterLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEMRClient(t)
	ctx := t.Context()

	clusterName := "test-cluster-" + uuid.NewString()[:8]

	// RunJobFlow
	runOut, err := client.RunJobFlow(ctx, &emr.RunJobFlowInput{
		Name:         aws.String(clusterName),
		ReleaseLabel: aws.String("emr-6.10.0"),
		Instances: &emrtypes.JobFlowInstancesConfig{
			InstanceGroups: []emrtypes.InstanceGroupConfig{
				{
					InstanceRole:  emrtypes.InstanceRoleTypeMaster,
					InstanceType:  aws.String("m5.xlarge"),
					InstanceCount: aws.Int32(1),
				},
				{
					InstanceRole:  emrtypes.InstanceRoleTypeCore,
					InstanceType:  aws.String("m5.xlarge"),
					InstanceCount: aws.Int32(2),
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(runOut.JobFlowId))

	clusterID := aws.ToString(runOut.JobFlowId)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.TerminateJobFlows(cleanupCtx, &emr.TerminateJobFlowsInput{
			JobFlowIds: []string{clusterID},
		})
	})

	// DescribeCluster
	descOut, err := client.DescribeCluster(ctx, &emr.DescribeClusterInput{
		ClusterId: aws.String(clusterID),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Cluster)
	assert.Equal(t, clusterName, aws.ToString(descOut.Cluster.Name))
	assert.NotEmpty(t, aws.ToString(descOut.Cluster.Id))

	// ListClusters
	listOut, err := client.ListClusters(ctx, &emr.ListClustersInput{})
	require.NoError(t, err)

	found := false

	for _, c := range listOut.Clusters {
		if aws.ToString(c.Id) == clusterID {
			found = true

			break
		}
	}

	assert.True(t, found, "created cluster should appear in ListClusters")

	// TerminateJobFlows
	_, err = client.TerminateJobFlows(ctx, &emr.TerminateJobFlowsInput{
		JobFlowIds: []string{clusterID},
	})
	require.NoError(t, err)

	// Verify terminated (removed from active list)
	listOut2, err := client.ListClusters(ctx, &emr.ListClustersInput{})
	require.NoError(t, err)

	for _, c := range listOut2.Clusters {
		assert.NotEqual(t, clusterID, aws.ToString(c.Id), "terminated cluster should not appear in list")
	}
}

func TestIntegration_EMR_DescribeClusterNotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEMRClient(t)
	ctx := t.Context()

	_, err := client.DescribeCluster(ctx, &emr.DescribeClusterInput{
		ClusterId: aws.String("j-DOESNOTEXIST"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidRequestException")
}

// TestIntegration_EMR_OptionalClusterFields drives RunJobFlow/DescribeCluster
// through a real aws-sdk-go-v2 client to prove MonitoringConfiguration,
// LogEncryptionKmsKeyId, RepoUpgradeOnBoot, and AmiVersion-derived
// RequestedAmiVersion/RunningAmiVersion actually deserialize into the real
// SDK's typed Cluster struct -- a unit test asserting on raw JSON can't catch
// a wrong field name/tag the way an SDK round-trip does.
func TestIntegration_EMR_OptionalClusterFields(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEMRClient(t)
	ctx := t.Context()

	clusterName := "test-optional-fields-" + uuid.NewString()[:8]

	runOut, err := client.RunJobFlow(ctx, &emr.RunJobFlowInput{
		Name:                  aws.String(clusterName),
		ReleaseLabel:          aws.String("emr-6.10.0"),
		Instances:             &emrtypes.JobFlowInstancesConfig{},
		LogEncryptionKmsKeyId: aws.String("arn:aws:kms:us-east-1:123456789012:key/abc"),
		RepoUpgradeOnBoot:     emrtypes.RepoUpgradeOnBootNone,
		AmiVersion:            aws.String("3.11.0"),
		MonitoringConfiguration: &emrtypes.MonitoringConfiguration{
			CloudWatchLogConfiguration: &emrtypes.CloudWatchLogConfiguration{
				Enabled:      aws.Bool(true),
				LogGroupName: aws.String("/emr/" + clusterName),
			},
		},
	})
	require.NoError(t, err)

	clusterID := aws.ToString(runOut.JobFlowId)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.TerminateJobFlows(cleanupCtx, &emr.TerminateJobFlowsInput{
			JobFlowIds: []string{clusterID},
		})
	})

	descOut, err := client.DescribeCluster(ctx, &emr.DescribeClusterInput{
		ClusterId: aws.String(clusterID),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Cluster)

	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/abc", aws.ToString(descOut.Cluster.LogEncryptionKmsKeyId))
	assert.Equal(t, emrtypes.RepoUpgradeOnBootNone, descOut.Cluster.RepoUpgradeOnBoot)
	assert.Equal(t, "3.11.0", aws.ToString(descOut.Cluster.RequestedAmiVersion))
	assert.Equal(t, "3.11.0", aws.ToString(descOut.Cluster.RunningAmiVersion))
	require.NotNil(t, descOut.Cluster.MonitoringConfiguration)
	require.NotNil(t, descOut.Cluster.MonitoringConfiguration.CloudWatchLogConfiguration)
	assert.True(t, aws.ToBool(descOut.Cluster.MonitoringConfiguration.CloudWatchLogConfiguration.Enabled))
	assert.Equal(t, "/emr/"+clusterName,
		aws.ToString(descOut.Cluster.MonitoringConfiguration.CloudWatchLogConfiguration.LogGroupName))
}

// TestIntegration_EMR_ListInstances_InstanceFleets drives AddInstanceFleet +
// ListInstances through a real aws-sdk-go-v2 client: before this pass,
// fleet-based clusters always returned zero instances from ListInstances,
// regardless of ProvisionedOnDemandCapacity/ProvisionedSpotCapacity.
func TestIntegration_EMR_ListInstances_InstanceFleets(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEMRClient(t)
	ctx := t.Context()

	clusterName := "test-fleet-instances-" + uuid.NewString()[:8]

	runOut, err := client.RunJobFlow(ctx, &emr.RunJobFlowInput{
		Name:         aws.String(clusterName),
		ReleaseLabel: aws.String("emr-6.10.0"),
		Instances:    &emrtypes.JobFlowInstancesConfig{},
	})
	require.NoError(t, err)

	clusterID := aws.ToString(runOut.JobFlowId)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.TerminateJobFlows(cleanupCtx, &emr.TerminateJobFlowsInput{
			JobFlowIds: []string{clusterID},
		})
	})

	addOut, err := client.AddInstanceFleet(ctx, &emr.AddInstanceFleetInput{
		ClusterId: aws.String(clusterID),
		InstanceFleet: &emrtypes.InstanceFleetConfig{
			InstanceFleetType:      emrtypes.InstanceFleetTypeTask,
			TargetOnDemandCapacity: aws.Int32(2),
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListInstances(ctx, &emr.ListInstancesInput{
		ClusterId: aws.String(clusterID),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Instances, 2)

	for _, inst := range listOut.Instances {
		assert.Equal(t, aws.ToString(addOut.InstanceFleetId), aws.ToString(inst.InstanceFleetId))
		assert.Empty(t, aws.ToString(inst.InstanceGroupId))
	}
}
