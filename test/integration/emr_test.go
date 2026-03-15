package integration_test

import (
	"fmt"
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

	clusterName := fmt.Sprintf("test-cluster-%s", uuid.NewString()[:8])

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
		_, _ = client.TerminateJobFlows(ctx, &emr.TerminateJobFlowsInput{
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
