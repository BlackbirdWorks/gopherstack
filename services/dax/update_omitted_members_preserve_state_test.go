package dax_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	daxsdk "github.com/aws/aws-sdk-go-v2/service/dax"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dax"
)

// TestUpdateCluster_PreservesOmittedMembers proves the zeroguard fix
// (cmd/zeroguard): UpdateClusterInput.PreferredMaintenanceWindow,
// .ParameterGroupName, .NotificationTopicArn and .NotificationTopicStatus
// are all plain strings in the real SDK's request type (api_op_UpdateCluster.go).
// Before the fix each was decoded as string guarded by `!= ""`, so an
// omitted field and an explicit empty string were indistinguishable.
// ClusterName is deliberately left a plain string: it is a required lookup
// identifier (validateOpUpdateClusterInput), never written back to state.
func TestUpdateCluster_PreservesOmittedMembers(t *testing.T) {
	t.Parallel()

	backend := dax.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestDAXSDKClient(t, dax.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateParameterGroup(ctx, &daxsdk.CreateParameterGroupInput{
		ParameterGroupName: aws.String("pg-1"),
	})
	require.NoError(t, err)

	_, err = client.CreateCluster(ctx, &daxsdk.CreateClusterInput{
		ClusterName:       aws.String("uds-cluster"),
		NodeType:          aws.String("dax.r5.large"),
		IamRoleArn:        aws.String("arn:aws:iam::123456789012:role/DAXRole"),
		ReplicationFactor: 1,
	})
	require.NoError(t, err)

	_, err = client.UpdateCluster(ctx, &daxsdk.UpdateClusterInput{
		ClusterName:                aws.String("uds-cluster"),
		PreferredMaintenanceWindow: aws.String("mon:01:00-mon:02:00"),
		ParameterGroupName:         aws.String("pg-1"),
		NotificationTopicArn:       aws.String("arn:aws:sns:us-east-1:123456789012:topic"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeClusters(ctx, &daxsdk.DescribeClustersInput{ClusterNames: []string{"uds-cluster"}})
	require.NoError(t, err)
	require.Len(t, desc.Clusters, 1)
	c := desc.Clusters[0]
	assert.Equal(t, "mon:01:00-mon:02:00", aws.ToString(c.PreferredMaintenanceWindow))
	assert.Equal(t, "pg-1", aws.ToString(c.ParameterGroup.ParameterGroupName))
	require.NotNil(t, c.NotificationConfiguration)
	assert.Equal(t, "arn:aws:sns:us-east-1:123456789012:topic", aws.ToString(c.NotificationConfiguration.TopicArn))
	assert.Equal(t, "active", aws.ToString(c.NotificationConfiguration.TopicStatus))

	// Omitted fields must preserve every prior value.
	_, err = client.UpdateCluster(ctx, &daxsdk.UpdateClusterInput{
		ClusterName: aws.String("uds-cluster"),
	})
	require.NoError(t, err)

	desc, err = client.DescribeClusters(ctx, &daxsdk.DescribeClustersInput{ClusterNames: []string{"uds-cluster"}})
	require.NoError(t, err)
	c = desc.Clusters[0]
	assert.Equal(t, "mon:01:00-mon:02:00", aws.ToString(c.PreferredMaintenanceWindow), "omitted window must survive")
	assert.Equal(t, "pg-1", aws.ToString(c.ParameterGroup.ParameterGroupName), "omitted param group must survive")
	require.NotNil(t, c.NotificationConfiguration)
	assert.Equal(t, "arn:aws:sns:us-east-1:123456789012:topic", aws.ToString(c.NotificationConfiguration.TopicArn),
		"omitted topic arn must survive")

	// Explicit NotificationTopicStatus alone (no ARN) updates status only.
	_, err = client.UpdateCluster(ctx, &daxsdk.UpdateClusterInput{
		ClusterName:             aws.String("uds-cluster"),
		NotificationTopicStatus: aws.String("inactive"),
	})
	require.NoError(t, err)

	desc, err = client.DescribeClusters(ctx, &daxsdk.DescribeClustersInput{ClusterNames: []string{"uds-cluster"}})
	require.NoError(t, err)
	c = desc.Clusters[0]
	assert.Equal(t, "inactive", aws.ToString(c.NotificationConfiguration.TopicStatus))
	assert.Equal(t, "arn:aws:sns:us-east-1:123456789012:topic", aws.ToString(c.NotificationConfiguration.TopicArn),
		"unrelated field untouched")
}

// TestUpdateSubnetGroup_PreservesOmittedDescription proves the zeroguard fix:
// UpdateSubnetGroupInput.Description is a plain string in the real SDK
// (api_op_UpdateSubnetGroup.go). SubnetGroupName is left a plain string: it
// is a required lookup identifier, never written back to state.
func TestUpdateSubnetGroup_PreservesOmittedDescription(t *testing.T) {
	t.Parallel()

	backend := dax.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestDAXSDKClient(t, dax.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateSubnetGroup(ctx, &daxsdk.CreateSubnetGroupInput{
		SubnetGroupName: aws.String("uds-sg"),
		Description:     aws.String("original"),
		SubnetIds:       []string{"subnet-11111111"},
	})
	require.NoError(t, err)

	_, err = client.UpdateSubnetGroup(ctx, &daxsdk.UpdateSubnetGroupInput{
		SubnetGroupName: aws.String("uds-sg"),
		Description:     aws.String("updated"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeSubnetGroups(ctx, &daxsdk.DescribeSubnetGroupsInput{
		SubnetGroupNames: []string{"uds-sg"},
	})
	require.NoError(t, err)
	require.Len(t, desc.SubnetGroups, 1)
	assert.Equal(t, "updated", aws.ToString(desc.SubnetGroups[0].Description))

	// Omitted description must preserve the prior value.
	_, err = client.UpdateSubnetGroup(ctx, &daxsdk.UpdateSubnetGroupInput{
		SubnetGroupName: aws.String("uds-sg"),
		SubnetIds:       []string{"subnet-11111111", "subnet-22222222"},
	})
	require.NoError(t, err)

	desc, err = client.DescribeSubnetGroups(ctx, &daxsdk.DescribeSubnetGroupsInput{
		SubnetGroupNames: []string{"uds-sg"},
	})
	require.NoError(t, err)
	assert.Equal(t, "updated", aws.ToString(desc.SubnetGroups[0].Description), "omitted description must survive")
	assert.Len(t, desc.SubnetGroups[0].Subnets, 2)

	// Explicit empty string clears it.
	_, err = client.UpdateSubnetGroup(ctx, &daxsdk.UpdateSubnetGroupInput{
		SubnetGroupName: aws.String("uds-sg"),
		Description:     aws.String(""),
	})
	require.NoError(t, err)

	desc, err = client.DescribeSubnetGroups(ctx, &daxsdk.DescribeSubnetGroupsInput{
		SubnetGroupNames: []string{"uds-sg"},
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(desc.SubnetGroups[0].Description))
}
