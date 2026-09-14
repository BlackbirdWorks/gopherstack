package docdb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	docdbsdk "github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/aws/aws-sdk-go-v2/service/docdb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

func newRealClient(t *testing.T) *docdbsdk.Client {
	t.Helper()

	backend := docdb.NewInMemoryBackend("123456789012", "us-east-1")

	return newTestDocDBClient(t, docdb.NewHandler(backend))
}

// TestRealClient_ClusterInstanceAndSnapshotManagement drives every gopherstack-n3zi
// uncovered docdb op through the real aws-sdk-go-v2 client
// (newTestDocDBClient, shared with handler_sdk_roundtrip_test.go).
func TestRealClient_ClusterInstanceAndSnapshotManagement(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testClusterParameterGroupsRealClient, "cluster_parameter_groups"},
		{testSubnetGroupsRealClient, "subnet_groups"},
		{testEventSubscriptionsRealClient, "event_subscriptions"},
		{testSnapshotsRealClient, "snapshots"},
		{testReferenceDataRealClient, "reference_data"},
		{testClusterLifecycleRealClient, "cluster_lifecycle"},
		{testInstanceLifecycleRealClient, "instance_lifecycle"},
		{testGlobalClusterRealClient, "global_cluster"},
		{testTagsRealClient, "tags"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

// testClusterParameterGroupsRealClient covers
// DescribeDBClusterParameterGroups, DeleteDBClusterParameterGroup,
// CopyDBClusterParameterGroup, DescribeEngineDefaultClusterParameters.
func testClusterParameterGroupsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDBClusterParameterGroup(ctx, &docdbsdk.CreateDBClusterParameterGroupInput{
		DBClusterParameterGroupName: aws.String("slice33-pg"),
		DBParameterGroupFamily:      aws.String("docdb4.0"),
		Description:                 aws.String("slice33 test"),
	})
	require.NoError(t, err)

	listOut, err := client.DescribeDBClusterParameterGroups(ctx, &docdbsdk.DescribeDBClusterParameterGroupsInput{
		DBClusterParameterGroupName: aws.String("slice33-pg"),
	})
	require.NoError(t, err)
	require.Len(t, listOut.DBClusterParameterGroups, 1)

	_, err = client.CopyDBClusterParameterGroup(ctx, &docdbsdk.CopyDBClusterParameterGroupInput{
		SourceDBClusterParameterGroupIdentifier:  aws.String("slice33-pg"),
		TargetDBClusterParameterGroupIdentifier:  aws.String("slice33-pg-copy"),
		TargetDBClusterParameterGroupDescription: aws.String("slice33 copy"),
	})
	require.NoError(t, err)

	defaultsOut, err := client.DescribeEngineDefaultClusterParameters(
		ctx, &docdbsdk.DescribeEngineDefaultClusterParametersInput{DBParameterGroupFamily: aws.String("docdb4.0")},
	)
	require.NoError(t, err)
	require.NotNil(t, defaultsOut.EngineDefaults)
	assert.NotEmpty(t, defaultsOut.EngineDefaults.Parameters)

	_, err = client.DeleteDBClusterParameterGroup(ctx, &docdbsdk.DeleteDBClusterParameterGroupInput{
		DBClusterParameterGroupName: aws.String("slice33-pg-copy"),
	})
	require.NoError(t, err)

	listOut2, err := client.DescribeDBClusterParameterGroups(ctx, &docdbsdk.DescribeDBClusterParameterGroupsInput{
		DBClusterParameterGroupName: aws.String("slice33-pg-copy"),
	})
	require.Error(t, err)
	_ = listOut2
}

// testSubnetGroupsRealClient covers DescribeDBSubnetGroups,
// ModifyDBSubnetGroup, DeleteDBSubnetGroup.
func testSubnetGroupsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDBSubnetGroup(ctx, &docdbsdk.CreateDBSubnetGroupInput{
		DBSubnetGroupName:        aws.String("slice33-subnet-group"),
		DBSubnetGroupDescription: aws.String("slice33 test"),
		SubnetIds:                []string{"subnet-aaaa1111"},
	})
	require.NoError(t, err)

	listOut, err := client.DescribeDBSubnetGroups(ctx, &docdbsdk.DescribeDBSubnetGroupsInput{
		DBSubnetGroupName: aws.String("slice33-subnet-group"),
	})
	require.NoError(t, err)
	require.Len(t, listOut.DBSubnetGroups, 1)

	updOut, err := client.ModifyDBSubnetGroup(ctx, &docdbsdk.ModifyDBSubnetGroupInput{
		DBSubnetGroupName: aws.String("slice33-subnet-group"),
		SubnetIds:         []string{"subnet-aaaa1111", "subnet-bbbb2222"},
	})
	require.NoError(t, err)
	require.Len(t, updOut.DBSubnetGroup.Subnets, 2)

	_, err = client.DeleteDBSubnetGroup(ctx, &docdbsdk.DeleteDBSubnetGroupInput{
		DBSubnetGroupName: aws.String("slice33-subnet-group"),
	})
	require.NoError(t, err)

	_, err = client.DescribeDBSubnetGroups(ctx, &docdbsdk.DescribeDBSubnetGroupsInput{
		DBSubnetGroupName: aws.String("slice33-subnet-group"),
	})
	require.Error(t, err)
}

// testEventSubscriptionsRealClient covers AddSourceIdentifierToSubscription,
// RemoveSourceIdentifierFromSubscription, ModifyEventSubscription,
// DescribeEventSubscriptions, DeleteEventSubscription.
func testEventSubscriptionsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateEventSubscription(ctx, &docdbsdk.CreateEventSubscriptionInput{
		SubscriptionName: aws.String("slice33-event-sub"),
		SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:123456789012:slice33-topic"),
		SourceType:       aws.String("db-cluster"),
		Enabled:          aws.Bool(true),
	})
	require.NoError(t, err)

	addOut, err := client.AddSourceIdentifierToSubscription(ctx, &docdbsdk.AddSourceIdentifierToSubscriptionInput{
		SubscriptionName: aws.String("slice33-event-sub"),
		SourceIdentifier: aws.String("slice33-source-cluster"),
	})
	require.NoError(t, err)
	require.Contains(t, addOut.EventSubscription.SourceIdsList, "slice33-source-cluster")

	updOut, err := client.ModifyEventSubscription(ctx, &docdbsdk.ModifyEventSubscriptionInput{
		SubscriptionName: aws.String("slice33-event-sub"),
		Enabled:          aws.Bool(false),
	})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(updOut.EventSubscription.Enabled))

	listOut, err := client.DescribeEventSubscriptions(ctx, &docdbsdk.DescribeEventSubscriptionsInput{
		SubscriptionName: aws.String("slice33-event-sub"),
	})
	require.NoError(t, err)
	require.Len(t, listOut.EventSubscriptionsList, 1)

	remOut, err := client.RemoveSourceIdentifierFromSubscription(
		ctx, &docdbsdk.RemoveSourceIdentifierFromSubscriptionInput{
			SubscriptionName: aws.String("slice33-event-sub"),
			SourceIdentifier: aws.String("slice33-source-cluster"),
		},
	)
	require.NoError(t, err)
	assert.NotContains(t, remOut.EventSubscription.SourceIdsList, "slice33-source-cluster")

	_, err = client.DeleteEventSubscription(ctx, &docdbsdk.DeleteEventSubscriptionInput{
		SubscriptionName: aws.String("slice33-event-sub"),
	})
	require.NoError(t, err)

	listOut2, err := client.DescribeEventSubscriptions(ctx, &docdbsdk.DescribeEventSubscriptionsInput{
		SubscriptionName: aws.String("slice33-event-sub"),
	})
	require.Error(t, err)
	_ = listOut2
}

// testSnapshotsRealClient covers DescribeDBClusterSnapshots,
// DeleteDBClusterSnapshot, DescribeDBClusterSnapshotAttributes,
// ModifyDBClusterSnapshotAttribute.
func testSnapshotsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("slice33-snap-cluster"),
		Engine:              aws.String("docdb"),
	})
	require.NoError(t, err)

	_, err = client.CreateDBClusterSnapshot(ctx, &docdbsdk.CreateDBClusterSnapshotInput{
		DBClusterSnapshotIdentifier: aws.String("slice33-snap"),
		DBClusterIdentifier:         aws.String("slice33-snap-cluster"),
	})
	require.NoError(t, err)

	listOut, err := client.DescribeDBClusterSnapshots(ctx, &docdbsdk.DescribeDBClusterSnapshotsInput{
		DBClusterSnapshotIdentifier: aws.String("slice33-snap"),
	})
	require.NoError(t, err)
	require.Len(t, listOut.DBClusterSnapshots, 1)

	attrOut, err := client.ModifyDBClusterSnapshotAttribute(ctx, &docdbsdk.ModifyDBClusterSnapshotAttributeInput{
		DBClusterSnapshotIdentifier: aws.String("slice33-snap"),
		AttributeName:               aws.String("restore"),
		ValuesToAdd:                 []string{"123456789013"},
	})
	require.NoError(t, err)
	require.NotNil(t, attrOut.DBClusterSnapshotAttributesResult)

	descAttrOut, err := client.DescribeDBClusterSnapshotAttributes(
		ctx,
		&docdbsdk.DescribeDBClusterSnapshotAttributesInput{DBClusterSnapshotIdentifier: aws.String("slice33-snap")},
	)
	require.NoError(t, err)
	require.NotNil(t, descAttrOut.DBClusterSnapshotAttributesResult)
	require.Len(t, descAttrOut.DBClusterSnapshotAttributesResult.DBClusterSnapshotAttributes, 1)
	assert.Contains(
		t, descAttrOut.DBClusterSnapshotAttributesResult.DBClusterSnapshotAttributes[0].AttributeValues, "123456789013",
	)

	_, err = client.DeleteDBClusterSnapshot(ctx, &docdbsdk.DeleteDBClusterSnapshotInput{
		DBClusterSnapshotIdentifier: aws.String("slice33-snap"),
	})
	require.NoError(t, err)

	_, err = client.DescribeDBClusterSnapshots(ctx, &docdbsdk.DescribeDBClusterSnapshotsInput{
		DBClusterSnapshotIdentifier: aws.String("slice33-snap"),
	})
	require.Error(t, err)
}

// testReferenceDataRealClient covers DescribeCertificates,
// DescribeDBEngineVersions, DescribeOrderableDBInstanceOptions.
func testReferenceDataRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	certsOut, err := client.DescribeCertificates(ctx, &docdbsdk.DescribeCertificatesInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, certsOut.Certificates)

	enginesOut, err := client.DescribeDBEngineVersions(ctx, &docdbsdk.DescribeDBEngineVersionsInput{
		Engine: aws.String("docdb"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, enginesOut.DBEngineVersions)

	optsOut, err := client.DescribeOrderableDBInstanceOptions(ctx, &docdbsdk.DescribeOrderableDBInstanceOptionsInput{
		Engine: aws.String("docdb"),
	})
	require.NoError(t, err)
	assert.NotNil(t, optsOut.OrderableDBInstanceOptions)
}

// testClusterLifecycleRealClient covers ModifyDBCluster, StopDBCluster,
// StartDBCluster, FailoverDBCluster, RestoreDBClusterToPointInTime.
func testClusterLifecycleRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("slice33-lifecycle-cluster"),
		Engine:              aws.String("docdb"),
	})
	require.NoError(t, err)

	modOut, err := client.ModifyDBCluster(ctx, &docdbsdk.ModifyDBClusterInput{
		DBClusterIdentifier: aws.String("slice33-lifecycle-cluster"),
		Port:                aws.Int32(27018),
		ApplyImmediately:    aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(27018), aws.ToInt32(modOut.DBCluster.Port))

	_, err = client.FailoverDBCluster(ctx, &docdbsdk.FailoverDBClusterInput{
		DBClusterIdentifier: aws.String("slice33-lifecycle-cluster"),
	})
	require.NoError(t, err)

	_, err = client.StopDBCluster(ctx, &docdbsdk.StopDBClusterInput{
		DBClusterIdentifier: aws.String("slice33-lifecycle-cluster"),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeDBClusters(ctx, &docdbsdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String("slice33-lifecycle-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.DBClusters, 1)
	assert.Equal(t, "stopped", aws.ToString(descOut.DBClusters[0].Status))

	_, err = client.StartDBCluster(ctx, &docdbsdk.StartDBClusterInput{
		DBClusterIdentifier: aws.String("slice33-lifecycle-cluster"),
	})
	require.NoError(t, err)

	restoreOut, err := client.RestoreDBClusterToPointInTime(ctx, &docdbsdk.RestoreDBClusterToPointInTimeInput{
		SourceDBClusterIdentifier: aws.String("slice33-lifecycle-cluster"),
		DBClusterIdentifier:       aws.String("slice33-restored-cluster"),
		UseLatestRestorableTime:   aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice33-restored-cluster", aws.ToString(restoreOut.DBCluster.DBClusterIdentifier))
}

// testInstanceLifecycleRealClient covers ModifyDBInstance and
// RebootDBInstance.
func testInstanceLifecycleRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("slice33-inst-cluster"),
		Engine:              aws.String("docdb"),
	})
	require.NoError(t, err)

	_, err = client.CreateDBInstance(ctx, &docdbsdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("slice33-instance"),
		DBClusterIdentifier:  aws.String("slice33-inst-cluster"),
		DBInstanceClass:      aws.String("db.r5.large"),
		Engine:               aws.String("docdb"),
	})
	require.NoError(t, err)

	modOut, err := client.ModifyDBInstance(ctx, &docdbsdk.ModifyDBInstanceInput{
		DBInstanceIdentifier: aws.String("slice33-instance"),
		DBInstanceClass:      aws.String("db.r5.xlarge"),
		ApplyImmediately:     aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, "db.r5.xlarge", aws.ToString(modOut.DBInstance.DBInstanceClass))

	rebootOut, err := client.RebootDBInstance(ctx, &docdbsdk.RebootDBInstanceInput{
		DBInstanceIdentifier: aws.String("slice33-instance"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice33-instance", aws.ToString(rebootOut.DBInstance.DBInstanceIdentifier))
}

// testGlobalClusterRealClient covers FailoverGlobalCluster,
// RemoveFromGlobalCluster, SwitchoverGlobalCluster.
func testGlobalClusterRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("slice33-gc-source"),
		Engine:              aws.String("docdb"),
	})
	require.NoError(t, err)

	_, err = client.CreateGlobalCluster(ctx, &docdbsdk.CreateGlobalClusterInput{
		GlobalClusterIdentifier:   aws.String("slice33-gc"),
		SourceDBClusterIdentifier: aws.String("slice33-gc-source"),
	})
	require.NoError(t, err)

	failOut, err := client.FailoverGlobalCluster(ctx, &docdbsdk.FailoverGlobalClusterInput{
		GlobalClusterIdentifier:   aws.String("slice33-gc"),
		TargetDbClusterIdentifier: aws.String("slice33-gc-source"),
	})
	require.NoError(t, err)
	assert.Equal(t, "failing-over", aws.ToString(failOut.GlobalCluster.Status))

	switchOut, err := client.SwitchoverGlobalCluster(ctx, &docdbsdk.SwitchoverGlobalClusterInput{
		GlobalClusterIdentifier:   aws.String("slice33-gc"),
		TargetDbClusterIdentifier: aws.String("slice33-gc-source"),
	})
	require.NoError(t, err)
	assert.Equal(t, "switching-over", aws.ToString(switchOut.GlobalCluster.Status))

	remOut, err := client.RemoveFromGlobalCluster(ctx, &docdbsdk.RemoveFromGlobalClusterInput{
		GlobalClusterIdentifier: aws.String("slice33-gc"),
		DbClusterIdentifier:     aws.String("slice33-gc-source"),
	})
	require.NoError(t, err)
	assert.Empty(t, remOut.GlobalCluster.GlobalClusterMembers)
}

// testTagsRealClient covers AddTagsToResource and RemoveTagsFromResource.
func testTagsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	createOut, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("slice33-tag-cluster"),
		Engine:              aws.String("docdb"),
	})
	require.NoError(t, err)
	clusterARN := aws.ToString(createOut.DBCluster.DBClusterArn)

	_, err = client.AddTagsToResource(ctx, &docdbsdk.AddTagsToResourceInput{
		ResourceName: aws.String(clusterARN),
		Tags: []types.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
			{Key: aws.String("team"), Value: aws.String("data")},
		},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(ctx, &docdbsdk.ListTagsForResourceInput{
		ResourceName: aws.String(clusterARN),
	})
	require.NoError(t, err)
	require.Len(t, listOut.TagList, 2)

	_, err = client.RemoveTagsFromResource(ctx, &docdbsdk.RemoveTagsFromResourceInput{
		ResourceName: aws.String(clusterARN),
		TagKeys:      []string{"team"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListTagsForResource(ctx, &docdbsdk.ListTagsForResourceInput{
		ResourceName: aws.String(clusterARN),
	})
	require.NoError(t, err)
	require.Len(t, listOut2.TagList, 1)
	assert.Equal(t, "env", aws.ToString(listOut2.TagList[0].Key))
}
