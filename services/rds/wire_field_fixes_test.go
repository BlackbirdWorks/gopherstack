package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeDBInstances_OptionGroupMemberships_RealClient covers a layer-3
// bug (gopherstack-g8k9): DBInstance.OptionGroupName is real, settable state
// -- CreateDBInstance and ModifyDBInstance both store it (db_instances.go) --
// but toXMLInstance never emitted it in any response. A real client's
// DBInstance.OptionGroupMemberships was always empty regardless of what
// OptionGroupName had been set at creation. Real wrapper/item shape confirmed
// against rds@v1.124.1 deserializers.go: DescribeDBInstances' DBInstance
// deserializer reads "OptionGroupMemberships" (case-insensitive query
// protocol) wrapping a list of OptionGroupMembership{OptionGroupName,Status}
// (deserializers.go:48533, 48554).
func TestDescribeDBInstances_OptionGroupMemberships_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler())
	ctx := t.Context()

	_, err := client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("db-with-option-group"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("mysql"),
		OptionGroupName:      aws.String("my-custom-og"),
	})
	require.NoError(t, err)

	out, err := client.DescribeDBInstances(ctx, &rdssdk.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String("db-with-option-group"),
	})
	require.NoError(t, err)
	require.Len(t, out.DBInstances, 1)

	inst := out.DBInstances[0]
	require.Len(t, inst.OptionGroupMemberships, 1,
		"OptionGroupMemberships must round-trip through Create -> Describe; pre-fix it was always empty")
	assert.Equal(t, "my-custom-og", aws.ToString(inst.OptionGroupMemberships[0].OptionGroupName))
	assert.Equal(t, "in-sync", aws.ToString(inst.OptionGroupMemberships[0].Status))
}

// TestDescribeDBClusters_HTTPEndpointEnabled_RealClient covers a layer-3 bug
// (gopherstack-g8k9): DBCluster.HTTPEndpointEnabled is real, live-toggled
// state -- EnableHttpEndpoint/DisableHttpEndpoint (data_api.go) flip it, and
// the RDS Data API presumably gates on it -- but toXMLCluster never emitted
// it, so a real client's DBCluster.HttpEndpointEnabled was always false
// (the Go zero value) regardless of what EnableHttpEndpoint had set. Real
// field name "HttpEndpointEnabled" confirmed against rds@v1.124.1
// deserializers.go's awsAwsquery_deserializeDocumentDBCluster.
func TestDescribeDBClusters_HTTPEndpointEnabled_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler())
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("http-endpoint-cluster"),
		Engine:              aws.String("aurora-postgresql"),
		MasterUsername:      aws.String("admin"),
	})
	require.NoError(t, err)

	describeBefore, err := client.DescribeDBClusters(ctx, &rdssdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String("http-endpoint-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, describeBefore.DBClusters, 1)
	assert.False(t, aws.ToBool(describeBefore.DBClusters[0].HttpEndpointEnabled))

	_, err = client.EnableHttpEndpoint(ctx, &rdssdk.EnableHttpEndpointInput{
		ResourceArn: describeBefore.DBClusters[0].DBClusterArn,
	})
	require.NoError(t, err)

	describeAfter, err := client.DescribeDBClusters(ctx, &rdssdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String("http-endpoint-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, describeAfter.DBClusters, 1)
	assert.True(t, aws.ToBool(describeAfter.DBClusters[0].HttpEndpointEnabled),
		"HttpEndpointEnabled must reflect EnableHttpEndpoint; pre-fix DescribeDBClusters never emitted it at all")
}
