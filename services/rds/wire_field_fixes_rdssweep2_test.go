package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// TestDescribeDBClusters_ServerlessCapacity_RealClient covers a missing-field
// bug: DBCluster.ServerlessCapacity is real, live-toggled state (set by
// ModifyCurrentDBClusterCapacity) but toXMLCluster never emitted it. Real
// field name "Capacity" confirmed against rds@v1.124.1 deserializers.go's
// awsAwsquery_deserializeDocumentDBCluster (case "Capacity").
func TestDescribeDBClusters_ServerlessCapacity_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler())
	ctx := t.Context()

	clusterID := "capacity-cluster"
	_, err := client.CreateDBCluster(ctx, &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String(clusterID),
		Engine:              aws.String("aurora-postgresql"),
		MasterUsername:      aws.String("admin"),
		MasterUserPassword:  aws.String("SuperSecret123!"),
		EngineMode:          aws.String("serverless"),
	})
	require.NoError(t, err)

	_, err = client.ModifyCurrentDBClusterCapacity(ctx, &rdssdk.ModifyCurrentDBClusterCapacityInput{
		DBClusterIdentifier: aws.String(clusterID),
		Capacity:            aws.Int32(8),
	})
	require.NoError(t, err)

	out, err := client.DescribeDBClusters(ctx, &rdssdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err)
	require.Len(t, out.DBClusters, 1)
	assert.Equal(t, int32(8), aws.ToInt32(out.DBClusters[0].Capacity),
		"Capacity zero - DescribeDBClusters dropped ServerlessCapacity entirely")
}

// TestDescribeDBClusters_MemberParamGroupStatus_RealClient covers a
// wrong-key bug: gopherstack emitted a cluster member's parameter group
// under <DBClusterParameterGroupName>, but the real DBClusterMember
// deserializer only recognizes <DBClusterParameterGroupStatus>
// (rds@v1.124.1 deserializers.go, awsAwsquery_deserializeDocumentDBClusterMember).
// A real client always saw an empty DBClusterParameterGroupStatus.
func TestDescribeDBClusters_MemberParamGroupStatus_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler())
	ctx := t.Context()

	clusterID := "member-status-cluster"
	_, err := client.CreateDBCluster(ctx, &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String(clusterID),
		Engine:              aws.String("aurora-postgresql"),
		MasterUsername:      aws.String("admin"),
		MasterUserPassword:  aws.String("SuperSecret123!"),
	})
	require.NoError(t, err)

	_, err = client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("member-status-inst"),
		DBInstanceClass:      aws.String("db.r6g.large"),
		Engine:               aws.String("aurora-postgresql"),
		DBClusterIdentifier:  aws.String(clusterID),
	})
	require.NoError(t, err)

	out, err := client.DescribeDBClusters(ctx, &rdssdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err)
	require.Len(t, out.DBClusters, 1)
	require.Len(t, out.DBClusters[0].DBClusterMembers, 1)
	assert.Equal(t, "in-sync", aws.ToString(out.DBClusters[0].DBClusterMembers[0].DBClusterParameterGroupStatus),
		"DBClusterParameterGroupStatus empty - DescribeDBClusters emitted the wrong wire key for cluster members")
}

// TestDescribeTenantDatabases_TenantDBName_RealClient covers a wrong-key bug:
// gopherstack emitted the tenant database name under <TenantDatabaseName>,
// but the real RDS TenantDatabase deserializer only recognizes
// <TenantDBName> (rds@v1.124.1 deserializers.go, case "TenantDBName" in
// awsAwsquery_deserializeDocumentTenantDatabase). A real client always saw an
// empty TenantDBName.
func TestDescribeTenantDatabases_TenantDBName_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler())
	ctx := t.Context()

	_, err := client.CreateTenantDatabase(ctx, &rdssdk.CreateTenantDatabaseInput{
		DBInstanceIdentifier: aws.String("db-1"),
		TenantDBName:         aws.String("mytenant"),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("SuperSecret123!"),
	})
	require.NoError(t, err)

	out, err := client.DescribeTenantDatabases(ctx, &rdssdk.DescribeTenantDatabasesInput{
		DBInstanceIdentifier: aws.String("db-1"),
	})
	require.NoError(t, err)
	require.Len(t, out.TenantDatabases, 1)
	assert.Equal(t, "mytenant", aws.ToString(out.TenantDatabases[0].TenantDBName),
		"TenantDBName empty - DescribeTenantDatabases emitted the wrong wire key")
}

// TestDescribeDBSnapshotTenantDatabases_TenantDBName_RealClient is the same
// wrong-key bug as above but on the DescribeDBSnapshotTenantDatabases path,
// whose xmlDBSnapshotTenantDatabase struct had the identical
// TenantDatabaseName-instead-of-TenantDBName mistake.
func TestDescribeDBSnapshotTenantDatabases_TenantDBName_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)
	ctx := t.Context()

	h.Backend.AddDBSnapshotTenantDatabase("snap-1", "db-1", "snaptenant", "postgres")

	out, err := client.DescribeDBSnapshotTenantDatabases(ctx, &rdssdk.DescribeDBSnapshotTenantDatabasesInput{
		DBSnapshotIdentifier: aws.String("snap-1"),
	})
	require.NoError(t, err)
	require.Len(t, out.DBSnapshotTenantDatabases, 1)
	assert.Equal(t, "snaptenant", aws.ToString(out.DBSnapshotTenantDatabases[0].TenantDBName),
		"TenantDBName empty - DescribeDBSnapshotTenantDatabases emitted the wrong wire key")
}

// TestDescribeGlobalClusters_GlobalWriteForwardingStatus_RealClient covers a
// wrong-type bug: gopherstack emitted GlobalClusterMember's write-forwarding
// field as a bool, but the real type is types.WriteForwardingStatus, a string
// enum whose only members are enabled/disabled/enabling/disabling/unknown
// (rds@v1.124.1 types/enums.go); a bool marshals to "true"/"false", neither a
// valid member.
func TestDescribeGlobalClusters_GlobalWriteForwardingStatus_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want                  types.WriteForwardingStatus
		name                  string
		globalWriteForwarding bool
	}{
		{name: "enabled", globalWriteForwarding: true, want: types.WriteForwardingStatusEnabled},
		{name: "disabled", globalWriteForwarding: false, want: types.WriteForwardingStatusDisabled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestRDSHandler()
			client := newTestRDSClient(t, h)
			ctx := t.Context()

			globalClusterID := "gwf-" + tt.name
			_, err := client.CreateGlobalCluster(ctx, &rdssdk.CreateGlobalClusterInput{
				GlobalClusterIdentifier: aws.String(globalClusterID),
			})
			require.NoError(t, err)

			h.Backend.AddGlobalClusterMemberInternal(globalClusterID, rds.GlobalClusterMember{
				DBClusterArn:          "arn:aws:rds:us-east-1:123456789012:cluster:member-1",
				GlobalWriteForwarding: tt.globalWriteForwarding,
				IsWriter:              true,
			})

			out, err := client.DescribeGlobalClusters(ctx, &rdssdk.DescribeGlobalClustersInput{
				GlobalClusterIdentifier: aws.String(globalClusterID),
			})
			require.NoError(t, err)
			require.Len(t, out.GlobalClusters, 1)
			require.Len(t, out.GlobalClusters[0].GlobalClusterMembers, 1)
			assert.Equal(t, tt.want, out.GlobalClusters[0].GlobalClusterMembers[0].GlobalWriteForwardingStatus,
				"GlobalWriteForwardingStatus not a valid WriteForwardingStatus enum member")
		})
	}
}

// TestDescribeDBInstances_Filters_RealClient covers a wrong-key bug:
// parseDescribeFilters read Filters.Filter.N.Values.member.M, but
// awsAwsquery_serializeDocumentFilterValueList (rds@v1.124.1
// serializers.go:11730) puts the real aws-sdk-go-v2 client's Values array
// under Filters.Filter.N.Values.Value.M -- "member" never appears on the
// wire for this shape. A real client's engine filter therefore matched
// nothing, so a Describe that should exclude the non-matching instance
// silently dropped every instance instead.
func TestDescribeDBInstances_Filters_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("filt-real-mysql"),
		Engine:               aws.String("mysql"),
		DBInstanceClass:      aws.String("db.t3.micro"),
	})
	require.NoError(t, err)

	_, err = client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("filt-real-postgres"),
		Engine:               aws.String("postgres"),
		DBInstanceClass:      aws.String("db.t3.micro"),
	})
	require.NoError(t, err)

	out, err := client.DescribeDBInstances(ctx, &rdssdk.DescribeDBInstancesInput{
		Filters: []types.Filter{
			{Name: aws.String("engine"), Values: []string{"mysql"}},
		},
	})
	require.NoError(t, err)

	gotIDs := make([]string, 0, len(out.DBInstances))
	for _, inst := range out.DBInstances {
		gotIDs = append(gotIDs, aws.ToString(inst.DBInstanceIdentifier))
	}
	assert.ElementsMatch(t, []string{"filt-real-mysql"}, gotIDs,
		"engine=mysql filter must include the matching instance and exclude the postgres one")
}
