package memorydb_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	memorydbsdk "github.com/aws/aws-sdk-go-v2/service/memorydb"
	memorydbtypes "github.com/aws/aws-sdk-go-v2/service/memorydb/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// newTestMemoryDBClient stands up the real aws-sdk-go-v2 memorydb client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production -- round-tripping
// through the genuine SDK serializer/deserializer, not ad-hoc JSON structs.
func newTestMemoryDBClient(t *testing.T, h *memorydb.Handler) *memorydbsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return memorydbsdk.NewFromConfig(cfg, func(o *memorydbsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// Test_SDKRoundTrip_SubnetGroupLifecycle drives Create/Describe/Update/Delete
// SubnetGroup through the real typed client and asserts decoded values.
func Test_SDKRoundTrip_SubnetGroupLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestMemoryDBClient(t, h)
	ctx := t.Context()

	created, err := client.CreateSubnetGroup(ctx, &memorydbsdk.CreateSubnetGroupInput{
		SubnetGroupName: aws.String("sg-31"),
		Description:     aws.String("slice31 subnet group"),
		SubnetIds:       []string{"subnet-aaa", "subnet-bbb"},
	})
	require.NoError(t, err)
	require.NotNil(t, created.SubnetGroup)
	assert.Equal(t, "sg-31", aws.ToString(created.SubnetGroup.Name))
	assert.Equal(t, "slice31 subnet group", aws.ToString(created.SubnetGroup.Description))
	require.Len(t, created.SubnetGroup.Subnets, 2)
	assert.NotEmpty(t, aws.ToString(created.SubnetGroup.Subnets[0].AvailabilityZone.Name))
	assert.Equal(t,
		[]memorydbtypes.NetworkType{memorydbtypes.NetworkTypeIpv4},
		created.SubnetGroup.SupportedNetworkTypes,
	)

	described, err := client.DescribeSubnetGroups(ctx, &memorydbsdk.DescribeSubnetGroupsInput{
		SubnetGroupName: aws.String("sg-31"),
	})
	require.NoError(t, err)
	require.Len(t, described.SubnetGroups, 1)
	assert.Equal(t, "sg-31", aws.ToString(described.SubnetGroups[0].Name))

	updated, err := client.UpdateSubnetGroup(ctx, &memorydbsdk.UpdateSubnetGroupInput{
		SubnetGroupName: aws.String("sg-31"),
		Description:     aws.String("updated description"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated description", aws.ToString(updated.SubnetGroup.Description))

	deleted, err := client.DeleteSubnetGroup(ctx, &memorydbsdk.DeleteSubnetGroupInput{
		SubnetGroupName: aws.String("sg-31"),
	})
	require.NoError(t, err)
	assert.Equal(t, "sg-31", aws.ToString(deleted.SubnetGroup.Name))
}

// Test_SDKRoundTrip_UserLifecycle drives Create/Describe/Update/Delete User
// through the real typed client, including the ACLNames-after-UpdateUser bug
// (handleUpdateUser previously hardcoded ACLNames to []string{} regardless of
// the user's real ACL membership -- types.User.ACLNames per
// aws-sdk-go-v2/service/memorydb@v1.36.4 types/types.go:910, "The names of
// the Access Control Lists to which the user belongs").
func Test_SDKRoundTrip_UserLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestMemoryDBClient(t, h)
	ctx := t.Context()

	created, err := client.CreateUser(ctx, &memorydbsdk.CreateUserInput{
		UserName:     aws.String("user-31"),
		AccessString: aws.String("on ~* &* +@all"),
		AuthenticationMode: &memorydbtypes.AuthenticationMode{
			Type: memorydbtypes.InputAuthenticationTypeIam,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.User)
	assert.Equal(t, "user-31", aws.ToString(created.User.Name))
	assert.Empty(t, created.User.ACLNames, "freshly created user belongs to no ACL yet")

	_, err = client.CreateACL(ctx, &memorydbsdk.CreateACLInput{
		ACLName:   aws.String("acl-31"),
		UserNames: []string{"user-31"},
	})
	require.NoError(t, err)

	updated, err := client.UpdateUser(ctx, &memorydbsdk.UpdateUserInput{
		UserName:     aws.String("user-31"),
		AccessString: aws.String("on ~* &* +@read"),
	})
	require.NoError(t, err)
	assert.Equal(t, "on ~* &* +@read", aws.ToString(updated.User.AccessString))
	assert.Equal(t, []string{"acl-31"}, updated.User.ACLNames,
		"UpdateUser must reflect the user's real current ACL membership, not always empty")

	described, err := client.DescribeUsers(
		ctx,
		&memorydbsdk.DescribeUsersInput{UserName: aws.String("user-31")},
	)
	require.NoError(t, err)
	require.Len(t, described.Users, 1)
	assert.Equal(t, []string{"acl-31"}, described.Users[0].ACLNames)

	deleted, err := client.DeleteUser(
		ctx,
		&memorydbsdk.DeleteUserInput{UserName: aws.String("user-31")},
	)
	require.NoError(t, err)
	assert.Equal(t, "user-31", aws.ToString(deleted.User.Name))
	assert.Empty(t, deleted.User.ACLNames, "DeleteUser cascades ACL removal before returning")
}

// Test_SDKRoundTrip_ACLUpdate drives UpdateACL through the real typed client.
func Test_SDKRoundTrip_ACLUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestMemoryDBClient(t, h)
	ctx := t.Context()

	_, err := client.CreateUser(ctx, &memorydbsdk.CreateUserInput{
		UserName:     aws.String("acl-user-a"),
		AccessString: aws.String("on ~* &* +@all"),
		AuthenticationMode: &memorydbtypes.AuthenticationMode{
			Type: memorydbtypes.InputAuthenticationTypeIam,
		},
	})
	require.NoError(t, err)

	_, err = client.CreateACL(
		ctx,
		&memorydbsdk.CreateACLInput{ACLName: aws.String("acl-update-31")},
	)
	require.NoError(t, err)

	updated, err := client.UpdateACL(ctx, &memorydbsdk.UpdateACLInput{
		ACLName:        aws.String("acl-update-31"),
		UserNamesToAdd: []string{"acl-user-a"},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.ACL)
	assert.Equal(t, []string{"acl-user-a"}, updated.ACL.UserNames)
	assert.Equal(t, "acl-update-31", aws.ToString(updated.ACL.Name))
}

// Test_SDKRoundTrip_ParameterGroupLifecycle drives Create/Describe/Update/
// Reset/Delete ParameterGroup plus DescribeParameters through the real typed
// client.
func Test_SDKRoundTrip_ParameterGroupLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestMemoryDBClient(t, h)
	ctx := t.Context()

	created, err := client.CreateParameterGroup(ctx, &memorydbsdk.CreateParameterGroupInput{
		ParameterGroupName: aws.String("pg-31"),
		Family:             aws.String("memorydb_redis7"),
		Description:        aws.String("slice31 pg"),
	})
	require.NoError(t, err)
	assert.Equal(t, "memorydb_redis7", aws.ToString(created.ParameterGroup.Family))

	_, err = client.UpdateParameterGroup(ctx, &memorydbsdk.UpdateParameterGroupInput{
		ParameterGroupName: aws.String("pg-31"),
		ParameterNameValues: []memorydbtypes.ParameterNameValue{
			{
				ParameterName:  aws.String("maxmemory-policy"),
				ParameterValue: aws.String("allkeys-lru"),
			},
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeParameterGroups(ctx, &memorydbsdk.DescribeParameterGroupsInput{
		ParameterGroupName: aws.String("pg-31"),
	})
	require.NoError(t, err)
	require.Len(t, described.ParameterGroups, 1)

	params, err := client.DescribeParameters(ctx, &memorydbsdk.DescribeParametersInput{
		ParameterGroupName: aws.String("pg-31"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, params.Parameters)

	var found bool
	for _, p := range params.Parameters {
		if aws.ToString(p.Name) == "maxmemory-policy" {
			found = true
			assert.Equal(t, "allkeys-lru", aws.ToString(p.Value))
		}
	}
	assert.True(t, found, "updated parameter must appear in DescribeParameters")

	reset, err := client.ResetParameterGroup(ctx, &memorydbsdk.ResetParameterGroupInput{
		ParameterGroupName: aws.String("pg-31"),
		AllParameters:      true,
	})
	require.NoError(t, err)
	assert.Equal(t, "pg-31", aws.ToString(reset.ParameterGroup.Name))

	deleted, err := client.DeleteParameterGroup(ctx, &memorydbsdk.DeleteParameterGroupInput{
		ParameterGroupName: aws.String("pg-31"),
	})
	require.NoError(t, err)
	assert.Equal(t, "pg-31", aws.ToString(deleted.ParameterGroup.Name))
}

// Test_SDKRoundTrip_SnapshotLifecycle drives CreateCluster, CreateSnapshot,
// CopySnapshot, DescribeSnapshots, DeleteSnapshot through the real typed
// client.
func Test_SDKRoundTrip_SnapshotLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestMemoryDBClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
		ClusterName: aws.String("snap-cluster-31"),
		NodeType:    aws.String("db.r6g.large"),
		ACLName:     aws.String("open-access"),
	})
	require.NoError(t, err)

	created, err := client.CreateSnapshot(ctx, &memorydbsdk.CreateSnapshotInput{
		ClusterName:  aws.String("snap-cluster-31"),
		SnapshotName: aws.String("snap-31"),
	})
	require.NoError(t, err)
	require.NotNil(t, created.Snapshot)
	assert.Equal(t, "snap-31", aws.ToString(created.Snapshot.Name))

	copied, err := client.CopySnapshot(ctx, &memorydbsdk.CopySnapshotInput{
		SourceSnapshotName: aws.String("snap-31"),
		TargetSnapshotName: aws.String("snap-31-copy"),
	})
	require.NoError(t, err)
	assert.Equal(t, "snap-31-copy", aws.ToString(copied.Snapshot.Name))

	described, err := client.DescribeSnapshots(ctx, &memorydbsdk.DescribeSnapshotsInput{
		ClusterName: aws.String("snap-cluster-31"),
	})
	require.NoError(t, err)
	assert.Len(t, described.Snapshots, 2)

	deleted, err := client.DeleteSnapshot(ctx, &memorydbsdk.DeleteSnapshotInput{
		SnapshotName: aws.String("snap-31"),
	})
	require.NoError(t, err)
	assert.Equal(t, "snap-31", aws.ToString(deleted.Snapshot.Name))
}

// Test_SDKRoundTrip_MultiRegionClusterLifecycle drives
// CreateMultiRegionCluster, UpdateMultiRegionCluster,
// ListAllowedMultiRegionClusterUpdates, DeleteMultiRegionCluster through the
// real typed client.
func Test_SDKRoundTrip_MultiRegionClusterLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestMemoryDBClient(t, h)
	ctx := t.Context()

	created, err := client.CreateMultiRegionCluster(ctx, &memorydbsdk.CreateMultiRegionClusterInput{
		MultiRegionClusterNameSuffix: aws.String("mrc31"),
		NodeType:                     aws.String("db.r6g.large"),
	})
	require.NoError(t, err)
	require.NotNil(t, created.MultiRegionCluster)
	name := aws.ToString(created.MultiRegionCluster.MultiRegionClusterName)
	require.NotEmpty(t, name)

	allowed, err := client.ListAllowedMultiRegionClusterUpdates(
		ctx,
		&memorydbsdk.ListAllowedMultiRegionClusterUpdatesInput{
			MultiRegionClusterName: aws.String(name),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, allowed.ScaleUpNodeTypes)

	updated, err := client.UpdateMultiRegionCluster(ctx, &memorydbsdk.UpdateMultiRegionClusterInput{
		MultiRegionClusterName: aws.String(name),
		Description:            aws.String("updated mrc"),
	})
	require.NoError(t, err)
	assert.Equal(t, "updated mrc", aws.ToString(updated.MultiRegionCluster.Description))

	deleted, err := client.DeleteMultiRegionCluster(ctx, &memorydbsdk.DeleteMultiRegionClusterInput{
		MultiRegionClusterName: aws.String(name),
	})
	require.NoError(t, err)
	assert.Equal(t, name, aws.ToString(deleted.MultiRegionCluster.MultiRegionClusterName))
}

// Test_SDKRoundTrip_BatchUpdateClusterAndFailoverShard drives
// BatchUpdateCluster, FailoverShard, ListAllowedNodeTypeUpdates, and
// DescribeServiceUpdates through the real typed client.
func Test_SDKRoundTrip_BatchUpdateClusterAndFailoverShard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestMemoryDBClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCluster(ctx, &memorydbsdk.CreateClusterInput{
		ClusterName: aws.String("batch-cluster-31"),
		NodeType:    aws.String("db.r6g.large"),
		ACLName:     aws.String("open-access"),
	})
	require.NoError(t, err)

	updates, err := client.DescribeServiceUpdates(ctx, &memorydbsdk.DescribeServiceUpdatesInput{
		ClusterNames: []string{"batch-cluster-31"},
	})
	require.NoError(t, err)
	require.NotEmpty(
		t,
		updates.ServiceUpdates,
		"default seed service updates should fan out to the new cluster",
	)
	updateName := aws.ToString(updates.ServiceUpdates[0].ServiceUpdateName)
	require.NotEmpty(t, updateName)

	batch, err := client.BatchUpdateCluster(ctx, &memorydbsdk.BatchUpdateClusterInput{
		ClusterNames: []string{"batch-cluster-31", "no-such-cluster"},
		ServiceUpdate: &memorydbtypes.ServiceUpdateRequest{
			ServiceUpdateNameToApply: aws.String(updateName),
		},
	})
	require.NoError(t, err)
	require.Len(t, batch.ProcessedClusters, 1)
	assert.Equal(t, "batch-cluster-31", aws.ToString(batch.ProcessedClusters[0].Name))
	require.Len(t, batch.UnprocessedClusters, 1)
	assert.Equal(t, "no-such-cluster", aws.ToString(batch.UnprocessedClusters[0].ClusterName))

	failed, err := client.FailoverShard(ctx, &memorydbsdk.FailoverShardInput{
		ClusterName: aws.String("batch-cluster-31"),
		ShardName:   aws.String("0001"),
	})
	require.NoError(t, err)
	assert.Equal(t, "batch-cluster-31", aws.ToString(failed.Cluster.Name))

	allowed, err := client.ListAllowedNodeTypeUpdates(
		ctx,
		&memorydbsdk.ListAllowedNodeTypeUpdatesInput{
			ClusterName: aws.String("batch-cluster-31"),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, allowed.ScaleUpNodeTypes)
}
