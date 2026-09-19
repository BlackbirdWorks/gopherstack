package redshift_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftsdk "github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// TestRealClient_ClusterLifecycleFields proves the CreateCluster/
// ModifyCluster/RestoreFromClusterSnapshot/DeleteCluster/
// ModifyClusterIamRoles/RestoreTableFromClusterSnapshot/
// GetClusterCredentials(WithIAM) fields the reqfielddiff tier-1 sweep found
// dropped (gopherstack-xhu2t) are now declared, applied, and echoed back on
// the wire through the real typed SDK client.
func TestRealClient_ClusterLifecycleFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testCreateClusterFieldsRealClient, "create_cluster_fields"},
		{testModifyClusterFieldsRealClient, "modify_cluster_fields"},
		{testRestoreFromClusterSnapshotFieldsRealClient, "restore_from_cluster_snapshot_fields"},
		{testDeleteClusterFinalSnapshotRetentionRealClient, "delete_cluster_final_snapshot_retention"},
		{testModifyClusterIamRolesDefaultArnRealClient, "modify_cluster_iam_roles_default_arn"},
		{testRestoreTableSourceSchemaNameRealClient, "restore_table_source_schema_name"},
		{testGetClusterCredentialsDurationRealClient, "get_cluster_credentials_duration"},
		{testGetClusterCredentialsWithIAMDurationRealClient, "get_cluster_credentials_with_iam_duration"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func newLifecycleFieldsBackendAndClient(t *testing.T) (*redshift.InMemoryBackend, *redshiftsdk.Client) {
	t.Helper()

	backend := redshift.NewInMemoryBackend("000000000000", rtTestRegion)
	client := newTestRedshiftClient(t, redshift.NewHandler(backend))

	return backend, client
}

func testCreateClusterFieldsRealClient(t *testing.T) {
	t.Helper()

	_, client := newLifecycleFieldsBackendAndClient(t)
	ctx := t.Context()

	created, err := client.CreateCluster(ctx, &redshiftsdk.CreateClusterInput{
		ClusterIdentifier:                    aws.String("cluster-fields-c1"),
		NodeType:                             aws.String("dc2.large"),
		MasterUsername:                       aws.String("admin"),
		MasterUserPassword:                   aws.String("Passw0rd1!"),
		AllowVersionUpgrade:                  aws.Bool(false),
		AutomatedSnapshotRetentionPeriod:     aws.Int32(5),
		ManualSnapshotRetentionPeriod:        aws.Int32(30),
		AvailabilityZone:                     aws.String("us-east-1a"),
		DefaultIamRoleArn:                    aws.String("arn:aws:iam::000000000000:role/default-role"),
		ExtraComputeForAutomaticOptimization: aws.Bool(true),
		Port:                                 aws.Int32(5555),
		VpcSecurityGroupIds:                  []string{"sg-abc123"},
	})
	require.NoError(t, err)
	require.NotNil(t, created.Cluster)

	c := created.Cluster
	assert.False(t, aws.ToBool(c.AllowVersionUpgrade))
	assert.Equal(t, int32(5), aws.ToInt32(c.AutomatedSnapshotRetentionPeriod))
	assert.Equal(t, int32(30), aws.ToInt32(c.ManualSnapshotRetentionPeriod))
	assert.Equal(t, "us-east-1a", aws.ToString(c.AvailabilityZone))
	assert.Equal(t, "arn:aws:iam::000000000000:role/default-role", aws.ToString(c.DefaultIamRoleArn))
	assert.Equal(t, "true", aws.ToString(c.ExtraComputeForAutomaticOptimization))
	require.NotNil(t, c.Endpoint)
	assert.Equal(t, int32(5555), aws.ToInt32(c.Endpoint.Port))
	require.Len(t, c.VpcSecurityGroups, 1)
	assert.Equal(t, "sg-abc123", aws.ToString(c.VpcSecurityGroups[0].VpcSecurityGroupId))

	// A ClusterSubnetGroupName referencing a real subnet group threads
	// through too -- proven separately since it requires its own resource.
	_, err = client.CreateClusterSubnetGroup(ctx, &redshiftsdk.CreateClusterSubnetGroupInput{
		ClusterSubnetGroupName: aws.String("csg-1"),
		Description:            aws.String("test subnet group"),
		SubnetIds:              []string{"subnet-1"},
	})
	require.NoError(t, err)

	created2, err := client.CreateCluster(ctx, &redshiftsdk.CreateClusterInput{
		ClusterIdentifier:      aws.String("cluster-fields-c2"),
		NodeType:               aws.String("dc2.large"),
		MasterUsername:         aws.String("admin"),
		MasterUserPassword:     aws.String("Passw0rd1!"),
		ClusterSubnetGroupName: aws.String("csg-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "csg-1", aws.ToString(created2.Cluster.ClusterSubnetGroupName))

	// Invalid AutomatedSnapshotRetentionPeriod (out of documented 0-35 range) is rejected.
	_, err = client.CreateCluster(ctx, &redshiftsdk.CreateClusterInput{
		ClusterIdentifier:                aws.String("cluster-fields-bad"),
		NodeType:                         aws.String("dc2.large"),
		MasterUsername:                   aws.String("admin"),
		MasterUserPassword:               aws.String("Passw0rd1!"),
		AutomatedSnapshotRetentionPeriod: aws.Int32(99),
	})
	require.Error(t, err)
}

func testModifyClusterFieldsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newLifecycleFieldsBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateCluster(
		"cluster-modify-fields", "dc2.large", "dev", "admin", nil, "", redshift.CreateClusterOptions{},
	)
	require.NoError(t, err)

	_, err = client.CreateClusterParameterGroup(ctx, &redshiftsdk.CreateClusterParameterGroupInput{
		ParameterGroupName:   aws.String("pg-modify-fields"),
		ParameterGroupFamily: aws.String("redshift-1.0"),
		Description:          aws.String("test"),
	})
	require.NoError(t, err)

	modified, err := client.ModifyCluster(ctx, &redshiftsdk.ModifyClusterInput{
		ClusterIdentifier:                    aws.String("cluster-modify-fields"),
		AllowVersionUpgrade:                  aws.Bool(false),
		AutomatedSnapshotRetentionPeriod:     aws.Int32(7),
		ManualSnapshotRetentionPeriod:        aws.Int32(-1),
		ExtraComputeForAutomaticOptimization: aws.Bool(true),
		ClusterParameterGroupName:            aws.String("pg-modify-fields"),
	})
	require.NoError(t, err)
	require.NotNil(t, modified.Cluster)

	c := modified.Cluster
	assert.False(t, aws.ToBool(c.AllowVersionUpgrade))
	assert.Equal(t, int32(7), aws.ToInt32(c.AutomatedSnapshotRetentionPeriod))
	assert.Equal(t, int32(-1), aws.ToInt32(c.ManualSnapshotRetentionPeriod))
	assert.Equal(t, "true", aws.ToString(c.ExtraComputeForAutomaticOptimization))
	require.Len(t, c.ClusterParameterGroups, 1)
	assert.Equal(t, "pg-modify-fields", aws.ToString(c.ClusterParameterGroups[0].ParameterGroupName))

	// A ClusterParameterGroupName that doesn't exist is rejected, not silently accepted.
	_, err = client.ModifyCluster(ctx, &redshiftsdk.ModifyClusterInput{
		ClusterIdentifier:         aws.String("cluster-modify-fields"),
		ClusterParameterGroupName: aws.String("does-not-exist"),
	})
	require.Error(t, err)

	// ManualSnapshotRetentionPeriod outside the documented -1-or-1..3653 range is rejected.
	_, err = client.ModifyCluster(ctx, &redshiftsdk.ModifyClusterInput{
		ClusterIdentifier:             aws.String("cluster-modify-fields"),
		ManualSnapshotRetentionPeriod: aws.Int32(0),
	})
	require.Error(t, err)
}

func testRestoreFromClusterSnapshotFieldsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newLifecycleFieldsBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateCluster(
		"restore-fields-src", "dc2.large", "dev", "admin", nil, "", redshift.CreateClusterOptions{},
	)
	require.NoError(t, err)

	_, err = backend.CreateClusterSnapshot("restore-fields-snap", "restore-fields-src")
	require.NoError(t, err)

	_, err = client.CreateClusterParameterGroup(ctx, &redshiftsdk.CreateClusterParameterGroupInput{
		ParameterGroupName:   aws.String("pg-restore-fields"),
		ParameterGroupFamily: aws.String("redshift-1.0"),
		Description:          aws.String("test"),
	})
	require.NoError(t, err)

	restored, err := client.RestoreFromClusterSnapshot(ctx, &redshiftsdk.RestoreFromClusterSnapshotInput{
		ClusterIdentifier:                aws.String("restore-fields-dst"),
		SnapshotIdentifier:               aws.String("restore-fields-snap"),
		AllowVersionUpgrade:              aws.Bool(false),
		AutomatedSnapshotRetentionPeriod: aws.Int32(3),
		ManualSnapshotRetentionPeriod:    aws.Int32(90),
		AvailabilityZone:                 aws.String("us-east-1b"),
		ClusterParameterGroupName:        aws.String("pg-restore-fields"),
		DefaultIamRoleArn:                aws.String("arn:aws:iam::000000000000:role/restore-role"),
		Port:                             aws.Int32(6543),
		VpcSecurityGroupIds:              []string{"sg-restored"},
	})
	require.NoError(t, err)
	require.NotNil(t, restored.Cluster)

	c := restored.Cluster
	assert.False(t, aws.ToBool(c.AllowVersionUpgrade))
	assert.Equal(t, int32(3), aws.ToInt32(c.AutomatedSnapshotRetentionPeriod))
	assert.Equal(t, int32(90), aws.ToInt32(c.ManualSnapshotRetentionPeriod))
	assert.Equal(t, "us-east-1b", aws.ToString(c.AvailabilityZone))
	assert.Equal(t, "arn:aws:iam::000000000000:role/restore-role", aws.ToString(c.DefaultIamRoleArn))
	require.NotNil(t, c.Endpoint)
	assert.Equal(t, int32(6543), aws.ToInt32(c.Endpoint.Port))
	require.Len(t, c.VpcSecurityGroups, 1)
	assert.Equal(t, "sg-restored", aws.ToString(c.VpcSecurityGroups[0].VpcSecurityGroupId))
	require.Len(t, c.ClusterParameterGroups, 1)
	assert.Equal(t, "pg-restore-fields", aws.ToString(c.ClusterParameterGroups[0].ParameterGroupName))
}

func testDeleteClusterFinalSnapshotRetentionRealClient(t *testing.T) {
	t.Helper()

	backend, client := newLifecycleFieldsBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateCluster(
		"delete-fields-c1", "dc2.large", "dev", "admin", nil, "", redshift.CreateClusterOptions{},
	)
	require.NoError(t, err)

	_, err = client.DeleteCluster(ctx, &redshiftsdk.DeleteClusterInput{
		ClusterIdentifier:                   aws.String("delete-fields-c1"),
		SkipFinalClusterSnapshot:            aws.Bool(false),
		FinalClusterSnapshotIdentifier:      aws.String("delete-fields-final-snap"),
		FinalClusterSnapshotRetentionPeriod: aws.Int32(45),
	})
	require.NoError(t, err)

	desc, err := client.DescribeClusterSnapshots(ctx, &redshiftsdk.DescribeClusterSnapshotsInput{
		SnapshotIdentifier: aws.String("delete-fields-final-snap"),
	})
	require.NoError(t, err)
	require.Len(t, desc.Snapshots, 1)
	assert.Equal(t, int32(45), aws.ToInt32(desc.Snapshots[0].ManualSnapshotRetentionPeriod))

	// A FinalClusterSnapshotRetentionPeriod outside -1-or-1..3653 is rejected.
	_, err = backend.CreateCluster(
		"delete-fields-c2", "dc2.large", "dev", "admin", nil, "", redshift.CreateClusterOptions{},
	)
	require.NoError(t, err)

	_, err = client.DeleteCluster(ctx, &redshiftsdk.DeleteClusterInput{
		ClusterIdentifier:                   aws.String("delete-fields-c2"),
		SkipFinalClusterSnapshot:            aws.Bool(false),
		FinalClusterSnapshotIdentifier:      aws.String("delete-fields-bad-snap"),
		FinalClusterSnapshotRetentionPeriod: aws.Int32(0),
	})
	require.Error(t, err)
}

func testModifyClusterIamRolesDefaultArnRealClient(t *testing.T) {
	t.Helper()

	backend, client := newLifecycleFieldsBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateCluster(
		"iam-roles-fields-c1", "dc2.large", "dev", "admin", nil, "", redshift.CreateClusterOptions{},
	)
	require.NoError(t, err)

	modified, err := client.ModifyClusterIamRoles(ctx, &redshiftsdk.ModifyClusterIamRolesInput{
		ClusterIdentifier: aws.String("iam-roles-fields-c1"),
		AddIamRoles:       []string{"arn:aws:iam::000000000000:role/added-role"},
		DefaultIamRoleArn: aws.String("arn:aws:iam::000000000000:role/default-role"),
	})
	require.NoError(t, err)
	require.NotNil(t, modified.Cluster)
	assert.Equal(t, "arn:aws:iam::000000000000:role/default-role", aws.ToString(modified.Cluster.DefaultIamRoleArn))

	desc, err := client.DescribeClusters(ctx, &redshiftsdk.DescribeClustersInput{
		ClusterIdentifier: aws.String("iam-roles-fields-c1"),
	})
	require.NoError(t, err)
	require.Len(t, desc.Clusters, 1)
	assert.Equal(t, "arn:aws:iam::000000000000:role/default-role", aws.ToString(desc.Clusters[0].DefaultIamRoleArn))
}

func testRestoreTableSourceSchemaNameRealClient(t *testing.T) {
	t.Helper()

	backend, client := newLifecycleFieldsBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateCluster(
		"table-restore-fields-c1", "dc2.large", "dev", "admin", nil, "", redshift.CreateClusterOptions{},
	)
	require.NoError(t, err)

	_, err = backend.CreateClusterSnapshot("table-restore-fields-snap", "table-restore-fields-c1")
	require.NoError(t, err)

	restored, err := client.RestoreTableFromClusterSnapshot(ctx, &redshiftsdk.RestoreTableFromClusterSnapshotInput{
		ClusterIdentifier:  aws.String("table-restore-fields-c1"),
		SnapshotIdentifier: aws.String("table-restore-fields-snap"),
		SourceDatabaseName: aws.String("db1"),
		SourceSchemaName:   aws.String("custom_src_schema"),
		SourceTableName:    aws.String("t1"),
		TargetDatabaseName: aws.String("db1"),
		TargetSchemaName:   aws.String("custom_dst_schema"),
		NewTableName:       aws.String("t1_restored"),
	})
	require.NoError(t, err)
	require.NotNil(t, restored.TableRestoreStatus)
	assert.Equal(t, "custom_src_schema", aws.ToString(restored.TableRestoreStatus.SourceSchemaName))
	assert.Equal(t, "custom_dst_schema", aws.ToString(restored.TableRestoreStatus.TargetSchemaName))

	// Omitting SourceSchemaName/TargetSchemaName falls back to the documented "public" default.
	restored2, err := client.RestoreTableFromClusterSnapshot(ctx, &redshiftsdk.RestoreTableFromClusterSnapshotInput{
		ClusterIdentifier:  aws.String("table-restore-fields-c1"),
		SnapshotIdentifier: aws.String("table-restore-fields-snap"),
		SourceDatabaseName: aws.String("db1"),
		SourceTableName:    aws.String("t2"),
		TargetDatabaseName: aws.String("db1"),
		NewTableName:       aws.String("t2_restored"),
	})
	require.NoError(t, err)
	assert.Equal(t, "public", aws.ToString(restored2.TableRestoreStatus.SourceSchemaName))
	assert.Equal(t, "public", aws.ToString(restored2.TableRestoreStatus.TargetSchemaName))
}

func testGetClusterCredentialsDurationRealClient(t *testing.T) {
	t.Helper()

	backend, client := newLifecycleFieldsBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateCluster(
		"creds-fields-c1", "dc2.large", "dev", "admin", nil, "", redshift.CreateClusterOptions{},
	)
	require.NoError(t, err)

	before := time.Now()

	creds, err := client.GetClusterCredentials(ctx, &redshiftsdk.GetClusterCredentialsInput{
		ClusterIdentifier: aws.String("creds-fields-c1"),
		DbUser:            aws.String("dbuser1"),
		DurationSeconds:   aws.Int32(1800),
	})
	require.NoError(t, err)
	require.NotNil(t, creds.Expiration)

	gotDuration := creds.Expiration.Sub(before)
	assert.InDelta(t, 1800, gotDuration.Seconds(), 5)

	// Below the documented minimum (900) is rejected.
	_, err = client.GetClusterCredentials(ctx, &redshiftsdk.GetClusterCredentialsInput{
		ClusterIdentifier: aws.String("creds-fields-c1"),
		DbUser:            aws.String("dbuser1"),
		DurationSeconds:   aws.Int32(100),
	})
	require.Error(t, err)
}

func testGetClusterCredentialsWithIAMDurationRealClient(t *testing.T) {
	t.Helper()

	backend, client := newLifecycleFieldsBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateCluster(
		"creds-iam-fields-c1", "dc2.large", "dev", "admin", nil, "", redshift.CreateClusterOptions{},
	)
	require.NoError(t, err)

	before := time.Now()

	creds, err := client.GetClusterCredentialsWithIAM(ctx, &redshiftsdk.GetClusterCredentialsWithIAMInput{
		ClusterIdentifier: aws.String("creds-iam-fields-c1"),
		DurationSeconds:   aws.Int32(3600),
	})
	require.NoError(t, err)
	require.NotNil(t, creds.Expiration)

	gotDuration := creds.Expiration.Sub(before)
	assert.InDelta(t, 3600, gotDuration.Seconds(), 5)

	// Above the documented maximum (3600) is rejected.
	_, err = client.GetClusterCredentialsWithIAM(ctx, &redshiftsdk.GetClusterCredentialsWithIAMInput{
		ClusterIdentifier: aws.String("creds-iam-fields-c1"),
		DurationSeconds:   aws.Int32(9999),
	})
	require.Error(t, err)
}
