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

// TestRealClient_CreateModifyRestoreFields proves the reqfielddiff tier-1 fix
// (gopherstack-xhu2t) for the "documented default" request-field family: ~77
// Create/Modify/Restore fields across DB clusters, DB instances, global
// clusters, DB proxies, custom engine versions, certificates and activity
// streams were declared on the wire but never read, applied to backend
// state, or echoed in the response. Each subtest sets one or more of these
// fields through the real typed client and asserts the stored/echoed effect.
func TestRealClient_CreateModifyRestoreFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testCreateDBClusterFieldsRealClient, "create_db_cluster_fields"},
		{testModifyDBClusterFieldsRealClient, "modify_db_cluster_fields"},
		{testCreateDBInstanceFieldsRealClient, "create_db_instance_fields"},
		{testModifyDBInstanceFieldsRealClient, "modify_db_instance_fields"},
		{testCreateDBInstanceReadReplicaFieldsRealClient, "create_read_replica_fields"},
		{testRestoreDBInstanceFromDBSnapshotFieldsRealClient, "restore_instance_from_snapshot_fields"},
		{testRestoreDBInstanceToPointInTimeFieldsRealClient, "restore_instance_to_point_in_time_fields"},
		{testRestoreDBInstanceFromS3FieldsRealClient, "restore_instance_from_s3_fields"},
		{testRestoreDBClusterFromSnapshotFieldsRealClient, "restore_cluster_from_snapshot_fields"},
		{testRestoreDBClusterToPointInTimeFieldsRealClient, "restore_cluster_to_point_in_time_fields"},
		{testRestoreDBClusterFromS3FieldsRealClient, "restore_cluster_from_s3_fields"},
		{testDeleteDBClusterAutomatedBackupsRealClient, "delete_cluster_automated_backups"},
		{testCopyDBClusterSnapshotTagsRealClient, "copy_cluster_snapshot_tags"},
		{testGlobalClusterFieldsRealClient, "global_cluster_fields"},
		{testDBProxyFieldsRealClient, "db_proxy_fields"},
		{testCreateCustomDBEngineVersionImageIDRealClient, "custom_engine_version_image_id"},
		{testDescribeDBEngineVersionsDefaultOnlyRealClient, "describe_db_engine_versions_default_only"},
		{testModifyCertificatesRemoveOverrideRealClient, "modify_certificates_remove_override"},
		{testStartActivityStreamNativeAuditFieldsRealClient, "start_activity_stream_native_audit_fields"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testCreateDBClusterFieldsRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	out, err := client.CreateDBCluster(ctx, &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier:                aws.String("fields-cluster"),
		Engine:                             aws.String("aurora-postgresql"),
		MasterUsername:                     aws.String("admin"),
		AutoMinorVersionUpgrade:            aws.Bool(true),
		EnableHttpEndpoint:                 aws.Bool(true),
		OptionGroupName:                    aws.String("my-cluster-og"),
		PubliclyAccessible:                 aws.Bool(true),
		EnableIAMDatabaseAuthentication:    aws.Bool(true),
		EnableGlobalWriteForwarding:        aws.Bool(true),
		EnableLocalWriteForwarding:         aws.Bool(true),
		EnablePerformanceInsights:          aws.Bool(true),
		PerformanceInsightsKMSKeyId:        aws.String("arn:aws:kms:us-east-1:123456789012:key/pi-key"),
		PerformanceInsightsRetentionPeriod: aws.Int32(731),
		ClusterScalabilityType:             types.ClusterScalabilityTypeStandard,
	})
	require.NoError(t, err)

	c := out.DBCluster
	assert.True(t, aws.ToBool(c.AutoMinorVersionUpgrade))
	assert.True(t, aws.ToBool(c.HttpEndpointEnabled))
	require.NotNil(t, c.DBClusterOptionGroupMemberships)
	require.Len(t, c.DBClusterOptionGroupMemberships, 1)
	assert.Equal(t, "my-cluster-og", aws.ToString(c.DBClusterOptionGroupMemberships[0].DBClusterOptionGroupName))
	assert.True(t, aws.ToBool(c.PubliclyAccessible))
	assert.True(t, aws.ToBool(c.IAMDatabaseAuthenticationEnabled))
	assert.True(t, aws.ToBool(c.GlobalWriteForwardingRequested))
	assert.Equal(t, types.LocalWriteForwardingStatusEnabled, c.LocalWriteForwardingStatus)
	assert.True(t, aws.ToBool(c.PerformanceInsightsEnabled))
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/pi-key", aws.ToString(c.PerformanceInsightsKMSKeyId))
	assert.Equal(t, int32(731), aws.ToInt32(c.PerformanceInsightsRetentionPeriod))
	assert.Equal(t, types.ClusterScalabilityTypeStandard, c.ClusterScalabilityType)
}

func testModifyDBClusterFieldsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBCluster(
		"mod-fields-cluster", "aurora-postgresql", "admin", "mydb", "", 5432, nil, rds.DBClusterOptions{},
	)
	require.NoError(t, err)
	_, err = backend.CreateDBInstance(
		"mod-fields-member", "aurora-postgresql", "db.r5.large", "", "admin", "",
		20, rds.DBInstanceOptions{DBClusterIdentifier: "mod-fields-cluster"},
	)
	require.NoError(t, err)

	out, err := client.ModifyDBCluster(ctx, &rdssdk.ModifyDBClusterInput{
		DBClusterIdentifier:                aws.String("mod-fields-cluster"),
		AutoMinorVersionUpgrade:            aws.Bool(true),
		EnableHttpEndpoint:                 aws.Bool(true),
		OptionGroupName:                    aws.String("mod-cluster-og"),
		EnableIAMDatabaseAuthentication:    aws.Bool(true),
		EnableGlobalWriteForwarding:        aws.Bool(true),
		EnablePerformanceInsights:          aws.Bool(true),
		PerformanceInsightsKMSKeyId:        aws.String("arn:aws:kms:us-east-1:123456789012:key/pi-key2"),
		PerformanceInsightsRetentionPeriod: aws.Int32(7),
		DBInstanceParameterGroupName:       aws.String("cascaded-pg"),
	})
	require.NoError(t, err)

	c := out.DBCluster
	assert.True(t, aws.ToBool(c.AutoMinorVersionUpgrade))
	assert.True(t, aws.ToBool(c.HttpEndpointEnabled))
	require.Len(t, c.DBClusterOptionGroupMemberships, 1)
	assert.Equal(t, "mod-cluster-og", aws.ToString(c.DBClusterOptionGroupMemberships[0].DBClusterOptionGroupName))
	assert.True(t, aws.ToBool(c.IAMDatabaseAuthenticationEnabled))
	assert.True(t, aws.ToBool(c.GlobalWriteForwardingRequested))
	assert.Equal(t, int32(7), aws.ToInt32(c.PerformanceInsightsRetentionPeriod))

	instances, err := backend.DescribeDBInstances("mod-fields-member")
	require.NoError(t, err)
	require.Len(t, instances, 1)
	assert.Equal(t, "cascaded-pg", instances[0].DBParameterGroupName)
}

func testCreateDBInstanceFieldsRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	out, err := client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier:               aws.String("fields-instance"),
		Engine:                             aws.String("postgres"),
		DBInstanceClass:                    aws.String("db.t3.micro"),
		MasterUsername:                     aws.String("admin"),
		AllocatedStorage:                   aws.Int32(20),
		AutoMinorVersionUpgrade:            aws.Bool(true),
		BackupTarget:                       aws.String("region"),
		MultiTenant:                        aws.Bool(true),
		PromotionTier:                      aws.Int32(2),
		EnablePerformanceInsights:          aws.Bool(true),
		PerformanceInsightsKMSKeyId:        aws.String("arn:aws:kms:us-east-1:123456789012:key/pi-inst"),
		PerformanceInsightsRetentionPeriod: aws.Int32(93),
	})
	require.NoError(t, err)

	i := out.DBInstance
	assert.True(t, aws.ToBool(i.AutoMinorVersionUpgrade))
	assert.Equal(t, "region", aws.ToString(i.BackupTarget))
	assert.True(t, aws.ToBool(i.MultiTenant))
	assert.Equal(t, int32(2), aws.ToInt32(i.PromotionTier))
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/pi-inst", aws.ToString(i.PerformanceInsightsKMSKeyId))
	assert.Equal(t, int32(93), aws.ToInt32(i.PerformanceInsightsRetentionPeriod))
}

func testModifyDBInstanceFieldsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBInstance(
		"mod-fields-instance", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{},
	)
	require.NoError(t, err)
	waitForInstanceStatus(t, backend, "mod-fields-instance", "available")

	out, err := client.ModifyDBInstance(ctx, &rdssdk.ModifyDBInstanceInput{
		DBInstanceIdentifier:               aws.String("mod-fields-instance"),
		ReplicaMode:                        types.ReplicaModeMounted,
		UseDefaultProcessorFeatures:        aws.Bool(true),
		PerformanceInsightsKMSKeyId:        aws.String("arn:aws:kms:us-east-1:123456789012:key/pi-mod"),
		PerformanceInsightsRetentionPeriod: aws.Int32(31),
		DBPortNumber:                       aws.Int32(6543),
		PromotionTier:                      aws.Int32(3),
		ApplyImmediately:                   aws.Bool(true),
	})
	require.NoError(t, err)

	i := out.DBInstance
	assert.Equal(t, types.ReplicaModeMounted, i.ReplicaMode)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/pi-mod", aws.ToString(i.PerformanceInsightsKMSKeyId))
	assert.Equal(t, int32(31), aws.ToInt32(i.PerformanceInsightsRetentionPeriod))
	assert.Equal(t, int32(6543), aws.ToInt32(i.Endpoint.Port))
	assert.Equal(t, int32(3), aws.ToInt32(i.PromotionTier))

	instances, err := backend.DescribeDBInstances("mod-fields-instance")
	require.NoError(t, err)
	require.Len(t, instances, 1)
	assert.True(t, instances[0].UseDefaultProcessorFeatures)
}

func testCreateDBInstanceReadReplicaFieldsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBInstance(
		"replica-fields-src", "mysql", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{},
	)
	require.NoError(t, err)

	out, err := client.CreateDBInstanceReadReplica(ctx, &rdssdk.CreateDBInstanceReadReplicaInput{
		DBInstanceIdentifier:               aws.String("replica-fields"),
		SourceDBInstanceIdentifier:         aws.String("replica-fields-src"),
		AutoMinorVersionUpgrade:            aws.Bool(true),
		EnableIAMDatabaseAuthentication:    aws.Bool(true),
		VpcSecurityGroupIds:                []string{"sg-replica-1"},
		ReplicaMode:                        types.ReplicaModeMounted,
		UseDefaultProcessorFeatures:        aws.Bool(true),
		PerformanceInsightsKMSKeyId:        aws.String("arn:aws:kms:us-east-1:123456789012:key/pi-replica"),
		PerformanceInsightsRetentionPeriod: aws.Int32(7),
	})
	require.NoError(t, err)

	i := out.DBInstance
	assert.True(t, aws.ToBool(i.AutoMinorVersionUpgrade))
	assert.True(t, aws.ToBool(i.IAMDatabaseAuthenticationEnabled))
	require.NotNil(t, i.VpcSecurityGroups)
	require.Len(t, i.VpcSecurityGroups, 1)
	assert.Equal(t, "sg-replica-1", aws.ToString(i.VpcSecurityGroups[0].VpcSecurityGroupId))
	assert.Equal(t, types.ReplicaModeMounted, i.ReplicaMode)
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/pi-replica", aws.ToString(i.PerformanceInsightsKMSKeyId))
	assert.Equal(t, int32(7), aws.ToInt32(i.PerformanceInsightsRetentionPeriod))
}

func testRestoreDBInstanceFromDBSnapshotFieldsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBInstance(
		"restore-snap-src", "mysql", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{},
	)
	require.NoError(t, err)
	_, err = backend.CreateDBSnapshot("restore-snap-fields", "restore-snap-src")
	require.NoError(t, err)

	out, err := client.RestoreDBInstanceFromDBSnapshot(ctx, &rdssdk.RestoreDBInstanceFromDBSnapshotInput{
		DBInstanceIdentifier:            aws.String("restored-from-snap-fields"),
		DBSnapshotIdentifier:            aws.String("restore-snap-fields"),
		EnableIAMDatabaseAuthentication: aws.Bool(true),
		VpcSecurityGroupIds:             []string{"sg-restore-1"},
		UseDefaultProcessorFeatures:     aws.Bool(true),
		BackupTarget:                    aws.String("outposts"),
	})
	require.NoError(t, err)

	i := out.DBInstance
	assert.True(t, aws.ToBool(i.IAMDatabaseAuthenticationEnabled))
	require.Len(t, i.VpcSecurityGroups, 1)
	assert.Equal(t, "sg-restore-1", aws.ToString(i.VpcSecurityGroups[0].VpcSecurityGroupId))
	assert.Equal(t, "outposts", aws.ToString(i.BackupTarget))

	instances, err := backend.DescribeDBInstances("restored-from-snap-fields")
	require.NoError(t, err)
	require.Len(t, instances, 1)
	assert.True(t, instances[0].UseDefaultProcessorFeatures)
}

func testRestoreDBInstanceToPointInTimeFieldsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBInstance(
		"restore-pit-src", "mysql", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{},
	)
	require.NoError(t, err)

	out, err := client.RestoreDBInstanceToPointInTime(ctx, &rdssdk.RestoreDBInstanceToPointInTimeInput{
		TargetDBInstanceIdentifier:      aws.String("restored-pit-fields"),
		SourceDBInstanceIdentifier:      aws.String("restore-pit-src"),
		EnableIAMDatabaseAuthentication: aws.Bool(true),
		VpcSecurityGroupIds:             []string{"sg-pit-1"},
		UseDefaultProcessorFeatures:     aws.Bool(true),
		BackupTarget:                    aws.String("outposts"),
	})
	require.NoError(t, err)

	i := out.DBInstance
	assert.True(t, aws.ToBool(i.IAMDatabaseAuthenticationEnabled))
	require.Len(t, i.VpcSecurityGroups, 1)
	assert.Equal(t, "sg-pit-1", aws.ToString(i.VpcSecurityGroups[0].VpcSecurityGroupId))
	assert.Equal(t, "outposts", aws.ToString(i.BackupTarget))

	instances, err := backend.DescribeDBInstances("restored-pit-fields")
	require.NoError(t, err)
	require.Len(t, instances, 1)
	assert.True(t, instances[0].UseDefaultProcessorFeatures)
}

func testRestoreDBInstanceFromS3FieldsRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	out, err := client.RestoreDBInstanceFromS3(ctx, &rdssdk.RestoreDBInstanceFromS3Input{
		DBInstanceIdentifier:               aws.String("restore-s3-fields"),
		Engine:                             aws.String("mysql"),
		DBInstanceClass:                    aws.String("db.t3.micro"),
		SourceEngine:                       aws.String("mysql"),
		SourceEngineVersion:                aws.String("8.0.28"),
		S3BucketName:                       aws.String("my-restore-bucket"),
		S3IngestionRoleArn:                 aws.String("arn:aws:iam::123456789012:role/s3-ingestion"),
		AutoMinorVersionUpgrade:            aws.Bool(true),
		EnableIAMDatabaseAuthentication:    aws.Bool(true),
		UseDefaultProcessorFeatures:        aws.Bool(true),
		PerformanceInsightsKMSKeyId:        aws.String("arn:aws:kms:us-east-1:123456789012:key/pi-s3"),
		PerformanceInsightsRetentionPeriod: aws.Int32(7),
	})
	require.NoError(t, err)

	i := out.DBInstance
	assert.True(t, aws.ToBool(i.AutoMinorVersionUpgrade))
	assert.True(t, aws.ToBool(i.IAMDatabaseAuthenticationEnabled))
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/pi-s3", aws.ToString(i.PerformanceInsightsKMSKeyId))
	assert.Equal(t, int32(7), aws.ToInt32(i.PerformanceInsightsRetentionPeriod))
}

func testRestoreDBClusterFromSnapshotFieldsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBCluster(
		"restore-clu-snap-src", "aurora-postgresql", "admin", "mydb", "", 5432, nil, rds.DBClusterOptions{},
	)
	require.NoError(t, err)
	_, err = backend.CreateDBClusterSnapshot("restore-clu-snap-fields", "restore-clu-snap-src")
	require.NoError(t, err)

	out, err := client.RestoreDBClusterFromSnapshot(ctx, &rdssdk.RestoreDBClusterFromSnapshotInput{
		DBClusterIdentifier:                aws.String("restored-clu-from-snap"),
		SnapshotIdentifier:                 aws.String("restore-clu-snap-fields"),
		Engine:                             aws.String("aurora-postgresql"),
		OptionGroupName:                    aws.String("restore-og"),
		PubliclyAccessible:                 aws.Bool(true),
		EnableIAMDatabaseAuthentication:    aws.Bool(true),
		PerformanceInsightsKMSKeyId:        aws.String("arn:aws:kms:us-east-1:123456789012:key/pi-restclu"),
		PerformanceInsightsRetentionPeriod: aws.Int32(7),
	})
	require.NoError(t, err)

	c := out.DBCluster
	require.Len(t, c.DBClusterOptionGroupMemberships, 1)
	assert.Equal(t, "restore-og", aws.ToString(c.DBClusterOptionGroupMemberships[0].DBClusterOptionGroupName))
	assert.True(t, aws.ToBool(c.PubliclyAccessible))
	assert.True(t, aws.ToBool(c.IAMDatabaseAuthenticationEnabled))
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/pi-restclu", aws.ToString(c.PerformanceInsightsKMSKeyId))
	assert.Equal(t, int32(7), aws.ToInt32(c.PerformanceInsightsRetentionPeriod))
}

func testRestoreDBClusterToPointInTimeFieldsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBCluster(
		"restore-clu-pit-src", "aurora-postgresql", "admin", "mydb", "", 5432, nil, rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	out, err := client.RestoreDBClusterToPointInTime(ctx, &rdssdk.RestoreDBClusterToPointInTimeInput{
		DBClusterIdentifier:                aws.String("restored-clu-pit"),
		SourceDBClusterIdentifier:          aws.String("restore-clu-pit-src"),
		OptionGroupName:                    aws.String("restore-pit-og"),
		PubliclyAccessible:                 aws.Bool(true),
		EnableIAMDatabaseAuthentication:    aws.Bool(true),
		PerformanceInsightsKMSKeyId:        aws.String("arn:aws:kms:us-east-1:123456789012:key/pi-restpit"),
		PerformanceInsightsRetentionPeriod: aws.Int32(7),
	})
	require.NoError(t, err)

	c := out.DBCluster
	require.Len(t, c.DBClusterOptionGroupMemberships, 1)
	assert.Equal(t, "restore-pit-og", aws.ToString(c.DBClusterOptionGroupMemberships[0].DBClusterOptionGroupName))
	assert.True(t, aws.ToBool(c.PubliclyAccessible))
	assert.True(t, aws.ToBool(c.IAMDatabaseAuthenticationEnabled))
	assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/pi-restpit", aws.ToString(c.PerformanceInsightsKMSKeyId))
	assert.Equal(t, int32(7), aws.ToInt32(c.PerformanceInsightsRetentionPeriod))
}

func testRestoreDBClusterFromS3FieldsRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	out, err := client.RestoreDBClusterFromS3(ctx, &rdssdk.RestoreDBClusterFromS3Input{
		DBClusterIdentifier:             aws.String("restore-clu-s3-fields"),
		Engine:                          aws.String("aurora-mysql"),
		MasterUsername:                  aws.String("admin"),
		SourceEngine:                    aws.String("mysql"),
		SourceEngineVersion:             aws.String("8.0.28"),
		S3BucketName:                    aws.String("my-restore-bucket"),
		S3IngestionRoleArn:              aws.String("arn:aws:iam::123456789012:role/s3-ingestion"),
		EnableIAMDatabaseAuthentication: aws.Bool(true),
	})
	require.NoError(t, err)

	assert.True(t, aws.ToBool(out.DBCluster.IAMDatabaseAuthenticationEnabled))
}

func testDeleteDBClusterAutomatedBackupsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBCluster(
		"del-backup-cluster", "aurora-postgresql", "admin", "mydb", "", 5432, nil,
		rds.DBClusterOptions{BackupRetentionPeriod: 7},
	)
	require.NoError(t, err)

	backups := backend.DescribeDBClusterAutomatedBackups("del-backup-cluster")
	require.NotEmpty(t, backups)

	_, err = client.DeleteDBCluster(ctx, &rdssdk.DeleteDBClusterInput{
		DBClusterIdentifier:    aws.String("del-backup-cluster"),
		SkipFinalSnapshot:      aws.Bool(true),
		DeleteAutomatedBackups: aws.Bool(true),
	})
	require.NoError(t, err)

	remaining := backend.DescribeDBClusterAutomatedBackups("del-backup-cluster")
	assert.Empty(t, remaining)
}

func testCopyDBClusterSnapshotTagsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBCluster(
		"copy-tags-cluster", "aurora-postgresql", "admin", "mydb", "", 5432, nil, rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	src, err := backend.CreateDBClusterSnapshot("copy-tags-src-snap", "copy-tags-cluster")
	require.NoError(t, err)
	backend.AddTagsToResource(src.DBClusterSnapshotArn, []rds.Tag{{Key: "env", Value: "prod"}})

	_, err = client.CopyDBClusterSnapshot(ctx, &rdssdk.CopyDBClusterSnapshotInput{
		SourceDBClusterSnapshotIdentifier: aws.String("copy-tags-src-snap"),
		TargetDBClusterSnapshotIdentifier: aws.String("copy-tags-dst-snap"),
		CopyTags:                          aws.Bool(true),
	})
	require.NoError(t, err)

	dstArn := "arn:aws:rds:us-east-1:123456789012:cluster-snapshot:copy-tags-dst-snap"
	tags := backend.ListTagsForResource(dstArn)
	require.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].Key)
	assert.Equal(t, "prod", tags[0].Value)
}

func testGlobalClusterFieldsRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	createOut, err := client.CreateGlobalCluster(ctx, &rdssdk.CreateGlobalClusterInput{
		GlobalClusterIdentifier: aws.String("fields-global"),
		Engine:                  aws.String("aurora-postgresql"),
		EngineVersion:           aws.String("14.9"),
		EngineLifecycleSupport:  aws.String("open-source-rds-extended-support"),
	})
	require.NoError(t, err)
	assert.Equal(t, "open-source-rds-extended-support", aws.ToString(createOut.GlobalCluster.EngineLifecycleSupport))

	_, err = client.ModifyGlobalCluster(ctx, &rdssdk.ModifyGlobalClusterInput{
		GlobalClusterIdentifier: aws.String("fields-global"),
		EngineVersion:           aws.String("15.4"),
	})
	require.Error(t, err, "a major version bump without AllowMajorVersionUpgrade must be rejected")

	modifyOut, err := client.ModifyGlobalCluster(ctx, &rdssdk.ModifyGlobalClusterInput{
		GlobalClusterIdentifier:  aws.String("fields-global"),
		EngineVersion:            aws.String("15.4"),
		AllowMajorVersionUpgrade: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, "15.4", aws.ToString(modifyOut.GlobalCluster.EngineVersion))
}

func testDBProxyFieldsRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	createOut, err := client.CreateDBProxy(ctx, &rdssdk.CreateDBProxyInput{
		DBProxyName:                 aws.String("fields-proxy"),
		EngineFamily:                types.EngineFamilyPostgresql,
		RoleArn:                     aws.String("arn:aws:iam::123456789012:role/proxy-role"),
		Auth:                        []types.UserAuthConfig{{AuthScheme: types.AuthSchemeSecrets}},
		VpcSubnetIds:                []string{"subnet-1"},
		DefaultAuthScheme:           types.DefaultAuthSchemeIamAuth,
		EndpointNetworkType:         types.EndpointNetworkTypeIpv4,
		TargetConnectionNetworkType: types.TargetConnectionNetworkTypeIpv4,
	})
	require.NoError(t, err)
	p := createOut.DBProxy
	assert.Equal(t, string(types.DefaultAuthSchemeIamAuth), aws.ToString(p.DefaultAuthScheme))
	assert.Equal(t, types.EndpointNetworkTypeIpv4, p.EndpointNetworkType)
	assert.Equal(t, types.TargetConnectionNetworkTypeIpv4, p.TargetConnectionNetworkType)

	modifyOut, err := client.ModifyDBProxy(ctx, &rdssdk.ModifyDBProxyInput{
		DBProxyName:       aws.String("fields-proxy"),
		DefaultAuthScheme: types.DefaultAuthSchemeIamAuth,
	})
	require.NoError(t, err)
	assert.Equal(t, string(types.DefaultAuthSchemeIamAuth), aws.ToString(modifyOut.DBProxy.DefaultAuthScheme))

	epOut, err := client.CreateDBProxyEndpoint(ctx, &rdssdk.CreateDBProxyEndpointInput{
		DBProxyName:         aws.String("fields-proxy"),
		DBProxyEndpointName: aws.String("fields-proxy-ep"),
		VpcSubnetIds:        []string{"subnet-1"},
		EndpointNetworkType: types.EndpointNetworkTypeIpv4,
	})
	require.NoError(t, err)
	assert.Equal(t, types.EndpointNetworkTypeIpv4, epOut.DBProxyEndpoint.EndpointNetworkType)
}

func testCreateCustomDBEngineVersionImageIDRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	out, err := client.CreateCustomDBEngineVersion(ctx, &rdssdk.CreateCustomDBEngineVersionInput{
		Engine:        aws.String("custom-oracle-ee"),
		EngineVersion: aws.String("19.fields"),
		ImageId:       aws.String("ami-0123456789abcdef0"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Image)
	assert.Equal(t, "ami-0123456789abcdef0", aws.ToString(out.Image.ImageId))
}

func testDescribeDBEngineVersionsDefaultOnlyRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	all, err := client.DescribeDBEngineVersions(ctx, &rdssdk.DescribeDBEngineVersionsInput{
		Engine: aws.String("postgres"),
	})
	require.NoError(t, err)
	require.Len(t, all.DBEngineVersions, 2, "postgres has two builtin versions in this backend's static catalog")

	defaultOnly, err := client.DescribeDBEngineVersions(ctx, &rdssdk.DescribeDBEngineVersionsInput{
		Engine:      aws.String("postgres"),
		DefaultOnly: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, defaultOnly.DBEngineVersions, 1)
	assert.Equal(t, "15.5", aws.ToString(defaultOnly.DBEngineVersions[0].EngineVersion))
}

func testModifyCertificatesRemoveOverrideRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := client.ModifyCertificates(ctx, &rdssdk.ModifyCertificatesInput{
		CertificateIdentifier: aws.String("rds-ca-2019"),
	})
	require.NoError(t, err)

	out, err := client.ModifyCertificates(ctx, &rdssdk.ModifyCertificatesInput{
		RemoveCustomerOverride: aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Certificate)
	assert.False(t, aws.ToBool(out.Certificate.CustomerOverride))
	assert.Equal(t, "rds-ca-rsa2048-g1", aws.ToString(out.Certificate.CertificateIdentifier))
}

func testStartActivityStreamNativeAuditFieldsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBCluster(
		"activity-native-audit-cluster", "aurora-postgresql", "admin", "mydb", "", 5432, nil, rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	out, err := client.StartActivityStream(ctx, &rdssdk.StartActivityStreamInput{
		ResourceArn: aws.String(
			"arn:aws:rds:us-east-1:123456789012:cluster:activity-native-audit-cluster",
		),
		KmsKeyId:                        aws.String("arn:aws:kms:us-east-1:123456789012:key/activity-stream-key"),
		Mode:                            types.ActivityStreamModeAsync,
		EngineNativeAuditFieldsIncluded: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(out.EngineNativeAuditFieldsIncluded))
}
