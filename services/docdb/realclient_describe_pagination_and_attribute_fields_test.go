package docdb_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	docdbsdk "github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_PaginationAndAttributeFields drives the gopherstack-xhu2t
// slice-13 docdb tier-1 fixes (Describe pagination, DefaultOnly filtering,
// StorageType, PerformanceInsightsKMSKeyId, and the
// RestoreDBClusterToPointInTime RestoreToTime/UseLatestRestorableTime
// exclusivity rule) through the real aws-sdk-go-v2 client.
func TestRealClient_PaginationAndAttributeFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testCertificatesPaginationRealClient, "certificates_pagination"},
		{testClusterParametersPaginationRealClient, "cluster_parameters_pagination"},
		{testEngineDefaultClusterParametersPaginationRealClient, "engine_default_cluster_parameters_pagination"},
		{testDBEngineVersionsDefaultOnlyRealClient, "db_engine_versions_default_only"},
		{testOrderableDBInstanceOptionsPaginationRealClient, "orderable_db_instance_options_pagination"},
		{testClusterStorageTypeRealClient, "cluster_storage_type"},
		{testRestoreStorageTypeRealClient, "restore_storage_type"},
		{testRestoreToPointInTimeExclusivityRealClient, "restore_to_point_in_time_exclusivity"},
		{testInstancePerformanceInsightsKMSKeyRealClient, "instance_performance_insights_kms_key"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func testCertificatesPaginationRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	first, err := client.DescribeCertificates(ctx, &docdbsdk.DescribeCertificatesInput{
		MaxRecords: aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Len(t, first.Certificates, 1)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeCertificates(ctx, &docdbsdk.DescribeCertificatesInput{
		Marker: first.Marker,
	})
	require.NoError(t, err)
	require.NotEmpty(t, second.Certificates)
	assert.NotEqual(t,
		aws.ToString(first.Certificates[0].CertificateIdentifier),
		aws.ToString(second.Certificates[0].CertificateIdentifier),
	)
}

func testClusterParametersPaginationRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDBClusterParameterGroup(ctx, &docdbsdk.CreateDBClusterParameterGroupInput{
		DBClusterParameterGroupName: aws.String("params-page-group"),
		DBParameterGroupFamily:      aws.String("docdb4.0"),
		Description:                 aws.String("pagination test"),
	})
	require.NoError(t, err)

	first, err := client.DescribeDBClusterParameters(ctx, &docdbsdk.DescribeDBClusterParametersInput{
		DBClusterParameterGroupName: aws.String("params-page-group"),
		MaxRecords:                  aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Len(t, first.Parameters, 1)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeDBClusterParameters(ctx, &docdbsdk.DescribeDBClusterParametersInput{
		DBClusterParameterGroupName: aws.String("params-page-group"),
		Marker:                      first.Marker,
	})
	require.NoError(t, err)
	require.NotEmpty(t, second.Parameters)
	assert.NotEqual(t,
		aws.ToString(first.Parameters[0].ParameterName),
		aws.ToString(second.Parameters[0].ParameterName),
	)
}

func testEngineDefaultClusterParametersPaginationRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	first, err := client.DescribeEngineDefaultClusterParameters(
		ctx,
		&docdbsdk.DescribeEngineDefaultClusterParametersInput{
			DBParameterGroupFamily: aws.String("docdb4.0"),
			MaxRecords:             aws.Int32(1),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, first.EngineDefaults)
	assert.Len(t, first.EngineDefaults.Parameters, 1)
	require.NotEmpty(t, aws.ToString(first.EngineDefaults.Marker))

	second, err := client.DescribeEngineDefaultClusterParameters(
		ctx,
		&docdbsdk.DescribeEngineDefaultClusterParametersInput{
			DBParameterGroupFamily: aws.String("docdb4.0"),
			Marker:                 first.EngineDefaults.Marker,
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, second.EngineDefaults.Parameters)
	assert.NotEqual(t,
		aws.ToString(first.EngineDefaults.Parameters[0].ParameterName),
		aws.ToString(second.EngineDefaults.Parameters[0].ParameterName),
	)
}

func testDBEngineVersionsDefaultOnlyRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	all, err := client.DescribeDBEngineVersions(ctx, &docdbsdk.DescribeDBEngineVersionsInput{})
	require.NoError(t, err)
	require.Greater(t, len(all.DBEngineVersions), 1)

	defaultOnly, err := client.DescribeDBEngineVersions(ctx, &docdbsdk.DescribeDBEngineVersionsInput{
		DefaultOnly: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, defaultOnly.DBEngineVersions, 1)
	assert.Equal(t, "4.0.0", aws.ToString(defaultOnly.DBEngineVersions[0].EngineVersion))

	paged, err := client.DescribeDBEngineVersions(ctx, &docdbsdk.DescribeDBEngineVersionsInput{
		MaxRecords: aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Len(t, paged.DBEngineVersions, 1)
	require.NotEmpty(t, aws.ToString(paged.Marker))
}

func testOrderableDBInstanceOptionsPaginationRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	first, err := client.DescribeOrderableDBInstanceOptions(
		ctx,
		&docdbsdk.DescribeOrderableDBInstanceOptionsInput{
			Engine:     aws.String("docdb"),
			MaxRecords: aws.Int32(1),
		},
	)
	require.NoError(t, err)
	assert.Len(t, first.OrderableDBInstanceOptions, 1)
	require.NotEmpty(t, aws.ToString(first.Marker))

	second, err := client.DescribeOrderableDBInstanceOptions(
		ctx,
		&docdbsdk.DescribeOrderableDBInstanceOptionsInput{
			Engine: aws.String("docdb"),
			Marker: first.Marker,
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, second.OrderableDBInstanceOptions)
}

func testClusterStorageTypeRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	created, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("storage-type-cluster"),
		Engine:              aws.String("docdb"),
		StorageType:         aws.String("iopt1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "iopt1", aws.ToString(created.DBCluster.StorageType))

	modified, err := client.ModifyDBCluster(ctx, &docdbsdk.ModifyDBClusterInput{
		DBClusterIdentifier: aws.String("storage-type-cluster"),
		StorageType:         aws.String("standard"),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(modified.DBCluster.StorageType))

	_, err = client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("bad-storage-type-cluster"),
		Engine:              aws.String("docdb"),
		StorageType:         aws.String("bogus"),
	})
	require.Error(t, err)
}

func testRestoreStorageTypeRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("restore-storage-source"),
		Engine:              aws.String("docdb"),
	})
	require.NoError(t, err)
	_, err = client.CreateDBClusterSnapshot(ctx, &docdbsdk.CreateDBClusterSnapshotInput{
		DBClusterSnapshotIdentifier: aws.String("restore-storage-snap"),
		DBClusterIdentifier:         aws.String("restore-storage-source"),
	})
	require.NoError(t, err)

	restored, err := client.RestoreDBClusterFromSnapshot(ctx, &docdbsdk.RestoreDBClusterFromSnapshotInput{
		DBClusterIdentifier: aws.String("restore-storage-target"),
		SnapshotIdentifier:  aws.String("restore-storage-snap"),
		Engine:              aws.String("docdb"),
		StorageType:         aws.String("iopt1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "iopt1", aws.ToString(restored.DBCluster.StorageType))

	restoredPIT, err := client.RestoreDBClusterToPointInTime(ctx, &docdbsdk.RestoreDBClusterToPointInTimeInput{
		SourceDBClusterIdentifier: aws.String("restore-storage-source"),
		DBClusterIdentifier:       aws.String("restore-storage-pit-target"),
		UseLatestRestorableTime:   aws.Bool(true),
		StorageType:               aws.String("iopt1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "iopt1", aws.ToString(restoredPIT.DBCluster.StorageType))
}

func testRestoreToPointInTimeExclusivityRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("pit-exclusivity-source"),
		Engine:              aws.String("docdb"),
	})
	require.NoError(t, err)

	_, err = client.RestoreDBClusterToPointInTime(ctx, &docdbsdk.RestoreDBClusterToPointInTimeInput{
		SourceDBClusterIdentifier: aws.String("pit-exclusivity-source"),
		DBClusterIdentifier:       aws.String("pit-exclusivity-neither"),
	})
	require.Error(t, err, "must specify RestoreToTime or UseLatestRestorableTime")

	_, err = client.RestoreDBClusterToPointInTime(ctx, &docdbsdk.RestoreDBClusterToPointInTimeInput{
		SourceDBClusterIdentifier: aws.String("pit-exclusivity-source"),
		DBClusterIdentifier:       aws.String("pit-exclusivity-both"),
		UseLatestRestorableTime:   aws.Bool(true),
		RestoreToTime:             aws.Time(time.Now()),
	})
	require.Error(t, err, "RestoreToTime and UseLatestRestorableTime are mutually exclusive")
}

func testInstancePerformanceInsightsKMSKeyRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("pi-kms-cluster"),
		Engine:              aws.String("docdb"),
	})
	require.NoError(t, err)

	created, err := client.CreateDBInstance(ctx, &docdbsdk.CreateDBInstanceInput{
		DBInstanceIdentifier:        aws.String("pi-kms-instance"),
		DBClusterIdentifier:         aws.String("pi-kms-cluster"),
		DBInstanceClass:             aws.String("db.t3.medium"),
		Engine:                      aws.String("docdb"),
		EnablePerformanceInsights:   aws.Bool(true),
		PerformanceInsightsKMSKeyId: aws.String("arn:aws:kms:us-east-1:123456789012:key/create-key"),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(created.DBInstance.PerformanceInsightsEnabled))
	assert.Equal(t,
		"arn:aws:kms:us-east-1:123456789012:key/create-key",
		aws.ToString(created.DBInstance.PerformanceInsightsKMSKeyId),
	)

	modified, err := client.ModifyDBInstance(ctx, &docdbsdk.ModifyDBInstanceInput{
		DBInstanceIdentifier:        aws.String("pi-kms-instance"),
		PerformanceInsightsKMSKeyId: aws.String("arn:aws:kms:us-east-1:123456789012:key/modify-key"),
		ApplyImmediately:            aws.Bool(false),
	})
	require.NoError(t, err)
	assert.Equal(t,
		"arn:aws:kms:us-east-1:123456789012:key/modify-key",
		aws.ToString(modified.DBInstance.PerformanceInsightsKMSKeyId),
		"ApplyImmediately=false is read for wire-declaration parity but this backend still applies changes immediately",
	)
}
