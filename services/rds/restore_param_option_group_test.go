package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateDBInstanceReadReplica_ParameterAndOptionGroups verifies
// DBParameterGroupName and OptionGroupName, documented on
// CreateDBInstanceReadReplicaInput (rds@v1.124.1
// api_op_CreateDBInstanceReadReplica.go:180,418), are honored when the caller
// supplies them. The handler previously never read either from url.Values,
// so an explicit override was silently discarded (gopherstack-uox6 method,
// reqfielddiff "field never declared" class).
func TestCreateDBInstanceReadReplica_ParameterAndOptionGroups(t *testing.T) {
	t.Parallel()

	client := newTestRDSClient(t, newTestRDSHandler())

	_, err := client.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("replica-groups-src"),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("postgres"),
	})
	require.NoError(t, err)

	out, err := client.CreateDBInstanceReadReplica(t.Context(), &rdssdk.CreateDBInstanceReadReplicaInput{
		DBInstanceIdentifier:       aws.String("replica-groups-target"),
		SourceDBInstanceIdentifier: aws.String("replica-groups-src"),
		DBParameterGroupName:       aws.String("custom-replica-pg"),
		OptionGroupName:            aws.String("custom-replica-og"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DBInstance)

	require.Len(t, out.DBInstance.DBParameterGroups, 1)
	assert.Equal(t, "custom-replica-pg", aws.ToString(out.DBInstance.DBParameterGroups[0].DBParameterGroupName))
	require.Len(t, out.DBInstance.OptionGroupMemberships, 1)
	assert.Equal(t, "custom-replica-og", aws.ToString(out.DBInstance.OptionGroupMemberships[0].OptionGroupName))
}

// TestRestoreDBInstanceFromDBSnapshot_ParameterAndOptionGroups covers
// DBParameterGroupName and OptionGroupName on
// RestoreDBInstanceFromDBSnapshotInput (rds@v1.124.1
// api_op_RestoreDBInstanceFromDBSnapshot.go:241,547). Neither was read by
// the handler at all. DBParameterGroupName additionally documents "If you
// don't specify a value ... RDS uses the default DBParameterGroup for the
// specified DB engine" -- the omitted case must be its own subtest per a
// value that is always supplied cannot exercise a default.
func TestRestoreDBInstanceFromDBSnapshot_ParameterAndOptionGroups(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) *rdssdk.Client {
		t.Helper()

		client := newTestRDSClient(t, newTestRDSHandler())

		_, err := client.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
			DBInstanceIdentifier: aws.String("snap-groups-src"),
			DBInstanceClass:      aws.String("db.t3.micro"),
			Engine:               aws.String("postgres"),
		})
		require.NoError(t, err)

		_, err = client.CreateDBSnapshot(t.Context(), &rdssdk.CreateDBSnapshotInput{
			DBSnapshotIdentifier: aws.String("snap-groups"),
			DBInstanceIdentifier: aws.String("snap-groups-src"),
		})
		require.NoError(t, err)

		return client
	}

	t.Run("explicit values honored", func(t *testing.T) {
		t.Parallel()

		client := setup(t)

		out, err := client.RestoreDBInstanceFromDBSnapshot(t.Context(), &rdssdk.RestoreDBInstanceFromDBSnapshotInput{
			DBInstanceIdentifier: aws.String("snap-restore-explicit"),
			DBSnapshotIdentifier: aws.String("snap-groups"),
			DBParameterGroupName: aws.String("custom-snap-pg"),
			OptionGroupName:      aws.String("custom-snap-og"),
		})
		require.NoError(t, err)

		require.Len(t, out.DBInstance.DBParameterGroups, 1)
		assert.Equal(t, "custom-snap-pg", aws.ToString(out.DBInstance.DBParameterGroups[0].DBParameterGroupName))
		require.Len(t, out.DBInstance.OptionGroupMemberships, 1)
		assert.Equal(t, "custom-snap-og", aws.ToString(out.DBInstance.OptionGroupMemberships[0].OptionGroupName))
	})

	t.Run("omitted DBParameterGroupName defaults to engine default", func(t *testing.T) {
		t.Parallel()

		client := setup(t)

		out, err := client.RestoreDBInstanceFromDBSnapshot(t.Context(), &rdssdk.RestoreDBInstanceFromDBSnapshotInput{
			DBInstanceIdentifier: aws.String("snap-restore-default"),
			DBSnapshotIdentifier: aws.String("snap-groups"),
		})
		require.NoError(t, err)

		require.Len(t, out.DBInstance.DBParameterGroups, 1)
		assert.Equal(t, "default.postgres", aws.ToString(out.DBInstance.DBParameterGroups[0].DBParameterGroupName))
	})
}

// TestRestoreDBInstanceToPointInTime_ParameterAndOptionGroups covers
// DBParameterGroupName and OptionGroupName on
// RestoreDBInstanceToPointInTimeInput (rds@v1.124.1
// api_op_RestoreDBInstanceToPointInTime.go:210,513). DBParameterGroupName
// was already defaulted from the source instance unconditionally; the bug
// was that an explicit override could never take effect. OptionGroupName
// was never read at all.
func TestRestoreDBInstanceToPointInTime_ParameterAndOptionGroups(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) *rdssdk.Client {
		t.Helper()

		client := newTestRDSClient(t, newTestRDSHandler())

		_, err := client.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
			DBInstanceIdentifier: aws.String("pit-groups-src"),
			DBInstanceClass:      aws.String("db.t3.micro"),
			Engine:               aws.String("postgres"),
			DBParameterGroupName: aws.String("source-pg"),
		})
		require.NoError(t, err)

		return client
	}

	t.Run("explicit values override the source instance's groups", func(t *testing.T) {
		t.Parallel()

		client := setup(t)

		out, err := client.RestoreDBInstanceToPointInTime(t.Context(), &rdssdk.RestoreDBInstanceToPointInTimeInput{
			TargetDBInstanceIdentifier: aws.String("pit-restore-explicit"),
			SourceDBInstanceIdentifier: aws.String("pit-groups-src"),
			DBParameterGroupName:       aws.String("custom-pit-pg"),
			OptionGroupName:            aws.String("custom-pit-og"),
		})
		require.NoError(t, err)

		require.Len(t, out.DBInstance.DBParameterGroups, 1)
		assert.Equal(t, "custom-pit-pg", aws.ToString(out.DBInstance.DBParameterGroups[0].DBParameterGroupName))
		require.Len(t, out.DBInstance.OptionGroupMemberships, 1)
		assert.Equal(t, "custom-pit-og", aws.ToString(out.DBInstance.OptionGroupMemberships[0].OptionGroupName))
	})

	t.Run("omitted DBParameterGroupName still defaults to the source's group", func(t *testing.T) {
		t.Parallel()

		client := setup(t)

		out, err := client.RestoreDBInstanceToPointInTime(t.Context(), &rdssdk.RestoreDBInstanceToPointInTimeInput{
			TargetDBInstanceIdentifier: aws.String("pit-restore-default"),
			SourceDBInstanceIdentifier: aws.String("pit-groups-src"),
		})
		require.NoError(t, err)

		require.Len(t, out.DBInstance.DBParameterGroups, 1)
		assert.Equal(t, "source-pg", aws.ToString(out.DBInstance.DBParameterGroups[0].DBParameterGroupName))
	})
}

// TestRestoreDBInstanceFromS3_ParameterAndOptionGroups covers
// DBParameterGroupName and OptionGroupName on RestoreDBInstanceFromS3Input
// (rds@v1.124.1 api_op_RestoreDBInstanceFromS3.go:171,402). Neither was read
// by the handler at all. DBParameterGroupName documents "If you do not
// specify a value ... the default DBParameterGroup for the specified DB
// engine is used" -- the omitted case is its own subtest.
func TestRestoreDBInstanceFromS3_ParameterAndOptionGroups(t *testing.T) {
	t.Parallel()

	baseInput := func(id string) *rdssdk.RestoreDBInstanceFromS3Input {
		return &rdssdk.RestoreDBInstanceFromS3Input{
			DBInstanceIdentifier: aws.String(id),
			DBInstanceClass:      aws.String("db.t3.micro"),
			Engine:               aws.String("mysql"),
			S3BucketName:         aws.String("my-backup-bucket"),
			S3IngestionRoleArn:   aws.String("arn:aws:iam::000000000000:role/rds-s3-ingestion"),
			SourceEngine:         aws.String("mysql"),
			SourceEngineVersion:  aws.String("5.7.40"),
		}
	}

	t.Run("explicit values honored", func(t *testing.T) {
		t.Parallel()

		client := newTestRDSClient(t, newTestRDSHandler())

		in := baseInput("s3-restore-explicit")
		in.DBParameterGroupName = aws.String("custom-s3-pg")
		in.OptionGroupName = aws.String("custom-s3-og")

		out, err := client.RestoreDBInstanceFromS3(t.Context(), in)
		require.NoError(t, err)

		require.Len(t, out.DBInstance.DBParameterGroups, 1)
		assert.Equal(t, "custom-s3-pg", aws.ToString(out.DBInstance.DBParameterGroups[0].DBParameterGroupName))
		require.Len(t, out.DBInstance.OptionGroupMemberships, 1)
		assert.Equal(t, "custom-s3-og", aws.ToString(out.DBInstance.OptionGroupMemberships[0].OptionGroupName))
	})

	t.Run("omitted DBParameterGroupName defaults to engine default", func(t *testing.T) {
		t.Parallel()

		client := newTestRDSClient(t, newTestRDSHandler())

		out, err := client.RestoreDBInstanceFromS3(t.Context(), baseInput("s3-restore-default"))
		require.NoError(t, err)

		require.Len(t, out.DBInstance.DBParameterGroups, 1)
		assert.Equal(t, "default.mysql", aws.ToString(out.DBInstance.DBParameterGroups[0].DBParameterGroupName))
	})
}
