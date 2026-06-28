package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_RDSAudit_LogFiles creates a DB instance and verifies that
// DescribeDBLogFiles returns at least one seeded log file and that
// DownloadDBLogFilePortion returns real (non-empty) log content for it.
func TestIntegration_RDSAudit_LogFiles(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createRDSClient(t)

	id := "sdk-audit-log-" + uuid.NewString()[:8]

	_, err := client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String(id),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("postgres"),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("password123"),
		AllocatedStorage:     aws.Int32(20),
	})
	require.NoError(t, err, "CreateDBInstance should succeed")

	t.Cleanup(func() {
		_, _ = client.DeleteDBInstance(context.Background(), &rdssdk.DeleteDBInstanceInput{
			DBInstanceIdentifier: aws.String(id),
			SkipFinalSnapshot:    aws.Bool(true),
		})
	})

	logsOut, err := client.DescribeDBLogFiles(ctx, &rdssdk.DescribeDBLogFilesInput{
		DBInstanceIdentifier: aws.String(id),
	})
	require.NoError(t, err, "DescribeDBLogFiles should succeed")
	require.GreaterOrEqual(t, len(logsOut.DescribeDBLogFiles), 1, "expected at least one log file")

	logName := aws.ToString(logsOut.DescribeDBLogFiles[0].LogFileName)
	require.NotEmpty(t, logName, "log file should have a name")
	assert.Positive(t, aws.ToInt64(logsOut.DescribeDBLogFiles[0].Size), "log file should have a size")

	portion, err := client.DownloadDBLogFilePortion(ctx, &rdssdk.DownloadDBLogFilePortionInput{
		DBInstanceIdentifier: aws.String(id),
		LogFileName:          aws.String(logName),
	})
	require.NoError(t, err, "DownloadDBLogFilePortion should succeed")
	require.NotEmpty(t, aws.ToString(portion.LogFileData), "LogFileData should be non-empty")
}

// TestIntegration_RDSAudit_LogFilesFilter verifies that the FilenameContains
// filter on DescribeDBLogFiles narrows the returned log files.
func TestIntegration_RDSAudit_LogFilesFilter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createRDSClient(t)

	id := "sdk-audit-flt-" + uuid.NewString()[:8]

	_, err := client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String(id),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("postgres"),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("password123"),
		AllocatedStorage:     aws.Int32(20),
	})
	require.NoError(t, err, "CreateDBInstance should succeed")

	t.Cleanup(func() {
		_, _ = client.DeleteDBInstance(context.Background(), &rdssdk.DeleteDBInstanceInput{
			DBInstanceIdentifier: aws.String(id),
			SkipFinalSnapshot:    aws.Bool(true),
		})
	})

	out, err := client.DescribeDBLogFiles(ctx, &rdssdk.DescribeDBLogFilesInput{
		DBInstanceIdentifier: aws.String(id),
		FilenameContains:     aws.String("error"),
	})
	require.NoError(t, err, "DescribeDBLogFiles with filter should succeed")
	require.GreaterOrEqual(t, len(out.DescribeDBLogFiles), 1, "expected at least one matching log file")
	for _, f := range out.DescribeDBLogFiles {
		assert.Contains(t, aws.ToString(f.LogFileName), "error",
			"filtered file %q should contain 'error'", aws.ToString(f.LogFileName))
	}
}
