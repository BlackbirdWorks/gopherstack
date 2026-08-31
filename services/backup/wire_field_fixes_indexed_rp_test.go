package backup_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	backupsdk "github.com/aws/aws-sdk-go-v2/service/backup"
	"github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

// TestListRecoveryPointsByResource_WireFields proves the real
// RecoveryPointByResource fields (backup@v1.59.4 deserializers.go:
// BackupVaultName, CreationDate) round-trip through the real typed SDK
// client. The prior handler emitted only RecoveryPointArn/Status even
// though RecoveryPoint already tracks both -- a real client's
// BackupVaultName/CreationDate were always nil regardless of the tracked
// value.
func TestListRecoveryPointsByResource_WireFields(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	mustVault(t, backend, "lrpbr-vault")
	resourceArn := "arn:aws:ec2:us-east-1:000000000000:instance/i-lrpbr"
	mustRP(
		t,
		backend,
		"lrpbr-vault",
		"arn:aws:backup:us-east-1:000000000000:recovery-point:lrpbr-1",
		resourceArn,
		"EC2",
	)

	out, err := client.ListRecoveryPointsByResource(t.Context(), &backupsdk.ListRecoveryPointsByResourceInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	require.Len(t, out.RecoveryPoints, 1)

	rp := out.RecoveryPoints[0]
	require.NotNil(t, rp.BackupVaultName, "BackupVaultName must not be nil")
	require.Equal(t, "lrpbr-vault", aws.ToString(rp.BackupVaultName))
	require.NotNil(t, rp.CreationDate, "CreationDate must not be nil")
	require.WithinDuration(t, time.Now().UTC(), *rp.CreationDate, time.Minute)
}

// TestListIndexedRecoveryPoints_WireFields proves the real
// IndexedRecoveryPoint fields (backup@v1.59.4 deserializers.go:
// BackupVaultArn, IndexStatus, IamRoleArn, ResourceType,
// SourceResourceArn, BackupCreationDate) round-trip through the real
// typed SDK client. The prior handler emitted RecoveryPointArn/Status
// under a "Status" key that IndexedRecoveryPoint's real deserializer has
// no case for at all -- IndexStatus (a distinct, already-tracked value
// via GetRecoveryPointIndexDetails/UpdateRecoveryPointIndexSettings) was
// always nil to a real client regardless of what this backend tracked.
func TestListIndexedRecoveryPoints_WireFields(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	v := mustVault(t, backend, "lirp-vault")
	resourceArn := "arn:aws:ec2:us-east-1:000000000000:instance/i-lirp"
	rpArn := "arn:aws:backup:us-east-1:000000000000:recovery-point:lirp-1"
	mustRP(t, backend, "lirp-vault", rpArn, resourceArn, "EC2")
	require.NoError(t, backend.UpdateRecoveryPointIndexSettings("lirp-vault", rpArn, "ACTIVE"))

	out, err := client.ListIndexedRecoveryPoints(t.Context(), &backupsdk.ListIndexedRecoveryPointsInput{})
	require.NoError(t, err)
	require.Len(t, out.IndexedRecoveryPoints, 1)

	irp := out.IndexedRecoveryPoints[0]
	require.NotNil(t, irp.BackupVaultArn, "BackupVaultArn must not be nil")
	require.Equal(t, v.BackupVaultArn, aws.ToString(irp.BackupVaultArn))
	require.Equal(t, "ACTIVE", string(irp.IndexStatus))
	require.NotNil(t, irp.ResourceType)
	require.Equal(t, "EC2", aws.ToString(irp.ResourceType))
	require.NotNil(t, irp.SourceResourceArn)
	require.Equal(t, resourceArn, aws.ToString(irp.SourceResourceArn))
	require.NotNil(t, irp.BackupCreationDate, "BackupCreationDate must not be nil")
}

// TestReportJob_WireFields proves DescribeReportJob/ListReportJobs emit the
// real ReportJob fields (backup@v1.59.4 deserializers.go: ReportPlanArn,
// CreationTime, CompletionTime) rather than only ReportJobId/Status --
// both are tracked on the backend's ReportJob at StartReportJob time but
// were previously dropped by both ops sharing the same narrow shape (the
// sibling-shares-the-gap pattern).
func TestReportJob_WireFields(t *testing.T) {
	t.Parallel()

	backend := backup.NewInMemoryBackend("000000000000", "us-east-1")
	h := backup.NewHandler(backend)
	client := newTestBackupClient(t, h)

	_, err := client.CreateReportPlan(t.Context(), &backupsdk.CreateReportPlanInput{
		ReportPlanName: aws.String("rj-plan"),
		ReportDeliveryChannel: &types.ReportDeliveryChannel{
			S3BucketName: aws.String("rj-bucket"),
		},
		ReportSetting: &types.ReportSetting{
			ReportTemplate: aws.String("BACKUP_JOB_REPORT"),
		},
	})
	require.NoError(t, err)

	started, err := client.StartReportJob(t.Context(), &backupsdk.StartReportJobInput{
		ReportPlanName: aws.String("rj-plan"),
	})
	require.NoError(t, err)

	describeOut, err := client.DescribeReportJob(t.Context(), &backupsdk.DescribeReportJobInput{
		ReportJobId: started.ReportJobId,
	})
	require.NoError(t, err)
	require.NotNil(t, describeOut.ReportJob.ReportPlanArn, "ReportPlanArn must not be nil")
	require.Contains(t, aws.ToString(describeOut.ReportJob.ReportPlanArn), "rj-plan")
	require.NotNil(t, describeOut.ReportJob.CreationTime, "CreationTime must not be nil")
	require.NotNil(t, describeOut.ReportJob.CompletionTime, "CompletionTime must not be nil")

	listOut, err := client.ListReportJobs(t.Context(), &backupsdk.ListReportJobsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.ReportJobs, 1)
	require.NotNil(t, listOut.ReportJobs[0].ReportPlanArn, "ListReportJobs ReportPlanArn must not be nil")
	require.NotNil(t, listOut.ReportJobs[0].CreationTime, "ListReportJobs CreationTime must not be nil")
}
