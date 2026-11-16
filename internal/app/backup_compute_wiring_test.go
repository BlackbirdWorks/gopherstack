package app

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	backupbk "github.com/blackbirdworks/gopherstack/services/backup"
	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
	tswbackend "github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

func TestBackupWiring_RestoreRecreatesComputeResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, backup *backupbk.InMemoryBackend, byName map[string]service.Registerable)
		name string
	}{
		{
			name: "ec2_instance",
			run: func(t *testing.T, backup *backupbk.InMemoryBackend, byName map[string]service.Registerable) {
				t.Helper()

				ec2H, ok := byName["EC2"].(*ec2backend.Handler)
				require.True(t, ok)

				var bk *ec2backend.InMemoryBackend
				for _, rb := range ec2H.RegionBackends() {
					if c, cok := rb.(*ec2backend.InMemoryBackend); cok && c.Region == backup.Region() {
						bk = c
					}
				}
				require.NotNil(t, bk)

				src, err := bk.RunInstances("ami-12345678", "t3.small", "", 1)
				require.NoError(t, err)

				rp := backupRecoveryPointFor(t, backup,
					"arn:aws:ec2:us-east-1:000000000000:instance/"+src[0].ID, "EC2")

				job, err := backup.StartRestoreJob(rp, backupWiringRole, "EC2",
					map[string]string{"instanceType": "t3.large"})
				require.NoError(t, err)
				require.Equal(t, "COMPLETED", job.Status, job.StatusMessage)

				newID := job.CreatedResourceArn[len("arn:aws:ec2:us-east-1:000000000000:instance/"):]
				assert.NotEqual(t, src[0].ID, newID)

				got := bk.DescribeInstances([]string{newID}, "")
				require.Len(t, got, 1)
				assert.Equal(t, "t3.large", got[0].InstanceType)
				assert.Equal(t, "ami-12345678", got[0].ImageID)
			},
		},
		{
			name: "efs_file_system",
			run: func(t *testing.T, backup *backupbk.InMemoryBackend, byName map[string]service.Registerable) {
				t.Helper()

				efsH, ok := byName["EFS"].(*efsbackend.Handler)
				require.True(t, ok)

				src, err := efsH.Backend.CreateFileSystem(t.Context(), efsbackend.CreateFileSystemRequest{
					CreationToken: "src", PerformanceMode: "maxIO", Tags: map[string]string{"Name": "data"},
				})
				require.NoError(t, err)

				rp := backupRecoveryPointFor(t, backup, src.FileSystemArn, "EFS")

				job, err := backup.StartRestoreJob(rp, backupWiringRole, "EFS",
					map[string]string{"newFileSystem": "true"})
				require.NoError(t, err)
				require.Equal(t, "COMPLETED", job.Status, job.StatusMessage)
				assert.NotEqual(t, src.FileSystemArn, job.CreatedResourceArn)

				all, _, err := efsH.Backend.DescribeFileSystems(t.Context(), "", "", "", 0)
				require.NoError(t, err)
				require.Len(t, all, 2)

				for _, fs := range all {
					assert.Equal(t, "maxIO", fs.PerformanceMode)
					assert.Equal(t, "data", fs.Name)
				}
			},
		},
		{
			name: "cloudformation_stack",
			run: func(t *testing.T, backup *backupbk.InMemoryBackend, byName map[string]service.Registerable) {
				t.Helper()

				cfnH, ok := byName["CloudFormation"].(*cfnbackend.Handler)
				require.True(t, ok)

				bk, ok := cfnH.Backend.(*cfnbackend.InMemoryBackend)
				require.True(t, ok)

				src, err := bk.CreateStack(t.Context(), "app", `{"Resources":{"Q":{"Type":"AWS::SQS::Queue"}}}`, nil,
					cfnbackend.StackOptions{})
				require.NoError(t, err)

				rp := backupRecoveryPointFor(t, backup, src.StackID, "CloudFormation")

				job, err := backup.StartRestoreJob(rp, backupWiringRole, "CloudFormation",
					map[string]string{"stackName": "app-copy"})
				require.NoError(t, err)
				require.Equal(t, "COMPLETED", job.Status, job.StatusMessage)

				got, err := bk.DescribeStack("app-copy")
				require.NoError(t, err, job.CreatedResourceArn)
				assert.Equal(t, job.CreatedResourceArn, got.StackID)
				assert.Equal(t, src.TemplateBody, got.TemplateBody)
			},
		},
		{
			name: "timestream_table",
			run: func(t *testing.T, backup *backupbk.InMemoryBackend, byName map[string]service.Registerable) {
				t.Helper()

				tswH, ok := byName["TimestreamWrite"].(*tswbackend.Handler)
				require.True(t, ok)

				_, err := tswH.Backend.CreateDatabase("metrics", "", nil)
				require.NoError(t, err)

				src, err := tswH.Backend.CreateTable("metrics", "cpu", nil, &tswbackend.CreateTableInput{
					RetentionProperties: &tswbackend.RetentionProperties{
						MemoryStoreRetentionPeriodInHours: 12, MagneticStoreRetentionPeriodInDays: 90,
					},
				})
				require.NoError(t, err)

				rp := backupRecoveryPointFor(t, backup, src.ARN, "Timestream")

				job, err := backup.StartRestoreJob(rp, backupWiringRole, "Timestream",
					map[string]string{"tableName": "cpu-copy"})
				require.NoError(t, err)
				require.Equal(t, "COMPLETED", job.Status, job.StatusMessage)

				got, err := tswH.Backend.DescribeTable("metrics", "cpu-copy")
				require.NoError(t, err)
				assert.Equal(t, job.CreatedResourceArn, got.ARN)
				assert.Equal(t, int64(90), got.RetentionProperties.MagneticStoreRetentionPeriodInDays)
			},
		},
		{
			name: "rds_db_instance",
			run: func(t *testing.T, backup *backupbk.InMemoryBackend, byName map[string]service.Registerable) {
				t.Helper()

				rdsH, ok := byName["RDS"].(*rdsbackend.Handler)
				require.True(t, ok)

				src, err := rdsH.Backend.CreateDBInstance(
					"orders", "postgres", "db.t3.micro", "app", "admin", "", 20,
					rdsbackend.DBInstanceOptions{MasterUserPassword: "password123"},
				)
				require.NoError(t, err)

				rp := backupRecoveryPointFor(t, backup, src.DBInstanceArn, "RDS")

				job, err := backup.StartRestoreJob(rp, backupWiringRole, "RDS",
					map[string]string{"DBInstanceIdentifier": "orders-restored"})
				require.NoError(t, err)
				require.Equal(t, "COMPLETED", job.Status, job.StatusMessage)

				got, err := rdsH.Backend.DescribeDBInstances("orders-restored")
				require.NoError(t, err)
				require.Len(t, got, 1)
				assert.Equal(t, job.CreatedResourceArn, got[0].DBInstanceArn)
				assert.Equal(t, "postgres", got[0].Engine)
				assert.Equal(t, 20, got[0].AllocatedStorage)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cli := &CLI{AccountID: backupWiringAccount, Region: "us-east-1"}
			appCtx := &service.AppContext{Logger: slog.Default(), Config: cli, JanitorCtx: t.Context()}
			cli.faultStore = chaos.NewFaultStore()

			services, err := initializeServicesT(t, appCtx)
			require.NoError(t, err)

			byName := serviceByName(services)
			backupH, ok := byName["Backup"].(*backupbk.Handler)
			require.True(t, ok)

			tt.run(t, backupH.Backend, byName)
		})
	}
}

func backupRecoveryPointFor(t *testing.T, b *backupbk.InMemoryBackend, resourceArn, resourceType string) string {
	t.Helper()

	vault, err := b.CreateBackupVault("v-"+resourceType, "", "", nil)
	require.NoError(t, err)

	job, err := b.StartBackupJob(vault.BackupVaultName, resourceArn, backupWiringRole, resourceType, nil, 0)
	require.NoError(t, err)
	require.NoError(t, b.CompleteBackupJob(job.BackupJobID))

	done, err := b.DescribeBackupJob(job.BackupJobID)
	require.NoError(t, err)
	require.Equal(t, "COMPLETED", done.State, done.StatusMessage)

	return done.RecoveryPointArn
}
