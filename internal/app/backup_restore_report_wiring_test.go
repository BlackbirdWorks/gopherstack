package app

import (
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	sdkddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sdks3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	backupbk "github.com/blackbirdworks/gopherstack/services/backup"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const (
	backupWiringRole    = "arn:aws:iam::000000000000:role/backup"
	backupWiringAccount = "000000000000"
)

type backupWiringEnv struct {
	backup *backupbk.InMemoryBackend
	ddb    *ddbbackend.InMemoryDB
	s3     *s3backend.InMemoryBackend
}

func newBackupWiringEnv(t *testing.T) backupWiringEnv {
	t.Helper()

	cli := &CLI{AccountID: backupWiringAccount, Region: "us-east-1"}
	appCtx := &service.AppContext{Logger: slog.Default(), Config: cli, JanitorCtx: t.Context()}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t, appCtx)
	require.NoError(t, err)

	byName := serviceByName(services)

	backupH, ok := byName["Backup"].(*backupbk.Handler)
	require.True(t, ok)
	ddbH, ok := byName["DynamoDB"].(*ddbbackend.DynamoDBHandler)
	require.True(t, ok)
	s3H, ok := byName["S3"].(*s3backend.S3Handler)
	require.True(t, ok)

	ddbBk, ok := ddbH.Backend.(*ddbbackend.InMemoryDB)
	require.True(t, ok)
	s3Bk, ok := s3H.Backend.(*s3backend.InMemoryBackend)
	require.True(t, ok)

	return backupWiringEnv{backup: backupH.Backend, ddb: ddbBk, s3: s3Bk}
}

// backupRecoveryPoint runs a backup job to completion and returns its recovery point ARN.
func (e backupWiringEnv) backupRecoveryPoint(t *testing.T, resourceArn, resourceType string) string {
	t.Helper()

	vault, err := e.backup.CreateBackupVault("v-"+resourceType, "", "", nil)
	require.NoError(t, err)

	job, err := e.backup.StartBackupJob(vault.BackupVaultName, resourceArn, backupWiringRole, resourceType, nil, 0)
	require.NoError(t, err)
	require.NoError(t, e.backup.CompleteBackupJob(job.BackupJobID))

	done, err := e.backup.DescribeBackupJob(job.BackupJobID)
	require.NoError(t, err)
	require.NotEmpty(t, done.RecoveryPointArn)

	return done.RecoveryPointArn
}

func TestBackupWiring_RestoreRecreatesResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, e backupWiringEnv)
		name string
	}{
		{
			name: "dynamodb_table_with_items",
			run: func(t *testing.T, e backupWiringEnv) {
				t.Helper()
				ctx := t.Context()

				_, err := e.ddb.CreateTable(ctx, &sdkddb.CreateTableInput{
					TableName: aws.String("orders"),
					KeySchema: []sdkddbtypes.KeySchemaElement{
						{AttributeName: aws.String("pk"), KeyType: sdkddbtypes.KeyTypeHash},
					},
					AttributeDefinitions: []sdkddbtypes.AttributeDefinition{
						{AttributeName: aws.String("pk"), AttributeType: sdkddbtypes.ScalarAttributeTypeS},
					},
					BillingMode: sdkddbtypes.BillingModePayPerRequest,
				})
				require.NoError(t, err)

				for _, id := range []string{"a", "b", "c"} {
					_, err = e.ddb.PutItem(ctx, &sdkddb.PutItemInput{
						TableName: aws.String("orders"),
						Item: map[string]sdkddbtypes.AttributeValue{
							"pk":  &sdkddbtypes.AttributeValueMemberS{Value: id},
							"qty": &sdkddbtypes.AttributeValueMemberN{Value: "7"},
						},
					})
					require.NoError(t, err)
				}

				rpArn := e.backupRecoveryPoint(t, "arn:aws:dynamodb:us-east-1:000000000000:table/orders", "DynamoDB")

				_, err = e.ddb.DeleteTable(ctx, &sdkddb.DeleteTableInput{TableName: aws.String("orders")})
				require.NoError(t, err)

				job, err := e.backup.StartRestoreJob(rpArn, backupWiringRole, "DynamoDB",
					map[string]string{"targetTableName": "orders"})
				require.NoError(t, err)
				assert.Equal(t, "COMPLETED", job.Status)
				assert.Equal(t, "arn:aws:dynamodb:us-east-1:000000000000:table/orders", job.CreatedResourceArn)

				scan, err := e.ddb.Scan(ctx, &sdkddb.ScanInput{TableName: aws.String("orders")})
				require.NoError(t, err)
				assert.Len(t, scan.Items, 3)
			},
		},
		{
			name: "s3_bucket_with_objects",
			run: func(t *testing.T, e backupWiringEnv) {
				t.Helper()
				ctx := t.Context()

				_, err := e.s3.CreateBucket(ctx, &sdks3.CreateBucketInput{Bucket: aws.String("src-bkt")})
				require.NoError(t, err)
				_, err = e.s3.PutObject(ctx, &sdks3.PutObjectInput{
					Bucket: aws.String("src-bkt"), Key: aws.String("dir/file.txt"),
					Body: strings.NewReader("hello"),
				})
				require.NoError(t, err)

				rpArn := e.backupRecoveryPoint(t, "arn:aws:s3:::src-bkt", "S3")

				job, err := e.backup.StartRestoreJob(rpArn, backupWiringRole, "S3",
					map[string]string{"DestinationBucketName": "restored-bkt", "NewBucket": "true"})
				require.NoError(t, err)
				assert.Equal(t, "arn:aws:s3:::restored-bkt", job.CreatedResourceArn)

				out, err := e.s3.GetObject(ctx, &sdks3.GetObjectInput{
					Bucket: aws.String("restored-bkt"), Key: aws.String("dir/file.txt"),
				})
				require.NoError(t, err)
				body, err := io.ReadAll(out.Body)
				require.NoError(t, err)
				assert.Equal(t, "hello", string(body))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newBackupWiringEnv(t))
		})
	}
}

func TestBackupWiring_ReportJobWritesToS3(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		template   string
		wantInBody string
		formats    []string
		wantSuffix []string
	}{
		{
			name: "csv_backup_job_report", template: "BACKUP_JOB_REPORT",
			formats: []string{"CSV"}, wantSuffix: []string{".csv"}, wantInBody: "arn:aws:dynamodb",
		},
		{
			name: "csv_and_json", template: "BACKUP_JOB_REPORT",
			formats: []string{"CSV", "JSON"}, wantSuffix: []string{".csv", ".json"}, wantInBody: "arn:aws:dynamodb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newBackupWiringEnv(t)
			ctx := t.Context()

			_, err := e.s3.CreateBucket(ctx, &sdks3.CreateBucketInput{Bucket: aws.String("reports")})
			require.NoError(t, err)

			vault, err := e.backup.CreateBackupVault("rv", "", "", nil)
			require.NoError(t, err)
			bj, err := e.backup.StartBackupJob(vault.BackupVaultName,
				"arn:aws:dynamodb:us-east-1:000000000000:table/t", backupWiringRole, "DynamoDB", nil, 0)
			require.NoError(t, err)
			require.NoError(t, e.backup.CompleteBackupJob(bj.BackupJobID))

			_, err = e.backup.CreateReportPlan("plan", "",
				&backupbk.ReportDeliveryChannel{S3BucketName: "reports", S3KeyPrefix: "out", Formats: tt.formats},
				&backupbk.ReportSetting{ReportTemplate: tt.template})
			require.NoError(t, err)

			job := e.backup.StartReportJob("plan")
			assert.Equal(t, "COMPLETED", job.Status)

			list, err := e.s3.ListObjectsV2(ctx, &sdks3.ListObjectsV2Input{Bucket: aws.String("reports")})
			require.NoError(t, err)
			require.Len(t, list.Contents, len(tt.wantSuffix))

			for i, obj := range list.Contents {
				key := aws.ToString(obj.Key)
				assert.True(t, strings.HasPrefix(key, "out/"), key)
				assert.True(t, strings.HasSuffix(key, tt.wantSuffix[i]), key)

				out, getErr := e.s3.GetObject(ctx, &sdks3.GetObjectInput{Bucket: aws.String("reports"), Key: obj.Key})
				require.NoError(t, getErr)
				body, readErr := io.ReadAll(out.Body)
				require.NoError(t, readErr)
				assert.Contains(t, string(body), tt.wantInBody)
			}
		})
	}
}
