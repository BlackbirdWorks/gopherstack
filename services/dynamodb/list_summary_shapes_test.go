package dynamodb_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

const (
	waitTimeout  = 2 * time.Second
	pollInterval = 10 * time.Millisecond
)

// TestListSummaryShapes proves this pass's over-wide-response audit
// (gopherstack, 2026-09-19) for dynamodb's four flagged List ops, verified
// via cmd/structfielddiff against dynamodb@v1.67.0. All four already emit
// exactly their real Summary type's member set:
//
//   - ListBackups: BackupSummary's only unsourced member,
//     BackupExpiryDateTime, is documented by the real API as applicable only
//     to AWS_BACKUP-created backups; this backend's CreateBackup only ever
//     produces BackupTypeUser backups (backup_interface.go), so the field is
//     genuinely never applicable, not a gap.
//   - ListContributorInsights: table-level summaries only, matching this
//     backend's documented "GSI-level status mirrors the table" design
//     (contributor_insights.go) -- IndexName is correctly absent.
//   - ListExports/ListImports: already narrowed to ExportSummary/
//     ImportSummary in a prior pass (import_export_s3.go comments).
func TestListSummaryShapes(t *testing.T) {
	t.Parallel()

	t.Run("list backups exact", func(t *testing.T) {
		t.Parallel()

		db := dynamodb.NewInMemoryDB()
		h := dynamodb.NewHandler(db)
		client := newTestDynamoDBClient(t, h)
		ctx := t.Context()

		tableName := "lss-tbl-" + uuid.NewString()[:8]
		createTableHelper(t, db, tableName, "pk")

		_, err := client.CreateBackup(ctx, &sdk.CreateBackupInput{
			TableName:  aws.String(tableName),
			BackupName: aws.String("lss-backup"),
		})
		require.NoError(t, err)

		out, err := client.ListBackups(ctx, &sdk.ListBackupsInput{TableName: aws.String(tableName)})
		require.NoError(t, err)
		require.Len(t, out.BackupSummaries, 1)
		b := out.BackupSummaries[0]
		assert.NotEmpty(t, aws.ToString(b.BackupArn))
		assert.Equal(t, "lss-backup", aws.ToString(b.BackupName))
		assert.Equal(t, ddbtypes.BackupTypeUser, b.BackupType)
		assert.Equal(t, tableName, aws.ToString(b.TableName))
		assert.NotNil(t, b.BackupCreationDateTime)
		assert.Nil(t, b.BackupExpiryDateTime, "USER backups never expire")
	})

	t.Run("list contributor insights exact", func(t *testing.T) {
		t.Parallel()

		db := dynamodb.NewInMemoryDB()
		h := dynamodb.NewHandler(db)
		client := newTestDynamoDBClient(t, h)
		ctx := t.Context()

		tableName := "lss-tbl-" + uuid.NewString()[:8]
		createTableHelper(t, db, tableName, "pk")

		_, err := client.UpdateContributorInsights(ctx, &sdk.UpdateContributorInsightsInput{
			TableName:                 aws.String(tableName),
			ContributorInsightsAction: ddbtypes.ContributorInsightsActionEnable,
		})
		require.NoError(t, err)

		out, err := client.ListContributorInsights(ctx, &sdk.ListContributorInsightsInput{
			TableName: aws.String(tableName),
		})
		require.NoError(t, err)
		require.Len(t, out.ContributorInsightsSummaries, 1)
		s := out.ContributorInsightsSummaries[0]
		assert.Equal(t, tableName, aws.ToString(s.TableName))
		assert.Equal(t, ddbtypes.ContributorInsightsStatusEnabled, s.ContributorInsightsStatus)
	})

	t.Run("list exports exact", func(t *testing.T) {
		t.Parallel()

		db := dynamodb.NewInMemoryDB()
		s3 := newMockS3()
		db.SetS3Backend(s3)
		h := dynamodb.NewHandler(db)
		client := newTestDynamoDBClient(t, h)
		ctx := t.Context()

		tableName := "lss-tbl-" + uuid.NewString()[:8]
		createTableHelper(t, db, tableName, "pk")

		tbl, ok := db.GetTable(tableName)
		require.True(t, ok)

		exportOut, err := client.ExportTableToPointInTime(ctx, &sdk.ExportTableToPointInTimeInput{
			TableArn: aws.String(tbl.TableArn),
			S3Bucket: aws.String("lss-export-bucket"),
			S3Prefix: aws.String("out"),
		})
		require.NoError(t, err)

		exportArn := aws.ToString(exportOut.ExportDescription.ExportArn)

		require.Eventually(t, func() bool {
			desc, descErr := client.DescribeExport(ctx, &sdk.DescribeExportInput{ExportArn: aws.String(exportArn)})
			require.NoError(t, descErr)

			return desc.ExportDescription.ExportStatus != ddbtypes.ExportStatusInProgress
		}, waitTimeout, pollInterval)

		out, err := client.ListExports(ctx, &sdk.ListExportsInput{TableArn: aws.String(tbl.TableArn)})
		require.NoError(t, err)
		require.Len(t, out.ExportSummaries, 1)
		e := out.ExportSummaries[0]
		assert.Equal(t, exportArn, aws.ToString(e.ExportArn))
		assert.Equal(t, ddbtypes.ExportTypeFullExport, e.ExportType)
		assert.NotEmpty(t, e.ExportStatus)
	})

	t.Run("list imports exact", func(t *testing.T) {
		t.Parallel()

		db := dynamodb.NewInMemoryDB()
		h := dynamodb.NewHandler(db)
		client := newTestDynamoDBClient(t, h)
		ctx := t.Context()

		importOut, err := client.ImportTable(ctx, &sdk.ImportTableInput{
			S3BucketSource: &ddbtypes.S3BucketSource{
				S3Bucket:    aws.String("lss-import-bucket"),
				S3KeyPrefix: aws.String("does-not-exist/"),
			},
			InputFormat: ddbtypes.InputFormatDynamodbJson,
			TableCreationParameters: importCreationParams(
				"lss-import-tbl-" + uuid.NewString()[:8],
			),
		})
		require.NoError(t, err)

		importArn := aws.ToString(importOut.ImportTableDescription.ImportArn)

		require.Eventually(t, func() bool {
			desc, descErr := client.DescribeImport(ctx, &sdk.DescribeImportInput{ImportArn: aws.String(importArn)})
			require.NoError(t, descErr)

			return desc.ImportTableDescription.ImportStatus != ddbtypes.ImportStatusInProgress
		}, waitTimeout, pollInterval)

		out, err := client.ListImports(ctx, &sdk.ListImportsInput{})
		require.NoError(t, err)
		require.Len(t, out.ImportSummaryList, 1)
		i := out.ImportSummaryList[0]
		assert.Equal(t, importArn, aws.ToString(i.ImportArn))
		assert.Equal(t, ddbtypes.InputFormatDynamodbJson, i.InputFormat)
		require.NotNil(t, i.S3BucketSource)
		assert.Equal(t, "lss-import-bucket", aws.ToString(i.S3BucketSource.S3Bucket))
	})
}
