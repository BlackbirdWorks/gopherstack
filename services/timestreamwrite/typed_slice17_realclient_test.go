package timestreamwrite_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	timestreamwritesdk "github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	twtypes "github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

// TestResumeBatchLoadTask_RealClient covers timestreamwrite's last typed-
// client-uncovered op (gopherstack-n3zi slice 17). A batch load task only
// resumes from PROGRESS_STOPPED or FAILED (batch_load_tasks.go's
// ResumeBatchLoadTask), states the backend never reaches on its own without
// a real S3 load pipeline, so this test uses the backend's own
// SetBatchLoadTaskStatus seed method to reach FAILED before resuming through
// the real aws-sdk-go-v2 client.
func TestResumeBatchLoadTask_RealClient(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	backend := timestreamwrite.NewInMemoryBackend()
	h := timestreamwrite.NewHandler(backend)
	client := newTestTimestreamWriteSDKClient(t, h)

	_, err := client.CreateDatabase(ctx, &timestreamwritesdk.CreateDatabaseInput{
		DatabaseName: aws.String("rbt-db"),
	})
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &timestreamwritesdk.CreateTableInput{
		DatabaseName: aws.String("rbt-db"),
		TableName:    aws.String("rbt-tbl"),
	})
	require.NoError(t, err)

	created, err := client.CreateBatchLoadTask(ctx, &timestreamwritesdk.CreateBatchLoadTaskInput{
		TargetDatabaseName: aws.String("rbt-db"),
		TargetTableName:    aws.String("rbt-tbl"),
		DataSourceConfiguration: &twtypes.DataSourceConfiguration{
			DataFormat: twtypes.BatchLoadDataFormatCsv,
			DataSourceS3Configuration: &twtypes.DataSourceS3Configuration{
				BucketName: aws.String("rbt-source-bucket"),
			},
		},
		ReportConfiguration: &twtypes.ReportConfiguration{
			ReportS3Configuration: &twtypes.ReportS3Configuration{
				BucketName: aws.String("rbt-report-bucket"),
			},
		},
	})
	require.NoError(t, err)

	taskID := aws.ToString(created.TaskId)
	require.NoError(t, backend.SetBatchLoadTaskStatus(taskID, string(twtypes.BatchLoadStatusFailed)))

	_, err = client.ResumeBatchLoadTask(ctx, &timestreamwritesdk.ResumeBatchLoadTaskInput{
		TaskId: aws.String(taskID),
	})
	require.NoError(t, err)

	described, err := client.DescribeBatchLoadTask(ctx, &timestreamwritesdk.DescribeBatchLoadTaskInput{
		TaskId: aws.String(taskID),
	})
	require.NoError(t, err)
	require.NotNil(t, described.BatchLoadTaskDescription)
	assert.Equal(t, twtypes.BatchLoadStatusCreated, described.BatchLoadTaskDescription.TaskStatus)
}
