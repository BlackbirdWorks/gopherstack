package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_TimestreamWrite_DatabaseLifecycle verifies the full database lifecycle.
func TestIntegration_TimestreamWrite_DatabaseLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createTimestreamWriteClient(t)
	ctx := t.Context()

	dbName := "integ-db-" + uuid.NewString()

	// CreateDatabase
	createOut, err := client.CreateDatabase(ctx, &timestreamwrite.CreateDatabaseInput{
		DatabaseName: aws.String(dbName),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.Database)
	assert.Equal(t, dbName, *createOut.Database.DatabaseName)
	assert.NotEmpty(t, *createOut.Database.Arn)

	// DescribeDatabase
	descOut, err := client.DescribeDatabase(ctx, &timestreamwrite.DescribeDatabaseInput{
		DatabaseName: aws.String(dbName),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Database)
	assert.Equal(t, dbName, *descOut.Database.DatabaseName)

	// ListDatabases – created DB should appear
	listOut, err := client.ListDatabases(ctx, &timestreamwrite.ListDatabasesInput{})
	require.NoError(t, err)
	found := false
	for _, db := range listOut.Databases {
		if *db.DatabaseName == dbName {
			found = true

			break
		}
	}
	assert.True(t, found, "created database should appear in ListDatabases")

	// UpdateDatabase
	kmsKey := "arn:aws:kms:us-east-1:000000000000:key/" + uuid.NewString()
	updateOut, err := client.UpdateDatabase(ctx, &timestreamwrite.UpdateDatabaseInput{
		DatabaseName: aws.String(dbName),
		KmsKeyId:     aws.String(kmsKey),
	})
	require.NoError(t, err)
	require.NotNil(t, updateOut.Database)
	assert.Equal(t, kmsKey, *updateOut.Database.KmsKeyId)

	// DeleteDatabase
	_, err = client.DeleteDatabase(ctx, &timestreamwrite.DeleteDatabaseInput{
		DatabaseName: aws.String(dbName),
	})
	require.NoError(t, err)

	// DescribeDatabase after delete – should fail
	_, err = client.DescribeDatabase(ctx, &timestreamwrite.DescribeDatabaseInput{
		DatabaseName: aws.String(dbName),
	})
	require.Error(t, err)
}

// TestIntegration_TimestreamWrite_TableLifecycle verifies the full table lifecycle.
func TestIntegration_TimestreamWrite_TableLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createTimestreamWriteClient(t)
	ctx := t.Context()

	dbName := "integ-db-tbl-" + uuid.NewString()
	tblName := "integ-tbl-" + uuid.NewString()

	// Create database
	_, err := client.CreateDatabase(ctx, &timestreamwrite.CreateDatabaseInput{
		DatabaseName: aws.String(dbName),
	})
	require.NoError(t, err)

	// CreateTable
	createOut, err := client.CreateTable(ctx, &timestreamwrite.CreateTableInput{
		DatabaseName: aws.String(dbName),
		TableName:    aws.String(tblName),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.Table)
	assert.Equal(t, tblName, *createOut.Table.TableName)
	assert.Equal(t, dbName, *createOut.Table.DatabaseName)
	assert.NotEmpty(t, *createOut.Table.Arn)

	// DescribeTable
	descOut, err := client.DescribeTable(ctx, &timestreamwrite.DescribeTableInput{
		DatabaseName: aws.String(dbName),
		TableName:    aws.String(tblName),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Table)
	assert.Equal(t, tblName, *descOut.Table.TableName)

	// ListTables
	listOut, err := client.ListTables(ctx, &timestreamwrite.ListTablesInput{
		DatabaseName: aws.String(dbName),
	})
	require.NoError(t, err)
	require.Len(t, listOut.Tables, 1)
	assert.Equal(t, tblName, *listOut.Tables[0].TableName)

	// UpdateTable
	updateOut, err := client.UpdateTable(ctx, &timestreamwrite.UpdateTableInput{
		DatabaseName: aws.String(dbName),
		TableName:    aws.String(tblName),
	})
	require.NoError(t, err)
	require.NotNil(t, updateOut.Table)
	assert.Equal(t, tblName, *updateOut.Table.TableName)

	// DeleteTable
	_, err = client.DeleteTable(ctx, &timestreamwrite.DeleteTableInput{
		DatabaseName: aws.String(dbName),
		TableName:    aws.String(tblName),
	})
	require.NoError(t, err)

	// DescribeTable after delete – should fail
	_, err = client.DescribeTable(ctx, &timestreamwrite.DescribeTableInput{
		DatabaseName: aws.String(dbName),
		TableName:    aws.String(tblName),
	})
	require.Error(t, err)

	// Cleanup
	_, err = client.DeleteDatabase(ctx, &timestreamwrite.DeleteDatabaseInput{
		DatabaseName: aws.String(dbName),
	})
	require.NoError(t, err)
}

// TestIntegration_TimestreamWrite_WriteRecords verifies that records can be written.
func TestIntegration_TimestreamWrite_WriteRecords(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createTimestreamWriteClient(t)
	ctx := t.Context()

	dbName := "integ-db-rec-" + uuid.NewString()
	tblName := "integ-tbl-rec-" + uuid.NewString()

	_, err := client.CreateDatabase(ctx, &timestreamwrite.CreateDatabaseInput{
		DatabaseName: aws.String(dbName),
	})
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &timestreamwrite.CreateTableInput{
		DatabaseName: aws.String(dbName),
		TableName:    aws.String(tblName),
	})
	require.NoError(t, err)

	writeOut, err := client.WriteRecords(ctx, &timestreamwrite.WriteRecordsInput{
		DatabaseName: aws.String(dbName),
		TableName:    aws.String(tblName),
		Records: []types.Record{
			{
				MeasureName:      aws.String("cpu_utilization"),
				MeasureValue:     aws.String("42.5"),
				MeasureValueType: types.MeasureValueTypeDouble,
				Time:             aws.String("1609459200000"),
				TimeUnit:         types.TimeUnitMilliseconds,
				Dimensions: []types.Dimension{
					{Name: aws.String("host"), Value: aws.String("server-1")},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, writeOut.RecordsIngested)
	assert.Equal(t, int32(1), writeOut.RecordsIngested.Total)

	// Cleanup
	_, _ = client.DeleteTable(
		ctx,
		&timestreamwrite.DeleteTableInput{DatabaseName: aws.String(dbName), TableName: aws.String(tblName)},
	)
	_, _ = client.DeleteDatabase(ctx, &timestreamwrite.DeleteDatabaseInput{DatabaseName: aws.String(dbName)})
}

// TestIntegration_TimestreamWrite_Tags verifies tag operations on databases.
func TestIntegration_TimestreamWrite_Tags(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createTimestreamWriteClient(t)
	ctx := t.Context()

	dbName := "integ-db-tags-" + uuid.NewString()

	createOut, err := client.CreateDatabase(ctx, &timestreamwrite.CreateDatabaseInput{
		DatabaseName: aws.String(dbName),
	})
	require.NoError(t, err)
	dbARN := *createOut.Database.Arn

	// TagResource
	_, err = client.TagResource(ctx, &timestreamwrite.TagResourceInput{
		ResourceARN: aws.String(dbARN),
		Tags: []types.Tag{
			{Key: aws.String("env"), Value: aws.String("integration")},
			{Key: aws.String("team"), Value: aws.String("platform")},
		},
	})
	require.NoError(t, err)

	// ListTagsForResource
	listOut, err := client.ListTagsForResource(ctx, &timestreamwrite.ListTagsForResourceInput{
		ResourceARN: aws.String(dbARN),
	})
	require.NoError(t, err)
	assert.Len(t, listOut.Tags, 2)
	tagMap := make(map[string]string)
	for _, tag := range listOut.Tags {
		tagMap[*tag.Key] = *tag.Value
	}
	assert.Equal(t, "integration", tagMap["env"])
	assert.Equal(t, "platform", tagMap["team"])

	// UntagResource
	_, err = client.UntagResource(ctx, &timestreamwrite.UntagResourceInput{
		ResourceARN: aws.String(dbARN),
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err)

	// ListTagsForResource after untag
	listOut2, err := client.ListTagsForResource(ctx, &timestreamwrite.ListTagsForResourceInput{
		ResourceARN: aws.String(dbARN),
	})
	require.NoError(t, err)
	assert.Len(t, listOut2.Tags, 1)
	assert.Equal(t, "env", *listOut2.Tags[0].Key)

	// Cleanup
	_, _ = client.DeleteDatabase(ctx, &timestreamwrite.DeleteDatabaseInput{DatabaseName: aws.String(dbName)})
}

// TestIntegration_TimestreamWrite_BatchLoadTask verifies the full batch load task lifecycle.
func TestIntegration_TimestreamWrite_BatchLoadTask(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createTimestreamWriteClient(t)
	ctx := t.Context()

	dbName := "integ-db-batch-" + uuid.NewString()
	tblName := "integ-tbl-batch-" + uuid.NewString()

	// Setup database and table required for batch load task
	_, err := client.CreateDatabase(ctx, &timestreamwrite.CreateDatabaseInput{
		DatabaseName: aws.String(dbName),
	})
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &timestreamwrite.CreateTableInput{
		DatabaseName: aws.String(dbName),
		TableName:    aws.String(tblName),
	})
	require.NoError(t, err)

	// CreateBatchLoadTask
	createOut, err := client.CreateBatchLoadTask(ctx, &timestreamwrite.CreateBatchLoadTaskInput{
		TargetDatabaseName: aws.String(dbName),
		TargetTableName:    aws.String(tblName),
		DataSourceConfiguration: &types.DataSourceConfiguration{
			DataFormat: types.BatchLoadDataFormatCsv,
			DataSourceS3Configuration: &types.DataSourceS3Configuration{
				BucketName: aws.String("my-data-bucket"),
			},
		},
		ReportConfiguration: &types.ReportConfiguration{},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.TaskId)
	taskID := *createOut.TaskId
	assert.NotEmpty(t, taskID)

	// DescribeBatchLoadTask
	descOut, err := client.DescribeBatchLoadTask(ctx, &timestreamwrite.DescribeBatchLoadTaskInput{
		TaskId: aws.String(taskID),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.BatchLoadTaskDescription)
	assert.Equal(t, taskID, *descOut.BatchLoadTaskDescription.TaskId)
	assert.Equal(t, dbName, *descOut.BatchLoadTaskDescription.TargetDatabaseName)
	assert.Equal(t, tblName, *descOut.BatchLoadTaskDescription.TargetTableName)
	assert.Equal(t, types.BatchLoadStatusCreated, descOut.BatchLoadTaskDescription.TaskStatus)

	// ListBatchLoadTasks – should include our task
	listOut, err := client.ListBatchLoadTasks(ctx, &timestreamwrite.ListBatchLoadTasksInput{})
	require.NoError(t, err)
	foundTask := false
	for _, task := range listOut.BatchLoadTasks {
		if *task.TaskId == taskID {
			foundTask = true

			break
		}
	}
	assert.True(t, foundTask, "created batch load task should appear in ListBatchLoadTasks")

	// ListBatchLoadTasks with status filter – CREATED status should include our task
	listFiltered, err := client.ListBatchLoadTasks(ctx, &timestreamwrite.ListBatchLoadTasksInput{
		TaskStatus: types.BatchLoadStatusCreated,
	})
	require.NoError(t, err)
	foundFiltered := false
	for _, task := range listFiltered.BatchLoadTasks {
		if *task.TaskId == taskID {
			foundFiltered = true

			break
		}
	}
	assert.True(t, foundFiltered, "task should be in filtered list for CREATED status")

	// ListBatchLoadTasks with non-matching status filter
	listFailed, err := client.ListBatchLoadTasks(ctx, &timestreamwrite.ListBatchLoadTasksInput{
		TaskStatus: types.BatchLoadStatusFailed,
	})
	require.NoError(t, err)
	for _, task := range listFailed.BatchLoadTasks {
		assert.NotEqual(t, taskID, *task.TaskId, "CREATED task should not appear in FAILED filter")
	}

	// DescribeBatchLoadTask – not found
	_, err = client.DescribeBatchLoadTask(ctx, &timestreamwrite.DescribeBatchLoadTaskInput{
		TaskId: aws.String("nonexistent-task-id"),
	})
	require.Error(t, err)

	// Cleanup
	_, _ = client.DeleteTable(
		ctx,
		&timestreamwrite.DeleteTableInput{DatabaseName: aws.String(dbName), TableName: aws.String(tblName)},
	)
	_, _ = client.DeleteDatabase(ctx, &timestreamwrite.DeleteDatabaseInput{DatabaseName: aws.String(dbName)})
}

// TestIntegration_TimestreamWrite_DescribeEndpoints verifies endpoints are returned.
func TestIntegration_TimestreamWrite_DescribeEndpoints(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createTimestreamWriteClient(t)
	ctx := t.Context()

	out, err := client.DescribeEndpoints(ctx, &timestreamwrite.DescribeEndpointsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.Endpoints)
	assert.NotEmpty(t, *out.Endpoints[0].Address)
}
