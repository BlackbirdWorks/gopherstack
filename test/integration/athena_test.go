package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_Athena_WorkGroupAndNamedQueryLifecycle exercises the Athena
// core workflow via the AWS SDK v2: create a workgroup, list/get it, create a
// named query bound to it, list/get the named query, then delete. Primary
// integration coverage for the Athena AmazonAthena. JSON-RPC handler.
func TestIntegration_Athena_WorkGroupAndNamedQueryLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createAthenaClient(t)
	ctx := t.Context()

	const (
		workGroup    = "it-athena-wg"
		queryName    = "it-athena-query"
		database     = "default"
		queryString  = "SELECT 1"
		outputBucket = "s3://it-athena-results/"
	)

	// CreateWorkGroup.
	_, err := client.CreateWorkGroup(ctx, &athenasdk.CreateWorkGroupInput{
		Name:        aws.String(workGroup),
		Description: aws.String("integration test workgroup"),
		Configuration: &athenatypes.WorkGroupConfiguration{
			ResultConfiguration: &athenatypes.ResultConfiguration{
				OutputLocation: aws.String(outputBucket),
			},
			PublishCloudWatchMetricsEnabled: aws.Bool(false),
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteWorkGroup(cleanupCtx, &athenasdk.DeleteWorkGroupInput{
			WorkGroup: aws.String(workGroup),
		})
	})

	// GetWorkGroup.
	getWGOut, err := client.GetWorkGroup(ctx, &athenasdk.GetWorkGroupInput{
		WorkGroup: aws.String(workGroup),
	})
	require.NoError(t, err)
	require.NotNil(t, getWGOut.WorkGroup)
	assert.Equal(t, workGroup, aws.ToString(getWGOut.WorkGroup.Name))

	// ListWorkGroups should contain the new workgroup.
	listWGOut, err := client.ListWorkGroups(ctx, &athenasdk.ListWorkGroupsInput{})
	require.NoError(t, err)

	foundWG := false

	for _, wg := range listWGOut.WorkGroups {
		if aws.ToString(wg.Name) == workGroup {
			foundWG = true

			break
		}
	}

	assert.True(t, foundWG, "newly created workgroup should be listed")

	// CreateNamedQuery referencing the workgroup.
	createNQOut, err := client.CreateNamedQuery(ctx, &athenasdk.CreateNamedQueryInput{
		Name:        aws.String(queryName),
		Database:    aws.String(database),
		QueryString: aws.String(queryString),
		Description: aws.String("integration test named query"),
		WorkGroup:   aws.String(workGroup),
	})
	require.NoError(t, err)
	queryID := aws.ToString(createNQOut.NamedQueryId)
	require.NotEmpty(t, queryID)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteNamedQuery(cleanupCtx, &athenasdk.DeleteNamedQueryInput{
			NamedQueryId: aws.String(queryID),
		})
	})

	// GetNamedQuery.
	getNQOut, err := client.GetNamedQuery(ctx, &athenasdk.GetNamedQueryInput{
		NamedQueryId: aws.String(queryID),
	})
	require.NoError(t, err)
	require.NotNil(t, getNQOut.NamedQuery)
	assert.Equal(t, queryName, aws.ToString(getNQOut.NamedQuery.Name))
	assert.Equal(t, queryString, aws.ToString(getNQOut.NamedQuery.QueryString))
	assert.Equal(t, database, aws.ToString(getNQOut.NamedQuery.Database))

	// ListNamedQueries scoped to the workgroup.
	listNQOut, err := client.ListNamedQueries(ctx, &athenasdk.ListNamedQueriesInput{
		WorkGroup: aws.String(workGroup),
	})
	require.NoError(t, err)
	assert.Contains(t, listNQOut.NamedQueryIds, queryID)
}

// TestIntegration_Athena_DDLAndDMLQueryLifecycle exercises the SQL engine beyond
// SELECT via the AWS SDK: a CREATE EXTERNAL TABLE over an S3 location (DDL)
// registers a Glue table, an INSERT (DML) writes rows to S3, and a SELECT
// returns them through GetQueryResults. Queries run asynchronously, so each
// is polled to a terminal state as with real Athena.
func TestIntegration_Athena_DDLAndDMLQueryLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createAthenaClient(t)
	glueClient := createGlueClient(t)
	s3Client := createS3Client(t)
	ctx := t.Context()

	const (
		workGroup = "it-athena-ddl-wg"
		database  = "it_athena_ddl_db"
		bucket    = "it-athena-ddl-data"
	)

	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = glueClient.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String(database)},
	})
	require.NoError(t, err)

	_, err = client.CreateWorkGroup(ctx, &athenasdk.CreateWorkGroupInput{
		Name: aws.String(workGroup),
		Configuration: &athenatypes.WorkGroupConfiguration{
			ResultConfiguration: &athenatypes.ResultConfiguration{
				OutputLocation: aws.String("s3://" + bucket + "/results/"),
			},
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteWorkGroup(cleanupCtx, &athenasdk.DeleteWorkGroupInput{
			WorkGroup:             aws.String(workGroup),
			RecursiveDeleteOption: aws.Bool(true),
		})
		listOut, lerr := s3Client.ListObjectsV2(cleanupCtx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		if lerr == nil {
			for _, obj := range listOut.Contents {
				_, _ = s3Client.DeleteObject(
					cleanupCtx,
					&s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: obj.Key},
				)
			}
		}

		_, _ = s3Client.DeleteBucket(cleanupCtx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
		_, _ = glueClient.DeleteDatabase(cleanupCtx, &gluesdk.DeleteDatabaseInput{
			Name: aws.String(database),
		})
	})

	queryCtx := &athenatypes.QueryExecutionContext{
		Catalog:  aws.String("AwsDataCatalog"),
		Database: aws.String(database),
	}

	runQuery := func(sql string) string {
		startOut, serr := client.StartQueryExecution(ctx, &athenasdk.StartQueryExecutionInput{
			QueryString:           aws.String(sql),
			WorkGroup:             aws.String(workGroup),
			QueryExecutionContext: queryCtx,
		})
		require.NoError(t, serr)

		id := aws.ToString(startOut.QueryExecutionId)
		require.NotEmpty(t, id)

		var last *athenatypes.QueryExecutionStatus

		require.Eventually(t, func() bool {
			getOut, gerr := client.GetQueryExecution(ctx, &athenasdk.GetQueryExecutionInput{
				QueryExecutionId: aws.String(id),
			})
			if gerr != nil || getOut.QueryExecution == nil {
				return false
			}

			last = getOut.QueryExecution.Status

			return last.State != athenatypes.QueryExecutionStateQueued &&
				last.State != athenatypes.QueryExecutionStateRunning
		}, 30*time.Second, 50*time.Millisecond, "query %q should reach a terminal state", sql)

		require.Equal(t, athenatypes.QueryExecutionStateSucceeded, last.State,
			"query %q should succeed: %s", sql, aws.ToString(last.StateChangeReason))

		return id
	}

	runQuery("CREATE EXTERNAL TABLE it_events (id bigint, label string) " +
		"ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
		"LOCATION 's3://" + bucket + "/events/'")

	runQuery("INSERT INTO it_events VALUES (1, 'alpha'), (2, 'beta')")

	selectID := runQuery("SELECT id, label FROM it_events")

	resultsOut, err := client.GetQueryResults(ctx, &athenasdk.GetQueryResultsInput{
		QueryExecutionId: aws.String(selectID),
	})
	require.NoError(t, err)
	require.NotNil(t, resultsOut.ResultSet)
	// Header row plus the two inserted data rows.
	assert.Len(t, resultsOut.ResultSet.Rows, 3)
}
