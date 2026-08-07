package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
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
// SELECT via the AWS SDK: a CREATE TABLE (DDL) mutates the catalog, an INSERT
// (DML) writes rows, and a subsequent SELECT returns those rows through
// GetQueryResults. Mirrors the query-execution flow that terraform-provider-aws
// drives against real Athena.
func TestIntegration_Athena_DDLAndDMLQueryLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createAthenaClient(t)
	ctx := t.Context()

	const (
		workGroup    = "it-athena-ddl-wg"
		database     = "default"
		outputBucket = "s3://it-athena-ddl-results/"
	)

	_, err := client.CreateWorkGroup(ctx, &athenasdk.CreateWorkGroupInput{
		Name: aws.String(workGroup),
		Configuration: &athenatypes.WorkGroupConfiguration{
			ResultConfiguration: &athenatypes.ResultConfiguration{
				OutputLocation: aws.String(outputBucket),
			},
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

		getOut, gerr := client.GetQueryExecution(ctx, &athenasdk.GetQueryExecutionInput{
			QueryExecutionId: aws.String(id),
		})
		require.NoError(t, gerr)
		require.NotNil(t, getOut.QueryExecution)
		require.Equal(t, athenatypes.QueryExecutionStateSucceeded,
			getOut.QueryExecution.Status.State, "query %q should succeed", sql)

		return id
	}

	// DDL: create a table.
	runQuery("CREATE EXTERNAL TABLE it_events (id bigint, label string)")

	// DML: insert rows.
	runQuery("INSERT INTO it_events VALUES (1, 'alpha'), (2, 'beta')")

	// SELECT the inserted rows and read them via GetQueryResults.
	selectID := runQuery("SELECT id, label FROM it_events")

	resultsOut, err := client.GetQueryResults(ctx, &athenasdk.GetQueryResultsInput{
		QueryExecutionId: aws.String(selectID),
	})
	require.NoError(t, err)
	require.NotNil(t, resultsOut.ResultSet)
	// Header row plus the two inserted data rows.
	assert.Len(t, resultsOut.ResultSet.Rows, 3)
}
