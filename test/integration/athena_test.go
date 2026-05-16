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
		_, _ = client.DeleteWorkGroup(ctx, &athenasdk.DeleteWorkGroupInput{
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
		_, _ = client.DeleteNamedQuery(ctx, &athenasdk.DeleteNamedQueryInput{
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
