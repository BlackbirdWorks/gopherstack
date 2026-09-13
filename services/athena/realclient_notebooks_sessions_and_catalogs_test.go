package athena_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

const (
	realClientAthenaRegion    = "us-east-1"
	realClientAthenaAccountID = "000000000000"
)

// newRealClient stands up a fresh backend/handler/client triple for
// gopherstack-n3zi.
func newRealClient(t *testing.T) *athenasdk.Client {
	t.Helper()

	backend := athena.NewInMemoryBackend(realClientAthenaRegion, realClientAthenaAccountID)
	h := athena.NewHandler(backend)

	return newTestAthenaClient(t, h)
}

// TestRealClient_NotebooksSessionsAndCatalogs drives every op the census
// still listed as uncovered before this pass (gopherstack-n3zi typed-client
// coverage).
func TestRealClient_NotebooksSessionsAndCatalogs(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "capacity_reservations", run: func(t *testing.T) {
			t.Helper()

			ctx := t.Context()
			client := newRealClient(t)

			_, err := client.CreateCapacityReservation(ctx, &athenasdk.CreateCapacityReservationInput{
				Name:       aws.String("slice18-cr"),
				TargetDpus: aws.Int32(24),
			})
			require.NoError(t, err)

			got, err := client.GetCapacityReservation(ctx, &athenasdk.GetCapacityReservationInput{
				Name: aws.String("slice18-cr"),
			})
			require.NoError(t, err)
			require.NotNil(t, got.CapacityReservation)
			assert.Equal(t, int32(24), aws.ToInt32(got.CapacityReservation.TargetDpus))

			listed, err := client.ListCapacityReservations(ctx, &athenasdk.ListCapacityReservationsInput{})
			require.NoError(t, err)

			var found bool

			for _, cr := range listed.CapacityReservations {
				if aws.ToString(cr.Name) == "slice18-cr" {
					found = true
				}
			}

			assert.True(t, found, "expected slice18-cr to appear in ListCapacityReservations")

			_, err = client.UpdateCapacityReservation(ctx, &athenasdk.UpdateCapacityReservationInput{
				Name:       aws.String("slice18-cr"),
				TargetDpus: aws.Int32(48),
			})
			require.NoError(t, err)

			afterUpdate, err := client.GetCapacityReservation(ctx, &athenasdk.GetCapacityReservationInput{
				Name: aws.String("slice18-cr"),
			})
			require.NoError(t, err)
			assert.Equal(t, int32(48), aws.ToInt32(afterUpdate.CapacityReservation.TargetDpus))

			_, err = client.PutCapacityAssignmentConfiguration(ctx, &athenasdk.PutCapacityAssignmentConfigurationInput{
				CapacityReservationName: aws.String("slice18-cr"),
				CapacityAssignments: []athenatypes.CapacityAssignment{
					{WorkGroupNames: []string{"primary"}},
				},
			})
			require.NoError(t, err)

			gotAssign, err := client.GetCapacityAssignmentConfiguration(
				ctx,
				&athenasdk.GetCapacityAssignmentConfigurationInput{CapacityReservationName: aws.String("slice18-cr")},
			)
			require.NoError(t, err)
			require.NotNil(t, gotAssign.CapacityAssignmentConfiguration)
			require.Len(t, gotAssign.CapacityAssignmentConfiguration.CapacityAssignments, 1)
			assert.Equal(
				t,
				[]string{"primary"},
				gotAssign.CapacityAssignmentConfiguration.CapacityAssignments[0].WorkGroupNames,
			)

			_, err = client.CancelCapacityReservation(ctx, &athenasdk.CancelCapacityReservationInput{
				Name: aws.String("slice18-cr"),
			})
			require.NoError(t, err)

			_, err = client.DeleteCapacityReservation(ctx, &athenasdk.DeleteCapacityReservationInput{
				Name: aws.String("slice18-cr"),
			})
			require.NoError(t, err)
		}},
		{name: "notebooks", run: func(t *testing.T) {
			t.Helper()

			ctx := t.Context()
			client := newRealClient(t)

			created, err := client.CreateNotebook(ctx, &athenasdk.CreateNotebookInput{
				WorkGroup: aws.String("primary"),
				Name:      aws.String("slice18-notebook"),
			})
			require.NoError(t, err)
			notebookID := aws.ToString(created.NotebookId)

			meta, err := client.GetNotebookMetadata(ctx, &athenasdk.GetNotebookMetadataInput{
				NotebookId: aws.String(notebookID),
			})
			require.NoError(t, err)
			require.NotNil(t, meta.NotebookMetadata)
			assert.Equal(t, "slice18-notebook", aws.ToString(meta.NotebookMetadata.Name))

			listed, err := client.ListNotebookMetadata(ctx, &athenasdk.ListNotebookMetadataInput{
				WorkGroup: aws.String("primary"),
			})
			require.NoError(t, err)

			var found bool

			for _, nb := range listed.NotebookMetadataList {
				if aws.ToString(nb.NotebookId) == notebookID {
					found = true
				}
			}

			assert.True(t, found, "expected slice18-notebook to appear in ListNotebookMetadata")

			_, err = client.UpdateNotebookMetadata(ctx, &athenasdk.UpdateNotebookMetadataInput{
				NotebookId: aws.String(notebookID),
				Name:       aws.String("slice18-notebook-renamed"),
			})
			require.NoError(t, err)

			afterRename, err := client.GetNotebookMetadata(ctx, &athenasdk.GetNotebookMetadataInput{
				NotebookId: aws.String(notebookID),
			})
			require.NoError(t, err)
			assert.Equal(t, "slice18-notebook-renamed", aws.ToString(afterRename.NotebookMetadata.Name))

			_, err = client.UpdateNotebook(ctx, &athenasdk.UpdateNotebookInput{
				NotebookId: aws.String(notebookID),
				Payload:    aws.String(`{"cells":[]}`),
				Type:       athenatypes.NotebookTypeIpynb,
			})
			require.NoError(t, err)

			exported, err := client.ExportNotebook(ctx, &athenasdk.ExportNotebookInput{
				NotebookId: aws.String(notebookID),
			})
			require.NoError(t, err)
			require.NotNil(t, exported.NotebookMetadata)
			assert.Equal(t, notebookID, aws.ToString(exported.NotebookMetadata.NotebookId))
			assert.JSONEq(t, `{"cells":[]}`, aws.ToString(exported.Payload))

			imported, err := client.ImportNotebook(ctx, &athenasdk.ImportNotebookInput{
				WorkGroup: aws.String("primary"),
				Name:      aws.String("slice18-imported"),
				Type:      athenatypes.NotebookTypeIpynb,
				Payload:   aws.String(`{"cells":["imported"]}`),
			})
			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(imported.NotebookId))

			presigned, err := client.CreatePresignedNotebookUrl(ctx, &athenasdk.CreatePresignedNotebookUrlInput{
				SessionId: aws.String("slice18-fake-session"),
			})
			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(presigned.NotebookUrl))

			_, err = client.DeleteNotebook(ctx, &athenasdk.DeleteNotebookInput{NotebookId: aws.String(notebookID)})
			require.NoError(t, err)

			_, err = client.GetNotebookMetadata(
				ctx,
				&athenasdk.GetNotebookMetadataInput{NotebookId: aws.String(notebookID)},
			)
			assert.Error(t, err, "expected deleted notebook to no longer be describable")
		}},
		{name: "sessions", run: func(t *testing.T) {
			t.Helper()

			ctx := t.Context()
			client := newRealClient(t)

			createdNotebook, err := client.CreateNotebook(ctx, &athenasdk.CreateNotebookInput{
				WorkGroup: aws.String("primary"),
				Name:      aws.String("slice18-session-notebook"),
			})
			require.NoError(t, err)
			notebookID := aws.ToString(createdNotebook.NotebookId)

			started, err := client.StartSession(ctx, &athenasdk.StartSessionInput{
				WorkGroup:       aws.String("primary"),
				NotebookVersion: aws.String("v1"),
				EngineConfiguration: &athenatypes.EngineConfiguration{
					MaxConcurrentDpus: aws.Int32(5),
					AdditionalConfigs: map[string]string{"NotebookId": notebookID},
				},
			})
			require.NoError(t, err)
			sessionID := aws.ToString(started.SessionId)

			status, err := client.GetSessionStatus(ctx, &athenasdk.GetSessionStatusInput{
				SessionId: aws.String(sessionID),
			})
			require.NoError(t, err)
			require.NotNil(t, status.Status)
			assert.NotEmpty(t, status.Status.State)

			endpoint, err := client.GetSessionEndpoint(ctx, &athenasdk.GetSessionEndpointInput{
				SessionId: aws.String(sessionID),
			})
			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(endpoint.EndpointUrl))

			notebookSessions, err := client.ListNotebookSessions(ctx, &athenasdk.ListNotebookSessionsInput{
				NotebookId: aws.String(notebookID),
			})
			require.NoError(t, err)
			require.Len(t, notebookSessions.NotebookSessionsList, 1)
			assert.Equal(t, sessionID, aws.ToString(notebookSessions.NotebookSessionsList[0].SessionId))

			executors, err := client.ListExecutors(ctx, &athenasdk.ListExecutorsInput{SessionId: aws.String(sessionID)})
			require.NoError(t, err)
			assert.NotNil(t, executors.ExecutorsSummary)

			engineVersions, err := client.ListEngineVersions(ctx, &athenasdk.ListEngineVersionsInput{})
			require.NoError(t, err)
			assert.NotEmpty(t, engineVersions.EngineVersions)

			dpuSizes, err := client.ListApplicationDPUSizes(ctx, &athenasdk.ListApplicationDPUSizesInput{})
			require.NoError(t, err)
			assert.NotEmpty(t, dpuSizes.ApplicationDPUSizes)

			sessionARN := "arn:aws:athena:" + realClientAthenaRegion + ":" + realClientAthenaAccountID + ":session/" + sessionID
			dashboard, err := client.GetResourceDashboard(ctx, &athenasdk.GetResourceDashboardInput{
				ResourceARN: aws.String(sessionARN),
			})
			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(dashboard.Url))

			terminated, err := client.TerminateSession(ctx, &athenasdk.TerminateSessionInput{
				SessionId: aws.String(sessionID),
			})
			require.NoError(t, err)
			assert.NotEmpty(t, terminated.State)
		}},
		{name: "calculations", run: func(t *testing.T) {
			t.Helper()

			ctx := t.Context()
			client := newRealClient(t)

			started, err := client.StartSession(ctx, &athenasdk.StartSessionInput{
				WorkGroup: aws.String("primary"),
				EngineConfiguration: &athenatypes.EngineConfiguration{
					MaxConcurrentDpus: aws.Int32(5),
				},
			})
			require.NoError(t, err)
			sessionID := aws.ToString(started.SessionId)

			startedCalc, err := client.StartCalculationExecution(ctx, &athenasdk.StartCalculationExecutionInput{
				SessionId: aws.String(sessionID),
				CodeBlock: aws.String("print('slice18')"),
			})
			require.NoError(t, err)
			calcID := aws.ToString(startedCalc.CalculationExecutionId)

			got, err := client.GetCalculationExecution(ctx, &athenasdk.GetCalculationExecutionInput{
				CalculationExecutionId: aws.String(calcID),
			})
			require.NoError(t, err)
			assert.Equal(t, sessionID, aws.ToString(got.SessionId))

			code, err := client.GetCalculationExecutionCode(ctx, &athenasdk.GetCalculationExecutionCodeInput{
				CalculationExecutionId: aws.String(calcID),
			})
			require.NoError(t, err)
			assert.Equal(t, "print('slice18')", aws.ToString(code.CodeBlock))

			listed, err := client.ListCalculationExecutions(ctx, &athenasdk.ListCalculationExecutionsInput{
				SessionId: aws.String(sessionID),
			})
			require.NoError(t, err)
			require.Len(t, listed.Calculations, 1)
			assert.Equal(t, calcID, aws.ToString(listed.Calculations[0].CalculationExecutionId))

			_, err = client.StopCalculationExecution(ctx, &athenasdk.StopCalculationExecutionInput{
				CalculationExecutionId: aws.String(calcID),
			})
			require.NoError(t, err)
		}},
		{name: "prepared_statements", run: func(t *testing.T) {
			t.Helper()

			ctx := t.Context()
			client := newRealClient(t)

			_, err := client.CreatePreparedStatement(ctx, &athenasdk.CreatePreparedStatementInput{
				StatementName:  aws.String("slice18_stmt"),
				WorkGroup:      aws.String("primary"),
				QueryStatement: aws.String("SELECT * FROM sample_table WHERE id = ?"),
			})
			require.NoError(t, err)

			got, err := client.GetPreparedStatement(ctx, &athenasdk.GetPreparedStatementInput{
				StatementName: aws.String("slice18_stmt"),
				WorkGroup:     aws.String("primary"),
			})
			require.NoError(t, err)
			require.NotNil(t, got.PreparedStatement)
			assert.Equal(
				t,
				"SELECT * FROM sample_table WHERE id = ?",
				aws.ToString(got.PreparedStatement.QueryStatement),
			)

			batch, err := client.BatchGetPreparedStatement(ctx, &athenasdk.BatchGetPreparedStatementInput{
				PreparedStatementNames: []string{"slice18_stmt"},
				WorkGroup:              aws.String("primary"),
			})
			require.NoError(t, err)
			require.Len(t, batch.PreparedStatements, 1)
			assert.Empty(t, batch.UnprocessedPreparedStatementNames)

			listed, err := client.ListPreparedStatements(ctx, &athenasdk.ListPreparedStatementsInput{
				WorkGroup: aws.String("primary"),
			})
			require.NoError(t, err)

			var found bool

			for _, ps := range listed.PreparedStatements {
				if aws.ToString(ps.StatementName) == "slice18_stmt" {
					found = true
				}
			}

			assert.True(t, found, "expected slice18_stmt to appear in ListPreparedStatements")

			_, err = client.UpdatePreparedStatement(ctx, &athenasdk.UpdatePreparedStatementInput{
				StatementName:  aws.String("slice18_stmt"),
				WorkGroup:      aws.String("primary"),
				QueryStatement: aws.String("SELECT * FROM sample_table WHERE id = ? AND value = ?"),
			})
			require.NoError(t, err)

			afterUpdate, err := client.GetPreparedStatement(ctx, &athenasdk.GetPreparedStatementInput{
				StatementName: aws.String("slice18_stmt"),
				WorkGroup:     aws.String("primary"),
			})
			require.NoError(t, err)
			assert.Equal(
				t,
				"SELECT * FROM sample_table WHERE id = ? AND value = ?",
				aws.ToString(afterUpdate.PreparedStatement.QueryStatement),
			)

			_, err = client.DeletePreparedStatement(ctx, &athenasdk.DeletePreparedStatementInput{
				StatementName: aws.String("slice18_stmt"),
				WorkGroup:     aws.String("primary"),
			})
			require.NoError(t, err)

			_, err = client.GetPreparedStatement(ctx, &athenasdk.GetPreparedStatementInput{
				StatementName: aws.String("slice18_stmt"),
				WorkGroup:     aws.String("primary"),
			})
			assert.Error(t, err, "expected deleted prepared statement to no longer be gettable")
		}},
		{name: "named_queries", run: func(t *testing.T) {
			t.Helper()

			ctx := t.Context()
			client := newRealClient(t)

			created, err := client.CreateNamedQuery(ctx, &athenasdk.CreateNamedQueryInput{
				Name:        aws.String("slice18-nq"),
				Database:    aws.String("default"),
				QueryString: aws.String("SELECT 1"),
			})
			require.NoError(t, err)
			queryID := aws.ToString(created.NamedQueryId)

			batch, err := client.BatchGetNamedQuery(ctx, &athenasdk.BatchGetNamedQueryInput{
				NamedQueryIds: []string{queryID},
			})
			require.NoError(t, err)
			require.Len(t, batch.NamedQueries, 1)
			assert.Equal(t, "slice18-nq", aws.ToString(batch.NamedQueries[0].Name))

			_, err = client.UpdateNamedQuery(ctx, &athenasdk.UpdateNamedQueryInput{
				NamedQueryId: aws.String(queryID),
				Name:         aws.String("slice18-nq-renamed"),
				QueryString:  aws.String("SELECT 2"),
			})
			require.NoError(t, err)

			got, err := client.GetNamedQuery(ctx, &athenasdk.GetNamedQueryInput{NamedQueryId: aws.String(queryID)})
			require.NoError(t, err)
			require.NotNil(t, got.NamedQuery)
			assert.Equal(t, "slice18-nq-renamed", aws.ToString(got.NamedQuery.Name))
			assert.Equal(t, "SELECT 2", aws.ToString(got.NamedQuery.QueryString))
		}},
		{name: "data_catalogs", run: func(t *testing.T) {
			t.Helper()

			ctx := t.Context()
			client := newRealClient(t)

			_, err := client.CreateDataCatalog(ctx, &athenasdk.CreateDataCatalogInput{
				Name: aws.String("slice18-catalog"),
				Type: athenatypes.DataCatalogTypeGlue,
			})
			require.NoError(t, err)

			listed, err := client.ListDataCatalogs(ctx, &athenasdk.ListDataCatalogsInput{})
			require.NoError(t, err)

			var found bool

			for _, dc := range listed.DataCatalogsSummary {
				if aws.ToString(dc.CatalogName) == "slice18-catalog" {
					found = true
				}
			}

			assert.True(t, found, "expected slice18-catalog to appear in ListDataCatalogs")

			deleted, err := client.DeleteDataCatalog(ctx, &athenasdk.DeleteDataCatalogInput{
				Name: aws.String("slice18-catalog"),
			})
			require.NoError(t, err)
			require.NotNil(t, deleted.DataCatalog)
			assert.Equal(t, "slice18-catalog", aws.ToString(deleted.DataCatalog.Name))

			_, err = client.GetDataCatalog(ctx, &athenasdk.GetDataCatalogInput{Name: aws.String("slice18-catalog")})
			assert.Error(t, err, "expected deleted data catalog to no longer be describable")
		}},
		{name: "databases_and_tables", run: func(t *testing.T) {
			t.Helper()

			ctx := t.Context()
			client := newRealClient(t)

			db, err := client.GetDatabase(ctx, &athenasdk.GetDatabaseInput{
				CatalogName:  aws.String("AwsDataCatalog"),
				DatabaseName: aws.String("default"),
			})
			require.NoError(t, err)
			require.NotNil(t, db.Database)
			assert.Equal(t, "default", aws.ToString(db.Database.Name))

			listedDBs, err := client.ListDatabases(ctx, &athenasdk.ListDatabasesInput{
				CatalogName: aws.String("AwsDataCatalog"),
			})
			require.NoError(t, err)
			require.NotEmpty(t, listedDBs.DatabaseList)

			table, err := client.GetTableMetadata(ctx, &athenasdk.GetTableMetadataInput{
				CatalogName:  aws.String("AwsDataCatalog"),
				DatabaseName: aws.String("default"),
				TableName:    aws.String("sample_table"),
			})
			require.NoError(t, err)
			require.NotNil(t, table.TableMetadata)
			assert.Equal(t, "sample_table", aws.ToString(table.TableMetadata.Name))

			listedTables, err := client.ListTableMetadata(ctx, &athenasdk.ListTableMetadataInput{
				CatalogName:  aws.String("AwsDataCatalog"),
				DatabaseName: aws.String("default"),
			})
			require.NoError(t, err)
			require.NotEmpty(t, listedTables.TableMetadataList)
		}},
		{name: "tags", run: func(t *testing.T) {
			t.Helper()

			ctx := t.Context()
			client := newRealClient(t)

			wgARN := "arn:aws:athena:" + realClientAthenaRegion + ":" + realClientAthenaAccountID + ":workgroup/primary"

			_, err := client.TagResource(ctx, &athenasdk.TagResourceInput{
				ResourceARN: aws.String(wgARN),
				Tags:        []athenatypes.Tag{{Key: aws.String("owner"), Value: aws.String("slice18")}},
			})
			require.NoError(t, err)

			listed, err := client.ListTagsForResource(ctx, &athenasdk.ListTagsForResourceInput{
				ResourceARN: aws.String(wgARN),
			})
			require.NoError(t, err)
			require.Len(t, listed.Tags, 1)
			assert.Equal(t, "owner", aws.ToString(listed.Tags[0].Key))

			_, err = client.UntagResource(ctx, &athenasdk.UntagResourceInput{
				ResourceARN: aws.String(wgARN),
				TagKeys:     []string{"owner"},
			})
			require.NoError(t, err)

			afterUntag, err := client.ListTagsForResource(ctx, &athenasdk.ListTagsForResourceInput{
				ResourceARN: aws.String(wgARN),
			})
			require.NoError(t, err)
			assert.Empty(t, afterUntag.Tags)
		}},
		{name: "query_executions", run: func(t *testing.T) {
			t.Helper()

			ctx := t.Context()
			backend := athena.NewInMemoryBackend(realClientAthenaRegion, realClientAthenaAccountID)
			h := athena.NewHandler(backend)
			client := newTestAthenaClient(t, h)

			started, err := client.StartQueryExecution(ctx, &athenasdk.StartQueryExecutionInput{
				QueryString: aws.String("SELECT * FROM sample_table"),
				WorkGroup:   aws.String("primary"),
				ResultConfiguration: &athenatypes.ResultConfiguration{
					OutputLocation: aws.String("s3://slice18-bucket/results/"),
				},
			})
			require.NoError(t, err)
			queryID := aws.ToString(started.QueryExecutionId)

			batch, err := client.BatchGetQueryExecution(ctx, &athenasdk.BatchGetQueryExecutionInput{
				QueryExecutionIds: []string{queryID},
			})
			require.NoError(t, err)
			require.Len(t, batch.QueryExecutions, 1)
			assert.Equal(t, queryID, aws.ToString(batch.QueryExecutions[0].QueryExecutionId))
			assert.Empty(t, batch.UnprocessedQueryExecutionIds)

			listed, err := client.ListQueryExecutions(ctx, &athenasdk.ListQueryExecutionsInput{
				WorkGroup: aws.String("primary"),
			})
			require.NoError(t, err)
			assert.Contains(t, listed.QueryExecutionIds, queryID)

			stats, err := client.GetQueryRuntimeStatistics(ctx, &athenasdk.GetQueryRuntimeStatisticsInput{
				QueryExecutionId: aws.String(queryID),
			})
			require.NoError(t, err)
			assert.NotNil(t, stats.QueryRuntimeStatistics)

			backend.SetQueryExecutionState(queryID, "RUNNING", 0)

			_, err = client.StopQueryExecution(ctx, &athenasdk.StopQueryExecutionInput{
				QueryExecutionId: aws.String(queryID),
			})
			require.NoError(t, err)

			afterStop, err := client.GetQueryExecution(ctx, &athenasdk.GetQueryExecutionInput{
				QueryExecutionId: aws.String(queryID),
			})
			require.NoError(t, err)
			require.NotNil(t, afterStop.QueryExecution)
			assert.Equal(t, athenatypes.QueryExecutionStateCancelled, afterStop.QueryExecution.Status.State)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
