package redshiftdata_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftdatasdk "github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	redshiftdatatypes "github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

// TestRealClient_StatementsAndSchemas drives the 9 ops that a real
// aws-sdk-go-v2 redshiftdata client had never exercised before this pass
// (gopherstack-n3zi): CancelStatement, DescribeStatement, DescribeTable,
// GetStatementResult, GetStatementResultV2, ListSchemas, ListSessions,
// ListStatements, ListTables.
func TestRealClient_StatementsAndSchemas(t *testing.T) {
	t.Parallel()

	backend := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	h := redshiftdata.NewHandler(backend)
	client := newTestRedshiftDataSDKClient(t, h)
	ctx := t.Context()

	exec, execErr := client.ExecuteStatement(ctx, &redshiftdatasdk.ExecuteStatementInput{
		Sql:               aws.String("SELECT * FROM orders"),
		Database:          aws.String("dev"),
		ClusterIdentifier: aws.String("my-cluster"),
	})
	require.NoError(t, execErr)
	stmtID := aws.ToString(exec.Id)

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "describe_statement",
			run: func(t *testing.T) {
				t.Helper()

				out, err := client.DescribeStatement(
					ctx,
					&redshiftdatasdk.DescribeStatementInput{Id: aws.String(stmtID)},
				)
				require.NoError(t, err)
				require.Equal(t, stmtID, aws.ToString(out.Id))
				require.Equal(t, redshiftdatatypes.StatusStringFinished, out.Status)
				require.True(t, aws.ToBool(out.HasResultSet))
			},
		},
		{
			name: "get_statement_result",
			run: func(t *testing.T) {
				t.Helper()

				out, err := client.GetStatementResult(
					ctx,
					&redshiftdatasdk.GetStatementResultInput{Id: aws.String(stmtID)},
				)
				require.NoError(t, err)
				require.Len(t, out.Records, 1)
				require.Len(t, out.ColumnMetadata, 1)
				require.Equal(t, int64(1), out.TotalNumRows)
			},
		},
		{
			name: "get_statement_result_v2",
			run: func(t *testing.T) {
				t.Helper()

				execCSV, err := client.ExecuteStatement(ctx, &redshiftdatasdk.ExecuteStatementInput{
					Sql:               aws.String("SELECT * FROM orders"),
					Database:          aws.String("dev"),
					ClusterIdentifier: aws.String("my-cluster"),
					ResultFormat:      redshiftdatatypes.ResultFormatStringCsv,
				})
				require.NoError(t, err)

				out, err := client.GetStatementResultV2(ctx, &redshiftdatasdk.GetStatementResultV2Input{
					Id: execCSV.Id,
				})
				require.NoError(t, err)
				require.Len(t, out.Records, 1)
				require.NotNil(t, out.Records[0])
			},
		},
		{
			name: "list_statements",
			run: func(t *testing.T) {
				t.Helper()

				out, err := client.ListStatements(ctx, &redshiftdatasdk.ListStatementsInput{})
				require.NoError(t, err)
				require.NotEmpty(t, out.Statements)
				found := false
				for _, s := range out.Statements {
					if aws.ToString(s.Id) == stmtID {
						found = true
					}
				}
				require.True(t, found, "ListStatements must include the executed statement")
			},
		},
		{
			name: "list_sessions",
			run: func(t *testing.T) {
				t.Helper()

				out, err := client.ListSessions(ctx, &redshiftdatasdk.ListSessionsInput{
					Database:          aws.String("dev"),
					ClusterIdentifier: aws.String("my-cluster"),
				})
				require.NoError(t, err)
				require.NotNil(t, out.Sessions)
			},
		},
		{
			name: "list_schemas",
			run: func(t *testing.T) {
				t.Helper()

				out, err := client.ListSchemas(ctx, &redshiftdatasdk.ListSchemasInput{Database: aws.String("dev")})
				require.NoError(t, err)
				require.NotEmpty(t, out.Schemas)
			},
		},
		{
			name: "list_tables",
			run: func(t *testing.T) {
				t.Helper()

				out, err := client.ListTables(ctx, &redshiftdatasdk.ListTablesInput{Database: aws.String("dev")})
				require.NoError(t, err)
				require.NotEmpty(t, out.Tables)
			},
		},
		{
			name: "describe_table",
			run: func(t *testing.T) {
				t.Helper()

				out, err := client.DescribeTable(ctx, &redshiftdatasdk.DescribeTableInput{Database: aws.String("dev")})
				require.NoError(t, err)
				require.NotEmpty(t, out.ColumnList)
			},
		},
		// CancelStatement: documented in PARITY.md as structurally unreachable --
		// ExecuteStatement/BatchExecuteStatement always complete synchronously to
		// FINISHED, so a statement is always already terminal by the time a
		// client can call CancelStatement. Real AWS: "To be canceled, a query
		// must be running." Asserting the real client decodes the error path
		// (ValidationException) is itself genuine typed coverage.

		{
			name: "cancel_statement_terminal_state",
			run: func(t *testing.T) {
				t.Helper()

				_, err := client.CancelStatement(ctx, &redshiftdatasdk.CancelStatementInput{Id: aws.String(stmtID)})
				require.Error(t, err)
				var valErr *redshiftdatatypes.ValidationException
				require.ErrorAs(
					t,
					err,
					&valErr,
					"CancelStatement on an already-terminal statement must decode as a typed ValidationException",
				)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
