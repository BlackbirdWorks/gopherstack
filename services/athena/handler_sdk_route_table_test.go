package athena_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Athena
// operation, extracted from athena@v1.60.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("AmazonAthena.<Op>")
// and always POSTs to "/" -- Athena is JSON-RPC 1.1 (services/_PROTOCOLS.md),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one header. ExtractOperation and Handler()
// both derive the action the same way (splitting on "." and taking the
// second segment), so the class of bug this table can catch is a
// dispatch-table key that doesn't exactly match the real op name (typo,
// wrong case -- Athena is case-sensitive JSON-RPC), not a route-template
// mismatch.
//
// This table covers all 70 real Athena ops, which is also gopherstack's full
// implemented set (h.GetSupportedOperations(), 70/70) as of athena@v1.60.4
// -- confirmed by diffing both GetSupportedOperations() and the actual
// buildDispatchTable() dispatch table (all eleven per-resource *Ops()
// builders combined) against this exact list, zero mismatches either
// direction: no dead key, no gap.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AmazonAthena.` and pulling the suffix
// after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"BatchGetNamedQuery", "AmazonAthena.BatchGetNamedQuery"},
		{"BatchGetPreparedStatement", "AmazonAthena.BatchGetPreparedStatement"},
		{"BatchGetQueryExecution", "AmazonAthena.BatchGetQueryExecution"},
		{"CancelCapacityReservation", "AmazonAthena.CancelCapacityReservation"},
		{"CreateCapacityReservation", "AmazonAthena.CreateCapacityReservation"},
		{"CreateDataCatalog", "AmazonAthena.CreateDataCatalog"},
		{"CreateNamedQuery", "AmazonAthena.CreateNamedQuery"},
		{"CreateNotebook", "AmazonAthena.CreateNotebook"},
		{"CreatePreparedStatement", "AmazonAthena.CreatePreparedStatement"},
		{"CreatePresignedNotebookUrl", "AmazonAthena.CreatePresignedNotebookUrl"},
		{"CreateWorkGroup", "AmazonAthena.CreateWorkGroup"},
		{"DeleteCapacityReservation", "AmazonAthena.DeleteCapacityReservation"},
		{"DeleteDataCatalog", "AmazonAthena.DeleteDataCatalog"},
		{"DeleteNamedQuery", "AmazonAthena.DeleteNamedQuery"},
		{"DeleteNotebook", "AmazonAthena.DeleteNotebook"},
		{"DeletePreparedStatement", "AmazonAthena.DeletePreparedStatement"},
		{"DeleteWorkGroup", "AmazonAthena.DeleteWorkGroup"},
		{"ExportNotebook", "AmazonAthena.ExportNotebook"},
		{"GetCalculationExecution", "AmazonAthena.GetCalculationExecution"},
		{"GetCalculationExecutionCode", "AmazonAthena.GetCalculationExecutionCode"},
		{"GetCalculationExecutionStatus", "AmazonAthena.GetCalculationExecutionStatus"},
		{"GetCapacityAssignmentConfiguration", "AmazonAthena.GetCapacityAssignmentConfiguration"},
		{"GetCapacityReservation", "AmazonAthena.GetCapacityReservation"},
		{"GetDatabase", "AmazonAthena.GetDatabase"},
		{"GetDataCatalog", "AmazonAthena.GetDataCatalog"},
		{"GetNamedQuery", "AmazonAthena.GetNamedQuery"},
		{"GetNotebookMetadata", "AmazonAthena.GetNotebookMetadata"},
		{"GetPreparedStatement", "AmazonAthena.GetPreparedStatement"},
		{"GetQueryExecution", "AmazonAthena.GetQueryExecution"},
		{"GetQueryResults", "AmazonAthena.GetQueryResults"},
		{"GetQueryRuntimeStatistics", "AmazonAthena.GetQueryRuntimeStatistics"},
		{"GetResourceDashboard", "AmazonAthena.GetResourceDashboard"},
		{"GetSession", "AmazonAthena.GetSession"},
		{"GetSessionEndpoint", "AmazonAthena.GetSessionEndpoint"},
		{"GetSessionStatus", "AmazonAthena.GetSessionStatus"},
		{"GetTableMetadata", "AmazonAthena.GetTableMetadata"},
		{"GetWorkGroup", "AmazonAthena.GetWorkGroup"},
		{"ImportNotebook", "AmazonAthena.ImportNotebook"},
		{"ListApplicationDPUSizes", "AmazonAthena.ListApplicationDPUSizes"},
		{"ListCalculationExecutions", "AmazonAthena.ListCalculationExecutions"},
		{"ListCapacityReservations", "AmazonAthena.ListCapacityReservations"},
		{"ListDatabases", "AmazonAthena.ListDatabases"},
		{"ListDataCatalogs", "AmazonAthena.ListDataCatalogs"},
		{"ListEngineVersions", "AmazonAthena.ListEngineVersions"},
		{"ListExecutors", "AmazonAthena.ListExecutors"},
		{"ListNamedQueries", "AmazonAthena.ListNamedQueries"},
		{"ListNotebookMetadata", "AmazonAthena.ListNotebookMetadata"},
		{"ListNotebookSessions", "AmazonAthena.ListNotebookSessions"},
		{"ListPreparedStatements", "AmazonAthena.ListPreparedStatements"},
		{"ListQueryExecutions", "AmazonAthena.ListQueryExecutions"},
		{"ListSessions", "AmazonAthena.ListSessions"},
		{"ListTableMetadata", "AmazonAthena.ListTableMetadata"},
		{"ListTagsForResource", "AmazonAthena.ListTagsForResource"},
		{"ListWorkGroups", "AmazonAthena.ListWorkGroups"},
		{"PutCapacityAssignmentConfiguration", "AmazonAthena.PutCapacityAssignmentConfiguration"},
		{"StartCalculationExecution", "AmazonAthena.StartCalculationExecution"},
		{"StartQueryExecution", "AmazonAthena.StartQueryExecution"},
		{"StartSession", "AmazonAthena.StartSession"},
		{"StopCalculationExecution", "AmazonAthena.StopCalculationExecution"},
		{"StopQueryExecution", "AmazonAthena.StopQueryExecution"},
		{"TagResource", "AmazonAthena.TagResource"},
		{"TerminateSession", "AmazonAthena.TerminateSession"},
		{"UntagResource", "AmazonAthena.UntagResource"},
		{"UpdateCapacityReservation", "AmazonAthena.UpdateCapacityReservation"},
		{"UpdateDataCatalog", "AmazonAthena.UpdateDataCatalog"},
		{"UpdateNamedQuery", "AmazonAthena.UpdateNamedQuery"},
		{"UpdateNotebook", "AmazonAthena.UpdateNotebook"},
		{"UpdateNotebookMetadata", "AmazonAthena.UpdateNotebookMetadata"},
		{"UpdatePreparedStatement", "AmazonAthena.UpdatePreparedStatement"},
		{"UpdateWorkGroup", "AmazonAthena.UpdateWorkGroup"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Athena operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler()
// does not fall through to the dispatch-miss sentinel a dispatch-table key
// mismatch would produce.
//
// Athena's dispatch-miss sentinel (ErrUnknownOperation, handler.go) is
// wire-typed as "InvalidRequestException" -- THE SAME wire type
// handleError's switch also assigns to ErrNotFound, ErrAlreadyExists,
// ErrProtected, and ErrValidation (all four also constructed as
// errors.New(errTypeInvalidRequestExc) in errors.go), so asserting on the
// response __type here would be the workmail/transfer trap: a false
// positive on ordinary working validation. This test instead asserts on the
// dispatch-miss message text, which is unique per op: doDispatch's single
// production call site (fmt.Errorf("%w: %s", ErrUnknownOperation, action))
// always renders as "InvalidRequestException: <action>", and no legitimate
// handler in this package produces that literal string for its own op on
// an empty-body request.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := athena.NewHandler(athena.NewInMemoryBackend("us-east-1", "000000000000"))
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "InvalidRequestException: "+tc.op,
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
