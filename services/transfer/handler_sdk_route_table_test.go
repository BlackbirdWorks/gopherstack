package transfer_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS
// Transfer Family operation, extracted from transfer@v1.75.4 serializers.go:
// each op's awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("TransferService.<Op>")
// and always request.Request.Method = "POST" against path "/" -- Transfer is
// JSON-RPC 1.1 (services/_PROTOCOLS.md), so unlike a REST-family service
// there is no path template to get wrong: dispatch is entirely by this one
// header. ExtractOperation and Handler() both derive the action the same way
// (TrimPrefix on "TransferService."), so the class of bug this table can
// catch is a dispatch-table key that doesn't exactly match the real op name
// (typo, wrong case -- Transfer is case-sensitive JSON-RPC), not a
// route-template mismatch.
//
// This table covers all 71 real Transfer ops, which is also gopherstack's
// full implemented set (h.GetSupportedOperations(), 71/71) as of
// transfer@v1.75.4 -- confirmed by diffing the actual buildOps() dispatch
// table against this exact list, zero mismatches either direction: no dead
// key, no gap.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("TransferService.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CreateAccess", "TransferService.CreateAccess"},
		{"CreateAgreement", "TransferService.CreateAgreement"},
		{"CreateConnector", "TransferService.CreateConnector"},
		{"CreateProfile", "TransferService.CreateProfile"},
		{"CreateServer", "TransferService.CreateServer"},
		{"CreateUser", "TransferService.CreateUser"},
		{"CreateWebApp", "TransferService.CreateWebApp"},
		{"CreateWorkflow", "TransferService.CreateWorkflow"},
		{"DeleteAccess", "TransferService.DeleteAccess"},
		{"DeleteAgreement", "TransferService.DeleteAgreement"},
		{"DeleteCertificate", "TransferService.DeleteCertificate"},
		{"DeleteConnector", "TransferService.DeleteConnector"},
		{"DeleteHostKey", "TransferService.DeleteHostKey"},
		{"DeleteProfile", "TransferService.DeleteProfile"},
		{"DeleteServer", "TransferService.DeleteServer"},
		{"DeleteSshPublicKey", "TransferService.DeleteSshPublicKey"},
		{"DeleteUser", "TransferService.DeleteUser"},
		{"DeleteWebApp", "TransferService.DeleteWebApp"},
		{"DeleteWebAppCustomization", "TransferService.DeleteWebAppCustomization"},
		{"DeleteWorkflow", "TransferService.DeleteWorkflow"},
		{"DescribeAccess", "TransferService.DescribeAccess"},
		{"DescribeAgreement", "TransferService.DescribeAgreement"},
		{"DescribeCertificate", "TransferService.DescribeCertificate"},
		{"DescribeConnector", "TransferService.DescribeConnector"},
		{"DescribeExecution", "TransferService.DescribeExecution"},
		{"DescribeHostKey", "TransferService.DescribeHostKey"},
		{"DescribeProfile", "TransferService.DescribeProfile"},
		{"DescribeSecurityPolicy", "TransferService.DescribeSecurityPolicy"},
		{"DescribeServer", "TransferService.DescribeServer"},
		{"DescribeUser", "TransferService.DescribeUser"},
		{"DescribeWebApp", "TransferService.DescribeWebApp"},
		{"DescribeWebAppCustomization", "TransferService.DescribeWebAppCustomization"},
		{"DescribeWorkflow", "TransferService.DescribeWorkflow"},
		{"ImportCertificate", "TransferService.ImportCertificate"},
		{"ImportHostKey", "TransferService.ImportHostKey"},
		{"ImportSshPublicKey", "TransferService.ImportSshPublicKey"},
		{"ListAccesses", "TransferService.ListAccesses"},
		{"ListAgreements", "TransferService.ListAgreements"},
		{"ListCertificates", "TransferService.ListCertificates"},
		{"ListConnectors", "TransferService.ListConnectors"},
		{"ListExecutions", "TransferService.ListExecutions"},
		{"ListFileTransferResults", "TransferService.ListFileTransferResults"},
		{"ListHostKeys", "TransferService.ListHostKeys"},
		{"ListProfiles", "TransferService.ListProfiles"},
		{"ListSecurityPolicies", "TransferService.ListSecurityPolicies"},
		{"ListServers", "TransferService.ListServers"},
		{"ListTagsForResource", "TransferService.ListTagsForResource"},
		{"ListUsers", "TransferService.ListUsers"},
		{"ListWebApps", "TransferService.ListWebApps"},
		{"ListWorkflows", "TransferService.ListWorkflows"},
		{"SendWorkflowStepState", "TransferService.SendWorkflowStepState"},
		{"StartDirectoryListing", "TransferService.StartDirectoryListing"},
		{"StartFileTransfer", "TransferService.StartFileTransfer"},
		{"StartRemoteDelete", "TransferService.StartRemoteDelete"},
		{"StartRemoteMove", "TransferService.StartRemoteMove"},
		{"StartServer", "TransferService.StartServer"},
		{"StopServer", "TransferService.StopServer"},
		{"TagResource", "TransferService.TagResource"},
		{"TestConnection", "TransferService.TestConnection"},
		{"TestIdentityProvider", "TransferService.TestIdentityProvider"},
		{"UntagResource", "TransferService.UntagResource"},
		{"UpdateAccess", "TransferService.UpdateAccess"},
		{"UpdateAgreement", "TransferService.UpdateAgreement"},
		{"UpdateCertificate", "TransferService.UpdateCertificate"},
		{"UpdateConnector", "TransferService.UpdateConnector"},
		{"UpdateHostKey", "TransferService.UpdateHostKey"},
		{"UpdateProfile", "TransferService.UpdateProfile"},
		{"UpdateServer", "TransferService.UpdateServer"},
		{"UpdateUser", "TransferService.UpdateUser"},
		{"UpdateWebApp", "TransferService.UpdateWebApp"},
		{"UpdateWebAppCustomization", "TransferService.UpdateWebAppCustomization"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Transfer operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler() does
// not fall through to the dispatch-miss sentinel a dispatch-table key
// mismatch would produce.
//
// Transfer's sentinel (errUnknownAction, "unknown action") is wire-mapped to
// "InvalidRequestException" alongside awserr.ErrInvalidParameter, a
// hand-rolled errInvalidRequest, and JSON decode errors -- see handleError's
// switch in handler.go. ErrValidation itself is defined as
// awserr.New("InvalidRequestException", ...), so ordinary bad-input
// responses share that exact wire type. Asserting on it here would produce
// false positives on this all-empty-body table, the same trap iam's
// InvalidAction reuse sets. So this test asserts on errUnknownAction's own
// message text ("unknown action") instead, which is unique in the package
// (grepped) and only ever produced by the dispatch miss at handler.go's
// single fmt.Errorf("%w: %s", errUnknownAction, action) call site.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := transfer.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
			h := transfer.NewHandler(backend)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown action",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
