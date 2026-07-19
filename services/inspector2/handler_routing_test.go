package inspector2_test

// This file locks in the method+path -> operation routing table for
// classifyPath/classifyExtendedPath (see handler.go and handler_routing.go).
// The (method, path, op) triples below were generated mechanically from the
// original flat switch statements before they were converted to lookup
// tables (CodeFactor "Very Complex Method" cleanup), so this test is a
// behaviour-preservation guardrail, not just new coverage.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// routingCase is one (method, path) -> op assertion for the route tables.
type routingCase struct {
	method string
	path   string
	wantOp string
}

// newRoutingTestHandler returns a fresh Handler for exercising ExtractOperation.
func newRoutingTestHandler(t *testing.T) *inspector2.Handler {
	t.Helper()

	return inspector2.NewHandler(inspector2.NewInMemoryBackend("123456789012", "us-east-1"))
}

// extractOperationFor builds a bare request/context for method+path and
// returns h.ExtractOperation(c), exercising the exact same code path
// (classifyPath -> classifyExtendedPath) that handleREST uses to dispatch.
func extractOperationFor(h *inspector2.Handler, method, path string) string {
	e := echo.New()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	return h.ExtractOperation(c)
}

func runRoutingCases(t *testing.T, cases []routingCase) {
	t.Helper()

	h := newRoutingTestHandler(t)
	for _, tc := range cases {
		got := extractOperationFor(h, tc.method, tc.path)
		assert.Equalf(t, tc.wantOp, got, "method=%s path=%q", tc.method, tc.path)
	}
}

// TestClassifyPath_CoreRoutes locks in every route handled directly by
// handler.go's classifyPath (the core, non-extended operations).
func TestClassifyPath_CoreRoutes(t *testing.T) {
	t.Parallel()

	cases := []routingCase{
		{method: http.MethodPost, path: "/enable", wantOp: "Enable"},
		{method: http.MethodPost, path: "/disable", wantOp: "Disable"},
		{method: http.MethodPost, path: "/status/batch/get", wantOp: "BatchGetAccountStatus"},
		{method: http.MethodPost, path: "/filters/create", wantOp: "CreateFilter"},
		{method: http.MethodPost, path: "/filters/update", wantOp: "UpdateFilter"},
		{method: http.MethodPost, path: "/filters/delete", wantOp: "DeleteFilter"},
		{method: http.MethodPost, path: "/filters/list", wantOp: "ListFilters"},
		{method: http.MethodPost, path: "/findings/list", wantOp: "ListFindings"},
		{method: http.MethodPost, path: "/configuration/get", wantOp: "GetConfiguration"},
		{method: http.MethodPost, path: "/configuration/update", wantOp: "UpdateConfiguration"},
	}

	runRoutingCases(t, cases)
}

// TestClassifyPath_TagsRoutes locks in the /tags/ prefix routes, the only
// non-equality cases in classifyPath (resolved by classifyTagsPath based on
// HTTP method rather than exact path, since the resource ARN is part of the
// path).
func TestClassifyPath_TagsRoutes(t *testing.T) {
	t.Parallel()

	const tagPath = "/tags/arn:aws:inspector2:us-east-1:123456789012:filter/abc"

	cases := []routingCase{
		{method: http.MethodGet, path: tagPath, wantOp: "ListTagsForResource"},
		{method: http.MethodPost, path: tagPath, wantOp: "TagResource"},
		{method: http.MethodDelete, path: tagPath, wantOp: "UntagResource"},
		{method: http.MethodGet, path: "/tags/", wantOp: "ListTagsForResource"}, // bare prefix still matches by method
	}

	runRoutingCases(t, cases)
}

// TestClassifyExtendedPath locks in every one of the 62 extended operations
// routed through classifyExtendedPath (handler_routing.go).
func TestClassifyExtendedPath(t *testing.T) {
	t.Parallel()

	cases := []routingCase{
		{method: http.MethodPost, path: "/members/associate", wantOp: "AssociateMember"},
		{method: http.MethodPost, path: "/members/disassociate", wantOp: "DisassociateMember"},
		{method: http.MethodPost, path: "/members/get", wantOp: "GetMember"},
		{method: http.MethodPost, path: "/members/list", wantOp: "ListMembers"},
		{method: http.MethodPost, path: "/delegatedadminaccounts/enable", wantOp: "EnableDelegatedAdminAccount"},
		{method: http.MethodPost, path: "/delegatedadminaccounts/disable", wantOp: "DisableDelegatedAdminAccount"},
		{method: http.MethodPost, path: "/delegatedadminaccounts/get", wantOp: "GetDelegatedAdminAccount"},
		{method: http.MethodPost, path: "/delegatedadminaccounts/list", wantOp: "ListDelegatedAdminAccounts"},
		{
			method: http.MethodPost,
			path:   "/organizationconfiguration/describe",
			wantOp: "DescribeOrganizationConfiguration",
		},
		{method: http.MethodPost, path: "/organizationconfiguration/update", wantOp: "UpdateOrganizationConfiguration"},
		{
			method: http.MethodPost,
			path:   "/ec2deepinspectionconfiguration/get",
			wantOp: "GetEc2DeepInspectionConfiguration",
		},
		{
			method: http.MethodPost,
			path:   "/ec2deepinspectionconfiguration/update",
			wantOp: "UpdateEc2DeepInspectionConfiguration",
		},
		{
			method: http.MethodPost,
			path:   "/ec2deepinspectionconfiguration/org/update",
			wantOp: "UpdateOrgEc2DeepInspectionConfiguration",
		},
		{
			method: http.MethodPost,
			path:   "/ec2deepinspectionstatus/member/batch/get",
			wantOp: "BatchGetMemberEc2DeepInspectionStatus",
		},
		{
			method: http.MethodPost,
			path:   "/ec2deepinspectionstatus/member/batch/update",
			wantOp: "BatchUpdateMemberEc2DeepInspectionStatus",
		},
		{method: http.MethodGet, path: "/encryptionkey/get", wantOp: "GetEncryptionKey"},
		{method: http.MethodPut, path: "/encryptionkey/reset", wantOp: "ResetEncryptionKey"},
		{method: http.MethodPut, path: "/encryptionkey/update", wantOp: "UpdateEncryptionKey"},
		{method: http.MethodPost, path: "/cis/scan-configuration/create", wantOp: "CreateCisScanConfiguration"},
		{method: http.MethodPost, path: "/cis/scan-configuration/delete", wantOp: "DeleteCisScanConfiguration"},
		{method: http.MethodPost, path: "/cis/scan-configuration/update", wantOp: "UpdateCisScanConfiguration"},
		{method: http.MethodPost, path: "/cis/scan-configuration/list", wantOp: "ListCisScanConfigurations"},
		{method: http.MethodPut, path: "/cissession/start", wantOp: "StartCisSession"},
		{method: http.MethodPut, path: "/cissession/stop", wantOp: "StopCisSession"},
		{method: http.MethodPut, path: "/cissession/health/send", wantOp: "SendCisSessionHealth"},
		{method: http.MethodPut, path: "/cissession/telemetry/send", wantOp: "SendCisSessionTelemetry"},
		{method: http.MethodPost, path: "/cis/scan/report/get", wantOp: "GetCisScanReport"},
		{method: http.MethodPost, path: "/cis/scan-result/details/get", wantOp: "GetCisScanResultDetails"},
		{method: http.MethodPost, path: "/cis/scan/list", wantOp: "ListCisScans"},
		{method: http.MethodPost, path: "/cis/scan-result/check/list", wantOp: "ListCisScanResultsAggregatedByChecks"},
		{
			method: http.MethodPost,
			path:   "/cis/scan-result/resource/list",
			wantOp: "ListCisScanResultsAggregatedByTargetResource",
		},
		{method: http.MethodPost, path: "/codesecurity/integration/create", wantOp: "CreateCodeSecurityIntegration"},
		{method: http.MethodPost, path: "/codesecurity/integration/delete", wantOp: "DeleteCodeSecurityIntegration"},
		{method: http.MethodPost, path: "/codesecurity/integration/get", wantOp: "GetCodeSecurityIntegration"},
		{method: http.MethodPost, path: "/codesecurity/integration/update", wantOp: "UpdateCodeSecurityIntegration"},
		{method: http.MethodPost, path: "/codesecurity/integration/list", wantOp: "ListCodeSecurityIntegrations"},
		{
			method: http.MethodPost,
			path:   "/codesecurity/scan-configuration/create",
			wantOp: "CreateCodeSecurityScanConfiguration",
		},
		{
			method: http.MethodPost,
			path:   "/codesecurity/scan-configuration/delete",
			wantOp: "DeleteCodeSecurityScanConfiguration",
		},
		{
			method: http.MethodPost,
			path:   "/codesecurity/scan-configuration/get",
			wantOp: "GetCodeSecurityScanConfiguration",
		},
		{
			method: http.MethodPost,
			path:   "/codesecurity/scan-configuration/update",
			wantOp: "UpdateCodeSecurityScanConfiguration",
		},
		{
			method: http.MethodPost,
			path:   "/codesecurity/scan-configuration/list",
			wantOp: "ListCodeSecurityScanConfigurations",
		},
		{
			method: http.MethodPost,
			path:   "/codesecurity/scan-configuration/batch/associate",
			wantOp: "BatchAssociateCodeSecurityScanConfiguration",
		},
		{
			method: http.MethodPost,
			path:   "/codesecurity/scan-configuration/batch/disassociate",
			wantOp: "BatchDisassociateCodeSecurityScanConfiguration",
		},
		{
			method: http.MethodPost,
			path:   "/codesecurity/scan-configuration/associations/list",
			wantOp: "ListCodeSecurityScanConfigurationAssociations",
		},
		{method: http.MethodPost, path: "/codesecurity/scan/start", wantOp: "StartCodeSecurityScan"},
		{method: http.MethodPost, path: "/codesecurity/scan/get", wantOp: "GetCodeSecurityScan"},
		{method: http.MethodPost, path: "/reporting/create", wantOp: "CreateFindingsReport"},
		{method: http.MethodPost, path: "/reporting/cancel", wantOp: "CancelFindingsReport"},
		{method: http.MethodPost, path: "/reporting/status/get", wantOp: "GetFindingsReportStatus"},
		{method: http.MethodPost, path: "/sbomexport/create", wantOp: "CreateSbomExport"},
		{method: http.MethodPost, path: "/sbomexport/cancel", wantOp: "CancelSbomExport"},
		{method: http.MethodPost, path: "/sbomexport/get", wantOp: "GetSbomExport"},
		{method: http.MethodPost, path: "/coverage/list", wantOp: "ListCoverage"},
		{method: http.MethodPost, path: "/coverage/statistics/list", wantOp: "ListCoverageStatistics"},
		{method: http.MethodPost, path: "/findings/aggregation/list", wantOp: "ListFindingAggregations"},
		{method: http.MethodPost, path: "/usage/list", wantOp: "ListUsageTotals"},
		{method: http.MethodPost, path: "/accountpermissions/list", wantOp: "ListAccountPermissions"},
		{method: http.MethodPost, path: "/vulnerabilities/search", wantOp: "SearchVulnerabilities"},
		{method: http.MethodPost, path: "/codesnippet/batchget", wantOp: "BatchGetCodeSnippet"},
		{method: http.MethodPost, path: "/findings/details/batch/get", wantOp: "BatchGetFindingDetails"},
		{method: http.MethodPost, path: "/freetrialinfo/batchget", wantOp: "BatchGetFreeTrialInfo"},
		{method: http.MethodPost, path: "/cluster/get", wantOp: "GetClustersForImage"},
	}

	const wantExtendedOpCount = 62 // matches extendedOps() in handler_routing.go

	assert.Len(t, cases, wantExtendedOpCount, "one routing case per extended op")
	runRoutingCases(t, cases)
}

// TestExtractOperation_Unmatched locks in the "no route matches" behaviour:
// ExtractOperation must return opUnknown ("Unknown") rather than panicking
// or misrouting.
func TestExtractOperation_Unmatched(t *testing.T) {
	t.Parallel()

	cases := []routingCase{
		{method: http.MethodPost, path: "/does-not-exist", wantOp: "Unknown"},
		{method: http.MethodGet, path: "/members/associate", wantOp: "Unknown"}, // right path, wrong method
		{method: http.MethodDelete, path: "/enable", wantOp: "Unknown"},         // right path, wrong method
		{method: http.MethodPost, path: "/members/associat", wantOp: "Unknown"}, // near-miss path
		{method: http.MethodPost, path: "/", wantOp: "Unknown"},                 // root path
	}

	runRoutingCases(t, cases)
}
