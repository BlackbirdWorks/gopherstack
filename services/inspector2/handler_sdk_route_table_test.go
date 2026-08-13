package inspector2_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// sdkRouteTableCases is the authoritative method+path for every real
// Inspector2 operation, extracted from inspector2@v1.54.1 serializers.go:
// each entry's "request.Method" and the string passed to
// httpbinding.SplitURI in that op's awsRestjson1_serializeOp<Op>.
// HandleSerialize. PLACEHOLDER stands in for any {Param} URI label -- the
// router does not validate ID shape, so the literal value doesn't matter
// here, only that the path matches Op.
//
// handler_routing_test.go already carries a routing regression suite, but
// it was generated FROM this package's own switch statements (a behavior-
// preservation guardrail for a refactor), not independently re-derived from
// the SDK, and it is missing all 6 Connector ops (CreateConnector/
// UpdateConnector/DeleteConnector/ListConnectors/
// ListConnectorScanConfigurations/UpdateConnectorScanConfiguration). This
// file is the independent, full 81/81 SDK-route-fidelity table the broader
// route audit calls for.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteTableCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AssociateMember", "POST", "/members/associate"},
		{"BatchAssociateCodeSecurityScanConfiguration", "POST", "/codesecurity/scan-configuration/batch/associate"},
		{
			"BatchDisassociateCodeSecurityScanConfiguration", "POST",
			"/codesecurity/scan-configuration/batch/disassociate",
		},
		{"BatchGetAccountStatus", "POST", "/status/batch/get"},
		{"BatchGetCodeSnippet", "POST", "/codesnippet/batchget"},
		{"BatchGetFindingDetails", "POST", "/findings/details/batch/get"},
		{"BatchGetFreeTrialInfo", "POST", "/freetrialinfo/batchget"},
		{"BatchGetMemberEc2DeepInspectionStatus", "POST", "/ec2deepinspectionstatus/member/batch/get"},
		{"BatchUpdateMemberEc2DeepInspectionStatus", "POST", "/ec2deepinspectionstatus/member/batch/update"},
		{"CancelFindingsReport", "POST", "/reporting/cancel"},
		{"CancelSbomExport", "POST", "/sbomexport/cancel"},
		{"CreateCisScanConfiguration", "POST", "/cis/scan-configuration/create"},
		{"CreateCodeSecurityIntegration", "POST", "/codesecurity/integration/create"},
		{"CreateCodeSecurityScanConfiguration", "POST", "/codesecurity/scan-configuration/create"},
		{"CreateConnector", "POST", "/connector/create"},
		{"CreateFilter", "POST", "/filters/create"},
		{"CreateFindingsReport", "POST", "/reporting/create"},
		{"CreateSbomExport", "POST", "/sbomexport/create"},
		{"DeleteCisScanConfiguration", "POST", "/cis/scan-configuration/delete"},
		{"DeleteCodeSecurityIntegration", "POST", "/codesecurity/integration/delete"},
		{"DeleteCodeSecurityScanConfiguration", "POST", "/codesecurity/scan-configuration/delete"},
		{"DeleteConnector", "POST", "/connector/delete"},
		{"DeleteFilter", "POST", "/filters/delete"},
		{"DescribeOrganizationConfiguration", "POST", "/organizationconfiguration/describe"},
		{"Disable", "POST", "/disable"},
		{"DisableDelegatedAdminAccount", "POST", "/delegatedadminaccounts/disable"},
		{"DisassociateMember", "POST", "/members/disassociate"},
		{"Enable", "POST", "/enable"},
		{"EnableDelegatedAdminAccount", "POST", "/delegatedadminaccounts/enable"},
		{"GetCisScanReport", "POST", "/cis/scan/report/get"},
		{"GetCisScanResultDetails", "POST", "/cis/scan-result/details/get"},
		{"GetClustersForImage", "POST", "/cluster/get"},
		{"GetCodeSecurityIntegration", "POST", "/codesecurity/integration/get"},
		{"GetCodeSecurityScan", "POST", "/codesecurity/scan/get"},
		{"GetCodeSecurityScanConfiguration", "POST", "/codesecurity/scan-configuration/get"},
		{"GetConfiguration", "POST", "/configuration/get"},
		{"GetDelegatedAdminAccount", "POST", "/delegatedadminaccounts/get"},
		{"GetEc2DeepInspectionConfiguration", "POST", "/ec2deepinspectionconfiguration/get"},
		{"GetEncryptionKey", "GET", "/encryptionkey/get"},
		{"GetFindingsReportStatus", "POST", "/reporting/status/get"},
		{"GetMember", "POST", "/members/get"},
		{"GetSbomExport", "POST", "/sbomexport/get"},
		{"ListAccountPermissions", "POST", "/accountpermissions/list"},
		{"ListCisScanConfigurations", "POST", "/cis/scan-configuration/list"},
		{"ListCisScanResultsAggregatedByChecks", "POST", "/cis/scan-result/check/list"},
		{"ListCisScanResultsAggregatedByTargetResource", "POST", "/cis/scan-result/resource/list"},
		{"ListCisScans", "POST", "/cis/scan/list"},
		{"ListCodeSecurityIntegrations", "POST", "/codesecurity/integration/list"},
		{
			"ListCodeSecurityScanConfigurationAssociations", "POST",
			"/codesecurity/scan-configuration/associations/list",
		},
		{"ListCodeSecurityScanConfigurations", "POST", "/codesecurity/scan-configuration/list"},
		{"ListConnectorScanConfigurations", "POST", "/connectorscanconfigurations/list"},
		{"ListConnectors", "POST", "/connector/list"},
		{"ListCoverage", "POST", "/coverage/list"},
		{"ListCoverageStatistics", "POST", "/coverage/statistics/list"},
		{"ListDelegatedAdminAccounts", "POST", "/delegatedadminaccounts/list"},
		{"ListFilters", "POST", "/filters/list"},
		{"ListFindingAggregations", "POST", "/findings/aggregation/list"},
		{"ListFindings", "POST", "/findings/list"},
		{"ListMembers", "POST", "/members/list"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"ListUsageTotals", "POST", "/usage/list"},
		{"ResetEncryptionKey", "PUT", "/encryptionkey/reset"},
		{"SearchVulnerabilities", "POST", "/vulnerabilities/search"},
		{"SendCisSessionHealth", "PUT", "/cissession/health/send"},
		{"SendCisSessionTelemetry", "PUT", "/cissession/telemetry/send"},
		{"StartCisSession", "PUT", "/cissession/start"},
		{"StartCodeSecurityScan", "POST", "/codesecurity/scan/start"},
		{"StopCisSession", "PUT", "/cissession/stop"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateCisScanConfiguration", "POST", "/cis/scan-configuration/update"},
		{"UpdateCodeSecurityIntegration", "POST", "/codesecurity/integration/update"},
		{"UpdateCodeSecurityScanConfiguration", "POST", "/codesecurity/scan-configuration/update"},
		{"UpdateConfiguration", "POST", "/configuration/update"},
		{"UpdateConnector", "POST", "/connector/update"},
		{"UpdateConnectorScanConfiguration", "POST", "/connectorscanconfiguration/update"},
		{"UpdateEc2DeepInspectionConfiguration", "POST", "/ec2deepinspectionconfiguration/update"},
		{"UpdateEncryptionKey", "PUT", "/encryptionkey/update"},
		{"UpdateFilter", "POST", "/filters/update"},
		{"UpdateOrgEc2DeepInspectionConfiguration", "POST", "/ec2deepinspectionconfiguration/org/update"},
		{"UpdateOrganizationConfiguration", "POST", "/organizationconfiguration/update"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Inspector2 op's
// authoritative method+path (see sdkRouteTableCases) through ExtractOperation
// and asserts the route table resolves it to the right op. gopherstack-jqh2
// pass 2: re-extracted all 81 inspector2 ops from the pinned SDK, including
// the 6 Connector ops the pre-existing handler_routing_test.go never
// covered, and found the existing classifyPath/classifyExtendedPath tables
// already correct for all 81 -- no bugs.
//
// It then drives the same request through the real Handler() and asserts it
// did not fall through to the "NotImplementedException" 501 that
// handleREST's final default emits (handler.go:244-247) after both
// classifyPath's switch and handleExtendedOps miss -- guarding against an
// operation name that resolves correctly but has no matching dispatch case
// (gopherstack-ey26).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := inspector2.NewHandler(inspector2.NewInMemoryBackend("123456789012", "us-east-1"))

	for _, tc := range sdkRouteTableCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "NotImplementedException",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
