package guardduty_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

// sdkRouteCases is the authoritative method+path for every real GuardDuty
// operation, extracted from guardduty@v1.85.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op.
//
// handler_route_matcher_test.go already carries a smaller, hand-picked
// regression suite (TestRouteMatcher_MethodSensitivity, ~45 of the 90 ops,
// focused on collision-prone families and the UpdateMalwareProtectionPlan
// PATCH bug); this file is the full 90/90 SDK-route-fidelity table the
// broader route audit calls for and is not a duplicate of that one.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AcceptAdministratorInvitation", "POST", "/detector/PLACEHOLDER/administrator"},
		{"AcceptInvitation", "POST", "/detector/PLACEHOLDER/master"},
		{"ArchiveFindings", "POST", "/detector/PLACEHOLDER/findings/archive"},
		{"CreateDetector", "POST", "/detector"},
		{"CreateFilter", "POST", "/detector/PLACEHOLDER/filter"},
		{"CreateIPSet", "POST", "/detector/PLACEHOLDER/ipset"},
		{"CreateInvestigation", "POST", "/detector/PLACEHOLDER/investigation"},
		{"CreateMalwareProtectionPlan", "POST", "/malware-protection-plan"},
		{"CreateMembers", "POST", "/detector/PLACEHOLDER/member"},
		{"CreatePublishingDestination", "POST", "/detector/PLACEHOLDER/publishingDestination"},
		{"CreateSampleFindings", "POST", "/detector/PLACEHOLDER/findings/create"},
		{"CreateThreatEntitySet", "POST", "/detector/PLACEHOLDER/threatentityset"},
		{"CreateThreatIntelSet", "POST", "/detector/PLACEHOLDER/threatintelset"},
		{"CreateTrustedEntitySet", "POST", "/detector/PLACEHOLDER/trustedentityset"},
		{"DeclineInvitations", "POST", "/invitation/decline"},
		{"DeleteDetector", "DELETE", "/detector/PLACEHOLDER"},
		{"DeleteFilter", "DELETE", "/detector/PLACEHOLDER/filter/PLACEHOLDER"},
		{"DeleteIPSet", "DELETE", "/detector/PLACEHOLDER/ipset/PLACEHOLDER"},
		{"DeleteInvitations", "POST", "/invitation/delete"},
		{"DeleteMalwareProtectionPlan", "DELETE", "/malware-protection-plan/PLACEHOLDER"},
		{"DeleteMembers", "POST", "/detector/PLACEHOLDER/member/delete"},
		{"DeletePublishingDestination", "DELETE", "/detector/PLACEHOLDER/publishingDestination/PLACEHOLDER"},
		{"DeleteThreatEntitySet", "DELETE", "/detector/PLACEHOLDER/threatentityset/PLACEHOLDER"},
		{"DeleteThreatIntelSet", "DELETE", "/detector/PLACEHOLDER/threatintelset/PLACEHOLDER"},
		{"DeleteTrustedEntitySet", "DELETE", "/detector/PLACEHOLDER/trustedentityset/PLACEHOLDER"},
		{"DescribeMalwareScans", "POST", "/detector/PLACEHOLDER/malware-scans"},
		{"DescribeOrganizationConfiguration", "GET", "/detector/PLACEHOLDER/admin"},
		{"DescribePublishingDestination", "GET", "/detector/PLACEHOLDER/publishingDestination/PLACEHOLDER"},
		{"DisableOrganizationAdminAccount", "POST", "/admin/disable"},
		{"DisassociateFromAdministratorAccount", "POST", "/detector/PLACEHOLDER/administrator/disassociate"},
		{"DisassociateFromMasterAccount", "POST", "/detector/PLACEHOLDER/master/disassociate"},
		{"DisassociateMembers", "POST", "/detector/PLACEHOLDER/member/disassociate"},
		{"EnableOrganizationAdminAccount", "POST", "/admin/enable"},
		{"GetAdministratorAccount", "GET", "/detector/PLACEHOLDER/administrator"},
		{"GetCoverageStatistics", "POST", "/detector/PLACEHOLDER/coverage/statistics"},
		{"GetDetector", "GET", "/detector/PLACEHOLDER"},
		{"GetFilter", "GET", "/detector/PLACEHOLDER/filter/PLACEHOLDER"},
		{"GetFindings", "POST", "/detector/PLACEHOLDER/findings/get"},
		{"GetFindingsStatistics", "POST", "/detector/PLACEHOLDER/findings/statistics"},
		{"GetIPSet", "GET", "/detector/PLACEHOLDER/ipset/PLACEHOLDER"},
		{"GetInvestigation", "GET", "/detector/PLACEHOLDER/investigation/PLACEHOLDER"},
		{"GetInvitationsCount", "GET", "/invitation/count"},
		{"GetMalwareProtectionPlan", "GET", "/malware-protection-plan/PLACEHOLDER"},
		{"GetMalwareScan", "GET", "/malware-scan/PLACEHOLDER"},
		{"GetMalwareScanSettings", "GET", "/detector/PLACEHOLDER/malware-scan-settings"},
		{"GetMasterAccount", "GET", "/detector/PLACEHOLDER/master"},
		{"GetMemberDetectors", "POST", "/detector/PLACEHOLDER/member/detector/get"},
		{"GetMembers", "POST", "/detector/PLACEHOLDER/member/get"},
		{"GetOrganizationStatistics", "GET", "/organization/statistics"},
		{"GetRemainingFreeTrialDays", "POST", "/detector/PLACEHOLDER/freeTrial/daysRemaining"},
		{"GetThreatEntitySet", "GET", "/detector/PLACEHOLDER/threatentityset/PLACEHOLDER"},
		{"GetThreatIntelSet", "GET", "/detector/PLACEHOLDER/threatintelset/PLACEHOLDER"},
		{"GetTrustedEntitySet", "GET", "/detector/PLACEHOLDER/trustedentityset/PLACEHOLDER"},
		{"GetUsageStatistics", "POST", "/detector/PLACEHOLDER/usage/statistics"},
		{"InviteMembers", "POST", "/detector/PLACEHOLDER/member/invite"},
		{"ListCoverage", "POST", "/detector/PLACEHOLDER/coverage"},
		{"ListDetectors", "GET", "/detector"},
		{"ListFilters", "GET", "/detector/PLACEHOLDER/filter"},
		{"ListFindings", "POST", "/detector/PLACEHOLDER/findings"},
		{"ListIPSets", "GET", "/detector/PLACEHOLDER/ipset"},
		{"ListInvestigations", "POST", "/detector/PLACEHOLDER/investigation/list"},
		{"ListInvitations", "GET", "/invitation"},
		{"ListMalwareProtectionPlans", "GET", "/malware-protection-plan"},
		{"ListMalwareScans", "POST", "/malware-scan"},
		{"ListMembers", "GET", "/detector/PLACEHOLDER/member"},
		{"ListOrganizationAdminAccounts", "GET", "/admin"},
		{"ListPublishingDestinations", "GET", "/detector/PLACEHOLDER/publishingDestination"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"ListThreatEntitySets", "GET", "/detector/PLACEHOLDER/threatentityset"},
		{"ListThreatIntelSets", "GET", "/detector/PLACEHOLDER/threatintelset"},
		{"ListTrustedEntitySets", "GET", "/detector/PLACEHOLDER/trustedentityset"},
		{"SendObjectMalwareScan", "POST", "/object-malware-scan/send"},
		{"StartMalwareScan", "POST", "/malware-scan/start"},
		{"StartMonitoringMembers", "POST", "/detector/PLACEHOLDER/member/start"},
		{"StopMonitoringMembers", "POST", "/detector/PLACEHOLDER/member/stop"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UnarchiveFindings", "POST", "/detector/PLACEHOLDER/findings/unarchive"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateDetector", "POST", "/detector/PLACEHOLDER"},
		{"UpdateFilter", "POST", "/detector/PLACEHOLDER/filter/PLACEHOLDER"},
		{"UpdateFindingsFeedback", "POST", "/detector/PLACEHOLDER/findings/feedback"},
		{"UpdateIPSet", "POST", "/detector/PLACEHOLDER/ipset/PLACEHOLDER"},
		{"UpdateMalwareProtectionPlan", "PATCH", "/malware-protection-plan/PLACEHOLDER"},
		{"UpdateMalwareScanSettings", "POST", "/detector/PLACEHOLDER/malware-scan-settings"},
		{"UpdateMemberDetectors", "POST", "/detector/PLACEHOLDER/member/detector/update"},
		{"UpdateOrganizationConfiguration", "POST", "/detector/PLACEHOLDER/admin"},
		{"UpdatePublishingDestination", "POST", "/detector/PLACEHOLDER/publishingDestination/PLACEHOLDER"},
		{"UpdateThreatEntitySet", "POST", "/detector/PLACEHOLDER/threatentityset/PLACEHOLDER"},
		{"UpdateThreatIntelSet", "POST", "/detector/PLACEHOLDER/threatintelset/PLACEHOLDER"},
		{"UpdateTrustedEntitySet", "POST", "/detector/PLACEHOLDER/trustedentityset/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real GuardDuty op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op. gopherstack-jqh2 pass
// 2: re-extracted all 90 guardduty ops from the pinned SDK and found the
// existing parseRESTPath table already correct -- no new bugs, on top of the
// UpdateMalwareProtectionPlan PATCH fix a prior pass already locked in via
// handler_route_matcher_test.go.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := guardduty.NewHandler(guardduty.NewInMemoryBackend("123456789012", "us-east-1"))

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)
		})
	}
}
