package directoryservice_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Directory
// Service operation, extracted from directoryservice@v1.41.4 serializers.go:
// each op's awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String(
// "DirectoryService_20150416.<Op>") and always POSTs to "/" -- Directory
// Service is JSON-RPC 1.1 (services/_PROTOCOLS.md), so unlike a REST-family
// service there is no path template to get wrong: dispatch is entirely by
// this one header. ExtractOperation and Handler() both derive the action the
// same way (CutPrefix on targetPrefix, handler.go), so the class of bug this
// table can catch is a dispatch-table key that doesn't exactly match the
// real op name (typo, wrong case -- Directory Service is case-sensitive
// JSON-RPC), not a route-template mismatch.
//
// This table covers all 80 real Directory Service ops, which is also
// gopherstack's full implemented set (h.GetSupportedOperations(), 80/80)
// as of directoryservice@v1.41.4 -- confirmed by diffing
// GetSupportedOperations() and the h.dispatch map's 80 keys (built in
// NewHandler) against this exact list: zero mismatches in either direction,
// and no dead or excluded keys found.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("DirectoryService_20150416.` and pulling
// the suffix after the dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AcceptSharedDirectory", "DirectoryService_20150416.AcceptSharedDirectory"},
		{"AddIpRoutes", "DirectoryService_20150416.AddIpRoutes"},
		{"AddRegion", "DirectoryService_20150416.AddRegion"},
		{"AddTagsToResource", "DirectoryService_20150416.AddTagsToResource"},
		{"CancelSchemaExtension", "DirectoryService_20150416.CancelSchemaExtension"},
		{"ConnectDirectory", "DirectoryService_20150416.ConnectDirectory"},
		{"CreateAlias", "DirectoryService_20150416.CreateAlias"},
		{"CreateComputer", "DirectoryService_20150416.CreateComputer"},
		{"CreateConditionalForwarder", "DirectoryService_20150416.CreateConditionalForwarder"},
		{"CreateDirectory", "DirectoryService_20150416.CreateDirectory"},
		{"CreateHybridAD", "DirectoryService_20150416.CreateHybridAD"},
		{"CreateLogSubscription", "DirectoryService_20150416.CreateLogSubscription"},
		{"CreateMicrosoftAD", "DirectoryService_20150416.CreateMicrosoftAD"},
		{"CreateSnapshot", "DirectoryService_20150416.CreateSnapshot"},
		{"CreateTrust", "DirectoryService_20150416.CreateTrust"},
		{"DeleteADAssessment", "DirectoryService_20150416.DeleteADAssessment"},
		{"DeleteConditionalForwarder", "DirectoryService_20150416.DeleteConditionalForwarder"},
		{"DeleteDirectory", "DirectoryService_20150416.DeleteDirectory"},
		{"DeleteLogSubscription", "DirectoryService_20150416.DeleteLogSubscription"},
		{"DeleteSnapshot", "DirectoryService_20150416.DeleteSnapshot"},
		{"DeleteTrust", "DirectoryService_20150416.DeleteTrust"},
		{"DeregisterCertificate", "DirectoryService_20150416.DeregisterCertificate"},
		{"DeregisterEventTopic", "DirectoryService_20150416.DeregisterEventTopic"},
		{"DescribeADAssessment", "DirectoryService_20150416.DescribeADAssessment"},
		{"DescribeCAEnrollmentPolicy", "DirectoryService_20150416.DescribeCAEnrollmentPolicy"},
		{"DescribeCertificate", "DirectoryService_20150416.DescribeCertificate"},
		{"DescribeClientAuthenticationSettings", "DirectoryService_20150416.DescribeClientAuthenticationSettings"},
		{"DescribeConditionalForwarders", "DirectoryService_20150416.DescribeConditionalForwarders"},
		{"DescribeDirectories", "DirectoryService_20150416.DescribeDirectories"},
		{"DescribeDirectoryDataAccess", "DirectoryService_20150416.DescribeDirectoryDataAccess"},
		{"DescribeDomainControllers", "DirectoryService_20150416.DescribeDomainControllers"},
		{"DescribeEventTopics", "DirectoryService_20150416.DescribeEventTopics"},
		{"DescribeHybridADUpdate", "DirectoryService_20150416.DescribeHybridADUpdate"},
		{"DescribeLDAPSSettings", "DirectoryService_20150416.DescribeLDAPSSettings"},
		{"DescribeRegions", "DirectoryService_20150416.DescribeRegions"},
		{"DescribeSettings", "DirectoryService_20150416.DescribeSettings"},
		{"DescribeSharedDirectories", "DirectoryService_20150416.DescribeSharedDirectories"},
		{"DescribeSnapshots", "DirectoryService_20150416.DescribeSnapshots"},
		{"DescribeTrusts", "DirectoryService_20150416.DescribeTrusts"},
		{"DescribeUpdateDirectory", "DirectoryService_20150416.DescribeUpdateDirectory"},
		{"DisableCAEnrollmentPolicy", "DirectoryService_20150416.DisableCAEnrollmentPolicy"},
		{"DisableClientAuthentication", "DirectoryService_20150416.DisableClientAuthentication"},
		{"DisableDirectoryDataAccess", "DirectoryService_20150416.DisableDirectoryDataAccess"},
		{"DisableLDAPS", "DirectoryService_20150416.DisableLDAPS"},
		{"DisableRadius", "DirectoryService_20150416.DisableRadius"},
		{"DisableSso", "DirectoryService_20150416.DisableSso"},
		{"EnableCAEnrollmentPolicy", "DirectoryService_20150416.EnableCAEnrollmentPolicy"},
		{"EnableClientAuthentication", "DirectoryService_20150416.EnableClientAuthentication"},
		{"EnableDirectoryDataAccess", "DirectoryService_20150416.EnableDirectoryDataAccess"},
		{"EnableLDAPS", "DirectoryService_20150416.EnableLDAPS"},
		{"EnableRadius", "DirectoryService_20150416.EnableRadius"},
		{"EnableSso", "DirectoryService_20150416.EnableSso"},
		{"GetDirectoryLimits", "DirectoryService_20150416.GetDirectoryLimits"},
		{"GetSnapshotLimits", "DirectoryService_20150416.GetSnapshotLimits"},
		{"ListADAssessments", "DirectoryService_20150416.ListADAssessments"},
		{"ListCertificates", "DirectoryService_20150416.ListCertificates"},
		{"ListIpRoutes", "DirectoryService_20150416.ListIpRoutes"},
		{"ListLogSubscriptions", "DirectoryService_20150416.ListLogSubscriptions"},
		{"ListSchemaExtensions", "DirectoryService_20150416.ListSchemaExtensions"},
		{"ListTagsForResource", "DirectoryService_20150416.ListTagsForResource"},
		{"RegisterCertificate", "DirectoryService_20150416.RegisterCertificate"},
		{"RegisterEventTopic", "DirectoryService_20150416.RegisterEventTopic"},
		{"RejectSharedDirectory", "DirectoryService_20150416.RejectSharedDirectory"},
		{"RemoveIpRoutes", "DirectoryService_20150416.RemoveIpRoutes"},
		{"RemoveRegion", "DirectoryService_20150416.RemoveRegion"},
		{"RemoveTagsFromResource", "DirectoryService_20150416.RemoveTagsFromResource"},
		{"ResetUserPassword", "DirectoryService_20150416.ResetUserPassword"},
		{"RestoreFromSnapshot", "DirectoryService_20150416.RestoreFromSnapshot"},
		{"ShareDirectory", "DirectoryService_20150416.ShareDirectory"},
		{"StartADAssessment", "DirectoryService_20150416.StartADAssessment"},
		{"StartSchemaExtension", "DirectoryService_20150416.StartSchemaExtension"},
		{"UnshareDirectory", "DirectoryService_20150416.UnshareDirectory"},
		{"UpdateConditionalForwarder", "DirectoryService_20150416.UpdateConditionalForwarder"},
		{"UpdateDirectorySetup", "DirectoryService_20150416.UpdateDirectorySetup"},
		{"UpdateHybridAD", "DirectoryService_20150416.UpdateHybridAD"},
		{"UpdateNumberOfDomainControllers", "DirectoryService_20150416.UpdateNumberOfDomainControllers"},
		{"UpdateRadius", "DirectoryService_20150416.UpdateRadius"},
		{"UpdateSettings", "DirectoryService_20150416.UpdateSettings"},
		{"UpdateTrust", "DirectoryService_20150416.UpdateTrust"},
		{"VerifyTrust", "DirectoryService_20150416.VerifyTrust"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Directory Service
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to the "unrecognized operation: " sentinel
// text (doDispatch's miss path, handler.go, its sole production call site
// for the InvalidRequestException wire code) that a dispatch-table key
// mismatch would produce.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unrecognized operation:",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
