package emr_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emr"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real EMR
// operation, extracted from emr@v1.64.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("ElasticMapReduce.<Op>")
// and always POSTs to "/" -- EMR is JSON-RPC 1.1 (services/_PROTOCOLS.md),
// so unlike a REST-family service there is no path template to get wrong:
// dispatch is entirely by this one header. ExtractOperation and Handler()
// both derive the action the same way (TrimPrefix on "ElasticMapReduce."),
// so the class of bug this table can catch is a dispatch-table key that
// doesn't exactly match the real op name (typo, wrong case -- EMR is
// case-sensitive JSON-RPC), not a route-template mismatch.
//
// This table covers all 65 real EMR ops -- confirmed by diffing
// GetSupportedOperations() against this exact list: zero mismatches either
// direction. gopherstack's buildOps() dispatch table carries one EXTRA key,
// "ListTagsForResource", deliberately excluded from both this table and
// GetSupportedOperations() (see handler.go's comment on GetSupportedOperations
// and on the ListTagsForResource map entry): the real EMR API has no such
// operation -- only AddTags/RemoveTags exist, with tags read back via
// DescribeCluster.Tags/DescribeStudio.Tags -- so no real X-Amz-Target could
// ever reach it. The route is kept only as test/tooling scaffolding for this
// package's own tests.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("ElasticMapReduce.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AddInstanceFleet", "ElasticMapReduce.AddInstanceFleet"},
		{"AddInstanceGroups", "ElasticMapReduce.AddInstanceGroups"},
		{"AddJobFlowSteps", "ElasticMapReduce.AddJobFlowSteps"},
		{"AddTags", "ElasticMapReduce.AddTags"},
		{"CancelSteps", "ElasticMapReduce.CancelSteps"},
		{"CreatePersistentAppUI", "ElasticMapReduce.CreatePersistentAppUI"},
		{"CreateSecurityConfiguration", "ElasticMapReduce.CreateSecurityConfiguration"},
		{"CreateStudio", "ElasticMapReduce.CreateStudio"},
		{"CreateStudioSessionMapping", "ElasticMapReduce.CreateStudioSessionMapping"},
		{"DeleteSecurityConfiguration", "ElasticMapReduce.DeleteSecurityConfiguration"},
		{"DeleteStudio", "ElasticMapReduce.DeleteStudio"},
		{"DeleteStudioSessionMapping", "ElasticMapReduce.DeleteStudioSessionMapping"},
		{"DescribeCluster", "ElasticMapReduce.DescribeCluster"},
		{"DescribeJobFlows", "ElasticMapReduce.DescribeJobFlows"},
		{"DescribeNotebookExecution", "ElasticMapReduce.DescribeNotebookExecution"},
		{"DescribePersistentAppUI", "ElasticMapReduce.DescribePersistentAppUI"},
		{"DescribeReleaseLabel", "ElasticMapReduce.DescribeReleaseLabel"},
		{"DescribeSecurityConfiguration", "ElasticMapReduce.DescribeSecurityConfiguration"},
		{"DescribeStep", "ElasticMapReduce.DescribeStep"},
		{"DescribeStudio", "ElasticMapReduce.DescribeStudio"},
		{"GetAutoTerminationPolicy", "ElasticMapReduce.GetAutoTerminationPolicy"},
		{"GetBlockPublicAccessConfiguration", "ElasticMapReduce.GetBlockPublicAccessConfiguration"},
		{"GetClusterSessionCredentials", "ElasticMapReduce.GetClusterSessionCredentials"},
		{"GetManagedScalingPolicy", "ElasticMapReduce.GetManagedScalingPolicy"},
		{"GetOnClusterAppUIPresignedURL", "ElasticMapReduce.GetOnClusterAppUIPresignedURL"},
		{"GetPersistentAppUIPresignedURL", "ElasticMapReduce.GetPersistentAppUIPresignedURL"},
		{"GetSession", "ElasticMapReduce.GetSession"},
		{"GetSessionEndpoint", "ElasticMapReduce.GetSessionEndpoint"},
		{"GetStudioSessionMapping", "ElasticMapReduce.GetStudioSessionMapping"},
		{"ListBootstrapActions", "ElasticMapReduce.ListBootstrapActions"},
		{"ListClusters", "ElasticMapReduce.ListClusters"},
		{"ListInstanceFleets", "ElasticMapReduce.ListInstanceFleets"},
		{"ListInstanceGroups", "ElasticMapReduce.ListInstanceGroups"},
		{"ListInstances", "ElasticMapReduce.ListInstances"},
		{"ListNotebookExecutions", "ElasticMapReduce.ListNotebookExecutions"},
		{"ListReleaseLabels", "ElasticMapReduce.ListReleaseLabels"},
		{"ListSecurityConfigurations", "ElasticMapReduce.ListSecurityConfigurations"},
		{"ListSessions", "ElasticMapReduce.ListSessions"},
		{"ListSteps", "ElasticMapReduce.ListSteps"},
		{"ListStudios", "ElasticMapReduce.ListStudios"},
		{"ListStudioSessionMappings", "ElasticMapReduce.ListStudioSessionMappings"},
		{"ListSupportedInstanceTypes", "ElasticMapReduce.ListSupportedInstanceTypes"},
		{"ModifyCluster", "ElasticMapReduce.ModifyCluster"},
		{"ModifyInstanceFleet", "ElasticMapReduce.ModifyInstanceFleet"},
		{"ModifyInstanceGroups", "ElasticMapReduce.ModifyInstanceGroups"},
		{"PutAutoScalingPolicy", "ElasticMapReduce.PutAutoScalingPolicy"},
		{"PutAutoTerminationPolicy", "ElasticMapReduce.PutAutoTerminationPolicy"},
		{"PutBlockPublicAccessConfiguration", "ElasticMapReduce.PutBlockPublicAccessConfiguration"},
		{"PutManagedScalingPolicy", "ElasticMapReduce.PutManagedScalingPolicy"},
		{"RemoveAutoScalingPolicy", "ElasticMapReduce.RemoveAutoScalingPolicy"},
		{"RemoveAutoTerminationPolicy", "ElasticMapReduce.RemoveAutoTerminationPolicy"},
		{"RemoveManagedScalingPolicy", "ElasticMapReduce.RemoveManagedScalingPolicy"},
		{"RemoveTags", "ElasticMapReduce.RemoveTags"},
		{"RunJobFlow", "ElasticMapReduce.RunJobFlow"},
		{"SetKeepJobFlowAliveWhenNoSteps", "ElasticMapReduce.SetKeepJobFlowAliveWhenNoSteps"},
		{"SetTerminationProtection", "ElasticMapReduce.SetTerminationProtection"},
		{"SetUnhealthyNodeReplacement", "ElasticMapReduce.SetUnhealthyNodeReplacement"},
		{"SetVisibleToAllUsers", "ElasticMapReduce.SetVisibleToAllUsers"},
		{"StartNotebookExecution", "ElasticMapReduce.StartNotebookExecution"},
		{"StartSession", "ElasticMapReduce.StartSession"},
		{"StopNotebookExecution", "ElasticMapReduce.StopNotebookExecution"},
		{"TerminateJobFlows", "ElasticMapReduce.TerminateJobFlows"},
		{"TerminateSession", "ElasticMapReduce.TerminateSession"},
		{"UpdateStudio", "ElasticMapReduce.UpdateStudio"},
		{"UpdateStudioSessionMapping", "ElasticMapReduce.UpdateStudioSessionMapping"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real EMR operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler() does
// not fall through to the "UnknownOperationException" sentinel that a
// dispatch-table key mismatch would produce.
//
// errUnknownAction (handler.go) is wire-typed as "UnknownOperationException",
// distinct from every other error type handleError produces (ordinary
// validation/not-found errors all map to "InvalidRequestException" instead --
// see handleError's switch), so it cannot collide with a legitimate error on
// this all-empty-body table. It has exactly one production call site: the
// h.ops map miss in dispatch().
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := emr.NewInMemoryBackend("000000000000", "us-east-1")
			h := emr.NewHandler(backend)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
