package codedeploy_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real CodeDeploy
// operation, extracted from codedeploy@v1.38.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("CodeDeploy_20141006.<Op>")
// and always POSTs to "/" -- CodeDeploy is JSON-RPC 1.1
// (services/_PROTOCOLS.md), so unlike a REST-family service there is no path
// template to get wrong: dispatch is entirely by this one header. The target
// prefix ("CodeDeploy_20141006", not guessed) is read directly from
// serializers.go. ExtractOperation and Handler() (via dispatchTable()'s map,
// dispatched through h.dispatch) both derive the action the same way, so the
// class of bug this table catches is a dispatch-table key that doesn't
// exactly match the real op name (typo, wrong case), not a route-template
// mismatch.
//
// This table covers all 47 real CodeDeploy ops (codedeploy@v1.38.4) --
// confirmed by diffing both GetSupportedOperations() and the actual
// dispatchTable() map's key set against this exact list: zero mismatches in
// either direction, no dead or excluded keys. GetSupportedOperations() here
// is a hand-maintained literal slice, not built by ranging over the dispatch
// map, so the two diffs are genuinely independent checks.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("CodeDeploy_20141006.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AddTagsToOnPremisesInstances", "CodeDeploy_20141006.AddTagsToOnPremisesInstances"},
		{"BatchGetApplicationRevisions", "CodeDeploy_20141006.BatchGetApplicationRevisions"},
		{"BatchGetApplications", "CodeDeploy_20141006.BatchGetApplications"},
		{"BatchGetDeploymentGroups", "CodeDeploy_20141006.BatchGetDeploymentGroups"},
		{"BatchGetDeploymentInstances", "CodeDeploy_20141006.BatchGetDeploymentInstances"},
		{"BatchGetDeployments", "CodeDeploy_20141006.BatchGetDeployments"},
		{"BatchGetDeploymentTargets", "CodeDeploy_20141006.BatchGetDeploymentTargets"},
		{"BatchGetOnPremisesInstances", "CodeDeploy_20141006.BatchGetOnPremisesInstances"},
		{"ContinueDeployment", "CodeDeploy_20141006.ContinueDeployment"},
		{"CreateApplication", "CodeDeploy_20141006.CreateApplication"},
		{"CreateDeployment", "CodeDeploy_20141006.CreateDeployment"},
		{"CreateDeploymentConfig", "CodeDeploy_20141006.CreateDeploymentConfig"},
		{"CreateDeploymentGroup", "CodeDeploy_20141006.CreateDeploymentGroup"},
		{"DeleteApplication", "CodeDeploy_20141006.DeleteApplication"},
		{"DeleteDeploymentConfig", "CodeDeploy_20141006.DeleteDeploymentConfig"},
		{"DeleteDeploymentGroup", "CodeDeploy_20141006.DeleteDeploymentGroup"},
		{"DeleteGitHubAccountToken", "CodeDeploy_20141006.DeleteGitHubAccountToken"},
		{"DeleteResourcesByExternalId", "CodeDeploy_20141006.DeleteResourcesByExternalId"},
		{"DeregisterOnPremisesInstance", "CodeDeploy_20141006.DeregisterOnPremisesInstance"},
		{"GetApplication", "CodeDeploy_20141006.GetApplication"},
		{"GetApplicationRevision", "CodeDeploy_20141006.GetApplicationRevision"},
		{"GetDeployment", "CodeDeploy_20141006.GetDeployment"},
		{"GetDeploymentConfig", "CodeDeploy_20141006.GetDeploymentConfig"},
		{"GetDeploymentGroup", "CodeDeploy_20141006.GetDeploymentGroup"},
		{"GetDeploymentInstance", "CodeDeploy_20141006.GetDeploymentInstance"},
		{"GetDeploymentTarget", "CodeDeploy_20141006.GetDeploymentTarget"},
		{"GetOnPremisesInstance", "CodeDeploy_20141006.GetOnPremisesInstance"},
		{"ListApplicationRevisions", "CodeDeploy_20141006.ListApplicationRevisions"},
		{"ListApplications", "CodeDeploy_20141006.ListApplications"},
		{"ListDeploymentConfigs", "CodeDeploy_20141006.ListDeploymentConfigs"},
		{"ListDeploymentGroups", "CodeDeploy_20141006.ListDeploymentGroups"},
		{"ListDeploymentInstances", "CodeDeploy_20141006.ListDeploymentInstances"},
		{"ListDeployments", "CodeDeploy_20141006.ListDeployments"},
		{"ListDeploymentTargets", "CodeDeploy_20141006.ListDeploymentTargets"},
		{"ListGitHubAccountTokenNames", "CodeDeploy_20141006.ListGitHubAccountTokenNames"},
		{"ListOnPremisesInstances", "CodeDeploy_20141006.ListOnPremisesInstances"},
		{"ListTagsForResource", "CodeDeploy_20141006.ListTagsForResource"},
		{"PutLifecycleEventHookExecutionStatus", "CodeDeploy_20141006.PutLifecycleEventHookExecutionStatus"},
		{"RegisterApplicationRevision", "CodeDeploy_20141006.RegisterApplicationRevision"},
		{"RegisterOnPremisesInstance", "CodeDeploy_20141006.RegisterOnPremisesInstance"},
		{"RemoveTagsFromOnPremisesInstances", "CodeDeploy_20141006.RemoveTagsFromOnPremisesInstances"},
		{"SkipWaitTimeForInstanceTermination", "CodeDeploy_20141006.SkipWaitTimeForInstanceTermination"},
		{"StopDeployment", "CodeDeploy_20141006.StopDeployment"},
		{"TagResource", "CodeDeploy_20141006.TagResource"},
		{"UntagResource", "CodeDeploy_20141006.UntagResource"},
		{"UpdateApplication", "CodeDeploy_20141006.UpdateApplication"},
		{"UpdateDeploymentGroup", "CodeDeploy_20141006.UpdateDeploymentGroup"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real CodeDeploy operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler()
// does not fall through to the dispatch-miss sentinel (errUnknownAction,
// handler.go's dispatch() single production call site) that a
// dispatch-table key mismatch would produce.
//
// Field-required validation across this package's ~35 call sites now uses
// per-operation sentinels (ErrApplicationNameRequired, ErrDeploymentIDRequired,
// etc.), each mapped to the specific Required exception that operation's own
// deserializer models -- there is no single generic code shared with
// errUnknownAction's dispatch-miss fallback ("InvalidRequestException")
// anymore. This test asserts on the dispatch-miss message text, which is
// unique: dispatch's fmt.Errorf("%w: %s", errUnknownAction, action) always
// renders as `unknown action: <op>`, a substring no field-required message
// ("<field> is required") ever produces.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			b := codedeploy.NewInMemoryBackend("000000000000", "us-east-1")
			h := codedeploy.NewHandler(b)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown action: "+tc.op,
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
