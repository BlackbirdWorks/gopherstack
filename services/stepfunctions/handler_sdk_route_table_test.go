package stepfunctions_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Step
// Functions operation, extracted from sfn@v1.45.4 serializers.go: each
// op's awsAwsjson10_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("AWSStepFunctions.<Op>")
// and always request.Request.Method = "POST" against path "/" -- Step
// Functions is JSON-RPC 1.0 (services/_PROTOCOLS.md, go.mod resolves
// "stepfunctions" to package "sfn"), so unlike a REST-family service there
// is no path template to get wrong: dispatch is entirely by this one
// header. ExtractOperation and Handler() both derive the action the same
// way (split on "."), so the class of bug this table can catch is a
// dispatch-table key that doesn't exactly match the real op name (typo,
// wrong case -- Step Functions is case-sensitive JSON-RPC), not a
// route-template mismatch. gopherstack's RouteMatcher also accepts an
// "AmazonStates." prefix; the real SDK only ever sends
// "AWSStepFunctions.", which is what this table asserts.
//
// This table covers all 37 real Step Functions ops. gopherstack's
// implemented set (h.GetSupportedOperations()) is also 37/37 against this
// list -- "DescribeStateMachineVersion" was previously and incorrectly
// listed as supported; handler.go now documents it as not a real sfn
// operation (no api_op_DescribeStateMachineVersion.go in the pinned SDK),
// confirmed independently here since it is absent from serializers.go too.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AWSStepFunctions.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CreateActivity", "AWSStepFunctions.CreateActivity"},
		{"CreateStateMachine", "AWSStepFunctions.CreateStateMachine"},
		{"CreateStateMachineAlias", "AWSStepFunctions.CreateStateMachineAlias"},
		{"DeleteActivity", "AWSStepFunctions.DeleteActivity"},
		{"DeleteStateMachine", "AWSStepFunctions.DeleteStateMachine"},
		{"DeleteStateMachineAlias", "AWSStepFunctions.DeleteStateMachineAlias"},
		{"DeleteStateMachineVersion", "AWSStepFunctions.DeleteStateMachineVersion"},
		{"DescribeActivity", "AWSStepFunctions.DescribeActivity"},
		{"DescribeExecution", "AWSStepFunctions.DescribeExecution"},
		{"DescribeMapRun", "AWSStepFunctions.DescribeMapRun"},
		{"DescribeStateMachine", "AWSStepFunctions.DescribeStateMachine"},
		{"DescribeStateMachineAlias", "AWSStepFunctions.DescribeStateMachineAlias"},
		{"DescribeStateMachineForExecution", "AWSStepFunctions.DescribeStateMachineForExecution"},
		{"GetActivityTask", "AWSStepFunctions.GetActivityTask"},
		{"GetExecutionHistory", "AWSStepFunctions.GetExecutionHistory"},
		{"ListActivities", "AWSStepFunctions.ListActivities"},
		{"ListExecutions", "AWSStepFunctions.ListExecutions"},
		{"ListMapRuns", "AWSStepFunctions.ListMapRuns"},
		{"ListStateMachineAliases", "AWSStepFunctions.ListStateMachineAliases"},
		{"ListStateMachines", "AWSStepFunctions.ListStateMachines"},
		{"ListStateMachineVersions", "AWSStepFunctions.ListStateMachineVersions"},
		{"ListTagsForResource", "AWSStepFunctions.ListTagsForResource"},
		{"PublishStateMachineVersion", "AWSStepFunctions.PublishStateMachineVersion"},
		{"RedriveExecution", "AWSStepFunctions.RedriveExecution"},
		{"SendTaskFailure", "AWSStepFunctions.SendTaskFailure"},
		{"SendTaskHeartbeat", "AWSStepFunctions.SendTaskHeartbeat"},
		{"SendTaskSuccess", "AWSStepFunctions.SendTaskSuccess"},
		{"StartExecution", "AWSStepFunctions.StartExecution"},
		{"StartSyncExecution", "AWSStepFunctions.StartSyncExecution"},
		{"StopExecution", "AWSStepFunctions.StopExecution"},
		{"TagResource", "AWSStepFunctions.TagResource"},
		{"TestState", "AWSStepFunctions.TestState"},
		{"UntagResource", "AWSStepFunctions.UntagResource"},
		{"UpdateMapRun", "AWSStepFunctions.UpdateMapRun"},
		{"UpdateStateMachine", "AWSStepFunctions.UpdateStateMachine"},
		{"UpdateStateMachineAlias", "AWSStepFunctions.UpdateStateMachineAlias"},
		{"ValidateStateMachineDefinition", "AWSStepFunctions.ValidateStateMachineDefinition"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Step Functions
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to the "UnknownOperationException"
// sentinel that a dispatch-table key mismatch would produce.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend())
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
