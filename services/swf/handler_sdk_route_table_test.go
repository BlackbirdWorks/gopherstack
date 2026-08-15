package swf_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Simple
// Workflow Service operation, extracted from swf@v1.37.4 serializers.go:
// each op's awsAwsjson10_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("SimpleWorkflowService.<Op>")
// and always POSTs to "/" -- SWF is one of the older services and, per the
// task, was read rather than assumed: it turns out to be plain JSON-RPC 1.0
// (awsAwsjson10_ prefix, services/_PROTOCOLS.md), not version-stamped like
// CodeBuild ("CodeBuild_20161006.") or EC2ContainerRegistry
// ("..._V20150921."), and its target prefix carries no date/version suffix
// at all -- just "SimpleWorkflowService.". Since there is no path template
// to get wrong, dispatch is entirely by this one header. ExtractOperation
// and Handler() both derive the action the same way (TrimPrefix on
// "SimpleWorkflowService."), so the class of bug this table can catch is a
// dispatch-table key that doesn't exactly match the real op name (typo,
// wrong case -- SWF is case-sensitive JSON-RPC), not a route-template
// mismatch.
//
// This table covers all 39 real SWF ops, which is also gopherstack's full
// implemented set (h.GetSupportedOperations(), 39/39) as of swf@v1.37.4 --
// confirmed by diffing both GetSupportedOperations() and the actual
// buildOps() dispatch table against this exact list, zero mismatches
// either direction: no dead key, no gap.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("SimpleWorkflowService.` and pulling
// the suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"CountClosedWorkflowExecutions", "SimpleWorkflowService.CountClosedWorkflowExecutions"},
		{"CountOpenWorkflowExecutions", "SimpleWorkflowService.CountOpenWorkflowExecutions"},
		{"CountPendingActivityTasks", "SimpleWorkflowService.CountPendingActivityTasks"},
		{"CountPendingDecisionTasks", "SimpleWorkflowService.CountPendingDecisionTasks"},
		{"DeleteActivityType", "SimpleWorkflowService.DeleteActivityType"},
		{"DeleteWorkflowType", "SimpleWorkflowService.DeleteWorkflowType"},
		{"DeprecateActivityType", "SimpleWorkflowService.DeprecateActivityType"},
		{"DeprecateDomain", "SimpleWorkflowService.DeprecateDomain"},
		{"DeprecateWorkflowType", "SimpleWorkflowService.DeprecateWorkflowType"},
		{"DescribeActivityType", "SimpleWorkflowService.DescribeActivityType"},
		{"DescribeDomain", "SimpleWorkflowService.DescribeDomain"},
		{"DescribeWorkflowExecution", "SimpleWorkflowService.DescribeWorkflowExecution"},
		{"DescribeWorkflowType", "SimpleWorkflowService.DescribeWorkflowType"},
		{"GetWorkflowExecutionHistory", "SimpleWorkflowService.GetWorkflowExecutionHistory"},
		{"ListActivityTypes", "SimpleWorkflowService.ListActivityTypes"},
		{"ListClosedWorkflowExecutions", "SimpleWorkflowService.ListClosedWorkflowExecutions"},
		{"ListDomains", "SimpleWorkflowService.ListDomains"},
		{"ListOpenWorkflowExecutions", "SimpleWorkflowService.ListOpenWorkflowExecutions"},
		{"ListTagsForResource", "SimpleWorkflowService.ListTagsForResource"},
		{"ListWorkflowTypes", "SimpleWorkflowService.ListWorkflowTypes"},
		{"PollForActivityTask", "SimpleWorkflowService.PollForActivityTask"},
		{"PollForDecisionTask", "SimpleWorkflowService.PollForDecisionTask"},
		{"RecordActivityTaskHeartbeat", "SimpleWorkflowService.RecordActivityTaskHeartbeat"},
		{"RegisterActivityType", "SimpleWorkflowService.RegisterActivityType"},
		{"RegisterDomain", "SimpleWorkflowService.RegisterDomain"},
		{"RegisterWorkflowType", "SimpleWorkflowService.RegisterWorkflowType"},
		{"RequestCancelWorkflowExecution", "SimpleWorkflowService.RequestCancelWorkflowExecution"},
		{"RespondActivityTaskCanceled", "SimpleWorkflowService.RespondActivityTaskCanceled"},
		{"RespondActivityTaskCompleted", "SimpleWorkflowService.RespondActivityTaskCompleted"},
		{"RespondActivityTaskFailed", "SimpleWorkflowService.RespondActivityTaskFailed"},
		{"RespondDecisionTaskCompleted", "SimpleWorkflowService.RespondDecisionTaskCompleted"},
		{"SignalWorkflowExecution", "SimpleWorkflowService.SignalWorkflowExecution"},
		{"StartWorkflowExecution", "SimpleWorkflowService.StartWorkflowExecution"},
		{"TagResource", "SimpleWorkflowService.TagResource"},
		{"TerminateWorkflowExecution", "SimpleWorkflowService.TerminateWorkflowExecution"},
		{"UndeprecateActivityType", "SimpleWorkflowService.UndeprecateActivityType"},
		{"UndeprecateDomain", "SimpleWorkflowService.UndeprecateDomain"},
		{"UndeprecateWorkflowType", "SimpleWorkflowService.UndeprecateWorkflowType"},
		{"UntagResource", "SimpleWorkflowService.UntagResource"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real SWF operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler()
// does not fall through to the "UnknownOperationException" sentinel that a
// dispatch-table key mismatch would produce.
//
// ErrUnknownOperation (handler.go) is returned bare from dispatch() (not
// wrapped with the action name, unlike most sibling services), and
// handleError's switch leaves errType empty for it, so the response carries
// no "__type" field at all on this path -- only {"message":
// "UnknownOperationException"}. That message text is unique in the package
// (grepped) and produced only at dispatch()'s single `return nil,
// ErrUnknownOperation` call site, so it cannot collide with any of SWF's
// eleven other, differently-typed/messaged sentinel errors.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := swf.NewHandler(swf.NewInMemoryBackend())
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
