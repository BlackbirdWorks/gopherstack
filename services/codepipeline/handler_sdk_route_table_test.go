package codepipeline_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real CodePipeline
// operation, extracted from codepipeline@v1.49.4 serializers.go: each op's
// awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("CodePipeline_20150709.<Op>")
// and always POSTs to "/" -- CodePipeline is JSON-RPC 1.1
// (services/_PROTOCOLS.md), so unlike a REST-family service there is no path
// template to get wrong: dispatch is entirely by this one header. The target
// prefix ("CodePipeline_20150709", not guessed) is read directly from
// serializers.go. ExtractOperation and Handler() (both via dispatchTable()'s
// map, ExtractOperation's own TrimPrefix and Handler()'s h.dispatch calling
// h.ops) derive the action the same way, so the class of bug this table
// catches is a dispatch-table key that doesn't exactly match the real op
// name (typo, wrong case), not a route-template mismatch.
//
// This table covers all 44 real CodePipeline ops (codepipeline@v1.49.4) --
// confirmed by diffing both GetSupportedOperations() and the actual
// dispatchTable() map's key set against this exact list: zero mismatches in
// either direction, no dead or excluded keys. GetSupportedOperations() here
// is a hand-maintained literal slice, not built by ranging over the dispatch
// map, so the two diffs are genuinely independent checks.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("CodePipeline_20150709.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AcknowledgeJob", "CodePipeline_20150709.AcknowledgeJob"},
		{"AcknowledgeThirdPartyJob", "CodePipeline_20150709.AcknowledgeThirdPartyJob"},
		{"CreateCustomActionType", "CodePipeline_20150709.CreateCustomActionType"},
		{"CreatePipeline", "CodePipeline_20150709.CreatePipeline"},
		{"DeleteCustomActionType", "CodePipeline_20150709.DeleteCustomActionType"},
		{"DeletePipeline", "CodePipeline_20150709.DeletePipeline"},
		{"DeleteWebhook", "CodePipeline_20150709.DeleteWebhook"},
		{"DeregisterWebhookWithThirdParty", "CodePipeline_20150709.DeregisterWebhookWithThirdParty"},
		{"DisableStageTransition", "CodePipeline_20150709.DisableStageTransition"},
		{"EnableStageTransition", "CodePipeline_20150709.EnableStageTransition"},
		{"GetActionType", "CodePipeline_20150709.GetActionType"},
		{"GetJobDetails", "CodePipeline_20150709.GetJobDetails"},
		{"GetPipeline", "CodePipeline_20150709.GetPipeline"},
		{"GetPipelineExecution", "CodePipeline_20150709.GetPipelineExecution"},
		{"GetPipelineState", "CodePipeline_20150709.GetPipelineState"},
		{"GetThirdPartyJobDetails", "CodePipeline_20150709.GetThirdPartyJobDetails"},
		{"ListActionExecutions", "CodePipeline_20150709.ListActionExecutions"},
		{"ListActionTypes", "CodePipeline_20150709.ListActionTypes"},
		{"ListDeployActionExecutionTargets", "CodePipeline_20150709.ListDeployActionExecutionTargets"},
		{"ListPipelineExecutions", "CodePipeline_20150709.ListPipelineExecutions"},
		{"ListPipelines", "CodePipeline_20150709.ListPipelines"},
		{"ListRuleExecutions", "CodePipeline_20150709.ListRuleExecutions"},
		{"ListRuleTypes", "CodePipeline_20150709.ListRuleTypes"},
		{"ListTagsForResource", "CodePipeline_20150709.ListTagsForResource"},
		{"ListWebhooks", "CodePipeline_20150709.ListWebhooks"},
		{"OverrideStageCondition", "CodePipeline_20150709.OverrideStageCondition"},
		{"PollForJobs", "CodePipeline_20150709.PollForJobs"},
		{"PollForThirdPartyJobs", "CodePipeline_20150709.PollForThirdPartyJobs"},
		{"PutActionRevision", "CodePipeline_20150709.PutActionRevision"},
		{"PutApprovalResult", "CodePipeline_20150709.PutApprovalResult"},
		{"PutJobFailureResult", "CodePipeline_20150709.PutJobFailureResult"},
		{"PutJobSuccessResult", "CodePipeline_20150709.PutJobSuccessResult"},
		{"PutThirdPartyJobFailureResult", "CodePipeline_20150709.PutThirdPartyJobFailureResult"},
		{"PutThirdPartyJobSuccessResult", "CodePipeline_20150709.PutThirdPartyJobSuccessResult"},
		{"PutWebhook", "CodePipeline_20150709.PutWebhook"},
		{"RegisterWebhookWithThirdParty", "CodePipeline_20150709.RegisterWebhookWithThirdParty"},
		{"RetryStageExecution", "CodePipeline_20150709.RetryStageExecution"},
		{"RollbackStage", "CodePipeline_20150709.RollbackStage"},
		{"StartPipelineExecution", "CodePipeline_20150709.StartPipelineExecution"},
		{"StopPipelineExecution", "CodePipeline_20150709.StopPipelineExecution"},
		{"TagResource", "CodePipeline_20150709.TagResource"},
		{"UntagResource", "CodePipeline_20150709.UntagResource"},
		{"UpdateActionType", "CodePipeline_20150709.UpdateActionType"},
		{"UpdatePipeline", "CodePipeline_20150709.UpdatePipeline"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real CodePipeline
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to the dispatch-miss sentinel
// (errUnknownAction, handler.go's dispatch() single production call site)
// that a dispatch-table key mismatch would produce.
//
// errUnknownAction maps to "InvalidActionException" in handleError's
// sentinel table -- a wire type not reused by any other sentinel there
// (errInvalidRequest and ErrValidation both map to the different
// "ValidationException"), so asserting on the wire type is safe here, unlike
// codedeploy/codecommit/textract, whose dispatch-miss sentinel shares its
// wire type with ordinary validation errors.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
			h := codepipeline.NewHandler(b)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "InvalidActionException",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
