package connect

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func (h *Handler) testCaseOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateTestCase"] = typedCtx(b.CreateTestCase)
	ops["DescribeTestCase"] = typed(b.DescribeTestCase)
	ops["UpdateTestCase"] = typedCtx(b.UpdateTestCase)
	ops["DeleteTestCase"] = deleteOp("TestCaseId", b.DeleteTestCase)
	ops["ListTestCases"] = listOp(
		"TestCaseSummaryList",
		func(r *restroute.Request, t string, m int32) ([]TestCaseSummary, string, error) {
			return b.ListTestCases(instOf(r), t, m)
		},
	)

	ops["StartTestCaseExecution"] = replay(h, "StartTestCaseExecution",
		func(in *StartTestCaseExecutionInput) string { return in.ClientToken },
		func(_ context.Context, in *StartTestCaseExecutionInput) (*StartExecutionOut, error) {
			return b.StartTestCaseExecution(in)
		})
	ops["StopTestCaseExecution"] = typed(b.StopTestCaseExecution)
	ops["ListTestCaseExecutions"] = typed(b.ListTestCaseExecutions)
	ops["ListTestCaseExecutionRecords"] = typed(b.ListTestCaseExecutionRecords)
	ops["GetTestCaseExecutionSummary"] = typed(b.GetTestCaseExecutionSummary)

	ops["StartContactEvaluation"] = replay(h, "StartContactEvaluation",
		func(in *StartContactEvaluationInput) string { return in.ClientToken }, b.StartContactEvaluation)
	ops["UpdateContactEvaluation"] = typed(b.UpdateContactEvaluation)
	ops["SubmitContactEvaluation"] = typed(b.SubmitContactEvaluation)
	ops["DescribeContactEvaluation"] = typed(b.DescribeContactEvaluation)
	ops["DeleteContactEvaluation"] = deleteOp("EvaluationId", b.DeleteContactEvaluation)
	ops["ListContactEvaluations"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		items, err := b.ListContactEvaluations(instOf(req), req.Query.Get("contactId"))
		if err != nil {
			return nil, 0, err
		}

		return map[string]any{"EvaluationSummaryList": orEmpty(items)}, 0, nil
	}
}
