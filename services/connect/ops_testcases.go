package connect

import (
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
}
