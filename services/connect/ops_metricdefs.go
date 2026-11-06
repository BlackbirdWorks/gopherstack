package connect

import "github.com/blackbirdworks/gopherstack/pkgs/restroute"

func (h *Handler) metricDefinitionOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateMetric"] = replay(h, "CreateMetric",
		func(in *CreateMetricInput) string { return in.ClientToken }, b.CreateMetric)
	ops["DescribeMetric"] = describeOp("MetricId", "Metric", b.DescribeMetric)
	ops["ListMetrics"] = listOp(
		"MetricSummaryList",
		func(r *restroute.Request, t string, m int32) ([]MetricSummary, string, error) {
			return b.ListMetrics(instOf(r), r.Query.Get("type"), t, m)
		},
	)
	ops["DeleteMetric"] = deleteOp("MetricId", b.DeleteMetric)
	ops["UpdateMetricContent"] = typedCtx(b.UpdateMetricContent)
	ops["UpdateMetricMetadata"] = typedCtx(b.UpdateMetricMetadata)
}

