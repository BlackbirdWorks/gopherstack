package resiliencehub

import (
	"context"
	"net/http"
)

func (h *Handler) handleListMetrics(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req listMetricsRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	h.Backend.ListMetrics()

	return marshalResponse(listMetricsResponse{Rows: [][]string{}})
}

func (h *Handler) handleStartMetricsExport(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req startMetricsExportRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	m := h.Backend.StartMetricsExport(&req)

	return marshalResponse(startMetricsExportResponse{MetricsExportID: m.ID, Status: m.Status})
}

func (h *Handler) handleDescribeMetricsExport(_ context.Context, _ *http.Request, body []byte) ([]byte, error) {
	var req metricsExportIDRequest
	if err := decodeJSONBody(body, &req); err != nil {
		return nil, err
	}

	m, err := h.Backend.DescribeMetricsExport(req.MetricsExportID)
	if err != nil {
		return nil, err
	}

	resp := describeMetricsExportResponse{MetricsExportID: m.ID, Status: m.Status, ErrorMessage: m.ErrorMessage}
	if m.ExportLocation != nil {
		resp.ExportLocation = &s3LocationWire{Bucket: m.ExportLocation.Bucket, Prefix: m.ExportLocation.Prefix}
	}

	return marshalResponse(resp)
}
