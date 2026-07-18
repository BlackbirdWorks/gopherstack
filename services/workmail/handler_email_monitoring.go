package workmail

import (
	"context"
)

// ---- Email Monitoring Configuration ----

type putEmailMonitoringConfigReq struct {
	OrganizationID string `json:"OrganizationId"`
	RoleArn        string `json:"RoleArn"`
	LogGroupArn    string `json:"LogGroupArn"`
}

func (h *Handler) handlePutEmailMonitoringConfiguration(
	_ context.Context, req *putEmailMonitoringConfigReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.PutEmailMonitoringConfiguration(req.OrganizationID, req.RoleArn, req.LogGroupArn)
}

type deleteEmailMonitoringConfigReq struct {
	OrganizationID string `json:"OrganizationId"`
}

func (h *Handler) handleDeleteEmailMonitoringConfiguration(
	_ context.Context, req *deleteEmailMonitoringConfigReq,
) (*struct{}, error) {
	return &struct{}{}, h.Backend.DeleteEmailMonitoringConfiguration(req.OrganizationID)
}

type describeEmailMonitoringConfigReq struct {
	OrganizationID string `json:"OrganizationId"`
}

type describeEmailMonitoringConfigResp struct {
	RoleArn     string `json:"RoleArn,omitempty"`
	LogGroupArn string `json:"LogGroupArn,omitempty"`
}

func (h *Handler) handleDescribeEmailMonitoringConfiguration(
	_ context.Context, req *describeEmailMonitoringConfigReq,
) (*describeEmailMonitoringConfigResp, error) {
	cfg, err := h.Backend.DescribeEmailMonitoringConfiguration(req.OrganizationID)
	if err != nil {
		return nil, err
	}

	return &describeEmailMonitoringConfigResp{RoleArn: cfg.RoleARN, LogGroupArn: cfg.LogGroupARN}, nil
}
