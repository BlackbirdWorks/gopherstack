package backup

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type reportDeliveryChannelJSON struct {
	S3BucketName string   `json:"S3BucketName"`
	Formats      []string `json:"Formats,omitempty"`
}

type reportSettingJSON struct {
	ReportTemplate string   `json:"ReportTemplate"`
	FrameworkArns  []string `json:"FrameworkArns,omitempty"`
}

type createReportPlanBody struct {
	ReportPlanName        string                     `json:"ReportPlanName"`
	ReportPlanDescription string                     `json:"ReportPlanDescription,omitempty"`
	ReportDeliveryChannel *reportDeliveryChannelJSON `json:"ReportDeliveryChannel,omitempty"`
	ReportSetting         *reportSettingJSON         `json:"ReportSetting,omitempty"`
	IdempotencyToken      string                     `json:"IdempotencyToken,omitempty"`
}

func (h *Handler) handleCreateReportPlan(c *echo.Context, body []byte) error {
	var in createReportPlanBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.ReportPlanName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "ReportPlanName is required"),
		)
	}

	var deliveryChannel *ReportDeliveryChannel
	if in.ReportDeliveryChannel != nil {
		deliveryChannel = &ReportDeliveryChannel{
			S3BucketName: in.ReportDeliveryChannel.S3BucketName,
			Formats:      in.ReportDeliveryChannel.Formats,
		}
	}
	var reportSetting *ReportSetting
	if in.ReportSetting != nil {
		reportSetting = &ReportSetting{
			ReportTemplate: in.ReportSetting.ReportTemplate,
			FrameworkArns:  in.ReportSetting.FrameworkArns,
		}
	}
	rp, err := h.Backend.CreateReportPlan(
		in.ReportPlanName,
		in.ReportPlanDescription,
		deliveryChannel,
		reportSetting,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyReportPlanArn:  rp.ReportPlanArn,
		keyReportPlanName: rp.ReportPlanName,
		keyCreationTime:   epochSeconds(rp.CreationTime),
	})
}

func (h *Handler) handleListReportPlans(c *echo.Context) error {
	plans := h.Backend.ListReportPlans()
	items := make([]map[string]any, 0, len(plans))

	for _, rp := range plans {
		items = append(items, map[string]any{
			keyReportPlanArn:        rp.ReportPlanArn,
			keyReportPlanName:       rp.ReportPlanName,
			"ReportPlanDescription": rp.ReportPlanDescription,
			keyCreationTime:         epochSeconds(rp.CreationTime),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ReportPlans": items,
	})
}

func (h *Handler) handleDescribeReportPlan(c *echo.Context, name string) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "ReportPlanName is required"),
		)
	}

	rp, err := h.Backend.DescribeReportPlan(name)
	if err != nil {
		return h.handleError(c, err)
	}

	rpDoc := map[string]any{
		keyReportPlanArn:        rp.ReportPlanArn,
		keyReportPlanName:       rp.ReportPlanName,
		"ReportPlanDescription": rp.ReportPlanDescription,
		keyCreationTime:         epochSeconds(rp.CreationTime),
	}
	if rp.ReportDeliveryChannel != nil {
		ch := map[string]any{"S3BucketName": rp.ReportDeliveryChannel.S3BucketName}
		if len(rp.ReportDeliveryChannel.Formats) > 0 {
			ch["Formats"] = rp.ReportDeliveryChannel.Formats
		}
		rpDoc["ReportDeliveryChannel"] = ch
	}
	if rp.ReportSetting != nil {
		rs := map[string]any{"ReportTemplate": rp.ReportSetting.ReportTemplate}
		if len(rp.ReportSetting.FrameworkArns) > 0 {
			rs["FrameworkArns"] = rp.ReportSetting.FrameworkArns
		}
		rpDoc["ReportSetting"] = rs
	}

	return c.JSON(http.StatusOK, map[string]any{"ReportPlan": rpDoc})
}

type updateReportPlanBody struct {
	ReportPlanDescription string `json:"ReportPlanDescription,omitempty"`
	IdempotencyToken      string `json:"IdempotencyToken,omitempty"`
}

func (h *Handler) handleUpdateReportPlan(c *echo.Context, name string, body []byte) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "ReportPlanName is required"),
		)
	}

	var in updateReportPlanBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	rp, err := h.Backend.UpdateReportPlan(name, in.ReportPlanDescription)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyReportPlanArn:  rp.ReportPlanArn,
		keyReportPlanName: rp.ReportPlanName,
	})
}

func (h *Handler) handleDeleteReportPlan(c *echo.Context, name string) error {
	if name == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "ReportPlanName is required"),
		)
	}

	if err := h.Backend.DeleteReportPlan(name); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
