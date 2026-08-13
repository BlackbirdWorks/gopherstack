package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// extractMonitoringDistID extracts distribution ID from monitoring subscription path.
func extractMonitoringDistID(path string) string {
	suffix := strings.TrimPrefix(path, cfPathPrefix+"distribution/")

	return strings.TrimSuffix(suffix, "/monitoring-subscription")
}

func monitoringSubscriptionXML(ns string, ms *MonitoringSubscription) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<MonitoringSubscription xmlns="%s">`+
		`<RealtimeMetricsSubscriptionConfig>`+
		`<RealtimeMetricsSubscriptionStatus>%s</RealtimeMetricsSubscriptionStatus>`+
		`</RealtimeMetricsSubscriptionConfig>`+
		`</MonitoringSubscription>`,
		ns, ms.RealtimeMetricsSubscriptionStatus)
}

func (h *Handler) handleCreateMonitoringSubscription(c *echo.Context, distributionID string) error {
	// Check if enabled from body.
	body, _ := readBody(c)

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}
	enabled := true
	if len(body) > 0 {
		var req struct {
			XMLName xml.Name `xml:"MonitoringSubscription"`
			Status  string   `xml:"RealtimeMetricsSubscriptionConfig>RealtimeMetricsSubscriptionStatus"`
		}
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid MonitoringSubscription XML"))
		}
		enabled = req.Status != metricDisabled
	}
	if err := h.Backend.CreateMonitoringSubscription(distributionID, enabled); err != nil {
		return h.handleError(c, err)
	}
	ms, _ := h.Backend.GetMonitoringSubscription(distributionID)

	return xmlResp(c, http.StatusOK, monitoringSubscriptionXML(cfNS, ms))
}

func (h *Handler) handleGetMonitoringSubscription(c *echo.Context, distributionID string) error {
	ms, err := h.Backend.GetMonitoringSubscription(distributionID)
	if err != nil {
		return h.handleError(c, err)
	}

	return xmlResp(c, http.StatusOK, monitoringSubscriptionXML(cfNS, ms))
}

func (h *Handler) handleDeleteMonitoringSubscription(c *echo.Context, distributionID string) error {
	if err := h.Backend.DeleteMonitoringSubscription(distributionID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// ResourcePolicy handlers
// ---------------------------------------------------------------------------
