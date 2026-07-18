package iotwireless

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

const stubServiceType = "CUPS"

type getServiceEndpointResponse struct {
	ServiceType     string `json:"ServiceType"`
	ServiceEndpoint string `json:"ServiceEndpoint"`
	ServerTrust     string `json:"ServerTrust"`
}

type summaryMetricConfigurationResponse struct {
	Status string `json:"Status,omitempty"`
}

type getMetricConfigurationResponse struct {
	SummaryMetric summaryMetricConfigurationResponse `json:"SummaryMetric"`
}

// summaryMetricQueryResultResponse mirrors one entry of GetMetrics's
// SummaryMetricQueryResults: a per-query echo of the QueryId with a status,
// since this emulator does not ingest telemetry to aggregate real metric
// values.
type summaryMetricQueryResultResponse struct {
	QueryID     string    `json:"QueryId,omitempty"`
	QueryStatus string    `json:"QueryStatus,omitempty"`
	MetricName  string    `json:"MetricName,omitempty"`
	Values      []float64 `json:"Values,omitempty"`
}

type getMetricsResponse struct {
	SummaryMetricQueryResults []summaryMetricQueryResultResponse `json:"SummaryMetricQueryResults"`
}

func (h *Handler) getMetricConfiguration(c *echo.Context) error {
	status := h.Backend.GetMetricConfigurationStatus()

	return writeJSON(c, http.StatusOK, getMetricConfigurationResponse{
		SummaryMetric: summaryMetricConfigurationResponse{Status: status},
	})
}

func (h *Handler) updateMetricConfiguration(c *echo.Context) error {
	var req struct {
		SummaryMetric struct {
			Status string `json:"Status"`
		} `json:"SummaryMetric"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateMetricConfigurationStatus(req.SummaryMetric.Status); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

// getServiceEndpoint returns the CUPS or LNS endpoint for the requested
// serviceType and the handler's configured region. AWS defaults to CUPS when
// serviceType is omitted.
func (h *Handler) getServiceEndpoint(c *echo.Context) error {
	serviceType := c.QueryParam("serviceType")
	if serviceType == "" {
		serviceType = stubServiceType
	}

	region := h.DefaultRegion
	if region == "" {
		region = "us-east-1"
	}

	var host string

	switch serviceType {
	case "CUPS":
		host = "cups.lorawan." + region + ".amazonaws.com"
	case "LNS":
		host = "lns.lorawan." + region + ".amazonaws.com"
	default:
		return writeError(c, http.StatusBadRequest, "ValidationException: invalid serviceType "+serviceType)
	}

	return writeJSON(c, http.StatusOK, getServiceEndpointResponse{
		ServiceType:     serviceType,
		ServiceEndpoint: "https://" + host,
		ServerTrust:     "",
	})
}

func (h *Handler) getMetrics(c *echo.Context) error {
	var req struct {
		SummaryMetricQueries []struct {
			QueryID    string `json:"QueryId"`
			MetricName string `json:"MetricName"`
		} `json:"SummaryMetricQueries"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	results := make([]summaryMetricQueryResultResponse, 0, len(req.SummaryMetricQueries))
	for _, q := range req.SummaryMetricQueries {
		results = append(results, summaryMetricQueryResultResponse{
			QueryID:     q.QueryID,
			QueryStatus: "Succeeded",
			MetricName:  q.MetricName,
			Values:      []float64{},
		})
	}

	return writeJSON(c, http.StatusOK, getMetricsResponse{
		SummaryMetricQueryResults: results,
	})
}
