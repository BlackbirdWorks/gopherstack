package route53

import (
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type xmlQueryLoggingConfig struct {
	XMLName                   xml.Name `xml:"QueryLoggingConfig"`
	ID                        string   `xml:"Id"`
	HostedZoneID              string   `xml:"HostedZoneId"`
	CloudWatchLogsLogGroupArn string   `xml:"CloudWatchLogsLogGroupArn"`
}

type xmlCreateQueryLoggingConfigRequest struct {
	XMLName                   xml.Name `xml:"CreateQueryLoggingConfigRequest"`
	HostedZoneID              string   `xml:"HostedZoneId"`
	CloudWatchLogsLogGroupArn string   `xml:"CloudWatchLogsLogGroupArn"`
}

type xmlCreateQueryLoggingConfigResponse struct {
	XMLName            xml.Name              `xml:"CreateQueryLoggingConfigResponse"`
	Xmlns              string                `xml:"xmlns,attr"`
	QueryLoggingConfig xmlQueryLoggingConfig `xml:"QueryLoggingConfig"`
}

func (h *Handler) routeQueryLogging(c *echo.Context, method string) error {
	switch method {
	case http.MethodPost:
		return h.createQueryLoggingConfig(c)
	case http.MethodGet:
		return h.listQueryLoggingConfigs(c)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation", "unsupported method on /queryloggingconfig")
	}
}

func (h *Handler) createQueryLoggingConfig(c *echo.Context) error {
	ctx := c.Request().Context()

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlCreateQueryLoggingConfigRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	cfg, err := h.Backend.CreateQueryLoggingConfig(req.HostedZoneID, req.CloudWatchLogsLogGroupArn)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 CreateQueryLoggingConfig", "id", cfg.ID)

	resp := xmlCreateQueryLoggingConfigResponse{
		Xmlns: route53Namespace,
		QueryLoggingConfig: xmlQueryLoggingConfig{
			ID:                        cfg.ID,
			HostedZoneID:              cfg.HostedZoneID,
			CloudWatchLogsLogGroupArn: cfg.CloudWatchLogsLogGroupArn,
		},
	}

	c.Response().Header().Set("Location", "/2013-04-01/queryloggingconfig/"+cfg.ID)

	return writeXML(c, http.StatusCreated, resp)
}

type getQueryLoggingConfigResponse struct {
	XMLName            xml.Name              `xml:"GetQueryLoggingConfigResponse"`
	Xmlns              string                `xml:"xmlns,attr"`
	QueryLoggingConfig xmlQueryLoggingConfig `xml:"QueryLoggingConfig"`
}

func (h *Handler) getQueryLoggingConfig(c *echo.Context, path string) error {
	configID := strings.TrimPrefix(path, route53QueryLoggingRoot+"/")

	cfg, err := h.Backend.GetQueryLoggingConfig(configID)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusOK, getQueryLoggingConfigResponse{
		Xmlns: route53Namespace,
		QueryLoggingConfig: xmlQueryLoggingConfig{
			ID:                        cfg.ID,
			HostedZoneID:              cfg.HostedZoneID,
			CloudWatchLogsLogGroupArn: cfg.CloudWatchLogsLogGroupArn,
		},
	})
}

type deleteQueryLoggingConfigResponse struct {
	XMLName xml.Name `xml:"DeleteQueryLoggingConfigResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) deleteQueryLoggingConfig(c *echo.Context, path string) error {
	configID := strings.TrimPrefix(path, route53QueryLoggingRoot+"/")

	if err := h.Backend.DeleteQueryLoggingConfig(configID); err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusOK, deleteQueryLoggingConfigResponse{Xmlns: route53Namespace})
}

// listQueryLoggingConfigs lists all query logging configs.
type listQueryLoggingConfigsResponse struct {
	XMLName             xml.Name                `xml:"ListQueryLoggingConfigsResponse"`
	Xmlns               string                  `xml:"xmlns,attr"`
	QueryLoggingConfigs []xmlQueryLoggingConfig `xml:"QueryLoggingConfigs>QueryLoggingConfig"`
}

func (h *Handler) listQueryLoggingConfigs(c *echo.Context) error {
	hostedZoneID := c.Request().URL.Query().Get("hostedzoneid")

	cfgs, err := h.Backend.ListQueryLoggingConfigs(hostedZoneID)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	items := make([]xmlQueryLoggingConfig, 0, len(cfgs))
	for _, cfg := range cfgs {
		items = append(items, xmlQueryLoggingConfig{
			ID:                        cfg.ID,
			HostedZoneID:              cfg.HostedZoneID,
			CloudWatchLogsLogGroupArn: cfg.CloudWatchLogsLogGroupArn,
		})
	}

	return writeXML(c, http.StatusOK, listQueryLoggingConfigsResponse{
		Xmlns:               route53Namespace,
		QueryLoggingConfigs: items,
	})
}
