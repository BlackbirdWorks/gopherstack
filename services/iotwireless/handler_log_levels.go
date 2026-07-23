package iotwireless

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// getResourceLogLevelResponse mirrors GetResourceLogLevelOutput, which
// carries only LogLevel -- not ResourceType/ResourceId (those are request
// parameters, not response fields).
type getResourceLogLevelResponse struct {
	LogLevel string `json:"LogLevel"`
}

type getLogLevelsByResourceTypesResponse struct {
	DefaultLogLevel           string           `json:"DefaultLogLevel"`
	FuotaTaskLogOptions       []map[string]any `json:"FuotaTaskLogOptions"`
	WirelessDeviceLogOptions  []map[string]any `json:"WirelessDeviceLogOptions"`
	WirelessGatewayLogOptions []map[string]any `json:"WirelessGatewayLogOptions"`
}

// nonNilList returns v unchanged if non-nil, or an empty (never null) slice
// otherwise -- real AWS always returns an array for these list fields, never
// a JSON null.
func nonNilList(v []map[string]any) []map[string]any {
	if v == nil {
		return []map[string]any{}
	}

	return v
}

func (h *Handler) getLogLevelsByResourceTypes(c *echo.Context) error {
	cfg := h.Backend.GetLogLevelsByResourceTypes()

	return writeJSON(c, http.StatusOK, getLogLevelsByResourceTypesResponse{
		DefaultLogLevel:           cfg.DefaultLogLevel,
		FuotaTaskLogOptions:       nonNilList(cfg.FuotaTaskLogOptions),
		WirelessDeviceLogOptions:  nonNilList(cfg.WirelessDeviceLogOptions),
		WirelessGatewayLogOptions: nonNilList(cfg.WirelessGatewayLogOptions),
	})
}

func (h *Handler) updateLogLevelsByResourceTypes(c *echo.Context) error {
	var req struct {
		DefaultLogLevel           string           `json:"DefaultLogLevel"`
		FuotaTaskLogOptions       []map[string]any `json:"FuotaTaskLogOptions"`
		WirelessDeviceLogOptions  []map[string]any `json:"WirelessDeviceLogOptions"`
		WirelessGatewayLogOptions []map[string]any `json:"WirelessGatewayLogOptions"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	cfg := LogLevelsConfig{
		DefaultLogLevel:           req.DefaultLogLevel,
		FuotaTaskLogOptions:       req.FuotaTaskLogOptions,
		WirelessDeviceLogOptions:  req.WirelessDeviceLogOptions,
		WirelessGatewayLogOptions: req.WirelessGatewayLogOptions,
	}

	if err := h.Backend.UpdateLogLevelsByResourceTypes(cfg); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) resetAllResourceLogLevels(c *echo.Context) error {
	if err := h.Backend.ResetAllResourceLogLevels(); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) getResourceLogLevel(c *echo.Context, id string) error {
	level := h.Backend.GetResourceLogLevel(id)

	return writeJSON(c, http.StatusOK, getResourceLogLevelResponse{
		LogLevel: level,
	})
}

func (h *Handler) putResourceLogLevel(c *echo.Context, id string) error {
	var req struct {
		LogLevel     string `json:"LogLevel"`
		ResourceType string `json:"ResourceType"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.PutResourceLogLevel(id, req.LogLevel); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) resetResourceLogLevel(c *echo.Context, id string) error {
	if err := h.Backend.ResetResourceLogLevel(id); err != nil {
		return handleError(c, err)
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}
