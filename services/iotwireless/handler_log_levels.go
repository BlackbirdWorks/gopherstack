package iotwireless

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type getResourceLogLevelResponse struct {
	LogLevel     string `json:"LogLevel"`
	ResourceType string `json:"ResourceType"`
	ResourceID   string `json:"ResourceId"`
}

type getLogLevelsByResourceTypesResponse struct {
	DefaultLogLevel           string     `json:"DefaultLogLevel"`
	WirelessGatewayLogOptions []struct{} `json:"WirelessGatewayLogOptions"`
	WirelessDeviceLogOptions  []struct{} `json:"WirelessDeviceLogOptions"`
}

func (h *Handler) getLogLevelsByResourceTypes(c *echo.Context) error {
	level := h.Backend.GetLogLevelsByResourceTypes()

	return writeJSON(c, http.StatusOK, getLogLevelsByResourceTypesResponse{
		DefaultLogLevel:           level,
		WirelessGatewayLogOptions: []struct{}{},
		WirelessDeviceLogOptions:  []struct{}{},
	})
}

func (h *Handler) updateLogLevelsByResourceTypes(c *echo.Context) error {
	var req struct {
		DefaultLogLevel string `json:"DefaultLogLevel"`
	}

	body := readStubBody(c)
	_ = json.Unmarshal(body, &req)

	if err := h.Backend.UpdateLogLevelsByResourceTypes(req.DefaultLogLevel); err != nil {
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
		LogLevel:   level,
		ResourceID: id,
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
