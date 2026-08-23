package iot

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func resolveV2LoggingOps(path, method string) string {
	switch {
	case path == pathV2LoggingOptions && method == http.MethodGet:

		return opGetV2LoggingOptions
	case path == pathV2LoggingOptions && method == http.MethodPost:

		return opSetV2LoggingOptions
	case path == pathV2LoggingLevel && method == http.MethodPost:

		return opSetV2LoggingLevel
	case path == pathV2LoggingLevel && method == http.MethodDelete:

		return opDeleteV2LoggingLevel
	case path == pathV2LoggingLevel && method == http.MethodGet:

		return opListV2LoggingLevels
	case path == pathLoggingOptions && method == http.MethodGet:

		return opGetLoggingOptions
	case path == pathLoggingOptions && method == http.MethodPost:

		return opSetLoggingOptions
	}

	return unknownOperation
}

func (h *Handler) handleGetV2LoggingOptions(c *echo.Context) error {
	opts := h.Backend.GetV2LoggingOptions()

	return c.JSON(http.StatusOK, opts)
}

func (h *Handler) handleSetV2LoggingOptions(c *echo.Context) error {
	var req struct {
		RoleARN             string                    `json:"roleArn"`
		DefaultLogLevel     string                    `json:"defaultLogLevel"`
		EventConfigurations []LogEventConfigurationV2 `json:"eventConfigurations"`
		DisableAllLogs      bool                      `json:"disableAllLogs"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.SetV2LoggingOptions(
		req.RoleARN, req.DefaultLogLevel, req.DisableAllLogs, req.EventConfigurations,
	); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleSetV2LoggingLevel(c *echo.Context) error {
	var req struct {
		LogTarget map[string]any `json:"logTarget"`
		LogLevel  string         `json:"logLevel"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.SetV2LoggingLevel(req.LogTarget, req.LogLevel); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteV2LoggingLevel(c *echo.Context) error {
	targetType := c.Request().URL.Query().Get("targetType")
	targetName := c.Request().URL.Query().Get("targetName")
	target := map[string]any{"targetType": targetType, "targetName": targetName}
	if err := h.Backend.DeleteV2LoggingLevel(target); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListV2LoggingLevels(c *echo.Context) error {
	targetType := c.QueryParam("targetType")

	items := h.Backend.ListV2LoggingLevels()
	if targetType != "" {
		filtered := make([]*V2LoggingLevel, 0, len(items))
		for _, l := range items {
			if tt, _ := l.Target["targetType"].(string); tt == targetType {
				filtered = append(filtered, l)
			}
		}
		items = filtered
	}

	pageSize, start := parseIoTPagination(c)
	page, nextToken := paginateMaps(items, pageSize, start)

	resp := map[string]any{"logTargetConfigurations": page}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleGetLoggingOptions(c *echo.Context) error {
	opts := h.Backend.GetLoggingOptions()

	return c.JSON(http.StatusOK, opts)
}

func (h *Handler) handleSetLoggingOptions(c *echo.Context) error {
	var req struct {
		RoleARN  string `json:"roleArn"`
		LogLevel string `json:"logLevel"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.SetLoggingOptions(req.RoleARN, req.LogLevel); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) dispatchV2LoggingOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opGetV2LoggingOptions:
		return true, h.handleGetV2LoggingOptions(c)
	case opSetV2LoggingOptions:
		return true, h.handleSetV2LoggingOptions(c)
	case opSetV2LoggingLevel:
		return true, h.handleSetV2LoggingLevel(c)
	case opDeleteV2LoggingLevel:
		return true, h.handleDeleteV2LoggingLevel(c)
	case opListV2LoggingLevels:
		return true, h.handleListV2LoggingLevels(c)
	case opGetLoggingOptions:
		return true, h.handleGetLoggingOptions(c)
	case opSetLoggingOptions:
		return true, h.handleSetLoggingOptions(c)
	}

	return false, nil
}
