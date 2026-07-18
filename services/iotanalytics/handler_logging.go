package iotanalytics

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleDescribeLoggingOptions(c *echo.Context) error {
	opts, err := h.Backend.DescribeLoggingOptions()
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeLoggingOptionsResponse{
		LoggingOptions: loggingOptionsDTO{
			RoleARN: opts.RoleARN,
			Level:   opts.Level,
			Enabled: opts.Enabled,
		},
	})
}

func (h *Handler) handlePutLoggingOptions(c *echo.Context, body []byte) error {
	var req putLoggingOptionsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	opts := &LoggingOptions{
		RoleARN: req.LoggingOptions.RoleARN,
		Level:   req.LoggingOptions.Level,
		Enabled: req.LoggingOptions.Enabled,
	}

	if err := h.Backend.PutLoggingOptions(opts); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
