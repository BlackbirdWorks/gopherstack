package lakeformation

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleGetDataLakeSettings(_ context.Context, c *echo.Context, body []byte) error {
	var in getDataLakeSettingsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}

	settings := h.Backend.GetDataLakeSettings()

	return c.JSON(http.StatusOK, getDataLakeSettingsOutput{DataLakeSettings: settings})
}

func (h *Handler) handlePutDataLakeSettings(_ context.Context, c *echo.Context, body []byte) error {
	var in putDataLakeSettingsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if in.DataLakeSettings == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "DataLakeSettings is required")
	}

	h.Backend.PutDataLakeSettings(in.DataLakeSettings)

	return c.JSON(http.StatusOK, struct{}{})
}
