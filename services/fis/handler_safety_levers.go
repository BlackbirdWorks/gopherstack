package fis

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// ----------------------------------------
// Safety lever handlers
// ----------------------------------------

func (h *Handler) handleGetSafetyLever(c *echo.Context, id string) error {
	lever, err := h.Backend.GetSafetyLever(id)
	if err != nil {
		return h.writeBackendError(c, err, id)
	}

	return c.JSON(http.StatusOK, safetyLeverResponseDTO{SafetyLever: toSafetyLeverDTO(lever)})
}

func (h *Handler) handleUpdateSafetyLeverState(c *echo.Context, id string, body []byte) error {
	var input updateSafetyLeverStateRequest
	if err := json.Unmarshal(body, &input); err != nil {
		return h.writeError(c, http.StatusBadRequest, "invalid request body: "+err.Error(), id)
	}

	lever, err := h.Backend.UpdateSafetyLeverState(id, &input)
	if err != nil {
		return h.writeBackendError(c, err, id)
	}

	return c.JSON(http.StatusOK, safetyLeverResponseDTO{SafetyLever: toSafetyLeverDTO(lever)})
}

// ----------------------------------------
// DTO conversion helpers
// ----------------------------------------

func toSafetyLeverDTO(lever *SafetyLever) safetyLeverDTO {
	return safetyLeverDTO{
		ID:   lever.ID,
		Arn:  lever.Arn,
		Tags: lever.Tags,
		State: safetyLeverStateDTO{
			Status: lever.State.Status,
			Reason: lever.State.Reason,
		},
	}
}
