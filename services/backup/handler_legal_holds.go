package backup

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCancelLegalHold(c *echo.Context, legalHoldID string) error {
	if legalHoldID == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "LegalHoldId is required"),
		)
	}

	if err := h.Backend.CancelLegalHold(legalHoldID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

type createLegalHoldBody struct {
	Title            string `json:"Title"`
	Description      string `json:"Description"`
	IdempotencyToken string `json:"IdempotencyToken,omitempty"`
}

func (h *Handler) handleCreateLegalHold(c *echo.Context, body []byte) error {
	var in createLegalHoldBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if in.Title == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "Title is required"))
	}

	if in.Description == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "Description is required"),
		)
	}

	lh, err := h.Backend.CreateLegalHold(in.Title, in.Description)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyLegalHoldID:  lh.LegalHoldID,
		"LegalHoldArn":  lh.LegalHoldArn,
		keyTitle:        lh.Title,
		"Description":   lh.Description,
		keyStatus:       lh.Status,
		keyCreationDate: epochSeconds(lh.CreationDate),
	})
}
