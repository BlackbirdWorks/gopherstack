package bedrock

import (
	"net/http"
	"time"

	"github.com/labstack/echo/v5"
)

func extractAccountDataRetentionOperation(path, method string) (string, bool) {
	switch {
	case path == dataRetentionPath && method == http.MethodGet:
		return "GetAccountDataRetention", true
	case path == dataRetentionPath && method == http.MethodPut:
		return "PutAccountDataRetention", true
	default:
		return "", false
	}
}

func (h *Handler) routeAccountDataRetention(c *echo.Context, path, method string, body []byte) (bool, error) {
	if path != dataRetentionPath {
		return false, nil
	}

	switch method {
	case http.MethodGet:
		return true, h.handleGetAccountDataRetention(c)
	case http.MethodPut:
		return true, h.handlePutAccountDataRetention(c, body)
	default:
		return false, nil
	}
}

type accountDataRetentionOutput struct {
	UpdatedAt string `json:"updatedAt,omitempty"`
	Mode      string `json:"mode"`
}

func accountDataRetentionToOutput(r *AccountDataRetention) accountDataRetentionOutput {
	out := accountDataRetentionOutput{Mode: r.Mode}
	if !r.UpdatedAt.IsZero() {
		out.UpdatedAt = r.UpdatedAt.Format(time.RFC3339)
	}

	return out
}

func (h *Handler) handleGetAccountDataRetention(c *echo.Context) error {
	r := h.Backend.GetAccountDataRetention()

	return c.JSON(http.StatusOK, accountDataRetentionToOutput(r))
}

type putAccountDataRetentionInput struct {
	Mode string `json:"mode"`
}

func (h *Handler) handlePutAccountDataRetention(c *echo.Context, body []byte) error {
	in, err := parseBody[putAccountDataRetentionInput](body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	r, opErr := h.Backend.PutAccountDataRetention(in.Mode)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusOK, accountDataRetentionToOutput(r))
}
