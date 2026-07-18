package mediaconvert

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func parseCertificateRoute(method, suffix string) mcRoute {
	certARN := strings.TrimPrefix(suffix, "/")

	if certARN == "" {
		if method == http.MethodPost {
			return mcRoute{operation: opAssociateCertificate}
		}

		return mcRoute{operation: opUnknown}
	}

	if method == http.MethodDelete {
		return mcRoute{operation: opDisassociateCertificate, resource: certARN}
	}

	return mcRoute{operation: opUnknown}
}

// --- Certificate handlers ---

type associateCertificateInput struct {
	Arn string `json:"arn"`
}

func (h *Handler) handleAssociateCertificate(c *echo.Context, body []byte) error {
	var in associateCertificateInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if in.Arn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "arn is required"))
	}

	if err := h.Backend.AssociateCertificate(in.Arn); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleDisassociateCertificate(c *echo.Context, certARN string) error {
	if err := h.Backend.DisassociateCertificate(certARN); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
