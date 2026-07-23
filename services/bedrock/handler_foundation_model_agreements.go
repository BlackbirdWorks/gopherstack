package bedrock

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// routeStubFoundationModelOps handles foundation model availability and agreement operations.
func (h *Handler) routeStubFoundationModelOps(c *echo.Context, path, method string, body []byte) (bool, error) {
	switch {
	case strings.HasPrefix(path, foundationModelAvailPath+"/") && method == http.MethodGet:
		modelID := decodePath(strings.TrimPrefix(path, foundationModelAvailPath+"/"))

		return true, h.handleGetFoundationModelAvailability(c, modelID)
	case strings.HasPrefix(path, foundationModelAgreementOffersPath+"/") && method == http.MethodGet:
		modelID := decodePath(strings.TrimPrefix(path, foundationModelAgreementOffersPath+"/"))

		return true, h.handleListFoundationModelAgreementOffers(c, modelID)
	case path == deleteFoundationModelAgreementPath && method == http.MethodPost:
		return true, h.handleDeleteFoundationModelAgreement(c, body)
	}

	return false, nil
}

// handleGetFoundationModelAvailability returns the full GetFoundationModelAvailabilityOutput
// shape: agreementAvailability{status,errorMessage}, authorizationStatus,
// entitlementAvailability, modelId, and regionAvailability are ALL required
// response fields on the real SDK type -- gopherstack previously returned only
// agreementAvailability, silently zero-valuing the other four for any real
// client that inspects them.
func (h *Handler) handleGetFoundationModelAvailability(c *echo.Context, modelID string) error {
	return c.JSON(http.StatusOK, map[string]any{
		"modelId":                 modelID,
		"agreementAvailability":   map[string]string{keyStatus: statusAvailable},
		"authorizationStatus":     "AUTHORIZED",
		"entitlementAvailability": statusAvailable,
		"regionAvailability":      statusAvailable,
	})
}

type createFoundationModelAgreementInput struct {
	ModelID    string `json:"modelId"`
	OfferToken string `json:"offerToken"`
}

type createFoundationModelAgreementOutput struct {
	ModelID string `json:"modelId"`
}

func (h *Handler) handleCreateFoundationModelAgreement(c *echo.Context, body []byte) error {
	in, err := parseBody[createFoundationModelAgreementInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	agreement, opErr := h.Backend.CreateFoundationModelAgreement(in.ModelID)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.JSON(http.StatusOK, createFoundationModelAgreementOutput{ModelID: agreement.ModelID})
}

func (h *Handler) handleListFoundationModelAgreementOffers(c *echo.Context, modelID string) error {
	if modelID == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "modelId is required"))
	}

	offers := h.Backend.ListFoundationModelAgreementOffers(modelID)
	wire := make([]map[string]any, 0, len(offers))

	for _, o := range offers {
		wire = append(wire, map[string]any{
			"offerToken": o.OfferToken,
			"offerId":    o.OfferID,
			"termDetails": map[string]any{
				"legalTerm":             map[string]any{"url": o.LegalTermURL},
				"supportTerm":           map[string]any{},
				"usageBasedPricingTerm": map[string]any{"rateCard": []any{}},
			},
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"modelId": modelID, "offers": wire})
}

type deleteFoundationModelAgreementInput struct {
	ModelID string `json:"modelId"`
}

func (h *Handler) handleDeleteFoundationModelAgreement(c *echo.Context, body []byte) error {
	in, err := parseBody[deleteFoundationModelAgreementInput](body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	if in.ModelID == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "modelId is required"))
	}

	if opErr := h.Backend.DeleteFoundationModelAgreement(in.ModelID); opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.NoContent(http.StatusOK)
}
