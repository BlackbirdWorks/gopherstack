package bedrock

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

// routeStubFoundationModelOps handles foundation model availability and agreement operations.
func (h *Handler) routeStubFoundationModelOps(c *echo.Context, path, method string) (bool, error) {
	switch {
	case strings.HasPrefix(path, foundationModelAvailPath+"/") && method == http.MethodGet:
		return true, c.JSON(http.StatusOK,
			map[string]any{"agreementAvailability": map[string]string{keyStatus: "AVAILABLE"}})
	case path == foundationModelAgreementsPath && method == http.MethodGet:
		return true, h.handleListFoundationModelAgreementOffers(c)
	case strings.HasPrefix(path, "/delete-foundation-model-agreement") && method == http.MethodDelete:
		modelID := strings.TrimPrefix(path, "/delete-foundation-model-agreement/")

		return true, h.handleDeleteFoundationModelAgreement(c, modelID)
	}

	return false, nil
}

// routeStubAccessOps handles use case for model access and enforced guardrail operations.
func (h *Handler) routeStubAccessOps(c *echo.Context, path, method string) (bool, error) {
	switch {
	case path == useCaseForModelAccessPath && method == http.MethodGet:
		return true, h.handleGetUseCaseForModelAccess(c)
	case path == useCaseForModelAccessPath && method == http.MethodPut:
		return true, h.handlePutUseCaseForModelAccess(c)
	case path == enforcedGuardrailsPath && method == http.MethodGet:
		return true, h.handleListEnforcedGuardrailsConfiguration(c)
	case path == enforcedGuardrailsPath && method == http.MethodPut:
		return true, h.handlePutEnforcedGuardrailConfiguration(c)
	case path == enforcedGuardrailsPath && method == http.MethodDelete:
		return true, h.handleDeleteEnforcedGuardrailConfiguration(c)
	}

	return false, nil
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

func (h *Handler) handleListFoundationModelAgreementOffers(c *echo.Context) error {
	agreements := h.Backend.ListFoundationModelAgreementOffers()
	offers := make([]map[string]any, 0, len(agreements))

	for _, a := range agreements {
		offers = append(offers, map[string]any{"modelId": a.ModelID})
	}

	return c.JSON(http.StatusOK, map[string]any{"modelAgreementOffers": offers})
}

func (h *Handler) handleDeleteFoundationModelAgreement(c *echo.Context, modelID string) error {
	if modelID == "" {
		return c.NoContent(http.StatusNoContent)
	}

	if err := h.Backend.DeleteFoundationModelAgreement(modelID); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

type putUseCaseInput struct {
	UseCaseType        string `json:"useCaseType"`
	UseCaseDescription string `json:"useCaseDescription"`
}

func (h *Handler) handleGetUseCaseForModelAccess(c *echo.Context) error {
	result := h.Backend.GetUseCaseForModelAccess()

	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handlePutUseCaseForModelAccess(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	in, parseErr := parseBody[putUseCaseInput](body)
	if parseErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	h.Backend.PutUseCaseForModelAccess(in.UseCaseType, in.UseCaseDescription)

	return c.NoContent(http.StatusNoContent)
}
