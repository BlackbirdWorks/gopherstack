package bedrock

import (
	"encoding/base64"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// extractUseCaseForModelAccessOperation mirrors routeUseCaseForModelAccess's
// dispatch order exactly, so ExtractOperation agrees with the real dispatch
// contract -- previously absent from ExtractOperation's extractor list
// entirely (found by gopherstack-n1mb's route table; Handler() itself
// already dispatched these correctly).
func extractUseCaseForModelAccessOperation(path, method string) (string, bool) {
	switch {
	case path == useCaseForModelAccessPath && method == http.MethodGet:
		return "GetUseCaseForModelAccess", true
	case path == useCaseForModelAccessPath && method == http.MethodPost:
		return "PutUseCaseForModelAccess", true
	}

	return "", false
}

// routeUseCaseForModelAccess handles GetUseCaseForModelAccess and
// PutUseCaseForModelAccess.
//
// Real AWS: GET /use-case-for-model-access -> {"formData": "<base64>"},
// POST /use-case-for-model-access with body {"formData": "<base64>"} -> 200
// empty. gopherstack previously routed this on the wrong path
// ("/usecase-for-model-access"), the wrong method (PUT instead of POST), and
// the wrong body shape (a structured {useCaseType,useCaseDescription} object
// instead of the real raw-bytes FormData field) -- all three are fixed here.
func (h *Handler) routeUseCaseForModelAccess(c *echo.Context, path, method string) (bool, error) {
	switch {
	case path == useCaseForModelAccessPath && method == http.MethodGet:
		return true, h.handleGetUseCaseForModelAccess(c)
	case path == useCaseForModelAccessPath && method == http.MethodPost:
		return true, h.handlePutUseCaseForModelAccess(c)
	}

	return false, nil
}

type useCaseForModelAccessFormData struct {
	FormData string `json:"formData"`
}

// putUseCaseForModelAccessInput uses a pointer so a present-but-empty
// "formData":"" body can be distinguished from an absent field -- FormData is
// a required member on the real PutUseCaseForModelAccessInput shape, so only
// the latter is a ValidationException.
type putUseCaseForModelAccessInput struct {
	FormData *string `json:"formData"`
}

func (h *Handler) handleGetUseCaseForModelAccess(c *echo.Context) error {
	data := h.Backend.GetUseCaseForModelAccess()

	return c.JSON(http.StatusOK, useCaseForModelAccessFormData{
		FormData: base64.StdEncoding.EncodeToString(data),
	})
}

func (h *Handler) handlePutUseCaseForModelAccess(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalServerException", "internal server error"))
	}

	in, parseErr := parseBody[putUseCaseForModelAccessInput](body)
	if parseErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	if in.FormData == nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "formData is required"))
	}

	decoded, decodeErr := base64.StdEncoding.DecodeString(*in.FormData)
	if decodeErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "formData must be base64-encoded"))
	}

	h.Backend.PutUseCaseForModelAccess(decoded)

	return c.NoContent(http.StatusOK)
}
