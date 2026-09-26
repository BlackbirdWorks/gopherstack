package lambda

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// resourcePolicyBody is PutResourcePolicy's real request body shape
// (lambda@v1.107.0 api_op_PutResourcePolicy.go: Policy required, RevisionId
// optional optimistic-concurrency field).
type resourcePolicyBody struct {
	Policy     string `json:"Policy"`
	RevisionID string `json:"RevisionId,omitempty"`
}

// handleResourcePolicyRoute handles GET/PUT/DELETE
// /2026-07-09/resource-policy/{ResourceArn}.
func (h *Handler) handleResourcePolicyRoute(c *echo.Context, path, method string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	resourceArn := strings.TrimPrefix(path, lambdaResourcePolicyPathPrefix+"/")
	if resourceArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ResourceArn is required")
	}

	switch method {
	case http.MethodGet:
		return h.handleGetResourcePolicy(c, lambdaBk, resourceArn)
	case http.MethodPut:
		return h.handlePutResourcePolicy(c, lambdaBk, resourceArn)
	case http.MethodDelete:
		return h.handleDeleteResourcePolicy(c, lambdaBk, resourceArn)
	default:
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

func (h *Handler) handleGetResourcePolicy(c *echo.Context, b *InMemoryBackend, resourceArn string) error {
	out, err := b.GetResourcePolicy(resourceArn)
	if err != nil {
		return h.writeResourcePolicyError(c, err, resourceArn)
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) handlePutResourcePolicy(c *echo.Context, b *InMemoryBackend, resourceArn string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input resourcePolicyBody
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
	}

	out, putErr := b.PutResourcePolicy(resourceArn, input.Policy, input.RevisionID)
	if putErr != nil {
		return h.writeResourcePolicyError(c, putErr, resourceArn)
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) handleDeleteResourcePolicy(c *echo.Context, b *InMemoryBackend, resourceArn string) error {
	revisionID := c.Request().URL.Query().Get("RevisionId")

	if err := b.DeleteResourcePolicy(resourceArn, revisionID); err != nil {
		return h.writeResourcePolicyError(c, err, resourceArn)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) writeResourcePolicyError(c *echo.Context, err error, resourceArn string) error {
	switch {
	case errors.Is(err, ErrFunctionNotFound), errors.Is(err, ErrVersionNotFound):
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
			"Function not found: "+resourceArn)
	case errors.Is(err, ErrNoPolicyFound):
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
			"No policy is associated with the given resource: "+resourceArn)
	case errors.Is(err, ErrPreconditionFailed):
		return h.writeError(c, http.StatusPreconditionFailed, "PreconditionFailedException",
			"The RevisionId provided does not match the latest RevisionId. Fetch the latest version "+
				"and try again.")
	case errors.Is(err, ErrInvalidParameterValue):
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	default:
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}
}
