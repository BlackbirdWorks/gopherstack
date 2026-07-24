package lambda

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

// --- AddPermission handler ---

// handleAddPermission handles POST /2015-03-31/functions/{name}/policy.
func (h *Handler) handleAddPermission(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input AddPermissionInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
	}

	if input.StatementID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "StatementId is required")
	}

	if input.Action == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "Action is required")
	}

	if input.Principal == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "Principal is required")
	}

	qualifier := c.Request().URL.Query().Get("Qualifier")

	out, addErr := lambdaBk.AddPermission(name, qualifier, &input)
	if addErr != nil {
		if errors.Is(addErr, ErrFunctionNotFound) || errors.Is(addErr, ErrVersionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(addErr, ErrFunctionAlreadyExists) {
			return h.writeError(c, http.StatusConflict, "ResourceConflictException",
				"Permission already exists: "+input.StatementID)
		}

		if errors.Is(addErr, ErrInvalidParameterValue) {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", addErr.Error())
		}

		if errors.Is(addErr, ErrPreconditionFailed) {
			return h.writeError(c, http.StatusPreconditionFailed, "PreconditionFailedException",
				"The RevisionId provided does not match the latest RevisionId. Fetch the latest version "+
					"and try again.")
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", addErr.Error())
	}

	return c.JSON(http.StatusCreated, out)
}

// handleGetPolicy handles GET /2015-03-31/functions/{name}/policy?Qualifier=xxx.
func (h *Handler) handleGetPolicy(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	qualifier := c.Request().URL.Query().Get("Qualifier")

	out, err := lambdaBk.GetPolicy(name, qualifier)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(err, ErrNoPolicyFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"No policy is associated with the given resource: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, out)
}

// handleRemovePermission handles
// DELETE /2015-03-31/functions/{name}/policy/{statementID}?Qualifier=xxx.
//
// Bug fix (parity-sweep-3): the real Lambda SDK sends StatementId as a URI path
// segment (see RemovePermissionInput's HTTP binding), never as a query
// parameter. The previous implementation read StatementId from the query
// string, so this route only matched when no real SDK client would ever call
// it — it always 400'd against genuine aws-sdk-go-v2 traffic. statementID is
// now extracted from the path by the route matcher (hasPolicyStatementSuffix
// + extractNameAndPolicyStatement).
func (h *Handler) handleRemovePermission(c *echo.Context, name, statementID string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	if statementID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "StatementId is required")
	}

	qualifier := c.Request().URL.Query().Get("Qualifier")
	revisionID := c.Request().URL.Query().Get("RevisionId")

	if err := lambdaBk.RemovePermission(name, qualifier, statementID, revisionID); err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(err, ErrPreconditionFailed) {
			return h.writeError(c, http.StatusPreconditionFailed, "PreconditionFailedException",
				"The RevisionId provided does not match the latest RevisionId. Fetch the latest version "+
					"and try again.")
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}
