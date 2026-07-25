package lambda

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

type publishVersionInput struct {
	Description string `json:"Description"`
	// RevisionID, when set, must match the function's ($LATEST) current RevisionId
	// or the publish is rejected with PreconditionFailedException (optimistic
	// concurrency) — the function config must not have changed since it was read.
	RevisionID string `json:"RevisionId"`
}

// isValidAliasName reports whether s is a valid Lambda alias name
// (letters, digits, hyphens, and underscores only).
func isValidAliasName(s string) bool {
	for _, ch := range s {
		if (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') && ch != '-' && ch != '_' {
			return false
		}
	}

	return true
}

// extractNameFromAliasPath extracts the function name from a rest path like /{name}/aliases
// or /{name}/aliases/{aliasName}.
func extractNameFromAliasPath(rest string) string {
	trimmed := strings.TrimPrefix(rest, "/")
	parts := strings.SplitN(trimmed, "/", 3) //nolint:mnd // at most: name, aliases, aliasName
	if len(parts) >= 1 {
		return parts[0]
	}

	return ""
}

// extractNameAndAlias extracts both the function name and optional alias name from rest path.
func extractNameAndAlias(rest string) (string, string) {
	trimmed := strings.TrimPrefix(rest, "/")
	parts := strings.SplitN(trimmed, "/", 3) //nolint:mnd // at most: name, aliases, aliasName

	var fnName, aliasName string

	if len(parts) >= 1 {
		fnName = parts[0]
	}

	if len(parts) >= 3 { //nolint:mnd // parts: name, "aliases", aliasName
		aliasName = parts[2]
	}

	return fnName, aliasName
}

// handlePublishVersion handles POST /2015-03-31/functions/{name}/versions.
func (h *Handler) handlePublishVersion(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input publishVersionInput

	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	ver, publishErr := lambdaBk.PublishVersionWithRevision(name, input.Description, input.RevisionID)
	if publishErr != nil {
		if errors.Is(publishErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(publishErr, ErrPreconditionFailed) {
			return h.writeError(c, http.StatusPreconditionFailed, "PreconditionFailedException",
				"The RevisionId provided does not match the latest RevisionId. Fetch the latest version "+
					"and try again.")
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", publishErr.Error())
	}

	return c.JSON(http.StatusCreated, ver)
}

// handleListVersionsByFunction handles GET /2015-03-31/functions/{name}/versions.
func (h *Handler) handleListVersionsByFunction(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	marker, maxItems := parsePaginationParams(c.Request())

	p, err := lambdaBk.ListVersionsByFunction(name, marker, maxItems)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &ListVersionsByFunctionOutput{Versions: p.Data, NextMarker: p.Next})
}

// handleCreateAlias handles POST /2015-03-31/functions/{name}/aliases.
func (h *Handler) handleCreateAlias(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input CreateAliasInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
	}

	if input.Name == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "Name is required")
	}

	if input.FunctionVersion == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "FunctionVersion is required")
	}

	alias, createErr := lambdaBk.CreateAlias(name, &input)
	if createErr != nil {
		if errors.Is(createErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(createErr, ErrAliasAlreadyExists) {
			return h.writeError(c, http.StatusConflict, "ResourceConflictException",
				"Alias already exists: "+input.Name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", createErr.Error())
	}

	return c.JSON(http.StatusCreated, alias)
}

// handleGetAlias handles GET /2015-03-31/functions/{name}/aliases/{aliasName}.
func (h *Handler) handleGetAlias(c *echo.Context, name, aliasName string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	alias, err := lambdaBk.GetAlias(name, aliasName)
	if err != nil {
		if errors.Is(err, ErrAliasNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Alias not found: "+aliasName)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, alias)
}

// handleListAliases handles GET /2015-03-31/functions/{name}/aliases.
func (h *Handler) handleListAliases(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	marker, maxItems := parsePaginationParams(c.Request())
	functionVersion := c.Request().URL.Query().Get("FunctionVersion")

	p, err := lambdaBk.ListAliases(name, functionVersion, marker, maxItems)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &ListAliasesOutput{Aliases: p.Data, NextMarker: p.Next})
}

// handleUpdateAlias handles PUT /2015-03-31/functions/{name}/aliases/{aliasName}.
func (h *Handler) handleUpdateAlias(c *echo.Context, name, aliasName string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input UpdateAliasInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
	}

	alias, updateErr := lambdaBk.UpdateAlias(name, aliasName, &input)
	if updateErr != nil {
		if errors.Is(updateErr, ErrAliasNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Alias not found: "+aliasName)
		}

		if errors.Is(updateErr, ErrPreconditionFailed) {
			return h.writeError(c, http.StatusPreconditionFailed, "PreconditionFailedException",
				"The RevisionId provided does not match the latest RevisionId. Fetch the latest version "+
					"and try again.")
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", updateErr.Error())
	}

	return c.JSON(http.StatusOK, alias)
}

// handleDeleteAlias handles DELETE /2015-03-31/functions/{name}/aliases/{aliasName}.
func (h *Handler) handleDeleteAlias(c *echo.Context, name, aliasName string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	if err := lambdaBk.DeleteAlias(name, aliasName); err != nil {
		if errors.Is(err, ErrAliasNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Alias not found: "+aliasName)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}
