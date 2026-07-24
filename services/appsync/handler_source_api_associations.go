package appsync

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

// handleSourceAPIs handles /v1/sourceApis/{sourceApiIdentifier}/mergedApiAssociations[/{assocId}].
func (h *Handler) handleSourceAPIs(ctx context.Context, c *echo.Context, segs []string) error {
	if segs[3] != "mergedApiAssociations" {
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}

	sourceAPIID := segs[2]

	if len(segs) == pathSegsAPISubresource {
		switch c.Request().Method {
		case http.MethodPost:
			return h.associateMergedGraphqlAPI(ctx, c, sourceAPIID)
		default:
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
		}
	}

	if len(segs) == pathSegsNamedResource {
		assocID := segs[4]

		if c.Request().Method == http.MethodDelete {
			if err := h.Backend.DisassociateMergedGraphqlAPI(sourceAPIID, assocID); err != nil {
				return h.handleError(ctx, c, "DisassociateMergedGraphqlApi", err)
			}

			return c.NoContent(http.StatusNoContent)
		}

		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
}

// associateMergedGraphqlAPI handles POST /v1/sourceApis/{sourceApiIdentifier}/mergedApiAssociations.
// associateAPIInput holds the common JSON fields for API association requests.
type associateAPIInput struct {
	SourceAPIAssociationConfig *SourceAPIAssociationConfig `json:"sourceApiAssociationConfig"`
	MergedAPIIdentifier        string                      `json:"mergedApiIdentifier"`
	SourceAPIIdentifier        string                      `json:"sourceApiIdentifier"`
	Description                string                      `json:"description"`
}

// doSourceAPIAssociation is the shared implementation for both Merged/Source GraphQL API association.
func (h *Handler) doSourceAPIAssociation(
	ctx context.Context,
	c *echo.Context,
	primaryAPIID, secondaryAPIID, requiredField, opName string,
	backendFn func(firstID, secondID, description, mergeType string) (*SourceAPIAssociation, error),
	input associateAPIInput,
) error {
	if secondaryAPIID == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", requiredField+" is required"))
	}

	mergeType := ""
	if input.SourceAPIAssociationConfig != nil {
		mergeType = input.SourceAPIAssociationConfig.MergeType
	}

	assoc, createErr := backendFn(primaryAPIID, secondaryAPIID, input.Description, mergeType)
	if createErr != nil {
		return h.handleError(ctx, c, opName, createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keySourceAPIAssociation: assoc})
}

func (h *Handler) associateMergedGraphqlAPI(ctx context.Context, c *echo.Context, sourceAPIID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input associateAPIInput
	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	return h.doSourceAPIAssociation(ctx, c, sourceAPIID, input.MergedAPIIdentifier,
		"mergedApiIdentifier", "AssociateMergedGraphqlApi",
		h.Backend.AssociateMergedGraphqlAPI, input)
}

// handleMergedAPIs handles
// /v1/mergedApis/{mergedApiIdentifier}/sourceApiAssociations[/{assocId}[/types|/merge]].
func (h *Handler) handleMergedAPIs(ctx context.Context, c *echo.Context, segs []string) error {
	if len(segs) < pathSegsAPISubresource || segs[3] != keySourceAPIAssociations {
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}

	mergedAPIID := segs[2]

	switch len(segs) {
	case pathSegsAPISubresource:
		return h.handleMergedAPIsCollection(ctx, c, mergedAPIID)
	case pathSegsNamedResource:
		return h.handleMergedAPIsItem(ctx, c, mergedAPIID, segs[4])
	case pathSegsTypeResolvers:
		return h.handleMergedAPIsAssocSubresource(ctx, c, mergedAPIID, segs[4], segs[5])
	}

	return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
}

// handleMergedAPIsCollection handles /v1/mergedApis/{mergedApiId}/sourceApiAssociations.
func (h *Handler) handleMergedAPIsCollection(ctx context.Context, c *echo.Context, mergedAPIID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.associateSourceGraphqlAPI(ctx, c, mergedAPIID)
	case http.MethodGet:
		return h.listSourceAPIAssociations(ctx, c, mergedAPIID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// handleMergedAPIsItem handles /v1/mergedApis/{mergedApiId}/sourceApiAssociations/{assocId}.
func (h *Handler) handleMergedAPIsItem(ctx context.Context, c *echo.Context, mergedAPIID, assocID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getSourceAPIAssociation(ctx, c, mergedAPIID, assocID)
	case http.MethodDelete:
		if err := h.Backend.DisassociateSourceGraphqlAPI(mergedAPIID, assocID); err != nil {
			return h.handleError(ctx, c, "DisassociateSourceGraphqlApi", err)
		}

		return c.NoContent(http.StatusNoContent)
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		return h.updateSourceAPIAssociation(ctx, c, mergedAPIID, assocID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// handleMergedAPIsAssocSubresource handles
// /v1/mergedApis/{mergedApiId}/sourceApiAssociations/{assocId}/{types|merge}.
func (h *Handler) handleMergedAPIsAssocSubresource(
	ctx context.Context, c *echo.Context, mergedAPIID, assocID, seg5 string,
) error {
	switch seg5 {
	case pathSegTypes:
		// /v1/mergedApis/{mergedApiId}/sourceApiAssociations/{assocId}/types
		return h.listTypesByAssociation(ctx, c, mergedAPIID, assocID)
	case "merge":
		// /v1/mergedApis/{mergedApiId}/sourceApiAssociations/{assocId}/merge
		return h.requireMethod(c, http.MethodPost, func() error {
			return h.startSchemaMerge(ctx, c, mergedAPIID, assocID)
		})
	}

	return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
}

// associateSourceGraphqlAPI handles POST /v1/mergedApis/{mergedApiIdentifier}/sourceApiAssociations.
func (h *Handler) associateSourceGraphqlAPI(ctx context.Context, c *echo.Context, mergedAPIID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input associateAPIInput
	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	return h.doSourceAPIAssociation(ctx, c, mergedAPIID, input.SourceAPIIdentifier,
		"sourceApiIdentifier", "AssociateSourceGraphqlApi",
		h.Backend.AssociateSourceGraphqlAPI, input)
}

// getSourceAPIAssociation handles GET /v1/mergedApis/{mergedApiId}/sourceApiAssociations/{assocId}.
func (h *Handler) getSourceAPIAssociation(ctx context.Context, c *echo.Context, mergedAPIID, assocID string) error {
	assoc, err := h.Backend.GetSourceAPIAssociation(mergedAPIID, assocID)
	if err != nil {
		return h.handleError(ctx, c, "GetSourceApiAssociation", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keySourceAPIAssociation: assoc})
}

// listSourceAPIAssociations handles GET /v1/mergedApis/{mergedApiId}/sourceApiAssociations.
func (h *Handler) listSourceAPIAssociations(ctx context.Context, c *echo.Context, mergedAPIID string) error {
	assocs, err := h.Backend.ListSourceAPIAssociations(mergedAPIID)
	if err != nil {
		return h.handleError(ctx, c, opListSourceAPIAssociations, err)
	}

	// The real AWS SDK's ListSourceApiAssociationsOutput wraps the list under
	// "sourceApiAssociationSummaries" — NOT "sourceApiAssociations" (that name is only
	// the URL path segment). A client would otherwise always see an empty list back.
	return c.JSON(http.StatusOK, map[string]any{"sourceApiAssociationSummaries": assocs})
}

// updateSourceAPIAssociation handles PUT /v1/mergedApis/{mergedApiId}/sourceApiAssociations/{assocId}.
func (h *Handler) updateSourceAPIAssociation(
	ctx context.Context,
	c *echo.Context,
	mergedAPIID, assocID string,
) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Description string `json:"description"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	assoc, updateErr := h.Backend.UpdateSourceAPIAssociation(mergedAPIID, assocID, input.Description)
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateSourceApiAssociation", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keySourceAPIAssociation: assoc})
}
