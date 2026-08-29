package lakeformation

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateLFTag(_ context.Context, c *echo.Context, body []byte) error {
	var in createLFTagInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if err := h.Backend.CreateLFTag(in.CatalogID, in.TagKey, in.TagValues); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, createLFTagOutput{})
}

func (h *Handler) handleDeleteLFTag(_ context.Context, c *echo.Context, body []byte) error {
	var in deleteLFTagInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if err := h.Backend.DeleteLFTag(in.CatalogID, in.TagKey); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, deleteLFTagOutput{})
}

func (h *Handler) handleGetLFTag(_ context.Context, c *echo.Context, body []byte) error {
	var in getLFTagInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	tag, err := h.Backend.GetLFTag(in.CatalogID, in.TagKey)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, getLFTagOutput{
		CatalogID: tag.CatalogID,
		TagKey:    tag.TagKey,
		TagValues: tag.TagValues,
	})
}

func (h *Handler) handleUpdateLFTag(_ context.Context, c *echo.Context, body []byte) error {
	var in updateLFTagInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if err := h.Backend.UpdateLFTag(in.CatalogID, in.TagKey, in.TagValuesToAdd, in.TagValuesToDelete); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, updateLFTagOutput{})
}

func (h *Handler) handleListLFTags(_ context.Context, c *echo.Context, body []byte) error {
	var in listLFTagsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}

	tags, nextToken := h.Backend.ListLFTags(in.CatalogID, in.ResourceShareType, in.MaxResults, in.NextToken)

	return c.JSON(http.StatusOK, listLFTagsOutput{
		LFTags:    tags,
		NextToken: nextToken,
	})
}

// checkAddRemoveLFTagsInput validates the Resource/LFTags shared by
// AddLFTagsToResource and RemoveLFTagsFromResource requests. ok is false when
// validation failed; err (possibly nil, if the JSON error write itself
// succeeded) is what the caller should return from its echo handler.
func (h *Handler) checkAddRemoveLFTagsInput(c *echo.Context, resource *Resource, lfTags []LFTagPair) (bool, error) {
	if resource == nil {
		return false, h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Resource is required")
	}

	if !validLFTagResourceKind(resource) {
		return false, h.writeError(c, http.StatusBadRequest, "InvalidInputException",
			"Resource must be a Database, Table, or TableWithColumns resource")
	}

	if len(lfTags) == 0 {
		return false, h.writeError(c, http.StatusBadRequest, "InvalidInputException", "LFTags is required")
	}

	return true, nil
}

func (h *Handler) handleAddLFTagsToResource(_ context.Context, c *echo.Context, body []byte) error {
	var in addLFTagsToResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if ok, err := h.checkAddRemoveLFTagsInput(c, in.Resource, in.LFTags); !ok {
		return err
	}

	failures := h.Backend.AddLFTagsToResource(in.CatalogID, in.Resource, in.LFTags)

	out := addLFTagsToResourceOutput{Failures: make([]LFTagError, 0, len(failures))}
	out.Failures = append(out.Failures, failures...)

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) handleRemoveLFTagsFromResource(_ context.Context, c *echo.Context, body []byte) error {
	var in removeLFTagsFromResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if ok, err := h.checkAddRemoveLFTagsInput(c, in.Resource, in.LFTags); !ok {
		return err
	}

	failures := h.Backend.RemoveLFTagsFromResource(in.CatalogID, in.Resource, in.LFTags)

	out := removeLFTagsFromResourceOutput{Failures: make([]LFTagError, 0, len(failures))}
	out.Failures = append(out.Failures, failures...)

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) handleGetResourceLFTags(_ context.Context, c *echo.Context, body []byte) error {
	var in getResourceLFTagsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
	}

	if in.Resource == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidInputException", "Resource is required")
	}

	pairs, err := h.Backend.GetResourceLFTags(in.CatalogID, in.Resource)
	if err != nil {
		return h.handleError(c, err)
	}

	out := getResourceLFTagsOutput{}
	switch {
	case in.Resource.Database != nil:
		out.LFTagOnDatabase = pairs
	case in.Resource.TableWithColumns != nil && len(in.Resource.TableWithColumns.ColumnNames) > 0 && len(pairs) > 0:
		cols := make([]ColumnLFTag, len(in.Resource.TableWithColumns.ColumnNames))
		for i, name := range in.Resource.TableWithColumns.ColumnNames {
			cols[i] = ColumnLFTag{Name: name, LFTags: pairs}
		}

		out.LFTagsOnColumns = cols
	default:
		out.LFTagsOnTable = pairs
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) handleSearchDatabasesByLFTags(_ context.Context, c *echo.Context, body []byte) error {
	var in searchDatabasesByLFTagsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	maxResults := 0
	if in.MaxResults != nil {
		maxResults = *in.MaxResults
	}
	dbs, nextToken := h.Backend.SearchDatabasesByLFTags(in.Expression, in.CatalogID, maxResults, in.NextToken)

	return c.JSON(http.StatusOK, searchDatabasesByLFTagsOutput{DatabaseList: dbs, NextToken: nextToken})
}

func (h *Handler) handleSearchTablesByLFTags(_ context.Context, c *echo.Context, body []byte) error {
	var in searchTablesByLFTagsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidInputException", err.Error())
		}
	}
	maxResults := 0
	if in.MaxResults != nil {
		maxResults = *in.MaxResults
	}
	tables, nextToken := h.Backend.SearchTablesByLFTags(in.Expression, in.CatalogID, maxResults, in.NextToken)

	return c.JSON(http.StatusOK, searchTablesByLFTagsOutput{TableList: tables, NextToken: nextToken})
}
