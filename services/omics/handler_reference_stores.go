package omics

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateReferenceStore(c *echo.Context) error {
	var req struct {
		Tags        map[string]string `json:"tags"`
		Name        string            `json:"name"`
		Description string            `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	rs, err := h.Backend.CreateReferenceStore(req.Name, req.Description, req.Tags)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, rs)
}

func (h *Handler) handleDeleteReferenceStore(c *echo.Context, id string) error {
	if err := h.Backend.DeleteReferenceStore(id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"id": id})
}

func (h *Handler) handleGetReferenceStore(c *echo.Context, id string) error {
	rs, err := h.Backend.GetReferenceStore(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, rs)
}

func (h *Handler) handleListReferenceStores(c *echo.Context) error {
	var req struct {
		Filter *ReferenceStoreFilter `json:"filter"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	maxResults, nextToken := listQueryParams(c)

	stores, next, err := h.Backend.ListReferenceStores(req.Filter, maxResults, nextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"referenceStores": stores,
		keyNextToken:      next,
	})
}

func (h *Handler) handleDeleteReference(c *echo.Context, storeID, id string) error {
	if err := h.Backend.DeleteReference(storeID, id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetReference(c *echo.Context, storeID, id string) error {
	data, err := h.Backend.GetReferenceBytes(storeID, id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.Blob(http.StatusOK, "application/octet-stream", data)
}

func (h *Handler) handleGetReferenceMetadata(c *echo.Context, storeID, id string) error {
	ref, err := h.Backend.GetReferenceMetadata(storeID, id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, ref)
}

func (h *Handler) handleListReferences(c *echo.Context, storeID string) error {
	var req struct {
		Filter *ReferenceFilter `json:"filter"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	maxResults, nextToken := listQueryParams(c)

	refs, next, err := h.Backend.ListReferences(storeID, req.Filter, maxResults, nextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"references": refs,
		keyNextToken: next,
	})
}

func (h *Handler) handleStartReferenceImportJob(c *echo.Context, storeID string) error {
	var req struct {
		RoleArn string                     `json:"roleArn"`
		Sources []ReferenceImportJobSource `json:"sources"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	job, err := h.Backend.StartReferenceImportJob(storeID, req.RoleArn, req.Sources)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, job)
}

func (h *Handler) handleGetReferenceImportJob(c *echo.Context, storeID, jobID string) error {
	job, err := h.Backend.GetReferenceImportJob(storeID, jobID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, job)
}

func (h *Handler) handleListReferenceImportJobs(c *echo.Context, storeID string) error {
	maxResults, nextToken := listQueryParams(c)

	jobs, next, err := h.Backend.ListReferenceImportJobs(storeID, maxResults, nextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyImportJobs: jobs,
		keyNextToken:  next,
	})
}
