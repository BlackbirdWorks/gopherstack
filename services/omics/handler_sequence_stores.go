package omics

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateSequenceStore(c *echo.Context) error {
	var req struct {
		Tags        map[string]string `json:"tags"`
		Name        string            `json:"name"`
		Description string            `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	ss, err := h.Backend.CreateSequenceStore(req.Name, req.Description, req.Tags)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, ss)
}

func (h *Handler) handleDeleteSequenceStore(c *echo.Context, id string) error {
	if err := h.Backend.DeleteSequenceStore(id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"id": id})
}

func (h *Handler) handleGetSequenceStore(c *echo.Context, id string) error {
	ss, err := h.Backend.GetSequenceStore(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, ss)
}

func (h *Handler) handleListSequenceStores(c *echo.Context) error {
	var req struct {
		Filter *SequenceStoreFilter `json:"filter"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	maxResults, nextToken := listQueryParams(c)

	stores, next, err := h.Backend.ListSequenceStores(req.Filter, maxResults, nextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"sequenceStores": stores,
		keyNextToken:     next,
	})
}

func (h *Handler) handleUpdateSequenceStore(c *echo.Context, id string) error {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	ss, err := h.Backend.UpdateSequenceStore(id, req.Name, req.Description)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, ss)
}
