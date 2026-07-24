package omics

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateConfiguration(c *echo.Context) error {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	cfg, err := h.Backend.CreateConfiguration(req.Name, req.Description)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, cfg)
}

func (h *Handler) handleDeleteConfiguration(c *echo.Context, name string) error {
	if err := h.Backend.DeleteConfiguration(name); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetConfiguration(c *echo.Context, name string) error {
	cfg, err := h.Backend.GetConfiguration(name)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, cfg)
}

func (h *Handler) handleListConfigurations(c *echo.Context) error {
	maxResults, nextToken := paginationQueryParams(c)
	cfgs, next, err := h.Backend.ListConfigurations(maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"configurations": cfgs, keyNextToken: next})
}

func (h *Handler) handlePutS3AccessPolicy(c *echo.Context, arn string) error {
	var req struct {
		S3AccessPolicy string `json:"s3AccessPolicy"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	if err := h.Backend.PutS3AccessPolicy(arn, req.S3AccessPolicy); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"s3AccessPointArn": arn})
}

func (h *Handler) handleGetS3AccessPolicy(c *echo.Context, arn string) error {
	p, err := h.Backend.GetS3AccessPolicy(arn)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, p)
}

func (h *Handler) handleDeleteS3AccessPolicy(c *echo.Context, arn string) error {
	if err := h.Backend.DeleteS3AccessPolicy(arn); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}
