package kafkaconnect

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

func (h *Handler) buildCustomPluginOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateCustomPlugin: func(c *echo.Context, _ string, body []byte) error {
			return h.handleCreateCustomPlugin(c, body)
		},
		opDescribeCustomPlugin: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleDescribeCustomPlugin(c, resource)
		},
		opListCustomPlugins: func(c *echo.Context, _ string, _ []byte) error {
			return h.handleListCustomPlugins(c)
		},
		opDeleteCustomPlugin: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleDeleteCustomPlugin(c, resource)
		},
	}
}

func (h *Handler) handleCreateCustomPlugin(c *echo.Context, body []byte) error {
	var req createCustomPluginRequest
	if err := decodeBody(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if req.Name == "" || req.ContentType == "" {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "missing required field")
	}

	plugin, err := h.Backend.CreateCustomPlugin(
		h.AccountID, h.DefaultRegion, req.Name, req.Description, req.ContentType,
		req.Location.S3Location.BucketArn, req.Location.S3Location.FileKey, req.Location.S3Location.ObjectVersion,
		req.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, createCustomPluginResponse{
		CustomPluginArn:   plugin.ARN,
		CustomPluginState: plugin.State,
		Name:              plugin.Name,
		Revision:          plugin.Revision,
	})
}

func (h *Handler) handleDescribeCustomPlugin(c *echo.Context, customPluginArn string) error {
	plugin, err := h.Backend.DescribeCustomPlugin(customPluginArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, describeCustomPluginResponse{
		CreationTime:      formatTime(plugin.CreationTime),
		CustomPluginArn:   plugin.ARN,
		CustomPluginState: plugin.State,
		Description:       plugin.Description,
		Name:              plugin.Name,
		LatestRevision:    customPluginToRevisionSummaryDTO(plugin),
	})
}

func (h *Handler) handleListCustomPlugins(c *echo.Context) error {
	q := c.Request().URL.Query()
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	plugins, next, err := h.Backend.ListCustomPlugins(q.Get("namePrefix"), q.Get("nextToken"), maxResults)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	dtos := make([]customPluginSummaryDTO, 0, len(plugins))
	for _, p := range plugins {
		dtos = append(dtos, customPluginToSummaryDTO(p))
	}

	return h.writeJSON(c, listCustomPluginsResponse{CustomPlugins: dtos, NextToken: next})
}

func (h *Handler) handleDeleteCustomPlugin(c *echo.Context, customPluginArn string) error {
	plugin, err := h.Backend.DeleteCustomPlugin(customPluginArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, deleteCustomPluginResponse{CustomPluginArn: plugin.ARN, CustomPluginState: plugin.State})
}
