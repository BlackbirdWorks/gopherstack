package kafkaconnect

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

func (h *Handler) buildWorkerConfigurationOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateWorkerConfiguration: func(c *echo.Context, _ string, body []byte) error {
			return h.handleCreateWorkerConfiguration(c, body)
		},
		opDescribeWorkerConfiguration: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleDescribeWorkerConfiguration(c, resource)
		},
		opListWorkerConfigurations: func(c *echo.Context, _ string, _ []byte) error {
			return h.handleListWorkerConfigurations(c)
		},
		opDeleteWorkerConfiguration: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleDeleteWorkerConfiguration(c, resource)
		},
	}
}

func (h *Handler) handleCreateWorkerConfiguration(c *echo.Context, body []byte) error {
	var req createWorkerConfigurationRequest
	if err := decodeBody(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if req.Name == "" || req.PropertiesFileContent == "" {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "missing required field")
	}

	cfg, err := h.Backend.CreateWorkerConfiguration(
		h.AccountID, h.DefaultRegion, req.Name, req.Description, req.PropertiesFileContent, req.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, createWorkerConfigurationResponse{
		CreationTime:             formatTime(cfg.CreationTime),
		Name:                     cfg.Name,
		WorkerConfigurationArn:   cfg.ARN,
		WorkerConfigurationState: cfg.State,
		LatestRevision:           workerConfigToRevisionSummaryDTO(cfg),
	})
}

func (h *Handler) handleDescribeWorkerConfiguration(c *echo.Context, workerConfigurationArn string) error {
	cfg, err := h.Backend.DescribeWorkerConfiguration(workerConfigurationArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, describeWorkerConfigurationResponse{
		CreationTime:             formatTime(cfg.CreationTime),
		Description:              cfg.Description,
		Name:                     cfg.Name,
		WorkerConfigurationArn:   cfg.ARN,
		WorkerConfigurationState: cfg.State,
		LatestRevision: &workerConfigurationRevisionDescriptionDTO{
			CreationTime:          formatTime(cfg.LatestRevision.CreationTime),
			Description:           cfg.LatestRevision.Description,
			PropertiesFileContent: cfg.LatestRevision.PropertiesFileContent,
			Revision:              cfg.LatestRevision.Revision,
		},
	})
}

func (h *Handler) handleListWorkerConfigurations(c *echo.Context) error {
	q := c.Request().URL.Query()
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	configs, next, err := h.Backend.ListWorkerConfigurations(q.Get("namePrefix"), q.Get("nextToken"), maxResults)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	dtos := make([]workerConfigurationSummaryDTO, 0, len(configs))
	for _, cfg := range configs {
		dtos = append(dtos, workerConfigToSummaryDTO(cfg))
	}

	return h.writeJSON(c, listWorkerConfigurationsResponse{WorkerConfigurations: dtos, NextToken: next})
}

func (h *Handler) handleDeleteWorkerConfiguration(c *echo.Context, workerConfigurationArn string) error {
	cfg, err := h.Backend.DeleteWorkerConfiguration(workerConfigurationArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, deleteWorkerConfigurationResponse{
		WorkerConfigurationArn:   cfg.ARN,
		WorkerConfigurationState: cfg.State,
	})
}
