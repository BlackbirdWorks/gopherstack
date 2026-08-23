package iotanalytics

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreatePipeline(c *echo.Context, body []byte) error {
	var req createPipelineRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if req.PipelineName == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "pipelineName is required")
	}

	if err := validateTags(req.Tags); err != nil {
		return h.writeBackendError(c, err)
	}

	tags := tagsToMap(req.Tags)

	p, err := h.Backend.CreatePipeline(c.Request().Context(), req.PipelineName, tags, req.PipelineActivities)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createPipelineResponse{
		PipelineName: p.Name,
		PipelineARN:  p.ARN,
	})
}

func (h *Handler) handleListPipelines(c *echo.Context) error {
	maxResults, cursor := parsePagination(c)
	pipelines := h.Backend.ListPipelines()

	summaries := make([]pipelineSummary, 0, len(pipelines))
	var nextToken *string

	count := 0

	for _, p := range pipelines {
		if cursor != "" && p.Name <= cursor {
			continue
		}

		if count >= maxResults {
			tok := encodeNextToken(summaries[len(summaries)-1].PipelineName)
			nextToken = &tok

			break
		}

		summaries = append(summaries, pipelineSummary{
			PipelineName:          p.Name,
			ReprocessingSummaries: reprocessingSummariesSorted(p.Reprocessings),
			CreationTime:          p.CreationTime,
			LastUpdateTime:        p.LastUpdate,
		})
		count++
	}

	return c.JSON(http.StatusOK, listPipelinesResponse{
		PipelineSummaries: summaries,
		NextToken:         nextToken,
	})
}

func (h *Handler) handleDescribePipeline(c *echo.Context, name string) error {
	p, err := h.Backend.DescribePipeline(name)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describePipelineResponse{
		Pipeline: pipelineDetail{
			Activities:            p.Activities,
			ReprocessingSummaries: reprocessingSummariesSorted(p.Reprocessings),
			Tags:                  mapToTagsSorted(p.Tags),
			Name:                  p.Name,
			ARN:                   p.ARN,
			CreationTime:          p.CreationTime,
			LastUpdateTime:        p.LastUpdate,
		},
	})
}

func (h *Handler) handleUpdatePipeline(c *echo.Context, name string, body []byte) error {
	var req updatePipelineRequest

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidRequestException",
				"invalid request body: "+err.Error(),
			)
		}
	}

	if err := h.Backend.UpdatePipeline(name, req.PipelineActivities); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeletePipeline(c *echo.Context, name string) error {
	if err := h.Backend.DeletePipeline(name); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleStartPipelineReprocessing(c *echo.Context, pipelineName string, body []byte) error {
	var req startPipelineReprocessingRequest

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return h.writeError(
				c,
				http.StatusBadRequest,
				"InvalidRequestException",
				"invalid request body: "+err.Error(),
			)
		}
	}

	reprocessingID, err := h.Backend.StartPipelineReprocessing(pipelineName, req.StartTime, req.EndTime)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusCreated, startPipelineReprocessingResponse{ReprocessingID: reprocessingID})
}

func (h *Handler) handleCancelPipelineReprocessing(c *echo.Context, resource string) error {
	parts := strings.SplitN(resource, "/", minNameSegments)
	if len(parts) != minNameSegments {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	pipelineName := parts[0]
	reprocessingID := parts[1]

	if err := h.Backend.CancelPipelineReprocessing(pipelineName, reprocessingID); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleRunPipelineActivity(c *echo.Context, body []byte) error {
	var req runPipelineActivityRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	const maxRunPayloads = 10
	if len(req.Payloads) > maxRunPayloads {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidRequestException",
			"payloads must not contain more than 10 items",
		)
	}

	payloads, err := h.Backend.RunPipelineActivity(c.Request().Context(), req.PipelineActivity, req.Payloads)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalFailureException", err.Error())
	}

	return c.JSON(http.StatusOK, runPipelineActivityResponse{
		Payloads:  payloads,
		LogResult: "",
	})
}
