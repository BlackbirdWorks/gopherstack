package fis

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// ----------------------------------------
// ExperimentTemplate handlers
// ----------------------------------------

func (h *Handler) handleCreateExperimentTemplate(
	_ context.Context,
	c *echo.Context,
	body []byte,
) error {
	var input createExperimentTemplateRequest
	if err := json.Unmarshal(body, &input); err != nil {
		return h.writeError(c, http.StatusBadRequest, "invalid request body: "+err.Error(), "")
	}

	tpl, err := h.Backend.CreateExperimentTemplate(&input, h.AccountID, h.DefaultRegion)
	if err != nil {
		return h.writeBackendError(c, err, "")
	}

	return c.JSON(http.StatusCreated, experimentTemplateResponseDTO{
		ExperimentTemplate: toTemplateDTO(tpl),
	})
}

func (h *Handler) handleGetExperimentTemplate(c *echo.Context, id string) error {
	tpl, err := h.Backend.GetExperimentTemplate(id)
	if err != nil {
		return h.writeBackendError(c, err, id)
	}

	return c.JSON(http.StatusOK, experimentTemplateResponseDTO{
		ExperimentTemplate: toTemplateDTO(tpl),
	})
}

func (h *Handler) handleUpdateExperimentTemplate(c *echo.Context, id string, body []byte) error {
	var input updateExperimentTemplateRequest
	if err := json.Unmarshal(body, &input); err != nil {
		return h.writeError(c, http.StatusBadRequest, "invalid request body: "+err.Error(), id)
	}

	tpl, err := h.Backend.UpdateExperimentTemplate(id, &input)
	if err != nil {
		return h.writeBackendError(c, err, id)
	}

	return c.JSON(http.StatusOK, experimentTemplateResponseDTO{
		ExperimentTemplate: toTemplateDTO(tpl),
	})
}

func (h *Handler) handleDeleteExperimentTemplate(c *echo.Context, id string) error {
	if err := h.Backend.DeleteExperimentTemplate(id); err != nil {
		return h.writeBackendError(c, err, id)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListExperimentTemplates(c *echo.Context) error {
	templates, err := h.Backend.ListExperimentTemplates()
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, err.Error(), "")
	}

	ids := make([]string, len(templates))
	for i, t := range templates {
		ids[i] = t.ID
	}

	q := c.Request().URL.Query()
	maxResults, start := paginateWithToken(ids, q)

	end := min(start+maxResults, len(templates))

	var nextTok string

	if end < len(templates) {
		nextTok = encodePageToken(end)
	}

	page := templates[start:end]
	dtos := make([]experimentTemplateDTO, len(page))

	for i, t := range page {
		dtos[i] = toTemplateDTO(t)
	}

	return c.JSON(http.StatusOK, listExperimentTemplatesResponseDTO{
		ExperimentTemplates: dtos,
		NextToken:           nextTok,
	})
}

// ----------------------------------------
// DTO conversion helpers
// ----------------------------------------

func toTemplateDTO(tpl *ExperimentTemplate) experimentTemplateDTO {
	targets := make(map[string]experimentTemplateTargetDTO, len(tpl.Targets))
	for name, t := range tpl.Targets {
		filters := make([]experimentTemplateTargetFilterDTO, len(t.Filters))
		for i, f := range t.Filters {
			filters[i] = experimentTemplateTargetFilterDTO(f)
		}

		targets[name] = experimentTemplateTargetDTO{
			ResourceType:  t.ResourceType,
			SelectionMode: t.SelectionMode,
			ResourceArns:  t.ResourceArns,
			ResourceTags:  t.ResourceTags,
			Filters:       filters,
			Parameters:    t.Parameters,
		}
	}

	actions := make(map[string]experimentTemplateActionDTO, len(tpl.Actions))
	for name, a := range tpl.Actions {
		actions[name] = experimentTemplateActionDTO(a)
	}

	stopConditions := make([]experimentTemplateStopConditionDTO, len(tpl.StopConditions))
	for i, sc := range tpl.StopConditions {
		stopConditions[i] = experimentTemplateStopConditionDTO(sc)
	}

	dto := experimentTemplateDTO{
		ID:             tpl.ID,
		Arn:            tpl.Arn,
		Description:    tpl.Description,
		RoleArn:        tpl.RoleArn,
		Tags:           tpl.Tags,
		Targets:        targets,
		Actions:        actions,
		StopConditions: stopConditions,
		CreationTime:   toUnix(tpl.CreationTime),
		LastUpdateTime: toUnix(tpl.LastUpdateTime),
	}

	if tpl.LogConfiguration != nil {
		lc := &experimentTemplateLogConfigurationDTO{
			LogSchemaVersion: tpl.LogConfiguration.LogSchemaVersion,
		}

		if tpl.LogConfiguration.CloudWatchLogsConfiguration != nil {
			lc.CloudWatchLogsConfiguration = &cwLogsConfigurationDTO{
				LogGroupArn: tpl.LogConfiguration.CloudWatchLogsConfiguration.LogGroupArn,
			}
		}

		if tpl.LogConfiguration.S3Configuration != nil {
			lc.S3Configuration = &experimentTemplateS3ConfigurationDTO{
				BucketName: tpl.LogConfiguration.S3Configuration.BucketName,
				Prefix:     tpl.LogConfiguration.S3Configuration.Prefix,
			}
		}

		dto.LogConfiguration = lc
	}

	if tpl.ExperimentOptions != nil {
		dto.ExperimentOptions = &experimentTemplateExperimentOptionsDTO{
			AccountTargeting:          tpl.ExperimentOptions.AccountTargeting,
			EmptyTargetResolutionMode: tpl.ExperimentOptions.EmptyTargetResolutionMode,
		}
	}

	return dto
}
