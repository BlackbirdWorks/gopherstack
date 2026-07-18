package fis

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// ----------------------------------------
// Experiment handlers
// ----------------------------------------

func (h *Handler) handleStartExperiment(ctx context.Context, c *echo.Context, body []byte) error {
	var input startExperimentRequest
	if err := json.Unmarshal(body, &input); err != nil {
		return h.writeError(c, http.StatusBadRequest, "invalid request body: "+err.Error(), "")
	}

	exp, err := h.Backend.StartExperiment(ctx, &input, h.AccountID, h.DefaultRegion)
	if err != nil {
		return h.writeBackendError(c, err, input.ExperimentTemplateID)
	}

	return c.JSON(http.StatusCreated, experimentResponseDTO{
		Experiment: toExperimentDTO(exp),
	})
}

func (h *Handler) handleGetExperiment(c *echo.Context, id string) error {
	exp, err := h.Backend.GetExperiment(id)
	if err != nil {
		return h.writeBackendError(c, err, id)
	}

	return c.JSON(http.StatusOK, experimentResponseDTO{
		Experiment: toExperimentDTO(exp),
	})
}

func (h *Handler) handleStopExperiment(c *echo.Context, id string) error {
	exp, err := h.Backend.StopExperiment(id)
	if err != nil {
		return h.writeBackendError(c, err, id)
	}

	return c.JSON(http.StatusOK, experimentResponseDTO{
		Experiment: toExperimentDTO(exp),
	})
}

func (h *Handler) handleListExperiments(c *echo.Context) error {
	experiments, err := h.Backend.ListExperiments()
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, err.Error(), "")
	}

	q := c.Request().URL.Query()

	// Filter by experimentTemplateId.
	if tplFilter := q.Get("experimentTemplateId"); tplFilter != "" {
		filtered := experiments[:0]

		for _, e := range experiments {
			if e.ExperimentTemplateID == tplFilter {
				filtered = append(filtered, e)
			}
		}

		experiments = filtered
	}

	// Filter by status.
	if statusFilter := q.Get("status"); statusFilter != "" {
		filtered := experiments[:0]

		for _, e := range experiments {
			if e.Status.Status == statusFilter {
				filtered = append(filtered, e)
			}
		}

		experiments = filtered
	}

	// Apply cursor-based pagination.
	ids := make([]string, len(experiments))
	for i, e := range experiments {
		ids[i] = e.ID
	}

	maxResults, start := paginateWithToken(ids, q)

	end := min(start+maxResults, len(experiments))

	var nextTok string

	if end < len(experiments) {
		nextTok = encodePageToken(end)
	}

	page := experiments[start:end]
	dtos := make([]experimentDTO, len(page))

	for i, e := range page {
		dtos[i] = toExperimentDTO(e)
	}

	return c.JSON(http.StatusOK, listExperimentsResponseDTO{
		Experiments: dtos,
		NextToken:   nextTok,
	})
}

// ----------------------------------------
// Resolved targets handlers
// ----------------------------------------

func (h *Handler) handleListExperimentResolvedTargets(c *echo.Context, id string) error {
	resolved, err := h.Backend.ListExperimentResolvedTargets(id)
	if err != nil {
		return h.writeBackendError(c, err, id)
	}

	dtos := make([]resolvedTargetDTO, len(resolved))
	for i, rt := range resolved {
		dtos[i] = resolvedTargetDTO(rt)
	}

	return c.JSON(http.StatusOK, listExperimentResolvedTargetsResponseDTO{
		ResolvedTargets: dtos,
	})
}

// ----------------------------------------
// DTO conversion helpers
// ----------------------------------------

func toExperimentDTO(exp *Experiment) experimentDTO {
	targets := make(map[string]experimentTargetDTO, len(exp.Targets))
	for name, t := range exp.Targets {
		targets[name] = experimentTargetDTO(t)
	}

	actions := make(map[string]experimentActionDTO, len(exp.Actions))
	for name, a := range exp.Actions {
		dto := experimentActionDTO{
			ActionID:   a.ActionID,
			Parameters: a.Parameters,
			Targets:    a.Targets,
			Status: &experimentActionStatusDTO{
				Status: a.Status.Status,
				Reason: a.Status.Reason,
			},
			State: &experimentActionStatusDTO{
				Status: a.Status.Status,
				Reason: a.Status.Reason,
			},
			StartTime: toUnixPtr(a.StartTime),
			EndTime:   toUnixPtr(a.EndTime),
		}

		actions[name] = dto
	}

	stopConditions := make([]experimentStopConditionDTO, len(exp.StopConditions))
	for i, sc := range exp.StopConditions {
		stopConditions[i] = experimentStopConditionDTO(sc)
	}

	statusDTO := experimentStatusDTO{
		Status: exp.Status.Status,
		Reason: exp.Status.Reason,
	}

	if exp.Status.Error != nil {
		statusDTO.Error = &experimentStatusErrorDTO{
			Code:      exp.Status.Error.Code,
			Location:  exp.Status.Error.Location,
			AccountID: exp.Status.Error.AccountID,
		}
	}

	dto := experimentDTO{
		ID:                               exp.ID,
		Arn:                              exp.Arn,
		ExperimentTemplateID:             exp.ExperimentTemplateID,
		RoleArn:                          exp.RoleArn,
		Status:                           statusDTO,
		State:                            statusDTO,
		Targets:                          targets,
		Actions:                          actions,
		StopConditions:                   stopConditions,
		Tags:                             exp.Tags,
		CreationTime:                     toUnix(exp.CreationTime),
		StartTime:                        toUnix(exp.StartTime),
		EndTime:                          toUnixPtr(exp.EndTime),
		TargetAccountConfigurationsCount: exp.TargetAccountConfigurationsCount,
	}

	if exp.LogConfiguration != nil {
		lc := &experimentLogConfigurationDTO{
			LogSchemaVersion: exp.LogConfiguration.LogSchemaVersion,
		}

		if exp.LogConfiguration.CloudWatchLogsConfiguration != nil {
			lc.CloudWatchLogsConfiguration = &experimentCloudWatchLogsConfigurationDTO{
				LogGroupArn: exp.LogConfiguration.CloudWatchLogsConfiguration.LogGroupArn,
			}
		}

		if exp.LogConfiguration.S3Configuration != nil {
			lc.S3Configuration = &experimentS3ConfigurationDTO{
				BucketName: exp.LogConfiguration.S3Configuration.BucketName,
				Prefix:     exp.LogConfiguration.S3Configuration.Prefix,
			}
		}

		dto.LogConfiguration = lc
	}

	if exp.ExperimentOptions != nil {
		dto.ExperimentOptions = &experimentExperimentOptionsDTO{
			AccountTargeting:          exp.ExperimentOptions.AccountTargeting,
			EmptyTargetResolutionMode: exp.ExperimentOptions.EmptyTargetResolutionMode,
		}
	}

	return dto
}
