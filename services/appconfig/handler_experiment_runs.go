package appconfig

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func (h *Handler) handleStartExperimentRun(c *echo.Context, applicationID, defIdentifier string) error {
	var req struct {
		DeploymentParameters *DeploymentParameters `json:"DeploymentParameters"`
		TreatmentOverrides   *TreatmentOverrides   `json:"TreatmentOverrides"`
		ExposurePercentage   *float32              `json:"ExposurePercentage"`
		Tags                 map[string]string     `json:"Tags"`
		Description          string                `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: errInvalidRequestBody})
	}

	var overrides map[string]string
	if req.TreatmentOverrides != nil {
		overrides = req.TreatmentOverrides.Inline
	}

	run, err := h.Backend.StartExperimentRun(
		applicationID, defIdentifier, req.Description, req.ExposurePercentage, overrides, req.Tags,
	)
	if err != nil {
		return experimentRunErrorResponse(c, err)
	}

	return c.JSON(http.StatusCreated, run)
}

func (h *Handler) handleGetExperimentRun(c *echo.Context, applicationID, defIdentifier string, run int32) error {
	r, err := h.Backend.GetExperimentRun(applicationID, defIdentifier, run)
	if err != nil {
		return experimentRunErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, r)
}

func (h *Handler) handleListExperimentRuns(c *echo.Context, applicationID, defIdentifier string) error {
	nextToken, maxResults := appConfigPaginationParams(c)
	status := c.Request().URL.Query().Get("status")

	runs, outToken, err := h.Backend.ListExperimentRuns(applicationID, defIdentifier, status, nextToken, maxResults)
	if err != nil {
		return experimentRunErrorResponse(c, err)
	}

	resp := map[string]any{keyItems: runs}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateExperimentRun(
	c *echo.Context,
	applicationID, defIdentifier string,
	run int32,
) error {
	var req struct {
		DeploymentParameters *DeploymentParameters `json:"DeploymentParameters"`
		TreatmentOverrides   *TreatmentOverrides   `json:"TreatmentOverrides"`
		Description          *string               `json:"Description"`
		ExposurePercentage   *float32              `json:"ExposurePercentage"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: errInvalidRequestBody})
	}

	r, err := h.Backend.UpdateExperimentRun(
		applicationID, defIdentifier, run, req.Description, req.ExposurePercentage, req.TreatmentOverrides,
	)
	if err != nil {
		return experimentRunErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, r)
}

func (h *Handler) handleStopExperimentRun(c *echo.Context, applicationID, defIdentifier string, run int32) error {
	var req struct {
		DeploymentParameters *DeploymentParameters `json:"DeploymentParameters"`
		Result               *ExperimentRunResult  `json:"Result"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: errInvalidRequestBody})
	}

	r, err := h.Backend.StopExperimentRun(applicationID, defIdentifier, run, req.Result)
	if err != nil {
		return experimentRunErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, r)
}

func (h *Handler) handleListExperimentRunEvents(
	c *echo.Context,
	applicationID, defIdentifier string,
	run int32,
) error {
	nextToken, maxResults := appConfigPaginationParams(c)

	events, outToken, err := h.Backend.ListExperimentRunEvents(
		applicationID, defIdentifier, run, nextToken, maxResults,
	)
	if err != nil {
		return experimentRunErrorResponse(c, err)
	}

	resp := map[string]any{keyItems: events}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

// experimentRunErrorResponse maps a StorageBackend experiment-run error to
// its AWS HTTP status/body, shared by every handler in this file.
func experimentRunErrorResponse(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return notFoundResponse(c, err)
	case errors.Is(err, awserr.ErrAlreadyExists):
		return conflictResponse(c, err)
	case errors.Is(err, awserr.ErrInvalidParameter):
		return badRequestResponse(c, err)
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{keyMessageField: err.Error()})
	}
}
