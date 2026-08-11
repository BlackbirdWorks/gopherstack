package appconfig

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func (h *Handler) handleCreateDeploymentStrategy(c *echo.Context) error {
	var req struct {
		Tags                        map[string]string `json:"Tags"`
		Name                        string            `json:"Name"`
		Description                 string            `json:"Description"`
		GrowthType                  string            `json:"GrowthType"`
		ReplicateTo                 string            `json:"ReplicateTo"`
		DeploymentDurationInMinutes int32             `json:"DeploymentDurationInMinutes"`
		FinalBakeTimeInMinutes      int32             `json:"FinalBakeTimeInMinutes"`
		GrowthFactor                float32           `json:"GrowthFactor"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	strategy, err := h.Backend.CreateDeploymentStrategy(
		req.Name, req.Description,
		req.DeploymentDurationInMinutes, req.FinalBakeTimeInMinutes,
		req.GrowthFactor, req.GrowthType, req.ReplicateTo,
		req.Tags,
	)
	if err != nil {
		return internalServerErrorResponse(c, err)
	}

	return c.JSON(http.StatusCreated, strategy)
}

func (h *Handler) handleGetDeploymentStrategy(c *echo.Context, strategyID string) error {
	strategy, err := h.Backend.GetDeploymentStrategy(strategyID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, strategy)
}

func (h *Handler) handleListDeploymentStrategies(c *echo.Context) error {
	nextToken, maxResults := appConfigPaginationParams(c)
	strategies, outToken := h.Backend.ListDeploymentStrategies(nextToken, maxResults)

	resp := map[string]any{keyItems: strategies}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateDeploymentStrategy(c *echo.Context, strategyID string) error {
	var req struct {
		DeploymentDurationInMinutes *int32   `json:"DeploymentDurationInMinutes"`
		FinalBakeTimeInMinutes      *int32   `json:"FinalBakeTimeInMinutes"`
		GrowthFactor                *float32 `json:"GrowthFactor"`
		Description                 *string  `json:"Description"`
		Name                        string   `json:"Name"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	// Fetch current values to use as defaults for omitted pointer fields.
	existing, err := h.Backend.GetDeploymentStrategy(strategyID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	deployDur := existing.DeploymentDurationInMinutes
	if req.DeploymentDurationInMinutes != nil {
		deployDur = *req.DeploymentDurationInMinutes
	}

	bakeTime := existing.FinalBakeTimeInMinutes
	if req.FinalBakeTimeInMinutes != nil {
		bakeTime = *req.FinalBakeTimeInMinutes
	}

	growthFactor := existing.GrowthFactor
	if req.GrowthFactor != nil {
		growthFactor = *req.GrowthFactor
	}

	strategy, err := h.Backend.UpdateDeploymentStrategy(
		strategyID, req.Name, req.Description,
		deployDur, bakeTime,
		growthFactor,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, strategy)
}

func (h *Handler) handleDeleteDeploymentStrategy(c *echo.Context, strategyID string) error {
	if err := h.Backend.DeleteDeploymentStrategy(strategyID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
