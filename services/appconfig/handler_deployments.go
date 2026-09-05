package appconfig

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func (h *Handler) handleStartDeployment(
	c *echo.Context,
	applicationID, environmentID string,
) error {
	var req struct {
		KmsKeyIdentifier       *string           `json:"KmsKeyIdentifier"`
		LatestDeploymentNumber *int32            `json:"LatestDeploymentNumber"`
		Tags                   map[string]string `json:"Tags"`
		ConfigurationProfileID string            `json:"ConfigurationProfileId"`
		DeploymentStrategyID   string            `json:"DeploymentStrategyId"`
		ConfigurationVersion   string            `json:"ConfigurationVersion"`
		Description            string            `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	deployment, err := h.Backend.StartDeployment(
		applicationID, environmentID,
		req.ConfigurationProfileID, req.DeploymentStrategyID,
		req.ConfigurationVersion, req.Description,
		req.KmsKeyIdentifier, req.LatestDeploymentNumber, req.Tags,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		if errors.Is(err, awserr.ErrAlreadyExists) {
			return conflictResponse(c, err)
		}

		if errors.Is(err, awserr.ErrInvalidParameter) {
			return badRequestResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	return c.JSON(http.StatusCreated, deployment)
}

func (h *Handler) handleGetDeployment(
	c *echo.Context,
	applicationID, environmentID string,
	deploymentNumber int32,
) error {
	deployment, err := h.Backend.GetDeployment(applicationID, environmentID, deploymentNumber)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, deployment)
}

func (h *Handler) handleListDeployments(
	c *echo.Context,
	applicationID, environmentID string,
) error {
	nextToken, maxResults := appConfigPaginationParams(c)
	deployments, outToken, err := h.Backend.ListDeployments(
		applicationID,
		environmentID,
		nextToken,
		maxResults,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	summaries := make([]DeploymentSummary, 0, len(deployments))
	for _, d := range deployments {
		summaries = append(summaries, deploymentToSummary(d))
	}

	resp := map[string]any{keyItems: summaries}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleStopDeployment(
	c *echo.Context,
	applicationID, environmentID string,
	deploymentNumber int32,
) error {
	// Real StopDeploymentInput binds AllowRevert as the "Allow-Revert"
	// request header, not a body/query field.
	allowRevert, _ := strconv.ParseBool(c.Request().Header.Get("Allow-Revert"))

	deployment, err := h.Backend.StopDeployment(applicationID, environmentID, deploymentNumber, allowRevert)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		if errors.Is(err, awserr.ErrInvalidParameter) {
			return badRequestResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	// Real StopDeploymentOutput echoes the full post-stop deployment state
	// (appconfig@v1.48.4 api_op_StopDeployment.go) -- previously this handler
	// returned 204 No Content, so a real client's StopDeployment always
	// decoded an all-zero output (json.Decoder tolerates the empty body via
	// io.EOF rather than erroring, so this was silent, not a hard failure).
	return c.JSON(http.StatusOK, deployment)
}
