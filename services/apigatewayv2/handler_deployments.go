package apigatewayv2

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func (h *Handler) handleCreateDeployment(c *echo.Context, apiID string) error {
	return handleCreateMulti(c, apiID, "deployment",
		func(input CreateDeploymentInput) (*Deployment, error) {
			return h.Backend.CreateDeployment(apiID, input)
		},
		ErrAPINotFound, ErrStageNotFound)
}

func (h *Handler) handleGetDeployments(c *echo.Context, apiID string) error {
	return handleGetList(c, apiID, "deployments", func() ([]Deployment, error) {
		return h.Backend.GetDeployments(apiID)
	}, func(items []Deployment, next string) any {
		return listDeploymentsOutput{Items: items, NextToken: next}
	})
}

func (h *Handler) handleGetDeployment(c *echo.Context, apiID, deploymentID string) error {
	log := logger.Load(c.Request().Context())

	deployment, err := h.Backend.GetDeployment(apiID, deploymentID)
	if err != nil {
		log.Error("apigatewayv2: get deployment failed", logKeyAPIID, apiID, "deploymentId", deploymentID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrDeploymentNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, deployment)
}

func (h *Handler) handleDeleteDeployment(c *echo.Context, apiID, deploymentID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteDeployment(apiID, deploymentID); err != nil {
		log.Error("apigatewayv2: delete deployment failed",
			logKeyAPIID, apiID, "deploymentId", deploymentID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrDeploymentNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateDeployment(c *echo.Context, apiID, deploymentID string) error {
	return handleUpdate(c, apiID, deploymentID, "deployment",
		func(input UpdateDeploymentInput) (*Deployment, error) {
			return h.Backend.UpdateDeployment(apiID, deploymentID, input)
		},
		ErrAPINotFound, ErrDeploymentNotFound)
}
