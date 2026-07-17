package apigatewayv2

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func (h *Handler) handleCreateIntegration(c *echo.Context, apiID string) error {
	return handleCreate(c, apiID, "integration", ErrAPINotFound,
		func(input CreateIntegrationInput) (*Integration, error) {
			return h.Backend.CreateIntegration(apiID, input)
		})
}

func (h *Handler) handleGetIntegrations(c *echo.Context, apiID string) error {
	return handleGetList(c, apiID, "integrations", func() ([]Integration, error) {
		return h.Backend.GetIntegrations(apiID)
	}, func(items []Integration, next string) any {
		return listIntegrationsOutput{Items: items, NextToken: next}
	})
}

func (h *Handler) handleGetIntegration(c *echo.Context, apiID, integrationID string) error {
	log := logger.Load(c.Request().Context())

	integration, err := h.Backend.GetIntegration(apiID, integrationID)
	if err != nil {
		log.Error("apigatewayv2: get integration failed",
			logKeyAPIID, apiID, "integrationId", integrationID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrIntegrationNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, integration)
}

func (h *Handler) handleDeleteIntegration(c *echo.Context, apiID, integrationID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteIntegration(apiID, integrationID); err != nil {
		log.Error("apigatewayv2: delete integration failed",
			logKeyAPIID, apiID, "integrationId", integrationID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrIntegrationNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateIntegration(c *echo.Context, apiID, integrationID string) error {
	return handleUpdate(c, apiID, integrationID, "integration",
		func(input UpdateIntegrationInput) (*Integration, error) {
			return h.Backend.UpdateIntegration(apiID, integrationID, input)
		},
		ErrAPINotFound, ErrIntegrationNotFound)
}
