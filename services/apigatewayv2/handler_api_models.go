package apigatewayv2

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func (h *Handler) handleGetModelTemplate(c *echo.Context, apiID, modelID string) error {
	model, err := h.Backend.GetModel(apiID, modelID)
	if err != nil {
		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrModelNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	// AWS returns the model's schema as the template value; fall back to an empty
	// object only when the model has no schema defined.
	value := model.Schema
	if value == "" {
		value = emptyModelTemplate
	}

	return c.JSON(http.StatusOK, map[string]string{"value": value})
}

func (h *Handler) handleCreateModel(c *echo.Context, apiID string) error {
	return handleCreate(c, apiID, "model", ErrAPINotFound, func(input CreateModelInput) (*Model, error) {
		return h.Backend.CreateModel(apiID, input)
	})
}

func (h *Handler) handleGetModels(c *echo.Context, apiID string) error {
	return handleGetList(c, apiID, "models", func() ([]Model, error) {
		return h.Backend.GetModels(apiID)
	}, func(items []Model, next string) any { return listModelsOutput{Items: items, NextToken: next} })
}

func (h *Handler) handleGetModel(c *echo.Context, apiID, modelID string) error {
	log := logger.Load(c.Request().Context())

	model, err := h.Backend.GetModel(apiID, modelID)
	if err != nil {
		log.Error("apigatewayv2: get model failed", logKeyAPIID, apiID, "modelId", modelID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrModelNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, model)
}

func (h *Handler) handleDeleteModel(c *echo.Context, apiID, modelID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteModel(apiID, modelID); err != nil {
		log.Error("apigatewayv2: delete model failed", logKeyAPIID, apiID, "modelId", modelID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrModelNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateModel(c *echo.Context, apiID, modelID string) error {
	return handleUpdate(c, apiID, modelID, "model",
		func(input UpdateModelInput) (*Model, error) {
			return h.Backend.UpdateModel(apiID, modelID, input)
		},
		ErrAPINotFound, ErrModelNotFound)
}
