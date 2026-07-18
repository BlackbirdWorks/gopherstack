package apigatewayv2

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func (h *Handler) handleDeleteAccessLogSettings(c *echo.Context, apiID, stageName string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteAccessLogSettings(apiID, stageName); err != nil {
		log.Error("apigatewayv2: delete access log settings failed",
			logKeyAPIID, apiID, "stageName", stageName, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrStageNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleDeleteRouteSettings(c *echo.Context, apiID, stageName, routeKey string) error {
	if routeKey == "" {
		return writeErr(c, http.StatusBadRequest, "routeKey is required")
	}

	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteRouteSettings(apiID, stageName, routeKey); err != nil {
		log.Error("apigatewayv2: delete route settings failed",
			logKeyAPIID, apiID, "stageName", stageName, "routeKey", routeKey, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrStageNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateStage(c *echo.Context, apiID string) error {
	return handleCreate(c, apiID, "stage", ErrAPINotFound, func(input CreateStageInput) (*Stage, error) {
		return h.Backend.CreateStage(apiID, input)
	})
}

func (h *Handler) handleGetStages(c *echo.Context, apiID string) error {
	return handleGetList(c, apiID, "stages", func() ([]Stage, error) {
		return h.Backend.GetStages(apiID)
	}, func(items []Stage, next string) any { return listStagesOutput{Items: items, NextToken: next} })
}

func (h *Handler) handleGetStage(c *echo.Context, apiID, stageName string) error {
	log := logger.Load(c.Request().Context())

	stage, err := h.Backend.GetStage(apiID, stageName)
	if err != nil {
		log.Error("apigatewayv2: get stage failed", logKeyAPIID, apiID, "stageName", stageName, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrStageNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, stage)
}

func (h *Handler) handleDeleteStage(c *echo.Context, apiID, stageName string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteStage(apiID, stageName); err != nil {
		log.Error("apigatewayv2: delete stage failed", logKeyAPIID, apiID, "stageName", stageName, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrStageNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateStage(c *echo.Context, apiID, stageName string) error {
	return handleUpdate(c, apiID, stageName, "stage",
		func(input UpdateStageInput) (*Stage, error) {
			return h.Backend.UpdateStage(apiID, stageName, input)
		},
		ErrAPINotFound, ErrStageNotFound)
}
