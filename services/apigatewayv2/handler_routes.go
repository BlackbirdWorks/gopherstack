package apigatewayv2

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func (h *Handler) handleDeleteRouteRequestParameter(c *echo.Context, apiID, routeID, requestParameterKey string) error {
	if requestParameterKey == "" {
		return writeErr(c, http.StatusBadRequest, "requestParameterKey is required")
	}

	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteRouteRequestParameter(apiID, routeID, requestParameterKey); err != nil {
		log.Error("apigatewayv2: delete route request parameter failed",
			logKeyAPIID, apiID, "routeId", routeID, "key", requestParameterKey, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrRouteNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateRoute(c *echo.Context, apiID string) error {
	return handleCreate(c, apiID, "route", ErrAPINotFound, func(input CreateRouteInput) (*Route, error) {
		return h.Backend.CreateRoute(apiID, input)
	})
}

func (h *Handler) handleGetRoutes(c *echo.Context, apiID string) error {
	return handleGetList(c, apiID, "routes", func() ([]Route, error) {
		return h.Backend.GetRoutes(apiID)
	}, func(items []Route, next string) any { return listRoutesOutput{Items: items, NextToken: next} })
}

func (h *Handler) handleGetRoute(c *echo.Context, apiID, routeID string) error {
	log := logger.Load(c.Request().Context())

	route, err := h.Backend.GetRoute(apiID, routeID)
	if err != nil {
		log.Error("apigatewayv2: get route failed", logKeyAPIID, apiID, "routeId", routeID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrRouteNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, route)
}

func (h *Handler) handleDeleteRoute(c *echo.Context, apiID, routeID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteRoute(apiID, routeID); err != nil {
		log.Error("apigatewayv2: delete route failed", logKeyAPIID, apiID, "routeId", routeID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrRouteNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateRoute(c *echo.Context, apiID, routeID string) error {
	return handleUpdate(c, apiID, routeID, "route",
		func(input UpdateRouteInput) (*Route, error) {
			return h.Backend.UpdateRoute(apiID, routeID, input)
		},
		ErrAPINotFound, ErrRouteNotFound)
}
