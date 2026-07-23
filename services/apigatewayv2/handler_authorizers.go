package apigatewayv2

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func (h *Handler) handleCreateAuthorizer(c *echo.Context, apiID string) error {
	return handleCreate(c, apiID, "authorizer", ErrAPINotFound, func(input CreateAuthorizerInput) (*Authorizer, error) {
		return h.Backend.CreateAuthorizer(apiID, input)
	})
}

func (h *Handler) handleGetAuthorizers(c *echo.Context, apiID string) error {
	return handleGetList(c, apiID, "authorizers", func() ([]Authorizer, error) {
		return h.Backend.GetAuthorizers(apiID)
	}, func(items []Authorizer, next string) any {
		return listAuthorizersOutput{Items: items, NextToken: next}
	})
}

func (h *Handler) handleGetAuthorizer(c *echo.Context, apiID, authorizerID string) error {
	log := logger.Load(c.Request().Context())

	authorizer, err := h.Backend.GetAuthorizer(apiID, authorizerID)
	if err != nil {
		log.Error("apigatewayv2: get authorizer failed", logKeyAPIID, apiID, "authorizerId", authorizerID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrAuthorizerNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, authorizer)
}

func (h *Handler) handleDeleteAuthorizer(c *echo.Context, apiID, authorizerID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteAuthorizer(apiID, authorizerID); err != nil {
		log.Error("apigatewayv2: delete authorizer failed",
			logKeyAPIID, apiID, "authorizerId", authorizerID, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrAuthorizerNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	// Drop any cached decisions for the now-deleted authorizer so they don't
	// linger in memory until their TTL expires (bd: gopherstack-wmh).
	if h.authCache != nil {
		h.authCache.purge(authorizerID)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateAuthorizer(c *echo.Context, apiID, authorizerID string) error {
	return handleUpdate(c, apiID, authorizerID, "authorizer",
		func(input UpdateAuthorizerInput) (*Authorizer, error) {
			return h.Backend.UpdateAuthorizer(apiID, authorizerID, input)
		},
		ErrAPINotFound, ErrAuthorizerNotFound)
}

func (h *Handler) handleResetAuthorizersCache(c *echo.Context, apiID, stageName string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.ResetAuthorizersCache(apiID, stageName); err != nil {
		log.Error("apigatewayv2: reset authorizers cache failed",
			logKeyAPIID, apiID, "stageName", stageName, "error", err)

		if errors.Is(err, ErrAPINotFound) || errors.Is(err, ErrStageNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	// Drop cached Lambda-authorizer decisions so subsequent requests re-invoke.
	if h.authCache != nil {
		h.authCache.reset()
	}

	return c.NoContent(http.StatusNoContent)
}
