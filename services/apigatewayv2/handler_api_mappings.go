package apigatewayv2

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func extractAPIMappingsCollOp(collection, method string) string {
	switch collection {
	case collAPIMappings:
		switch method {
		case http.MethodPost:
			return "CreateApiMapping"
		case http.MethodGet:
			return "GetApiMappings"
		}
	case collRoutingRules:
		switch method {
		case http.MethodPost:
			return "CreateRoutingRule"
		case http.MethodGet:
			return "ListRoutingRules"
		}
	}

	return opUnknown
}

func extractAPIMappingResourceOp(collection, method string) string {
	switch collection {
	case collAPIMappings:
		switch method {
		case http.MethodGet:
			return "GetApiMapping"
		case http.MethodDelete:
			return "DeleteApiMapping"
		case http.MethodPatch:
			return "UpdateApiMapping"
		}
	case collRoutingRules:
		switch method {
		case http.MethodGet:
			return "GetRoutingRule"
		case http.MethodDelete:
			return "DeleteRoutingRule"
		case http.MethodPut:
			return "PutRoutingRule"
		}
	}

	return opUnknown
}

func (h *Handler) handleAPIMappingsCollection(c *echo.Context, method, domainName string) error {
	switch method {
	case http.MethodPost:
		return handleCreateMulti(c, domainName, "api mapping",
			func(input CreateAPIMappingInput) (*APIMapping, error) {
				return h.Backend.CreateAPIMapping(domainName, input)
			},
			ErrDomainNameNotFound, ErrAPINotFound, ErrStageNotFound)
	case http.MethodGet:
		return h.handleGetAPIMappings(c, domainName)
	}

	return writeErr(c, http.StatusNotFound, msgNotFound)
}

func (h *Handler) handleAPIMappingResource(c *echo.Context, method, domainName, mappingID string) error {
	switch method {
	case http.MethodGet:
		return h.handleGetAPIMapping(c, domainName, mappingID)
	case http.MethodDelete:
		return h.handleDeleteAPIMapping(c, domainName, mappingID)
	case http.MethodPatch:
		return h.handleUpdateAPIMapping(c, domainName, mappingID)
	}

	return writeErr(c, http.StatusNotFound, msgNotFound)
}

func (h *Handler) handleGetAPIMappings(c *echo.Context, domainName string) error {
	log := logger.Load(c.Request().Context())

	items, err := h.Backend.GetAPIMappings(domainName)
	if err != nil {
		log.Error("apigatewayv2: get api mappings failed", "domainName", domainName, "error", err)

		if errors.Is(err, ErrDomainNameNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	maxResults, nextToken := apigwPaginationParams(c)
	p := page.New(items, nextToken, maxResults, apigwDefaultPageSize)

	return c.JSON(http.StatusOK, listAPIMappingsOutput{Items: p.Data, NextToken: p.Next})
}

func (h *Handler) handleGetAPIMapping(c *echo.Context, domainName, mappingID string) error {
	log := logger.Load(c.Request().Context())

	m, err := h.Backend.GetAPIMapping(domainName, mappingID)
	if err != nil {
		log.Error("apigatewayv2: get api mapping failed",
			"domainName", domainName, "mappingId", mappingID, "error", err)

		if errors.Is(err, ErrDomainNameNotFound) || errors.Is(err, ErrAPIMappingNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, m)
}

func (h *Handler) handleDeleteAPIMapping(c *echo.Context, domainName, mappingID string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteAPIMapping(domainName, mappingID); err != nil {
		log.Error("apigatewayv2: delete api mapping failed",
			"domainName", domainName, "mappingId", mappingID, "error", err)

		if errors.Is(err, ErrDomainNameNotFound) || errors.Is(err, ErrAPIMappingNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateAPIMapping(c *echo.Context, domainName, mappingID string) error {
	return handleUpdate(c, domainName, mappingID, "api mapping",
		func(input UpdateAPIMappingInput) (*APIMapping, error) {
			return h.Backend.UpdateAPIMapping(domainName, mappingID, input)
		},
		ErrDomainNameNotFound, ErrAPIMappingNotFound, ErrAPINotFound, ErrStageNotFound)
}
