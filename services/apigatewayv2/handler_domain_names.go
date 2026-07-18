package apigatewayv2

import (
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func extractDomainNamesOp(path, method string) string {
	suffix := strings.Trim(strings.TrimPrefix(path, domainNamesPrefix), "/")

	if suffix == "" {
		switch method {
		case http.MethodPost:
			return "CreateDomainName"
		case http.MethodGet:
			return "GetDomainNames"
		}

		return opUnknown
	}

	parts := strings.SplitN(suffix, "/", maxPathParts)

	switch len(parts) {
	case pathPartOne:
		return extractDomainNameByIDOp(method)
	case pathPartTwo:
		return extractAPIMappingsCollOp(parts[1], method)
	case maxPathParts:
		return extractAPIMappingResourceOp(parts[1], method)
	}

	return opUnknown
}

func extractDomainNameByIDOp(method string) string {
	switch method {
	case http.MethodGet:
		return "GetDomainName"
	case http.MethodDelete:
		return "DeleteDomainName"
	case http.MethodPatch:
		return "UpdateDomainName"
	}

	return opUnknown
}

// handleDomainNamesPath handles requests for /v2/domainnames and /v2/domainnames/{domainName}/...
func (h *Handler) handleDomainNamesPath(c *echo.Context, method, path string) error {
	suffix := strings.TrimPrefix(path, domainNamesPrefix)
	suffix = strings.Trim(suffix, "/")

	if suffix == "" {
		switch method {
		case http.MethodPost:
			return handleCreateNoParent(c, "domain name", func(input CreateDomainNameInput) (*DomainName, error) {
				return h.Backend.CreateDomainName(c.Request().Context(), input)
			})
		case http.MethodGet:
			return h.handleGetDomainNames(c)
		default:
			return writeErr(c, http.StatusMethodNotAllowed, msgMethodNotAllowed)
		}
	}

	parts := strings.Split(suffix, "/")

	switch len(parts) {
	case pathPartOne:
		domainName := parts[0]
		switch method {
		case http.MethodGet:
			return h.handleGetDomainName(c, domainName)
		case http.MethodDelete:
			return h.handleDeleteDomainName(c, domainName)
		case http.MethodPatch:
			return h.handleUpdateDomainName(c, domainName)
		}
	case pathPartTwo:
		if parts[1] == collAPIMappings {
			return h.handleAPIMappingsCollection(c, method, parts[0])
		}
		if parts[1] == collRoutingRules {
			return h.handleRoutingRulesCollection(c, method, parts[0])
		}
	case maxPathParts:
		if parts[1] == collAPIMappings {
			return h.handleAPIMappingResource(c, method, parts[0], parts[pathPartTwo])
		}
		if parts[1] == collRoutingRules {
			return h.handleRoutingRuleResource(c, method, parts[0], parts[pathPartTwo])
		}
	}

	return writeErr(c, http.StatusNotFound, msgNotFound)
}

func (h *Handler) handleRoutingRulesCollection(c *echo.Context, method, domainName string) error {
	switch method {
	case http.MethodPost:
		return handleCreate(c, domainName, "routing rule", ErrDomainNameNotFound,
			func(input CreateRoutingRuleInput) (*RoutingRule, error) {
				return h.Backend.CreateRoutingRule(c.Request().Context(), domainName, input)
			})
	case http.MethodGet:
		rules, err := h.Backend.ListRoutingRules(domainName)
		if err != nil {
			if errors.Is(err, ErrDomainNameNotFound) || errors.Is(err, ErrRoutingRuleNotFound) {
				return writeErr(c, http.StatusNotFound, msgNotFound)
			}

			return writeErr(c, http.StatusInternalServerError, err.Error())
		}

		return c.JSON(http.StatusOK, listRoutingRulesOutput{Items: rules})
	}

	return writeErr(c, http.StatusNotFound, msgNotFound)
}

func (h *Handler) handleRoutingRuleResource(c *echo.Context, method, domainName, ruleID string) error {
	switch method {
	case http.MethodGet:
		rule, err := h.Backend.GetRoutingRule(domainName, ruleID)
		if err != nil {
			if errors.Is(err, ErrDomainNameNotFound) || errors.Is(err, ErrRoutingRuleNotFound) {
				return writeErr(c, http.StatusNotFound, msgNotFound)
			}

			return writeErr(c, http.StatusInternalServerError, err.Error())
		}

		return c.JSON(http.StatusOK, rule)
	case http.MethodPut:
		return handleUpdate(c, domainName, ruleID, "routing rule",
			func(input PutRoutingRuleInput) (*RoutingRule, error) {
				return h.Backend.PutRoutingRule(domainName, ruleID, input)
			},
			ErrDomainNameNotFound, ErrRoutingRuleNotFound)
	case http.MethodDelete:
		err := h.Backend.DeleteRoutingRule(domainName, ruleID)
		if err != nil {
			if errors.Is(err, ErrDomainNameNotFound) || errors.Is(err, ErrRoutingRuleNotFound) {
				return writeErr(c, http.StatusNotFound, msgNotFound)
			}

			return writeErr(c, http.StatusInternalServerError, err.Error())
		}

		return c.NoContent(http.StatusNoContent)
	}

	return writeErr(c, http.StatusNotFound, msgNotFound)
}

func (h *Handler) handleGetDomainNames(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	items, err := h.Backend.GetDomainNames()
	if err != nil {
		log.Error("apigatewayv2: get domain names failed", "error", err)

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, listDomainNamesOutput{Items: items})
}

func (h *Handler) handleGetDomainName(c *echo.Context, domainName string) error {
	log := logger.Load(c.Request().Context())

	dn, err := h.Backend.GetDomainName(domainName)
	if err != nil {
		log.Error("apigatewayv2: get domain name failed", "domainName", domainName, "error", err)

		if errors.Is(err, ErrDomainNameNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, dn)
}

func (h *Handler) handleDeleteDomainName(c *echo.Context, domainName string) error {
	log := logger.Load(c.Request().Context())

	if err := h.Backend.DeleteDomainName(domainName); err != nil {
		log.Error("apigatewayv2: delete domain name failed", "domainName", domainName, "error", err)

		if errors.Is(err, ErrDomainNameNotFound) {
			return writeErr(c, http.StatusNotFound, msgNotFound)
		}

		return writeErr(c, http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUpdateDomainName(c *echo.Context, domainName string) error {
	return handleUpdate(c, domainName, "", "domain name",
		func(input UpdateDomainNameInput) (*DomainName, error) {
			return h.Backend.UpdateDomainName(domainName, input)
		},
		ErrDomainNameNotFound)
}
