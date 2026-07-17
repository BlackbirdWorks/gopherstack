package codeartifact

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type createPackageGroupBody struct {
	Pattern     string           `json:"pattern"`
	Description string           `json:"description"`
	ContactInfo string           `json:"contactInfo"`
	Tags        []map[string]any `json:"tags"`
}

func packageGroupToMap(pg *PackageGroup) map[string]any {
	m := map[string]any{
		keyArn:         pg.ARN,
		keyDomainName:  pg.DomainName,
		keyDomainOwner: pg.DomainOwner,
		"pattern":      pg.Pattern,
		keyCreatedTime: epochSeconds(pg.CreatedTime),
	}
	if pg.Description != "" {
		m["description"] = pg.Description
	}
	if pg.ContactInfo != "" {
		m["contactInfo"] = pg.ContactInfo
	}

	return m
}

func (h *Handler) handleCreatePackageGroup(c *echo.Context, domainName string, body []byte) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}

	var in createPackageGroupBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	pattern := in.Pattern
	if pattern == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "pattern is required"))
	}

	pg, err := h.Backend.CreatePackageGroup(
		c.Request().Context(),
		domainName,
		pattern,
		in.Description,
		in.ContactInfo,
		tagsFromSlice(in.Tags),
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyPackageGroup: packageGroupToMap(pg),
	})
}

func (h *Handler) handleDescribePackageGroup(c *echo.Context, domainName, pattern string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if pattern == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "packageGroup is required"))
	}

	pg, err := h.Backend.DescribePackageGroup(c.Request().Context(), domainName, pattern)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyPackageGroup: packageGroupToMap(pg),
	})
}

func (h *Handler) handleDeletePackageGroup(c *echo.Context, domainName, pattern string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if pattern == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "packageGroup is required"))
	}

	pg, err := h.Backend.DeletePackageGroup(c.Request().Context(), domainName, pattern)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyPackageGroup: packageGroupToMap(pg),
	})
}

func (h *Handler) handleGetAssociatedPackageGroup(c *echo.Context, domainName, format, namespace, name string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}

	pg, err := h.Backend.GetAssociatedPackageGroup(c.Request().Context(), domainName, format, namespace, name)
	if err != nil {
		return h.handleError(c, err)
	}

	if pg == nil {
		return c.JSON(http.StatusOK, map[string]any{keyPackageGroup: nil})
	}

	return c.JSON(http.StatusOK, map[string]any{keyPackageGroup: packageGroupToMap(pg)})
}

func (h *Handler) handleListAllowedRepositoriesForGroup(c *echo.Context, domainName, pattern string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if pattern == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "packageGroup is required"))
	}

	repos, err := h.Backend.ListAllowedRepositoriesForGroup(c.Request().Context(), domainName, pattern)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"allowedRepositories": repos})
}

func (h *Handler) handleListAssociatedPackages(c *echo.Context, domainName, pattern string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if pattern == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "packageGroup is required"))
	}

	pkgs, err := h.Backend.ListAssociatedPackages(c.Request().Context(), domainName, pattern)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(pkgs))
	for _, pkg := range pkgs {
		items = append(items, packageToMap(pkg))
	}

	return c.JSON(http.StatusOK, map[string]any{"packages": items})
}

func (h *Handler) handleListPackageGroups(c *echo.Context, domainName, prefix string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}

	q := c.Request().URL.Query()
	maxResults := parseMaxResults(q.Get("max-results"))
	nextToken := q.Get("next-token")

	all, err := h.Backend.ListPackageGroups(c.Request().Context(), domainName, prefix)
	if err != nil {
		return h.handleError(c, err)
	}

	page, next := paginateSlice(all, maxResults, nextToken, func(pg *PackageGroup) string { return pg.Pattern })

	items := make([]map[string]any, 0, len(page))
	for _, pg := range page {
		items = append(items, packageGroupToMap(pg))
	}

	resp := map[string]any{"packageGroups": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListSubPackageGroups(c *echo.Context, domainName, pattern string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if pattern == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "packageGroup is required"))
	}

	q := c.Request().URL.Query()
	maxResults := parseMaxResults(q.Get("max-results"))
	nextToken := q.Get("next-token")

	all, err := h.Backend.ListSubPackageGroups(c.Request().Context(), domainName, pattern)
	if err != nil {
		return h.handleError(c, err)
	}

	page, next := paginateSlice(all, maxResults, nextToken, func(pg *PackageGroup) string { return pg.Pattern })

	items := make([]map[string]any, 0, len(page))
	for _, pg := range page {
		items = append(items, packageGroupToMap(pg))
	}

	resp := map[string]any{"packageGroups": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

type updatePackageGroupBody struct {
	Description  string `json:"description"`
	ContactInfo  string `json:"contactInfo"`
	PackageGroup string `json:"packageGroup"`
}

func (h *Handler) handleUpdatePackageGroup(c *echo.Context, domainName string, body []byte) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}

	var in updatePackageGroupBody
	if len(body) > 0 {
		_ = json.Unmarshal(body, &in)
	}

	pattern := c.Request().URL.Query().Get(keyPackageGroup)
	if pattern == "" {
		pattern = in.PackageGroup
	}

	pg, err := h.Backend.UpdatePackageGroup(c.Request().Context(), domainName, pattern, in.Description, in.ContactInfo)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"packageGroup": packageGroupToMap(pg)})
}

func (h *Handler) handleUpdatePackageGroupOriginConfiguration(c *echo.Context, domainName, pattern string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if pattern == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "packageGroup is required"))
	}

	pg, err := h.Backend.UpdatePackageGroupOriginConfiguration(c.Request().Context(), domainName, pattern)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"packageGroup": packageGroupToMap(pg)})
}
