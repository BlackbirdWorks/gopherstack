package codeartifact

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func packageToMap(pkg *Package) map[string]any {
	m := map[string]any{
		keyFormat:      pkg.Format,
		keyName:        pkg.Name,
		keyDomainName:  pkg.DomainName,
		keyDomainOwner: pkg.DomainOwner,
		keyRepository:  pkg.Repository,
	}
	if pkg.Namespace != "" {
		m["namespace"] = pkg.Namespace
	}
	if pkg.OriginConfigPublish != "" || pkg.OriginConfigUpstream != "" {
		m["originConfiguration"] = originConfigurationToMap(pkg.OriginConfigPublish, pkg.OriginConfigUpstream)
	}

	return m
}

// originConfigurationToMap builds the wire shape of PackageOriginConfiguration --
// verified against aws-sdk-go-v2 deserializers.go's
// awsRestjson1_deserializeDocumentPackageOriginConfiguration /
// ...PackageOriginRestrictions.

func originConfigurationToMap(publish, upstream string) map[string]any {
	return map[string]any{
		"restrictions": map[string]any{
			"publish":  publish,
			"upstream": upstream,
		},
	}
}

func (h *Handler) handleDescribePackage(c *echo.Context, domainName, repoName, format, namespace, name string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}

	pkg, err := h.Backend.DescribePackage(c.Request().Context(), domainName, repoName, format, namespace, name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyPackageKey: packageToMap(pkg),
	})
}

func (h *Handler) handleDeletePackage(c *echo.Context, domainName, repoName, format, namespace, name string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}

	pkg, err := h.Backend.DeletePackage(c.Request().Context(), domainName, repoName, format, namespace, name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"deletedPackage": packageToMap(pkg),
	})
}

func (h *Handler) handleListPackages(c *echo.Context, domainName, repoName, format, namespace string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}

	q := c.Request().URL.Query()
	maxResults := parseMaxResults(q.Get("max-results"))
	nextToken := q.Get("next-token")

	all, err := h.Backend.ListPackages(c.Request().Context(), domainName, repoName, format, namespace)
	if err != nil {
		return h.handleError(c, err)
	}

	page, next := paginateSlice(all, maxResults, nextToken, func(p *Package) string { return p.Name })

	items := make([]map[string]any, 0, len(page))
	for _, pkg := range page {
		items = append(items, packageToMap(pkg))
	}

	resp := map[string]any{"packages": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

type putPackageOriginConfigurationBody struct {
	Restrictions struct {
		Publish  string `json:"publish"`
		Upstream string `json:"upstream"`
	} `json:"restrictions"`
}

func (h *Handler) handlePutPackageOriginConfiguration(
	c *echo.Context, domainName, repoName, format, namespace, name string, body []byte,
) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}

	var in putPackageOriginConfigurationBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	pkg, err := h.Backend.PutPackageOriginConfiguration(
		c.Request().Context(),
		domainName,
		repoName,
		format,
		namespace,
		name,
		in.Restrictions.Publish,
		in.Restrictions.Upstream,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	// PutPackageOriginConfigurationOutput is a FLAT {"originConfiguration": ...} object,
	// not wrapped in "package" -- verified against aws-sdk-go-v2 deserializers.go's
	// awsRestjson1_deserializeOpDocumentPutPackageOriginConfigurationOutput.
	return c.JSON(http.StatusOK, map[string]any{
		"originConfiguration": originConfigurationToMap(pkg.OriginConfigPublish, pkg.OriginConfigUpstream),
	})
}
