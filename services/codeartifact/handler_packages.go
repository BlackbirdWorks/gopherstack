package codeartifact

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// packageToMap builds the types.PackageDescription shape used by
// DescribePackage -- confirmed against aws-sdk-go-v2/service/codeartifact@
// v1.41.4's deserializers.go (awsRestjson1_deserializeDocumentPackageDescription)
// and types/types.go, which together declare exactly format/name/namespace/
// originConfiguration. domainName, domainOwner and repository are not
// members of PackageDescription at all -- those three were leaking onto the
// wire with no real field to correspond to.
func packageToMap(pkg *Package) map[string]any {
	m := map[string]any{
		keyFormat: pkg.Format,
		keyName:   pkg.Name,
	}
	if pkg.Namespace != "" {
		m["namespace"] = pkg.Namespace
	}
	if pkg.OriginConfigPublish != "" || pkg.OriginConfigUpstream != "" {
		m["originConfiguration"] = originConfigurationToMap(pkg.OriginConfigPublish, pkg.OriginConfigUpstream)
	}

	return m
}

// packageSummaryToMap builds the types.PackageSummary shape (types.go:404)
// -- no domainName, domainOwner, or repository, all of which are Get-only
// (types.PackageDescription, not types.PackageSummary). Also fixes an
// inverse bug: the real deserializer recognises the package identifier
// only under key "package" (deserializers.go:10044), not "name" -- the
// unscoped converter's "name" key was silently dropped by every real
// client, so ListPackages leaked Get-only fields AND lost the identifier
// at the same time.
func packageSummaryToMap(pkg *Package) map[string]any {
	m := map[string]any{
		keyFormat:     pkg.Format,
		keyPackageKey: pkg.Name,
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

	// DeletePackageOutput.DeletedPackage is types.PackageSummary, NOT
	// types.PackageDescription (confirmed against aws-sdk-go-v2
	// api_op_DeletePackage.go) -- the same Get-vs-List split
	// packageSummaryToMap's own doc comment covers, not the full packageToMap
	// shape. Using packageToMap here dropped the required "package" key
	// (PackageSummary has no "name" member) and leaked domainName/
	// domainOwner/repository, none of which DeletePackageOutput declares.
	return c.JSON(http.StatusOK, map[string]any{
		"deletedPackage": packageSummaryToMap(pkg),
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
	// package-prefix/publish/upstream are real ListPackagesInput query-bound
	// members (serializers.go's SetQuery("package-prefix")/SetQuery("publish")/
	// SetQuery("upstream")) that were silently discarded -- every call
	// returned every package in the repository regardless of what was
	// requested.
	packagePrefix := q.Get("package-prefix")
	publish := q.Get("publish")
	upstream := q.Get("upstream")

	all, err := h.Backend.ListPackages(
		c.Request().Context(), domainName, repoName, format, namespace, packagePrefix, publish, upstream,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	page, next := paginateSlice(all, maxResults, nextToken, func(p *Package) string { return p.Name })

	items := make([]map[string]any, 0, len(page))
	for _, pkg := range page {
		items = append(items, packageSummaryToMap(pkg))
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
