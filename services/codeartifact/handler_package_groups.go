package codeartifact

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

// createPackageGroupBody's Pattern field is tagged json:"packageGroup" --
// verified against aws-sdk-go-v2's generated
// awsRestjson1_serializeOpDocumentCreatePackageGroupInput, which serializes
// CreatePackageGroupInput.PackageGroup (the pattern string) under the JSON
// key "packageGroup", not "pattern". Every existing unit test constructed
// its request body by hand using "pattern" (matching this bug, not the real
// wire), so a real aws-sdk-go-v2 client's CreatePackageGroup call always
// failed with "pattern is required" against gopherstack before this fix --
// caught by test/integration/codeartifact_test.go's SDK-driven coverage.
type createPackageGroupBody struct {
	Pattern     string           `json:"packageGroup"`
	Description string           `json:"description"`
	ContactInfo string           `json:"contactInfo"`
	Tags        []map[string]any `json:"tags"`
}

// packageGroupToMap builds a PackageGroupDescription/PackageGroupSummary
// wire object (the two SDK types share an identical field set) -- verified
// against aws-sdk-go-v2 deserializers.go's
// awsRestjson1_deserializeDocumentPackageGroupDescription. info may be nil
// (e.g. when the caller couldn't resolve it), in which case
// originConfiguration/parent are omitted rather than guessed at.
func packageGroupToMap(pg *PackageGroup, info *PackageGroupOriginInfo) map[string]any {
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

	if info == nil {
		return m
	}

	if info.Parent != nil {
		m["parent"] = packageGroupReferenceToMap(info.Parent)
	}

	restrictions := make(map[string]any, len(info.Restrictions))
	for restrictionType, r := range info.Restrictions {
		restrictions[restrictionType] = effectiveRestrictionToMap(r)
	}

	m["originConfiguration"] = map[string]any{"restrictions": restrictions}

	return m
}

// packageGroupReferenceToMap builds a PackageGroupReference wire object
// ({"arn","pattern"}).
func packageGroupReferenceToMap(pg *PackageGroup) map[string]any {
	return map[string]any{keyArn: pg.ARN, "pattern": pg.Pattern}
}

// effectiveRestrictionToMap builds a PackageGroupOriginRestriction wire
// object ({"effectiveMode","inheritedFrom","mode","repositoriesCount"}).
func effectiveRestrictionToMap(r EffectiveRestriction) map[string]any {
	m := map[string]any{
		"mode":              r.Mode,
		"effectiveMode":     r.EffectiveMode,
		"repositoriesCount": r.RepositoriesCount,
	}
	if r.InheritedFrom != nil {
		m["inheritedFrom"] = packageGroupReferenceToMap(r.InheritedFrom)
	}

	return m
}

// describeOriginInfo resolves pg's PackageGroupOriginInfo, logging nothing
// and falling back to nil (basic wire shape, no originConfiguration/parent)
// on error -- pg's own pattern was already validated when it was created,
// so failure here should not occur in practice.
func (h *Handler) describeOriginInfo(
	ctx context.Context,
	domainName string,
	pg *PackageGroup,
) *PackageGroupOriginInfo {
	info, err := h.Backend.DescribeOriginInfo(ctx, domainName, pg)
	if err != nil {
		return nil
	}

	return info
}

// enrichedPackageGroupMap is the common path every package-group response
// builder uses: resolve pg's origin info and build its full wire map.
func (h *Handler) enrichedPackageGroupMap(
	ctx context.Context,
	domainName string,
	pg *PackageGroup,
) map[string]any {
	return packageGroupToMap(pg, h.describeOriginInfo(ctx, domainName, pg))
}

func (h *Handler) handleCreatePackageGroup(c *echo.Context, domainName string, body []byte) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}

	var in createPackageGroupBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
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
		keyPackageGroup: h.enrichedPackageGroupMap(c.Request().Context(), domainName, pg),
	})
}

func (h *Handler) handleDescribePackageGroup(c *echo.Context, domainName, pattern string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if pattern == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "packageGroup is required"),
		)
	}

	pg, err := h.Backend.DescribePackageGroup(c.Request().Context(), domainName, pattern)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyPackageGroup: h.enrichedPackageGroupMap(c.Request().Context(), domainName, pg),
	})
}

func (h *Handler) handleDeletePackageGroup(c *echo.Context, domainName, pattern string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if pattern == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "packageGroup is required"),
		)
	}

	pg, err := h.Backend.DeletePackageGroup(c.Request().Context(), domainName, pattern)
	if err != nil {
		return h.handleError(c, err)
	}

	// The group is already gone by the time we'd resolve its origin info
	// (parent/hierarchy lookups only make sense for groups still in the
	// domain), so DeletePackageGroupOutput's packageGroup is the basic shape.
	return c.JSON(http.StatusOK, map[string]any{
		keyPackageGroup: packageGroupToMap(pg, nil),
	})
}

func (h *Handler) handleGetAssociatedPackageGroup(
	c *echo.Context,
	domainName, format, namespace, name string,
) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}

	pg, assocType, err := h.Backend.GetAssociatedPackageGroup(
		c.Request().Context(),
		domainName,
		format,
		namespace,
		name,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	if pg == nil {
		return c.JSON(http.StatusOK, map[string]any{keyPackageGroup: nil})
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyPackageGroup:   h.enrichedPackageGroupMap(c.Request().Context(), domainName, pg),
		"associationType": assocType,
	})
}

func (h *Handler) handleListAllowedRepositoriesForGroup(
	c *echo.Context,
	domainName, pattern string,
) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if pattern == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "packageGroup is required"),
		)
	}

	q := c.Request().URL.Query()

	restrictionType := q.Get("originRestrictionType")
	if restrictionType == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "originRestrictionType is required"),
		)
	}

	maxResults := parseMaxResults(q.Get("max-results"))
	nextToken := q.Get("next-token")

	all, err := h.Backend.ListAllowedRepositoriesForGroup(
		c.Request().Context(),
		domainName,
		pattern,
		restrictionType,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	page, next := paginateSlice(
		all,
		maxResults,
		nextToken,
		func(name string) string { return name },
	)

	resp := map[string]any{"allowedRepositories": page}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListAssociatedPackages(c *echo.Context, domainName, pattern string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if pattern == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "packageGroup is required"),
		)
	}

	q := c.Request().URL.Query()
	maxResults := parseMaxResults(q.Get("max-results"))
	nextToken := q.Get("next-token")
	preview, _ := strconv.ParseBool(q.Get("preview"))

	all, err := h.Backend.ListAssociatedPackages(c.Request().Context(), domainName, pattern, preview)
	if err != nil {
		return h.handleError(c, err)
	}

	page, next := paginateSlice(all, maxResults, nextToken, func(ap AssociatedPackage) string {
		return ap.Package.Format + "/" + ap.Package.Namespace + "/" + ap.Package.Name
	})

	items := make([]map[string]any, 0, len(page))
	for _, ap := range page {
		items = append(items, associatedPackageToMap(ap))
	}

	resp := map[string]any{"packages": items}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// associatedPackageToMap builds an AssociatedPackage wire object -- verified
// against aws-sdk-go-v2 types.AssociatedPackage
// ({"associationType","format","namespace","package"}).
func associatedPackageToMap(ap AssociatedPackage) map[string]any {
	m := map[string]any{
		"associationType": ap.AssociationType,
		keyFormat:         ap.Package.Format,
		keyPackageKey:     ap.Package.Name,
	}
	if ap.Package.Namespace != "" {
		m["namespace"] = ap.Package.Namespace
	}

	return m
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

	page, next := paginateSlice(
		all,
		maxResults,
		nextToken,
		func(pg *PackageGroup) string { return pg.Pattern },
	)

	items := make([]map[string]any, 0, len(page))
	for _, pg := range page {
		items = append(items, h.enrichedPackageGroupMap(c.Request().Context(), domainName, pg))
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
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "packageGroup is required"),
		)
	}

	q := c.Request().URL.Query()
	maxResults := parseMaxResults(q.Get("max-results"))
	nextToken := q.Get("next-token")

	all, err := h.Backend.ListSubPackageGroups(c.Request().Context(), domainName, pattern)
	if err != nil {
		return h.handleError(c, err)
	}

	page, next := paginateSlice(
		all,
		maxResults,
		nextToken,
		func(pg *PackageGroup) string { return pg.Pattern },
	)

	items := make([]map[string]any, 0, len(page))
	for _, pg := range page {
		items = append(items, h.enrichedPackageGroupMap(c.Request().Context(), domainName, pg))
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

	// PackageGroup is "This member is required." on the real
	// UpdatePackageGroupInput (api_op_UpdatePackageGroup.go), same as on
	// Create/Describe/Delete's own pattern params (already validated below
	// them) -- was falling straight through to the backend and surfacing as
	// a 404 "package group not found" instead of the real 400
	// ValidationException.
	if pattern == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "packageGroup is required"))
	}

	pg, err := h.Backend.UpdatePackageGroup(
		c.Request().Context(),
		domainName,
		pattern,
		in.Description,
		in.ContactInfo,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"packageGroup": h.enrichedPackageGroupMap(c.Request().Context(), domainName, pg),
	})
}

// updatePackageGroupOriginConfigurationBody is
// UpdatePackageGroupOriginConfigurationInput's request shape -- verified
// against the API reference's request syntax
// (addAllowedRepositories/removeAllowedRepositories/restrictions).
type updatePackageGroupOriginConfigurationBody struct {
	Restrictions              map[string]string       `json:"restrictions"`
	AddAllowedRepositories    []allowedRepositoryBody `json:"addAllowedRepositories"`
	RemoveAllowedRepositories []allowedRepositoryBody `json:"removeAllowedRepositories"`
}

type allowedRepositoryBody struct {
	OriginRestrictionType string `json:"originRestrictionType"`
	RepositoryName        string `json:"repositoryName"`
}

func toAllowedRepoOps(body []allowedRepositoryBody) []AllowedRepoOp {
	ops := make([]AllowedRepoOp, 0, len(body))
	for _, b := range body {
		ops = append(ops, AllowedRepoOp(b))
	}

	return ops
}

func (h *Handler) handleUpdatePackageGroupOriginConfiguration(
	c *echo.Context, domainName, pattern string, body []byte,
) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if pattern == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "packageGroup is required"),
		)
	}

	var in updatePackageGroupOriginConfigurationBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("ValidationException", "invalid request body"),
			)
		}
	}

	pg, updates, err := h.Backend.UpdatePackageGroupOriginConfiguration(
		c.Request().Context(),
		domainName,
		pattern,
		in.Restrictions,
		toAllowedRepoOps(in.AddAllowedRepositories),
		toAllowedRepoOps(in.RemoveAllowedRepositories),
	)
	if err != nil {
		return h.handleError(c, err)
	}

	updatesAny := make(map[string]any, len(updates))
	for restrictionType, actions := range updates {
		actionsAny := make(map[string]any, len(actions))
		for action, repos := range actions {
			actionsAny[action] = repos
		}

		updatesAny[restrictionType] = actionsAny
	}

	return c.JSON(http.StatusOK, map[string]any{
		"packageGroup": h.enrichedPackageGroupMap(
			c.Request().Context(),
			domainName,
			pg,
		),
		"allowedRepositoryUpdates": updatesAny,
	})
}
