package quicksight

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"
)

func isDashboardOp(op string) bool {
	switch op {
	case opCreateDashboard, opDescribeDashboard, opUpdateDashboard, opDeleteDashboard,
		opListDashboards, opListDashboardVersions,
		opDescribeDashboardDefinition, opDescribeDashboardPerms, opUpdateDashboardPerms,
		opUpdateDashboardPublishedVersion, opUpdateDashboardLinks:
		return true
	}

	return false
}

func (h *Handler) dispatchDashboard(c *echo.Context, op string) error {
	switch op {
	case opCreateDashboard:
		return h.handleCreateDashboard(c)
	case opDescribeDashboard:
		return h.handleDescribeDashboard(c)
	case opUpdateDashboard:
		return h.handleUpdateDashboard(c)
	case opDeleteDashboard:
		return h.handleDeleteDashboard(c)
	case opListDashboards:
		return h.handleListDashboards(c)
	case opListDashboardVersions:
		return h.handleListDashboardVersions(c)
	case opDescribeDashboardDefinition:
		return h.handleDescribeDashboardDefinition(c)
	case opDescribeDashboardPerms:
		return h.handleDescribeDashboardPermissions(c)
	case opUpdateDashboardPerms:
		return h.handleUpdateDashboardPermissions(c)
	case opUpdateDashboardPublishedVersion:
		return h.handleUpdateDashboardPublishedVersion(c)
	case opUpdateDashboardLinks:
		return h.handleUpdateDashboardLinks(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		fmt.Sprintf("operation %q not implemented", op),
	)
}

// ---- Dashboard handlers ----

func (h *Handler) handleCreateDashboard(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	name := strField(body, "Name")
	if name == "" {
		name = dashboardID
	}

	d, err := h.Backend.CreateDashboard(
		accountID,
		dashboardID,
		name,
		mapField(body, keyDefinition),
		permissionsField(body, keyPermissions),
		tagsFromBody(body),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:            d.Arn,
		keyCreationStatus: d.Status,
		keyDashboardID:    d.DashboardID,
		keyRequestID:      newReqID(),
		keyStatus:         http.StatusOK,
		"VersionArn":      fmt.Sprintf("%s/version/1", d.Arn),
	})
}

func (h *Handler) handleDescribeDashboard(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)

	d, err := h.Backend.DescribeDashboard(accountID, dashboardID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyDashboard: dashboardToMap(d),
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUpdateDashboard(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	d, err := h.Backend.UpdateDashboard(accountID, dashboardID, strField(body, "Name"), mapField(body, keyDefinition))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:            d.Arn,
		keyCreationStatus: d.Status,
		keyDashboardID:    d.DashboardID,
		keyRequestID:      newReqID(),
		keyStatus:         http.StatusOK,
		"VersionArn":      fmt.Sprintf("%s/version/%d", d.Arn, d.VersionNumber),
	})
}

func (h *Handler) handleDeleteDashboard(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)

	if err := h.Backend.DeleteDashboard(accountID, dashboardID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListDashboards(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	dashboards, next, err := h.Backend.ListDashboards(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(dashboards))
	for _, d := range dashboards {
		items = append(items, dashboardToMap(d))
	}

	resp := map[string]any{
		keyDashboardSummaryList: items,
		keyRequestID:            newReqID(),
		keyStatus:               http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleListDashboardVersions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)

	versions, next, err := h.Backend.ListDashboardVersions(
		accountID,
		dashboardID,
		maxResultsParam(c),
		nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		items = append(items, map[string]any{
			keyArn:          v.Arn,
			keyCreatedTime:  v.CreatedTime.Unix(),
			keyStatus:       v.Status,
			"VersionNumber": v.VersionNumber,
		})
	}

	resp := map[string]any{
		"DashboardVersionSummaryList": items,
		keyRequestID:                  newReqID(),
		keyStatus:                     http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func dashboardToMap(d *Dashboard) map[string]any {
	return map[string]any{
		keyArn:                   d.Arn,
		keyCreatedTime:           d.CreatedTime.Unix(),
		keyDashboardID:           d.DashboardID,
		keyLastUpdatedTime:       d.LastUpdatedTime.Unix(),
		keyName:                  d.Name,
		"PublishedVersionNumber": d.PublishedVersionNumber,
	}
}

func (h *Handler) handleDescribeDashboardDefinition(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)

	d, err := h.Backend.DescribeDashboard(accountID, dashboardID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyName:           d.Name,
		keyDashboardID:    d.DashboardID,
		keyResourceStatus: d.Status,
		keyDefinition:     d.Definition,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
	})
}

func (h *Handler) handleDescribeDashboardPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)

	d, perms, err := h.Backend.DescribeDashboardPermissions(accountID, dashboardID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyDashboardID:  dashboardID,
		keyDashboardArn: d.Arn,
		keyPermissions:  permissionsToMaps(perms),
		keyRequestID:    reqIDPlaceholder,
		keyStatus:       http.StatusOK,
	})
}

func (h *Handler) handleUpdateDashboardPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	d, perms, err := h.Backend.UpdateDashboardPermissions(
		accountID,
		dashboardID,
		permissionsField(body, "GrantPermissions"),
		permissionsField(body, "RevokePermissions"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyDashboardID:  dashboardID,
		keyDashboardArn: d.Arn,
		keyPermissions:  permissionsToMaps(perms),
		keyRequestID:    reqIDPlaceholder,
		keyStatus:       http.StatusOK,
	})
}

// handleUpdateDashboardPublishedVersion flips which stored version of a
// dashboard is published. The version number is a path segment (.../versions/
// {VersionNumber}), not a body field.
func (h *Handler) handleUpdateDashboardPublishedVersion(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)
	versionNumber, _ := strconv.ParseInt(seg(segs, segSubResID), 10, 64)

	d, err := h.Backend.UpdateDashboardPublishedVersion(accountID, dashboardID, versionNumber)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyDashboardArn: d.Arn,
		keyDashboardID:  d.DashboardID,
		keyRequestID:    reqIDPlaceholder,
		keyStatus:       http.StatusOK,
	})
}

func (h *Handler) handleUpdateDashboardLinks(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	d, err := h.Backend.UpdateDashboardLinks(accountID, dashboardID, stringsFromBody(body, "LinkEntities"))
	if err != nil {
		return httpErr(c, err)
	}

	linkEntities := d.LinkEntities
	if linkEntities == nil {
		linkEntities = []string{}
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyDashboardArn: d.Arn,
		"LinkEntities":  linkEntities,
		keyRequestID:    reqIDPlaceholder,
		keyStatus:       http.StatusOK,
	})
}

func (h *Handler) handleSearchDashboards(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	dashboards, next, err := h.Backend.SearchDashboards(
		accountID, folderFiltersFromBody(body), maxResultsParam(c), nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(dashboards))
	for _, d := range dashboards {
		items = append(items, dashboardToMap(d))
	}

	resp := map[string]any{
		keyDashboardSummaryList: items,
		keyRequestID:            reqIDPlaceholder,
		keyStatus:               http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}
