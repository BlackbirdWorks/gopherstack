package quicksight

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func isAppOp(op string) bool {
	switch op {
	case opDescribeApp, opDeleteApp, opListApps, opSearchApps,
		opDescribeAppPermissions, opUpdateAppPermissions:
		return true
	}

	return false
}

func (h *Handler) dispatchApp(c *echo.Context, op string) error {
	switch op {
	case opDescribeApp:
		return h.handleDescribeApp(c)
	case opDeleteApp:
		return h.handleDeleteApp(c)
	case opListApps:
		return h.handleListApps(c)
	case opSearchApps:
		return h.handleSearchApps(c)
	case opDescribeAppPermissions:
		return h.handleDescribeAppPermissions(c)
	case opUpdateAppPermissions:
		return h.handleUpdateAppPermissions(c)
	}

	return writeError(c, http.StatusNotImplemented, "UnsupportedOperationException",
		"operation not implemented: "+op)
}

func appSummaryToMap(a *App) map[string]any {
	return map[string]any{
		keyAppID:           a.AppID,
		keyArn:             a.Arn,
		keyCreatedTime:     a.CreatedTime.Unix(),
		keyLastUpdatedTime: a.LastUpdatedTime.Unix(),
		keyName:            a.Name,
		keyVisibility:      a.Visibility,
	}
}

func (h *Handler) handleDescribeApp(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	appID := seg(segs, segResID)

	a, err := h.Backend.DescribeApp(accountID, appID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyApp:       appSummaryToMap(a),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteApp(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	appID := seg(segs, segResID)

	if err := h.Backend.DeleteApp(accountID, appID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListApps(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	apps, next, err := h.Backend.ListApps(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(apps))
	for _, a := range apps {
		items = append(items, appSummaryToMap(a))
	}

	resp := map[string]any{
		keyAppSummaryList: items,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

// handleSearchApps reads MaxResults/NextToken/Filters from the JSON body,
// not query params: unlike ListApps (GET, max-results/next-token as query
// params), SearchApps is a POST whose entire input -- Filters, MaxResults,
// NextToken alike -- is serialized as the JSON payload (confirmed against
// quicksight@v1.129.0 serializers.go's
// awsRestjson1_serializeOpDocumentSearchAppsInput; only AwsAccountId binds to
// the URI).
func (h *Handler) handleSearchApps(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	apps, next, err := h.Backend.SearchApps(
		accountID, folderFiltersFromBody(body), intField(body, "MaxResults"), strField(body, "NextToken"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(apps))
	for _, a := range apps {
		items = append(items, appSummaryToMap(a))
	}

	resp := map[string]any{
		keyAppSummaryList: items,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleDescribeAppPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	appID := seg(segs, segResID)

	a, perms, err := h.Backend.DescribeAppPermissions(accountID, appID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAppID:       a.AppID,
		keyArn:         a.Arn,
		keyPermissions: permissionsToMaps(perms),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	})
}

func (h *Handler) handleUpdateAppPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	appID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	a, perms, err := h.Backend.UpdateAppPermissions(
		accountID,
		appID,
		permissionsField(body, "GrantPermissions"),
		permissionsField(body, "RevokePermissions"),
		strField(body, keyVisibility),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAppID:       a.AppID,
		keyArn:         a.Arn,
		keyPermissions: permissionsToMaps(perms),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
		keyVisibility:  a.Visibility,
	})
}
