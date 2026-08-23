package quicksight

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

func isDataSourceOp(op string) bool {
	switch op {
	case opCreateDataSource, opDescribeDataSource, opUpdateDataSource, opDeleteDataSource, opListDataSources,
		opDescribeDataSourcePerms, opUpdateDataSourcePerms:
		return true
	}

	return false
}

func (h *Handler) dispatchDataSource(c *echo.Context, op string) error {
	switch op {
	case opCreateDataSource:
		return h.handleCreateDataSource(c)
	case opDescribeDataSource:
		return h.handleDescribeDataSource(c)
	case opUpdateDataSource:
		return h.handleUpdateDataSource(c)
	case opDeleteDataSource:
		return h.handleDeleteDataSource(c)
	case opListDataSources:
		return h.handleListDataSources(c)
	case opDescribeDataSourcePerms:
		return h.handleDescribeDataSourcePermissions(c)
	case opUpdateDataSourcePerms:
		return h.handleUpdateDataSourcePermissions(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		fmt.Sprintf("operation %q not implemented", op),
	)
}

// ---- DataSource handlers ----

func (h *Handler) handleCreateDataSource(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	ds, err := h.Backend.CreateDataSource(
		accountID,
		strField(body, "DataSourceId"),
		strField(body, "Name"),
		strField(body, "Type"),
		permissionsField(body, keyPermissions),
		tagsFromBody(body),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusCreated, map[string]any{
		keyArn:            ds.Arn,
		keyCreationStatus: ds.Status,
		keyDataSourceID:   ds.DataSourceID,
		keyRequestID:      newReqID(),
		keyStatus:         http.StatusCreated,
	})
}

func (h *Handler) handleDescribeDataSource(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSourceID := seg(segs, segResID)

	ds, err := h.Backend.DescribeDataSource(accountID, dataSourceID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyDataSource: dataSourceToMap(ds),
		keyRequestID:  newReqID(),
		keyStatus:     http.StatusOK,
	})
}

func (h *Handler) handleUpdateDataSource(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSourceID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	ds, err := h.Backend.UpdateDataSource(accountID, dataSourceID, strField(body, "Name"))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:          ds.Arn,
		keyDataSourceID: ds.DataSourceID,
		keyRequestID:    newReqID(),
		keyStatus:       http.StatusOK,
		keyUpdateStatus: ds.Status,
	})
}

func (h *Handler) handleDeleteDataSource(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSourceID := seg(segs, segResID)

	if err := h.Backend.DeleteDataSource(accountID, dataSourceID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListDataSources(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	sources, next, err := h.Backend.ListDataSources(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(sources))
	for _, ds := range sources {
		items = append(items, dataSourceSummaryToMap(ds))
	}

	resp := map[string]any{
		keyDataSources: items,
		keyRequestID:   newReqID(),
		keyStatus:      http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func dataSourceToMap(ds *DataSource) map[string]any {
	m := dataSourceSummaryToMap(ds)
	m[keyStatus] = ds.Status

	return m
}

// dataSourceSummaryToMap builds the types.DataSourceSummary shape
// ListDataSources/SearchDataSources return. Confirmed against types.go:
// DataSourceSummary has no Status member -- that's DataSource-only,
// populated by DescribeDataSource.
func dataSourceSummaryToMap(ds *DataSource) map[string]any {
	return map[string]any{
		keyArn:             ds.Arn,
		keyCreatedTime:     ds.CreatedTime.Unix(),
		keyDataSourceID:    ds.DataSourceID,
		keyLastUpdatedTime: ds.LastUpdatedTime.Unix(),
		keyName:            ds.Name,
		"Type":             ds.Type,
	}
}

func (h *Handler) handleDescribeDataSourcePermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSourceID := seg(segs, segResID)

	ds, perms, err := h.Backend.DescribeDataSourcePermissions(accountID, dataSourceID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyDataSourceID: dataSourceID,
		"DataSourceArn": ds.Arn,
		keyPermissions:  permissionsToMaps(perms),
		keyRequestID:    reqIDPlaceholder,
		keyStatus:       http.StatusOK,
	})
}

func (h *Handler) handleUpdateDataSourcePermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSourceID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	ds, _, err := h.Backend.UpdateDataSourcePermissions(
		accountID,
		dataSourceID,
		permissionsField(body, "GrantPermissions"),
		permissionsField(body, "RevokePermissions"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyDataSourceID: dataSourceID,
		"DataSourceArn": ds.Arn,
		keyRequestID:    reqIDPlaceholder,
		keyStatus:       http.StatusOK,
	})
}

func (h *Handler) handleSearchDataSources(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	dataSources, next, err := h.Backend.SearchDataSources(
		accountID, folderFiltersFromBody(body), maxResultsParam(c), nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(dataSources))
	for _, ds := range dataSources {
		items = append(items, dataSourceSummaryToMap(ds))
	}

	resp := map[string]any{
		keyDataSources: items,
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}
