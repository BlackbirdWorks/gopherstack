package quicksight

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

func isDataSetOp(op string) bool {
	switch op {
	case opCreateDataSet, opDescribeDataSet, opUpdateDataSet, opDeleteDataSet, opListDataSets,
		opCreateIngestion, opDescribeIngestion, opCancelIngestion, opListIngestions,
		opDescribeDataSetPerms, opUpdateDataSetPerms:
		return true
	}

	return false
}

func (h *Handler) dispatchDataSet(c *echo.Context, op string) error {
	switch op {
	case opCreateDataSet:
		return h.handleCreateDataSet(c)
	case opDescribeDataSet:
		return h.handleDescribeDataSet(c)
	case opUpdateDataSet:
		return h.handleUpdateDataSet(c)
	case opDeleteDataSet:
		return h.handleDeleteDataSet(c)
	case opListDataSets:
		return h.handleListDataSets(c)
	case opCreateIngestion:
		return h.handleCreateIngestion(c)
	case opDescribeIngestion:
		return h.handleDescribeIngestion(c)
	case opCancelIngestion:
		return h.handleCancelIngestion(c)
	case opListIngestions:
		return h.handleListIngestions(c)
	case opDescribeDataSetPerms:
		return h.handleDescribeDataSetPermissions(c)
	case opUpdateDataSetPerms:
		return h.handleUpdateDataSetPermissions(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		fmt.Sprintf("operation %q not implemented", op),
	)
}

// ---- DataSet handlers ----

func (h *Handler) handleCreateDataSet(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	physicalTableMap, err := physicalTableMapFromBody(body)
	if err != nil {
		return httpErr(c, err)
	}
	logicalTableMap, err := logicalTableMapFromBody(body)
	if err != nil {
		return httpErr(c, err)
	}

	ds, ingestion, err := h.Backend.CreateDataSet(
		accountID,
		strField(body, "DataSetId"),
		strField(body, "Name"),
		strField(body, "ImportMode"),
		permissionsField(body, keyPermissions),
		tagsFromBody(body),
		physicalTableMap,
		logicalTableMap,
	)
	if err != nil {
		return httpErr(c, err)
	}

	resp := map[string]any{
		keyArn:       ds.Arn,
		keyDataSetID: ds.DataSetID,
		keyRequestID: newReqID(),
		keyStatus:    http.StatusCreated,
	}
	// AWS only triggers (and reports) an ingestion when the dataset's import
	// mode is SPICE; DIRECT_QUERY datasets omit these fields.
	if ingestion != nil {
		resp["IngestionArn"] = ingestion.Arn
		resp["IngestionId"] = ingestion.IngestionID
	}

	return writeJSON(c, http.StatusCreated, resp)
}

func (h *Handler) handleDescribeDataSet(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSetID := seg(segs, segResID)

	ds, err := h.Backend.DescribeDataSet(accountID, dataSetID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyDataSet:   dataSetDetailToMap(ds),
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUpdateDataSet(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSetID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	physicalTableMap, err := physicalTableMapFromBody(body)
	if err != nil {
		return httpErr(c, err)
	}
	logicalTableMap, err := logicalTableMapFromBody(body)
	if err != nil {
		return httpErr(c, err)
	}

	ds, ingestion, err := h.Backend.UpdateDataSet(
		accountID, dataSetID, strField(body, "Name"), strField(body, "ImportMode"),
		physicalTableMap, logicalTableMap,
	)
	if err != nil {
		return httpErr(c, err)
	}

	resp := map[string]any{
		keyArn:       ds.Arn,
		keyDataSetID: ds.DataSetID,
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	}
	// As with CreateDataSet, AWS only triggers (and reports) an ingestion
	// when the dataset's resulting import mode is SPICE.
	if ingestion != nil {
		resp["IngestionArn"] = ingestion.Arn
		resp["IngestionId"] = ingestion.IngestionID
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleDeleteDataSet(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSetID := seg(segs, segResID)

	if err := h.Backend.DeleteDataSet(accountID, dataSetID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListDataSets(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	datasets, next, err := h.Backend.ListDataSets(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(datasets))
	for _, ds := range datasets {
		items = append(items, dataSetToMap(ds))
	}

	resp := map[string]any{
		keyDataSetSummaries: items,
		keyRequestID:        newReqID(),
		keyStatus:           http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

// dataSetToMap builds the DataSetSummary shape used by
// ListDataSets/SearchDataSets (quicksight@v1.123.1 types.DataSetSummary),
// which -- unlike the full DataSet type -- carries no PhysicalTableMap/
// LogicalTableMap.
func dataSetToMap(ds *DataSet) map[string]any {
	return map[string]any{
		keyArn:             ds.Arn,
		keyCreatedTime:     ds.CreatedTime.Unix(),
		keyDataSetID:       ds.DataSetID,
		"ImportMode":       ds.ImportMode,
		keyLastUpdatedTime: ds.LastUpdatedTime.Unix(),
		keyName:            ds.Name,
	}
}

// dataSetDetailToMap builds the full DataSet shape returned by
// DescribeDataSet (quicksight@v1.123.1 types.DataSet), which includes
// PhysicalTableMap and (when set) LogicalTableMap on top of the summary
// fields.
func dataSetDetailToMap(ds *DataSet) map[string]any {
	m := dataSetToMap(ds)
	m["PhysicalTableMap"] = physicalTableMapToWire(ds.PhysicalTableMap)
	if lt := logicalTableMapToWire(ds.LogicalTableMap); lt != nil {
		m["LogicalTableMap"] = lt
	}

	return m
}

func (h *Handler) handleDescribeDataSetPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSetID := seg(segs, segResID)

	ds, perms, err := h.Backend.DescribeDataSetPermissions(accountID, dataSetID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyDataSetID:   dataSetID,
		"DataSetArn":   ds.Arn,
		keyPermissions: permissionsToMaps(perms),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	})
}

func (h *Handler) handleUpdateDataSetPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSetID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	ds, _, err := h.Backend.UpdateDataSetPermissions(
		accountID,
		dataSetID,
		permissionsField(body, "GrantPermissions"),
		permissionsField(body, "RevokePermissions"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyDataSetID: dataSetID,
		"DataSetArn": ds.Arn,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

// ---- Ingestion handlers ----

func (h *Handler) handleCreateIngestion(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSetID := seg(segs, segResID)
	ingestionID := seg(segs, segSubResID)

	ing, err := h.Backend.CreateIngestion(accountID, dataSetID, ingestionID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusCreated, map[string]any{
		keyArn:             ing.Arn,
		keyIngestionID:     ing.IngestionID,
		keyIngestionStatus: ing.IngestionStatus,
		keyRequestID:       newReqID(),
		keyStatus:          http.StatusCreated,
	})
}

func (h *Handler) handleDescribeIngestion(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSetID := seg(segs, segResID)
	ingestionID := seg(segs, segSubResID)

	ing, err := h.Backend.DescribeIngestion(accountID, dataSetID, ingestionID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyIngestion: map[string]any{
			keyArn:             ing.Arn,
			keyCreatedTime:     ing.CreatedTime.Unix(),
			keyIngestionID:     ing.IngestionID,
			keyIngestionStatus: ing.IngestionStatus,
		},
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleCancelIngestion(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSetID := seg(segs, segResID)
	ingestionID := seg(segs, segSubResID)

	if err := h.Backend.CancelIngestion(accountID, dataSetID, ingestionID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListIngestions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dataSetID := seg(segs, segResID)

	ingestions, next, err := h.Backend.ListIngestions(accountID, dataSetID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(ingestions))
	for _, ing := range ingestions {
		items = append(items, map[string]any{
			keyArn:             ing.Arn,
			keyCreatedTime:     ing.CreatedTime.Unix(),
			keyIngestionID:     ing.IngestionID,
			keyIngestionStatus: ing.IngestionStatus,
		})
	}

	resp := map[string]any{
		keyIngestions: items,
		keyRequestID:  newReqID(),
		keyStatus:     http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleSearchDataSets(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	dataSets, next, err := h.Backend.SearchDataSets(
		accountID, folderFiltersFromBody(body), maxResultsParam(c), nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(dataSets))
	for _, ds := range dataSets {
		items = append(items, dataSetToMap(ds))
	}

	resp := map[string]any{
		keyDataSetSummaries: items,
		keyRequestID:        reqIDPlaceholder,
		keyStatus:           http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}
