package quicksight

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response keys used only by Flow operations.
const (
	keyFlowID          = "FlowId"
	keyFlowSummaryList = "FlowSummaryList"
	keyPublishState    = "PublishState"
	keyRunCount        = "RunCount"
	keyUserCount       = "UserCount"
	keyCreatedBy       = "CreatedBy"
	keyLastPublishedAt = "LastPublishedAt"
	keyLastPublishedBy = "LastPublishedBy"
	keyLastUpdatedBy   = "LastUpdatedBy"
)

func isFlowOp(op string) bool {
	switch op {
	case opListFlows, opSearchFlows, opGetFlowMetadata, opGetFlowPermissions, opUpdateFlowPerms:
		return true
	}

	return false
}

func (h *Handler) dispatchFlow(c *echo.Context, op string) error {
	switch op {
	case opListFlows:
		return h.handleListFlows(c)
	case opSearchFlows:
		return h.handleSearchFlows(c)
	case opGetFlowMetadata:
		return h.handleGetFlowMetadata(c)
	case opGetFlowPermissions:
		return h.handleGetFlowPermissions(c)
	case opUpdateFlowPerms:
		return h.handleUpdateFlowPermissions(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

func flowSummaryToMap(f *Flow) map[string]any {
	m := map[string]any{
		keyFlowID:          f.FlowID,
		keyArn:             f.Arn,
		keyName:            f.Name,
		keyCreatedTime:     f.CreatedTime.Unix(),
		keyLastUpdatedTime: f.LastUpdatedTime.Unix(),
		keyPublishState:    f.PublishState,
		keyRunCount:        f.RunCount,
		keyUserCount:       f.UserCount,
	}
	if f.Description != "" {
		m[keyDescription] = f.Description
	}
	if f.CreatedBy != "" {
		m[keyCreatedBy] = f.CreatedBy
	}
	if f.LastUpdatedBy != "" {
		m[keyLastUpdatedBy] = f.LastUpdatedBy
	}
	if !f.LastPublishedAt.IsZero() {
		m[keyLastPublishedAt] = f.LastPublishedAt.Unix()
	}
	if f.LastPublishedBy != "" {
		m[keyLastPublishedBy] = f.LastPublishedBy
	}

	return m
}

func (h *Handler) handleListFlows(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	flows, next, err := h.Backend.ListFlows(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(flows))
	for _, f := range flows {
		items = append(items, flowSummaryToMap(f))
	}

	resp := map[string]any{
		keyFlowSummaryList: items,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleSearchFlows(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	flows, next, err := h.Backend.SearchFlows(
		accountID, folderFiltersFromBody(body), maxResultsParam(c), nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(flows))
	for _, f := range flows {
		items = append(items, flowSummaryToMap(f))
	}

	resp := map[string]any{
		keyFlowSummaryList: items,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleGetFlowMetadata(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	flowID := seg(segs, segResID)

	f, err := h.Backend.GetFlowMetadata(accountID, flowID)
	if err != nil {
		return httpErr(c, err)
	}

	resp := flowSummaryToMap(f)
	resp[keyFlowID] = f.FlowID
	resp[keyRequestID] = reqIDPlaceholder
	resp[keyStatus] = http.StatusOK

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleGetFlowPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	flowID := seg(segs, segResID)

	f, perms, err := h.Backend.GetFlowPermissions(accountID, flowID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyFlowID:      f.FlowID,
		keyArn:         f.Arn,
		keyPermissions: permissionsToMaps(perms),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	})
}

func (h *Handler) handleUpdateFlowPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	flowID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	f, perms, err := h.Backend.UpdateFlowPermissions(
		accountID,
		flowID,
		permissionsField(body, "GrantPermissions"),
		permissionsField(body, "RevokePermissions"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyFlowID:      f.FlowID,
		keyArn:         f.Arn,
		keyPermissions: permissionsToMaps(perms),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	})
}
