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
	keyFlow            = "Flow"
	keyFlowDefinition  = "FlowDefinition"
)

func isFlowOp(op string) bool {
	switch op {
	case opListFlows, opSearchFlows, opGetFlowMetadata, opGetFlowPermissions, opUpdateFlowPerms,
		opCreateFlow, opDescribeFlow, opUpdateFlow, opDeleteFlow:
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
	case opCreateFlow:
		return h.handleCreateFlow(c)
	case opDescribeFlow:
		return h.handleDescribeFlow(c)
	case opUpdateFlow:
		return h.handleUpdateFlow(c)
	case opDeleteFlow:
		return h.handleDeleteFlow(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

func (h *Handler) handleCreateFlow(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	f, err := h.Backend.CreateFlow(
		accountID,
		strField(body, keyName),
		strField(body, keyDescription),
		mapField(body, keyFlowDefinition),
		permissionsField(body, keyPermissions),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyFlowID:    f.FlowID,
		keyArn:       f.Arn,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDescribeFlow(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	flowID := seg(segs, segResID)

	f, err := h.Backend.DescribeFlow(accountID, flowID)
	if err != nil {
		return httpErr(c, err)
	}

	fm := map[string]any{
		keyFlowID:          f.FlowID,
		keyArn:             f.Arn,
		keyName:            f.Name,
		keyCreatedTime:     f.CreatedTime.Unix(),
		keyLastUpdatedTime: f.LastUpdatedTime.Unix(),
		keyPublishState:    f.PublishState,
		"StepAliases":      []map[string]any{},
	}
	if f.Description != "" {
		fm[keyDescription] = f.Description
	}
	if f.CreatedBy != "" {
		fm[keyCreatedBy] = f.CreatedBy
	}
	if f.LastUpdatedBy != "" {
		fm[keyLastUpdatedBy] = f.LastUpdatedBy
	}
	if f.FlowDefinition != nil {
		fm[keyFlowDefinition] = f.FlowDefinition
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyFlow:      fm,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUpdateFlow(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	flowID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	f, err := h.Backend.UpdateFlow(
		accountID,
		flowID,
		strField(body, keyName),
		strField(body, keyDescription),
		mapField(body, keyFlowDefinition),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyFlowID:    f.FlowID,
		keyArn:       f.Arn,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteFlow(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	flowID := seg(segs, segResID)

	if err := h.Backend.DeleteFlow(accountID, flowID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
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

// classifyFlowPaths routes /accounts/{id}/flows/... paths. Per the real
// QuickSight API, SearchFlows lives at /flows/searchFlows (POST) and
// GetFlowMetadata at /flows/{FlowId}/metadata (GET) -- distinct from the
// newer bare /flows/{FlowId} endpoint (GET/PUT/DELETE for
// Describe/Update/DeleteFlow), which coexists alongside them.
func classifyFlowPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method {
		case http.MethodGet:
			return opListFlows, accountID
		case http.MethodPost:
			return opCreateFlow, accountID
		}
	case nSegsAccountResID:
		return classifyFlowByID(method, segs, accountID)
	case nSegsSubRes:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		switch sub {
		case pathSegMetadata:
			if method == http.MethodGet {
				return opGetFlowMetadata, id
			}
		case pathSegPermissions:
			switch method {
			case http.MethodGet:
				return opGetFlowPermissions, id
			case http.MethodPut:
				return opUpdateFlowPerms, id
			}
		}
	}

	return opUnknown, ""
}

// classifyFlowByID classifies a 4-segment /accounts/{id}/flows/{flowId}
// path -- either the bare Describe/Update/DeleteFlow endpoint, or (when the
// "ID" segment is literally "searchFlows") SearchFlows. Split out of
// classifyFlowPaths purely to keep its complexity in budget.
func classifyFlowByID(method string, segs []string, accountID string) (string, string) {
	id := seg(segs, segResID)
	if method == http.MethodPost && id == pathSegSearchFlows {
		return opSearchFlows, accountID
	}

	switch method {
	case http.MethodGet:
		return opDescribeFlow, id
	case http.MethodPut:
		return opUpdateFlow, id
	case http.MethodDelete:
		return opDeleteFlow, id
	}

	return opUnknown, ""
}
