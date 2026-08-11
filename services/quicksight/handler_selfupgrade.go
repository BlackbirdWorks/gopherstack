package quicksight

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// isSelfUpgradeOp reports whether op is one of the namespace self-upgrade
// operations (DescribeSelfUpgradeConfiguration, UpdateSelfUpgradeConfiguration,
// ListSelfUpgrades, UpdateSelfUpgrade).
func isSelfUpgradeOp(op string) bool {
	switch op {
	case opDescribeSelfUpgradeConfig, opUpdateSelfUpgradeConfig, opListSelfUpgrades, opUpdateSelfUpgrade:
		return true
	}

	return false
}

func (h *Handler) dispatchSelfUpgrade(c *echo.Context, op string) error {
	switch op {
	case opDescribeSelfUpgradeConfig:
		return h.handleDescribeSelfUpgradeConfiguration(c)
	case opUpdateSelfUpgradeConfig:
		return h.handleUpdateSelfUpgradeConfiguration(c)
	case opListSelfUpgrades:
		return h.handleListSelfUpgrades(c)
	case opUpdateSelfUpgrade:
		return h.handleUpdateSelfUpgrade(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

func (h *Handler) handleDescribeSelfUpgradeConfiguration(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)

	status, err := h.Backend.DescribeSelfUpgradeConfiguration(accountID, namespace)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"SelfUpgradeConfiguration": map[string]any{"SelfUpgradeStatus": status},
		keyRequestID:               reqIDPlaceholder,
		keyStatus:                  http.StatusOK,
	})
}

func (h *Handler) handleUpdateSelfUpgradeConfiguration(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	if _, err = h.Backend.UpdateSelfUpgradeConfiguration(
		accountID, namespace, strField(body, "SelfUpgradeStatus"),
	); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListSelfUpgrades(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)

	reqs, next, err := h.Backend.ListSelfUpgrades(accountID, namespace, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(reqs))
	for _, r := range reqs {
		items = append(items, selfUpgradeRequestDetailToMap(r))
	}

	resp := map[string]any{
		"SelfUpgradeRequestDetails": items,
		keyRequestID:                reqIDPlaceholder,
		keyStatus:                   http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleUpdateSelfUpgrade(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	detail, err := h.Backend.UpdateSelfUpgrade(
		accountID, namespace, strField(body, "Action"), strField(body, "UpgradeRequestId"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		"SelfUpgradeRequestDetail": selfUpgradeRequestDetailToMap(detail),
		keyRequestID:               reqIDPlaceholder,
		keyStatus:                  http.StatusOK,
	})
}

func selfUpgradeRequestDetailToMap(r *SelfUpgradeRequestDetail) map[string]any {
	m := map[string]any{
		"UpgradeRequestId": r.UpgradeRequestID,
		"OriginalRole":     r.OriginalRole,
		"RequestedRole":    r.RequestedRole,
		"RequestStatus":    r.RequestStatus,
		"CreationTime":     r.CreationTime,
	}
	if r.RequestNote != "" {
		m["RequestNote"] = r.RequestNote
	}
	if r.LastUpdateAttemptTime != 0 {
		m["LastUpdateAttemptTime"] = r.LastUpdateAttemptTime
	}
	if r.LastUpdateFailureReason != "" {
		m["LastUpdateFailureReason"] = r.LastUpdateFailureReason
	}
	if r.UserName != "" {
		m["UserName"] = r.UserName
	}

	return m
}
