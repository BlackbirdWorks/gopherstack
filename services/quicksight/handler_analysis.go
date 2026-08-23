package quicksight

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

func isAnalysisOp(op string) bool {
	switch op {
	case opCreateAnalysis, opDescribeAnalysis, opUpdateAnalysis, opDeleteAnalysis,
		opListAnalyses, opRestoreAnalysis,
		opDescribeAnalysisDefinition, opDescribeAnalysisPerms, opUpdateAnalysisPerms:
		return true
	}

	return false
}

func (h *Handler) dispatchAnalysis(c *echo.Context, op string) error {
	switch op {
	case opCreateAnalysis:
		return h.handleCreateAnalysis(c)
	case opDescribeAnalysis:
		return h.handleDescribeAnalysis(c)
	case opUpdateAnalysis:
		return h.handleUpdateAnalysis(c)
	case opDeleteAnalysis:
		return h.handleDeleteAnalysis(c)
	case opListAnalyses:
		return h.handleListAnalyses(c)
	case opRestoreAnalysis:
		return h.handleRestoreAnalysis(c)
	case opDescribeAnalysisDefinition:
		return h.handleDescribeAnalysisDefinition(c)
	case opDescribeAnalysisPerms:
		return h.handleDescribeAnalysisPermissions(c)
	case opUpdateAnalysisPerms:
		return h.handleUpdateAnalysisPermissions(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		fmt.Sprintf("operation %q not implemented", op),
	)
}

// ---- Analysis handlers ----

func (h *Handler) handleCreateAnalysis(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	analysisID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	name := strField(body, "Name")
	if name == "" {
		name = analysisID
	}

	a, err := h.Backend.CreateAnalysis(
		accountID,
		analysisID,
		name,
		strField(body, keyThemeArn),
		mapField(body, keyDefinition),
		permissionsField(body, keyPermissions),
		tagsFromBody(body),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAnalysisID:     a.AnalysisID,
		keyArn:            a.Arn,
		keyCreationStatus: a.Status,
		keyRequestID:      newReqID(),
		keyStatus:         http.StatusOK,
	})
}

func (h *Handler) handleDescribeAnalysis(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	analysisID := seg(segs, segResID)

	a, err := h.Backend.DescribeAnalysis(accountID, analysisID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAnalysis:  analysisToMap(a),
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUpdateAnalysis(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	analysisID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	a, err := h.Backend.UpdateAnalysis(
		accountID, analysisID, strField(body, "Name"), strField(body, keyThemeArn), mapField(body, keyDefinition),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAnalysisID:   a.AnalysisID,
		keyArn:          a.Arn,
		keyRequestID:    newReqID(),
		keyStatus:       http.StatusOK,
		keyUpdateStatus: a.Status,
	})
}

func (h *Handler) handleDeleteAnalysis(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	analysisID := seg(segs, segResID)

	force := c.Request().URL.Query().Get("forceDeleteWithoutRecovery") == queryValueTrue

	if err := h.Backend.DeleteAnalysis(accountID, analysisID, force); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAnalysisID: analysisID,
		keyRequestID:  newReqID(),
		keyStatus:     http.StatusOK,
	})
}

func (h *Handler) handleListAnalyses(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	analyses, next, err := h.Backend.ListAnalyses(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(analyses))
	for _, a := range analyses {
		items = append(items, analysisSummaryToMap(a))
	}

	resp := map[string]any{
		keyAnalysisSummaryList: items,
		keyRequestID:           newReqID(),
		keyStatus:              http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleRestoreAnalysis(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	// path: /accounts/{id}/restore/analyses/{analysisId}
	analysisID := seg(segs, segSubRes)

	a, err := h.Backend.RestoreAnalysis(accountID, analysisID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAnalysisID: a.AnalysisID,
		keyArn:        a.Arn,
		keyRequestID:  newReqID(),
		keyStatus:     http.StatusOK,
	})
}

func analysisToMap(a *Analysis) map[string]any {
	m := analysisSummaryToMap(a)
	if a.ThemeArn != "" {
		m[keyThemeArn] = a.ThemeArn
	}

	return m
}

// analysisSummaryToMap builds the types.AnalysisSummary shape ListAnalyses/
// SearchAnalyses return. Confirmed against types.go: AnalysisSummary has no
// ThemeArn, DataSetArns, Errors, Sheets, or TopicArns members -- those are
// Analysis-only, populated by DescribeAnalysis.
func analysisSummaryToMap(a *Analysis) map[string]any {
	return map[string]any{
		keyAnalysisID:      a.AnalysisID,
		keyArn:             a.Arn,
		keyCreatedTime:     a.CreatedTime.Unix(),
		keyLastUpdatedTime: a.LastUpdatedTime.Unix(),
		keyName:            a.Name,
		keyStatus:          a.Status,
	}
}

func (h *Handler) handleDescribeAnalysisDefinition(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	analysisID := seg(segs, segResID)

	a, err := h.Backend.DescribeAnalysis(accountID, analysisID)
	if err != nil {
		return httpErr(c, err)
	}

	resp := map[string]any{
		keyName:           a.Name,
		keyAnalysisID:     a.AnalysisID,
		keyResourceStatus: a.Status,
		keyDefinition:     a.Definition,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
	}
	if a.ThemeArn != "" {
		resp[keyThemeArn] = a.ThemeArn
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleDescribeAnalysisPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	analysisID := seg(segs, segResID)

	a, perms, err := h.Backend.DescribeAnalysisPermissions(accountID, analysisID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAnalysisID:  analysisID,
		"AnalysisArn":  a.Arn,
		keyPermissions: permissionsToMaps(perms),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	})
}

func (h *Handler) handleUpdateAnalysisPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	analysisID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	a, perms, err := h.Backend.UpdateAnalysisPermissions(
		accountID,
		analysisID,
		permissionsField(body, "GrantPermissions"),
		permissionsField(body, "RevokePermissions"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAnalysisID:  analysisID,
		"AnalysisArn":  a.Arn,
		keyPermissions: permissionsToMaps(perms),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	})
}

func (h *Handler) handleSearchAnalyses(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	analyses, next, err := h.Backend.SearchAnalyses(
		accountID, folderFiltersFromBody(body), maxResultsParam(c), nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(analyses))
	for _, a := range analyses {
		items = append(items, analysisSummaryToMap(a))
	}

	resp := map[string]any{
		keyAnalysisSummaryList: items,
		keyRequestID:           reqIDPlaceholder,
		keyStatus:              http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}
