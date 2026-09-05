package quicksight

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response/request keys used only by Knowledge Base operations.
const (
	keyKnowledgeBaseID        = "KnowledgeBaseId"
	keyKnowledgeBaseArn       = "KnowledgeBaseArn"
	keyKnowledgeBase          = "KnowledgeBase"
	keyKnowledgeBaseSummaries = "KnowledgeBaseSummaries"
	keyDataSourceArn          = "DataSourceArn"
	keyKnowledgeBaseConfig    = "KnowledgeBaseConfiguration"
	keyAccessControlConfig    = "AccessControlConfiguration"
	keyMediaExtractionConfig  = "MediaExtractionConfiguration"
	keyPrimaryOwnerArn        = "PrimaryOwnerArn"
	keyDocumentCount          = "DocumentCount"
	keyKnowledgeBaseSizeBytes = "KnowledgeBaseSizeBytes"
	keyIsEmailNotifOptedIn    = "IsEmailNotificationOptedForIngestionFailures"
	keyKnowledgeBaseIDs       = "KnowledgeBaseIds"
	keyDeleted                = "Deleted"
	keyErrors                 = "Errors"
)

func isKnowledgeBaseOp(op string) bool {
	switch op {
	case opCreateKnowledgeBase, opDescribeKnowledgeBase, opUpdateKnowledgeBase, opDeleteKnowledgeBase,
		opBatchDeleteKnowledgeBase, opListKnowledgeBases, opSearchKnowledgeBases,
		opDescribeKnowledgeBasePerms, opUpdateKnowledgeBasePerms:
		return true
	}

	return false
}

func (h *Handler) dispatchKnowledgeBase(c *echo.Context, op string) error {
	switch op {
	case opCreateKnowledgeBase:
		return h.handleCreateKnowledgeBase(c)
	case opDescribeKnowledgeBase:
		return h.handleDescribeKnowledgeBase(c)
	case opUpdateKnowledgeBase:
		return h.handleUpdateKnowledgeBase(c)
	case opDeleteKnowledgeBase:
		return h.handleDeleteKnowledgeBase(c)
	case opBatchDeleteKnowledgeBase:
		return h.handleBatchDeleteKnowledgeBase(c)
	case opListKnowledgeBases:
		return h.handleListKnowledgeBases(c)
	case opSearchKnowledgeBases:
		return h.handleSearchKnowledgeBases(c)
	case opDescribeKnowledgeBasePerms:
		return h.handleDescribeKnowledgeBasePerms(c)
	case opUpdateKnowledgeBasePerms:
		return h.handleUpdateKnowledgeBasePerms(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

func knowledgeBaseToMap(k *KnowledgeBase) map[string]any {
	m := map[string]any{
		keyKnowledgeBaseID:     k.KnowledgeBaseID,
		keyKnowledgeBaseArn:    k.Arn,
		keyDataSourceArn:       k.DataSourceArn,
		keyName:                k.Name,
		keyStatus:              k.Status,
		keyCreatedAt:           k.CreatedAt.Unix(),
		keyUpdatedAt:           k.UpdatedAt.Unix(),
		keyDocumentCount:       k.DocumentCount,
		keyIsEmailNotifOptedIn: k.EmailNotificationOptedIn,
	}
	if k.Description != "" {
		m[keyDescription] = k.Description
	}
	if k.PrimaryOwnerArn != "" {
		m[keyPrimaryOwnerArn] = k.PrimaryOwnerArn
	}
	if k.Configuration != nil {
		m[keyKnowledgeBaseConfig] = k.Configuration
	}
	if k.AccessControlConfiguration != nil {
		m[keyAccessControlConfig] = k.AccessControlConfiguration
	}
	if k.MediaExtractionConfiguration != nil {
		m[keyMediaExtractionConfig] = k.MediaExtractionConfiguration
	}
	if k.SizeBytes != 0 {
		m[keyKnowledgeBaseSizeBytes] = k.SizeBytes
	}

	return m
}

func knowledgeBaseSummaryToMap(k *KnowledgeBase) map[string]any {
	m := map[string]any{
		keyKnowledgeBaseID:  k.KnowledgeBaseID,
		keyKnowledgeBaseArn: k.Arn,
		keyDataSourceArn:    k.DataSourceArn,
		keyName:             k.Name,
		keyStatus:           k.Status,
		keyCreatedAt:        k.CreatedAt.Unix(),
		keyUpdatedAt:        k.UpdatedAt.Unix(),
		keyDocumentCount:    k.DocumentCount,
	}
	if k.PrimaryOwnerArn != "" {
		m[keyPrimaryOwnerArn] = k.PrimaryOwnerArn
	}
	if k.SizeBytes != 0 {
		m[keyKnowledgeBaseSizeBytes] = k.SizeBytes
	}

	return m
}

func (h *Handler) handleCreateKnowledgeBase(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	k, err := h.Backend.CreateKnowledgeBase(
		accountID,
		strField(body, keyKnowledgeBaseID),
		strField(body, keyName),
		strField(body, keyDescription),
		strField(body, keyDataSourceArn),
		strField(body, keyPrimaryOwnerArn),
		mapField(body, keyKnowledgeBaseConfig),
		mapField(body, keyAccessControlConfig),
		mapField(body, keyMediaExtractionConfig),
		permissionsField(body, keyPermissions),
		tagsFromBody(body),
	)
	if err != nil {
		if errors.Is(err, ErrKnowledgeBaseAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyKnowledgeBaseID:  k.KnowledgeBaseID,
		keyKnowledgeBaseArn: k.Arn,
		keyCreationStatus:   k.Status,
		keyRequestID:        reqIDPlaceholder,
		keyStatus:           http.StatusOK,
	})
}

func (h *Handler) handleDescribeKnowledgeBase(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	id := seg(segs, segResID)

	k, err := h.Backend.DescribeKnowledgeBase(accountID, id)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyKnowledgeBase: knowledgeBaseToMap(k),
		keyRequestID:     reqIDPlaceholder,
		keyStatus:        http.StatusOK,
	})
}

func (h *Handler) handleUpdateKnowledgeBase(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	id := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	var emailOptIn *bool
	if v, present := body[keyIsEmailNotifOptedIn]; present {
		b, _ := v.(bool)
		emailOptIn = &b
	}

	k, err := h.Backend.UpdateKnowledgeBase(
		accountID,
		id,
		strField(body, keyName),
		strField(body, keyDescription),
		emailOptIn,
		mapField(body, keyKnowledgeBaseConfig),
		mapField(body, keyAccessControlConfig),
		mapField(body, keyMediaExtractionConfig),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyKnowledgeBaseID:  k.KnowledgeBaseID,
		keyKnowledgeBaseArn: k.Arn,
		keyRequestID:        reqIDPlaceholder,
		keyStatus:           http.StatusOK,
	})
}

func (h *Handler) handleDeleteKnowledgeBase(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	id := seg(segs, segResID)

	k, err := h.Backend.DeleteKnowledgeBase(accountID, id)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyKnowledgeBaseID:  k.KnowledgeBaseID,
		keyKnowledgeBaseArn: k.Arn,
		keyRequestID:        reqIDPlaceholder,
		keyStatus:           http.StatusOK,
	})
}

func (h *Handler) handleBatchDeleteKnowledgeBase(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	deleted, failed := h.Backend.BatchDeleteKnowledgeBase(accountID, strSliceField(body, keyKnowledgeBaseIDs))

	deletedList := make([]map[string]any, 0, len(deleted))
	for _, d := range deleted {
		deletedList = append(deletedList, map[string]any{
			keyKnowledgeBaseID:  d.KnowledgeBaseID,
			keyKnowledgeBaseArn: d.Arn,
		})
	}

	errList := make([]map[string]any, 0, len(failed))
	for _, f := range failed {
		errList = append(errList, map[string]any{
			keyKnowledgeBaseID:   f.KnowledgeBaseID,
			keyErrorCode:         f.ErrorCode,
			keyErrorMessageField: f.ErrorMessage,
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyDeleted:   deletedList,
		keyErrors:    errList,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListKnowledgeBases(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	items, next, err := h.Backend.ListKnowledgeBases(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	summaries := make([]map[string]any, 0, len(items))
	for _, k := range items {
		summaries = append(summaries, knowledgeBaseSummaryToMap(k))
	}

	resp := map[string]any{
		keyKnowledgeBaseSummaries: summaries,
		keyRequestID:              reqIDPlaceholder,
		keyStatus:                 http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleSearchKnowledgeBases(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	items, next, err := h.Backend.SearchKnowledgeBases(
		accountID, lowercaseFiltersFromBody(body), maxResultsParam(c), nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	summaries := make([]map[string]any, 0, len(items))
	for _, k := range items {
		summaries = append(summaries, knowledgeBaseSummaryToMap(k))
	}

	resp := map[string]any{
		keyKnowledgeBaseSummaries: summaries,
		keyRequestID:              reqIDPlaceholder,
		keyStatus:                 http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleDescribeKnowledgeBasePerms(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	id := seg(segs, segResID)

	k, perms, err := h.Backend.DescribeKnowledgeBasePermissions(accountID, id)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyKnowledgeBaseID:  k.KnowledgeBaseID,
		keyKnowledgeBaseArn: k.Arn,
		keyPermissions:      permissionsToMaps(perms),
		keyRequestID:        reqIDPlaceholder,
		keyStatus:           http.StatusOK,
	})
}

func (h *Handler) handleUpdateKnowledgeBasePerms(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	id := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	k, perms, err := h.Backend.UpdateKnowledgeBasePermissions(
		accountID,
		id,
		permissionsField(body, "GrantPermissions"),
		permissionsField(body, "RevokePermissions"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyKnowledgeBaseID:  k.KnowledgeBaseID,
		keyKnowledgeBaseArn: k.Arn,
		keyPermissions:      permissionsToMaps(perms),
		keyRequestID:        reqIDPlaceholder,
		keyStatus:           http.StatusOK,
	})
}

// classifyKnowledgeBasePaths routes /v1/accounts/{id}/knowledge-bases/...
// paths (already stripped of their "v1" prefix by classifyRequest). Per the
// real API, UpdateKnowledgeBase and UpdateKnowledgeBasePermissions are POST
// (not PUT) -- confirmed against the SDK's serializers.go, unlike every
// other resource family's Update* op in this backend.
func classifyKnowledgeBasePaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method {
		case http.MethodPost:
			return opCreateKnowledgeBase, accountID
		case http.MethodGet:
			return opListKnowledgeBases, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		if id == pathSegBatchDelete && method == http.MethodPost {
			return opBatchDeleteKnowledgeBase, accountID
		}
		switch method {
		case http.MethodGet:
			return opDescribeKnowledgeBase, id
		case http.MethodPost:
			return opUpdateKnowledgeBase, id
		case http.MethodDelete:
			return opDeleteKnowledgeBase, id
		}
	case nSegsSubRes:
		if seg(segs, segSubRes) == pathSegPermissions {
			id := seg(segs, segResID)
			switch method {
			case http.MethodGet:
				return opDescribeKnowledgeBasePerms, id
			case http.MethodPost:
				return opUpdateKnowledgeBasePerms, id
			}
		}
	}

	return opUnknown, ""
}
