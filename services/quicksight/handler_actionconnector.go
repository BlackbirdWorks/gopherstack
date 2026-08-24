package quicksight

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response/request keys used only by action connector operations.
const (
	keyActionConnectorID     = "ActionConnectorId"
	keyActionConnector       = "ActionConnector"
	keyActionConnectorSums   = "ActionConnectorSummaries"
	keyConnectorType         = "Type"
	keyVPCConnectionArnField = "VpcConnectionArn"
	keyAuthenticationConfig  = "AuthenticationConfig"
)

func isActionConnectorOp(op string) bool {
	switch op {
	case opCreateActionConnector, opDescribeActionConnector, opUpdateActionConnector,
		opDeleteActionConnector, opListActionConnectors, opSearchActionConnectors,
		opDescribeActionConnectorPerms, opUpdateActionConnectorPerms:
		return true
	}

	return false
}

func (h *Handler) dispatchActionConnector(c *echo.Context, op string) error {
	switch op {
	case opCreateActionConnector:
		return h.handleCreateActionConnector(c)
	case opDescribeActionConnector:
		return h.handleDescribeActionConnector(c)
	case opUpdateActionConnector:
		return h.handleUpdateActionConnector(c)
	case opDeleteActionConnector:
		return h.handleDeleteActionConnector(c)
	case opListActionConnectors:
		return h.handleListActionConnectors(c)
	case opSearchActionConnectors:
		return h.handleSearchActionConnectors(c)
	case opDescribeActionConnectorPerms:
		return h.handleDescribeActionConnectorPerms(c)
	case opUpdateActionConnectorPerms:
		return h.handleUpdateActionConnectorPerms(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

func actionConnectorToMap(a *ActionConnector) map[string]any {
	m := map[string]any{
		keyActionConnectorID:    a.ActionConnectorID,
		keyArn:                  a.Arn,
		keyName:                 a.Name,
		keyConnectorType:        a.Type,
		keyStatus:               a.Status,
		keyCreatedTime:          a.CreatedTime.Unix(),
		keyLastUpdatedTime:      a.LastUpdatedTime.Unix(),
		keyAuthenticationConfig: redactAuthenticationConfig(a.AuthenticationConfig, a.Arn),
	}
	if a.Description != "" {
		m[keyDescription] = a.Description
	}
	if a.VPCConnectionArn != "" {
		m[keyVPCConnectionArnField] = a.VPCConnectionArn
	}

	return m
}

func actionConnectorSummaryToMap(a *ActionConnector) map[string]any {
	return map[string]any{
		keyActionConnectorID: a.ActionConnectorID,
		keyArn:               a.Arn,
		keyName:              a.Name,
		keyConnectorType:     a.Type,
		keyStatus:            a.Status,
		keyCreatedTime:       a.CreatedTime.Unix(),
		keyLastUpdatedTime:   a.LastUpdatedTime.Unix(),
	}
}

func (h *Handler) handleCreateActionConnector(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	a, err := h.Backend.CreateActionConnector(
		accountID,
		strField(body, keyActionConnectorID),
		strField(body, keyName),
		strField(body, keyConnectorType),
		strField(body, keyDescription),
		strField(body, keyVPCConnectionArnField),
		mapField(body, keyAuthenticationConfig),
		permissionsField(body, "Permissions"),
		tagsFromBody(body),
	)
	if err != nil {
		if errors.Is(err, ErrActionConnectorAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyActionConnectorID: a.ActionConnectorID,
		keyArn:               a.Arn,
		keyCreationStatus:    a.Status,
		keyRequestID:         reqIDPlaceholder,
		keyStatus:            http.StatusOK,
	})
}

func (h *Handler) handleDescribeActionConnector(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	actionConnectorID := seg(segs, segResID)

	a, err := h.Backend.DescribeActionConnector(accountID, actionConnectorID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyActionConnector: actionConnectorToMap(a),
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleUpdateActionConnector(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	actionConnectorID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	a, err := h.Backend.UpdateActionConnector(
		accountID,
		actionConnectorID,
		strField(body, keyName),
		strField(body, keyDescription),
		strField(body, keyVPCConnectionArnField),
		mapField(body, keyAuthenticationConfig),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyActionConnectorID: a.ActionConnectorID,
		keyArn:               a.Arn,
		keyUpdateStatus:      a.Status,
		keyRequestID:         reqIDPlaceholder,
		keyStatus:            http.StatusOK,
	})
}

func (h *Handler) handleDeleteActionConnector(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	actionConnectorID := seg(segs, segResID)

	a, err := h.Backend.DeleteActionConnector(accountID, actionConnectorID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyActionConnectorID: a.ActionConnectorID,
		keyArn:               a.Arn,
		keyRequestID:         reqIDPlaceholder,
		keyStatus:            http.StatusOK,
	})
}

func (h *Handler) handleListActionConnectors(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	items, next, err := h.Backend.ListActionConnectors(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	summaries := make([]map[string]any, 0, len(items))
	for _, a := range items {
		summaries = append(summaries, actionConnectorSummaryToMap(a))
	}

	resp := map[string]any{
		keyActionConnectorSums: summaries,
		keyRequestID:           reqIDPlaceholder,
		keyStatus:              http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleSearchActionConnectors(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	items, next, err := h.Backend.SearchActionConnectors(
		accountID, folderFiltersFromBody(body), maxResultsParam(c), nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	summaries := make([]map[string]any, 0, len(items))
	for _, a := range items {
		summaries = append(summaries, actionConnectorSummaryToMap(a))
	}

	resp := map[string]any{
		keyActionConnectorSums: summaries,
		keyRequestID:           reqIDPlaceholder,
		keyStatus:              http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleDescribeActionConnectorPerms(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	actionConnectorID := seg(segs, segResID)

	a, perms, err := h.Backend.DescribeActionConnectorPermissions(accountID, actionConnectorID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyActionConnectorID: a.ActionConnectorID,
		keyArn:               a.Arn,
		keyPermissions:       permissionsToMaps(perms),
		keyRequestID:         reqIDPlaceholder,
		keyStatus:            http.StatusOK,
	})
}

func (h *Handler) handleUpdateActionConnectorPerms(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	actionConnectorID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	a, perms, err := h.Backend.UpdateActionConnectorPermissions(
		accountID,
		actionConnectorID,
		permissionsField(body, "GrantPermissions"),
		permissionsField(body, "RevokePermissions"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyActionConnectorID: a.ActionConnectorID,
		keyArn:               a.Arn,
		keyPermissions:       permissionsToMaps(perms),
		keyRequestID:         reqIDPlaceholder,
		keyStatus:            http.StatusOK,
	})
}

// classifyActionConnectorPaths routes /accounts/{id}/action-connectors/... paths.
func classifyActionConnectorPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method {
		case http.MethodPost:
			return opCreateActionConnector, accountID
		case http.MethodGet:
			return opListActionConnectors, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodGet:
			return opDescribeActionConnector, id
		case http.MethodPut:
			return opUpdateActionConnector, id
		case http.MethodDelete:
			return opDeleteActionConnector, id
		}
	case nSegsSubRes:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		switch sub {
		case pathSegPermissions:
			switch method {
			case http.MethodGet:
				return opDescribeActionConnectorPerms, id
			// UpdateActionConnectorPermissions' real wire method is POST
			// (quicksight@v1.123.1 serializers.go), not PUT -- found
			// unreachable by gopherstack-n1mb's route table. PUT is kept
			// too as a non-canonical route wired for this package's own
			// tests (handler_actionconnector_test.go).
			case http.MethodPost, http.MethodPut:
				return opUpdateActionConnectorPerms, id
			}
		case pathSegSearch:
			if method == http.MethodPost {
				return opSearchActionConnectors, accountID
			}
		}
	}

	return opUnknown, ""
}
