package quicksight

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response/request keys used only by Agent operations.
const (
	keyAgentID           = "AgentId"
	keyAgentName         = "AgentName"
	keyAgent             = "Agent"
	keyAgentSummaries    = "AgentSummaries"
	keyAgentStatus       = "AgentStatus"
	keyAgentLifecycle    = "AgentLifecycle"
	keyIconID            = "IconId"
	keyWelcomeMessage    = "WelcomeMessage"
	keyCreator           = "Creator"
	keyErrorMessageField = "ErrorMessage"
	keyActionConnectors  = "ActionConnectors"
	keySpaces            = "Spaces"
	keyStarterPrompts    = "StarterPrompts"
	keyUpdatedAt         = "UpdatedAt"

	keyActionConnectorsToAdd    = "ActionConnectorsToAdd"
	keyActionConnectorsToRemove = "ActionConnectorsToRemove"
	keySpacesToAdd              = "SpacesToAdd"
	keySpacesToRemove           = "SpacesToRemove"

	keyFailedToAddActionConnectors    = "FailedToAddActionConnectors"
	keyFailedToAddSpaces              = "FailedToAddSpaces"
	keyFailedToRemoveActionConnectors = "FailedToRemoveActionConnectors"
	keyFailedToRemoveSpaces           = "FailedToRemoveSpaces"

	keyErrorCode = "ErrorCode"
)

func isAgentOp(op string) bool {
	switch op {
	case opCreateAgent, opDescribeAgent, opUpdateAgent, opDeleteAgent,
		opListAgents, opSearchAgents, opDescribeAgentPerms, opUpdateAgentPerms:
		return true
	}

	return false
}

func (h *Handler) dispatchAgent(c *echo.Context, op string) error {
	switch op {
	case opCreateAgent:
		return h.handleCreateAgent(c)
	case opDescribeAgent:
		return h.handleDescribeAgent(c)
	case opUpdateAgent:
		return h.handleUpdateAgent(c)
	case opDeleteAgent:
		return h.handleDeleteAgent(c)
	case opListAgents:
		return h.handleListAgents(c)
	case opSearchAgents:
		return h.handleSearchAgents(c)
	case opDescribeAgentPerms:
		return h.handleDescribeAgentPerms(c)
	case opUpdateAgentPerms:
		return h.handleUpdateAgentPerms(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

func agentToMap(a *Agent) map[string]any {
	m := map[string]any{
		keyAgentID:        a.AgentID,
		keyArn:            a.Arn,
		keyName:           a.Name,
		keyAgentLifecycle: a.AgentLifecycle,
		keyAgentStatus:    a.AgentStatus,
		keyCreatedAt:      a.CreatedAt.Unix(),
		keyUpdatedAt:      a.UpdatedAt.Unix(),
		keyCreator:        a.Creator,
	}
	if a.Description != "" {
		m[keyDescription] = a.Description
	}
	if a.IconID != "" {
		m[keyIconID] = a.IconID
	}
	if a.WelcomeMessage != "" {
		m[keyWelcomeMessage] = a.WelcomeMessage
	}
	if a.ErrorMessage != "" {
		m[keyErrorMessageField] = a.ErrorMessage
	}
	if len(a.ActionConnectors) > 0 {
		m[keyActionConnectors] = a.ActionConnectors
	}
	if len(a.Spaces) > 0 {
		m[keySpaces] = a.Spaces
	}
	if len(a.StarterPrompts) > 0 {
		m[keyStarterPrompts] = a.StarterPrompts
	}

	return m
}

func agentSummaryToMap(a *Agent) map[string]any {
	m := map[string]any{
		keyAgentID:   a.AgentID,
		keyArn:       a.Arn,
		keyName:      a.Name,
		keyCreatedAt: a.CreatedAt.Unix(),
		keyUpdatedAt: a.UpdatedAt.Unix(),
	}
	if a.Description != "" {
		m[keyDescription] = a.Description
	}
	if a.IconID != "" {
		m[keyIconID] = a.IconID
	}

	return m
}

func (h *Handler) handleCreateAgent(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	a, err := h.Backend.CreateAgent(
		accountID,
		strField(body, keyAgentID),
		strField(body, keyName),
		strField(body, keyDescription),
		strField(body, keyIconID),
		strField(body, keyWelcomeMessage),
		strField(body, keyAgentLifecycle),
		strSliceField(body, keyActionConnectors),
		strSliceField(body, keySpaces),
		strSliceField(body, keyStarterPrompts),
		permissionsField(body, keyPermissions),
		tagsFromBody(body),
	)
	if err != nil {
		if errors.Is(err, ErrAgentAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAgentID:     a.AgentID,
		keyAgentName:   a.Name,
		keyAgentStatus: a.AgentStatus,
		keyArn:         a.Arn,
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	})
}

func (h *Handler) handleDescribeAgent(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	agentID := seg(segs, segResID)

	a, err := h.Backend.DescribeAgent(accountID, agentID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAgent:     agentToMap(a),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUpdateAgent(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	agentID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	a, assoc, err := h.Backend.UpdateAgent(
		accountID,
		agentID,
		strField(body, keyName),
		strField(body, keyDescription),
		strField(body, keyIconID),
		strField(body, keyWelcomeMessage),
		strSliceField(body, keyActionConnectorsToAdd),
		strSliceField(body, keyActionConnectorsToRemove),
		strSliceField(body, keySpacesToAdd),
		strSliceField(body, keySpacesToRemove),
		strSliceField(body, keyStarterPrompts),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAgentID:                        a.AgentID,
		keyArn:                            a.Arn,
		keyAgentStatus:                    a.AgentStatus,
		keyFailedToAddActionConnectors:    associationFailuresToMaps(assoc.FailedToAddActionConnectors),
		keyFailedToAddSpaces:              associationFailuresToMaps(assoc.FailedToAddSpaces),
		keyFailedToRemoveActionConnectors: associationFailuresToMaps(assoc.FailedToRemoveActionConnectors),
		keyFailedToRemoveSpaces:           associationFailuresToMaps(assoc.FailedToRemoveSpaces),
		keyRequestID:                      reqIDPlaceholder,
		keyStatus:                         http.StatusOK,
	})
}

func associationFailuresToMaps(fails []AssociationFailure) []map[string]any {
	out := make([]map[string]any, 0, len(fails))
	for _, f := range fails {
		out = append(out, map[string]any{
			keyArn:               f.Arn,
			keyErrorCode:         f.ErrorCode,
			keyErrorMessageField: f.ErrorMessage,
		})
	}

	return out
}

func (h *Handler) handleDeleteAgent(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	agentID := seg(segs, segResID)

	if err := h.Backend.DeleteAgent(accountID, agentID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListAgents(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	items, next, err := h.Backend.ListAgents(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	summaries := make([]map[string]any, 0, len(items))
	for _, a := range items {
		summaries = append(summaries, agentSummaryToMap(a))
	}

	resp := map[string]any{
		keyAgentSummaries: summaries,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleSearchAgents(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	items, next, err := h.Backend.SearchAgents(
		accountID, folderFiltersFromBody(body), maxResultsParam(c), nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	summaries := make([]map[string]any, 0, len(items))
	for _, a := range items {
		summaries = append(summaries, agentSummaryToMap(a))
	}

	resp := map[string]any{
		keyAgentSummaries: summaries,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleDescribeAgentPerms(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	agentID := seg(segs, segResID)

	a, perms, err := h.Backend.DescribeAgentPermissions(accountID, agentID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAgentID:     a.AgentID,
		keyArn:         a.Arn,
		keyPermissions: permissionsToMaps(perms),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	})
}

func (h *Handler) handleUpdateAgentPerms(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	agentID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	a, perms, err := h.Backend.UpdateAgentPermissions(
		accountID,
		agentID,
		permissionsField(body, "GrantPermissions"),
		permissionsField(body, "RevokePermissions"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAgentID:     a.AgentID,
		keyArn:         a.Arn,
		keyPermissions: permissionsToMaps(perms),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	})
}

// classifyAgentPaths routes /accounts/{id}/agents/... paths.
func classifyAgentPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method {
		case http.MethodPost:
			return opCreateAgent, accountID
		case http.MethodGet:
			return opListAgents, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodGet:
			return opDescribeAgent, id
		case http.MethodPut:
			return opUpdateAgent, id
		case http.MethodDelete:
			return opDeleteAgent, id
		}
	case nSegsSubRes:
		if seg(segs, segSubRes) == pathSegPermissions {
			id := seg(segs, segResID)
			switch method {
			case http.MethodGet:
				return opDescribeAgentPerms, id
			case http.MethodPut:
				return opUpdateAgentPerms, id
			}
		}
	}

	return opUnknown, ""
}
