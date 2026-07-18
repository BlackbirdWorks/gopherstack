package wafv2

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// checkCapacityRequest is the request body for CheckCapacity.
type checkCapacityRequest struct {
	Scope string           `json:"Scope"`
	Rules []map[string]any `json:"Rules"`
}

func (h *Handler) handleCheckCapacity(ctx context.Context, body []byte) ([]byte, error) {
	var req checkCapacityRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	capacity, err := h.Backend.CheckCapacity(ctx, req.Scope, req.Rules)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"ConsumedCapacity": capacity,
	})
}

// createRuleGroupRequest is the request body for CreateRuleGroup.
type createRuleGroupRequest struct {
	Name             string           `json:"Name"`
	Scope            string           `json:"Scope"`
	Description      string           `json:"Description"`
	VisibilityConfig json.RawMessage  `json:"VisibilityConfig"`
	Rules            []map[string]any `json:"Rules"`
	Tags             []tagItem        `json:"Tags"`
	Capacity         int64            `json:"Capacity"`
}

func (h *Handler) handleCreateRuleGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req createRuleGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if err := validateResourceName(req.Name); err != nil {
		return nil, err
	}

	if err := validateDescription(req.Description); err != nil {
		return nil, err
	}

	if req.Scope == "" {
		return nil, fmt.Errorf("%w: Scope is required", errInvalidRequest)
	}

	if !validScope(req.Scope) {
		return nil, fmt.Errorf("%w: Scope must be %s or %s", errInvalidRequest, ScopeRegional, ScopeCloudFront)
	}

	if req.Capacity < minRuleGroupCapacity || req.Capacity > maxRuleGroupCapacity {
		return nil, fmt.Errorf(
			"%w: Capacity must be between %d and %d",
			errInvalidRequest,
			minRuleGroupCapacity,
			maxRuleGroupCapacity,
		)
	}

	if err := validateVisibilityConfig(req.VisibilityConfig); err != nil {
		return nil, err
	}

	tags := tagsFromItems(req.Tags)
	if err := validateTags(tags); err != nil {
		return nil, err
	}

	rg, err := h.Backend.CreateRuleGroup(
		ctx,
		req.Name,
		req.Scope,
		req.Description,
		string(req.VisibilityConfig),
		req.Capacity,
		req.Rules,
		tags,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: created rule group", "name", rg.Name, "id", rg.ID)

	arnStr := h.Backend.RuleGroupARN(rg.Name, rg.ID, rg.Scope)

	return json.Marshal(map[string]any{
		keySummary: map[string]string{
			"Id":           rg.ID,
			keyName:        rg.Name,
			keyARN:         arnStr,
			keyLockToken:   rg.LockToken,
			keyDescription: rg.Description,
		},
	})
}

// getRuleGroupRequest is the request body for GetRuleGroup.
type getRuleGroupRequest struct {
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	Scope string `json:"Scope"`
	ARN   string `json:"ARN"`
}

func (h *Handler) handleGetRuleGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req getRuleGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	rg, err := h.Backend.GetRuleGroup(ctx, req.ID)
	if err != nil {
		return nil, err
	}

	if req.Scope != "" && rg.Scope != req.Scope {
		return nil, fmt.Errorf(
			"%w: rule group %q has scope %s, not %s",
			ErrRuleGroupNotFound,
			req.ID,
			rg.Scope,
			req.Scope,
		)
	}

	arnStr := h.Backend.RuleGroupARN(rg.Name, rg.ID, rg.Scope)
	visConfig := parseVisibilityConfig(json.RawMessage(rg.VisibilityConfig), rg.Name)

	return json.Marshal(map[string]any{
		"RuleGroup": map[string]any{
			"Id":                rg.ID,
			keyName:             rg.Name,
			keyARN:              arnStr,
			keyDescription:      rg.Description,
			keyCapacity:         rg.Capacity,
			keyRules:            rg.Rules,
			keyVisibilityConfig: visConfig,
		},
		keyLockToken: rg.LockToken,
	})
}

func (h *Handler) handleListRuleGroups(ctx context.Context, body []byte) ([]byte, error) {
	return handleListResourceFamily(
		body,
		h.Backend.ListRuleGroups(ctx),
		"RuleGroups",
		func(rg *RuleGroup) string { return rg.Scope },
		func(rg *RuleGroup) string { return rg.Name },
		func(rg *RuleGroup) map[string]string {
			return map[string]string{
				"Id":           rg.ID,
				keyName:        rg.Name,
				keyARN:         h.Backend.RuleGroupARN(rg.Name, rg.ID, rg.Scope),
				keyLockToken:   rg.LockToken,
				keyDescription: rg.Description,
			}
		},
	)
}

// updateRuleGroupRequest is the request body for UpdateRuleGroup.
type updateRuleGroupRequest struct {
	ID               string           `json:"Id"`
	Name             string           `json:"Name"`
	Scope            string           `json:"Scope"`
	LockToken        string           `json:"LockToken"`
	Description      string           `json:"Description"`
	VisibilityConfig json.RawMessage  `json:"VisibilityConfig"`
	Rules            []map[string]any `json:"Rules"`
}

func (h *Handler) handleUpdateRuleGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req updateRuleGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	if err := validateVisibilityConfig(req.VisibilityConfig); err != nil {
		return nil, err
	}

	rg, err := h.Backend.UpdateRuleGroup(
		ctx,
		req.ID,
		req.Description,
		string(req.VisibilityConfig),
		req.LockToken,
		req.Rules,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: updated rule group", "id", req.ID)

	return json.Marshal(map[string]string{keyNextLockToken: rg.LockToken})
}

// deleteRuleGroupRequest is the request body for DeleteRuleGroup.
type deleteRuleGroupRequest struct {
	ID        string `json:"Id"`
	Name      string `json:"Name"`
	Scope     string `json:"Scope"`
	LockToken string `json:"LockToken"`
}

func (h *Handler) handleDeleteRuleGroup(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteRuleGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteRuleGroup(ctx, req.ID, req.LockToken); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: deleted rule group", "id", req.ID)

	return nil, nil
}

// ruleGroupDispatchOps returns the RuleGroup-family operation dispatch entries. Each entry
// is a bound method value -- handleCheckCapacity et al. already match the dispatchFn
// signature, so no wrapper closure is needed.
func (h *Handler) ruleGroupDispatchOps() map[string]dispatchFn {
	return map[string]dispatchFn{
		"CheckCapacity":   h.handleCheckCapacity,
		"GetRuleGroup":    h.handleGetRuleGroup,
		"DeleteRuleGroup": h.handleDeleteRuleGroup,
		"ListRuleGroups":  h.handleListRuleGroups,
		"UpdateRuleGroup": h.handleUpdateRuleGroup,
		"CreateRuleGroup": h.handleCreateRuleGroup,
	}
}
