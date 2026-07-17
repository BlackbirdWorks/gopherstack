package wafv2

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// createWebACLRequest is the request body for CreateWebACL.
type createWebACLRequest struct {
	DefaultAction        json.RawMessage  `json:"DefaultAction"`
	VisibilityConfig     json.RawMessage  `json:"VisibilityConfig"`
	CustomResponseBodies json.RawMessage  `json:"CustomResponseBodies"`
	AssociationConfig    json.RawMessage  `json:"AssociationConfig"`
	CaptchaConfig        json.RawMessage  `json:"CaptchaConfig"`
	ChallengeConfig      json.RawMessage  `json:"ChallengeConfig"`
	Name                 string           `json:"Name"`
	Scope                string           `json:"Scope"`
	Description          string           `json:"Description"`
	Tags                 []tagItem        `json:"Tags"`
	TokenDomains         []string         `json:"TokenDomains"`
	Rules                []map[string]any `json:"Rules"`
}

func (h *Handler) handleCreateWebACL(ctx context.Context, body []byte) ([]byte, error) {
	var req createWebACLRequest
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

	if err := validateVisibilityConfig(req.VisibilityConfig); err != nil {
		return nil, err
	}

	if err := validateDefaultAction(req.DefaultAction); err != nil {
		return nil, err
	}

	if err := validateRules(req.Rules); err != nil {
		return nil, err
	}

	tags := tagsFromItems(req.Tags)
	if err := validateTags(tags); err != nil {
		return nil, err
	}

	w, err := h.Backend.CreateWebACL(
		ctx,
		req.Name,
		req.Scope,
		req.Description,
		req.DefaultAction,
		req.VisibilityConfig,
		req.Rules,
		req.TokenDomains,
		req.CustomResponseBodies,
		req.AssociationConfig,
		req.CaptchaConfig,
		req.ChallengeConfig,
		tags,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: created web ACL", "name", w.Name, "id", w.ID)

	arnStr := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)

	return json.Marshal(map[string]any{
		keySummary: map[string]string{
			"Id":           w.ID,
			keyName:        w.Name,
			keyARN:         arnStr,
			keyLockToken:   w.LockToken,
			keyDescription: w.Description,
		},
	})
}

// validateDefaultAction validates that exactly one of Allow/Block is set in DefaultAction.
func validateDefaultAction(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return fmt.Errorf("%w: invalid DefaultAction JSON: %w", errInvalidRequest, err)
	}

	_, hasAllow := m["Allow"]
	_, hasBlock := m["Block"]

	if hasAllow && hasBlock {
		return fmt.Errorf("%w: DefaultAction must specify exactly one of Allow or Block", errInvalidRequest)
	}

	return nil
}

// getWebACLRequest is the request body for GetWebACL.
type getWebACLRequest struct {
	ID    string `json:"Id"`
	Name  string `json:"Name"`
	Scope string `json:"Scope"`
}

func (h *Handler) handleGetWebACL(ctx context.Context, body []byte) ([]byte, error) {
	var req getWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	w, err := h.Backend.GetWebACL(ctx, req.ID)
	if err != nil {
		return nil, err
	}

	if req.Scope != "" && w.Scope != req.Scope {
		return nil, fmt.Errorf("%w: web ACL %q has scope %s, not %s", ErrWebACLNotFound, req.ID, w.Scope, req.Scope)
	}

	return h.marshalWebACL(w)
}

// marshalWebACL builds the canonical WebACL JSON response.
func (h *Handler) marshalWebACL(w *WebACL) ([]byte, error) {
	arnStr := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)
	visConfig := parseVisibilityConfig(w.VisibilityConfig, w.Name)

	defaultActionJSON := w.DefaultAction
	if len(defaultActionJSON) == 0 {
		defaultActionJSON = json.RawMessage(`{"Allow":{}}`)
	}

	var defaultActionMap any
	if err := json.Unmarshal(defaultActionJSON, &defaultActionMap); err != nil {
		defaultActionMap = map[string]any{"Allow": map[string]any{}}
	}

	rules := w.Rules
	if rules == nil {
		rules = []map[string]any{}
	}

	webACLMap := map[string]any{
		"Id":                w.ID,
		keyName:             w.Name,
		keyARN:              arnStr,
		keyLockToken:        w.LockToken,
		keyDescription:      w.Description,
		"DefaultAction":     defaultActionMap,
		keyVisibilityConfig: visConfig,
		keyRules:            rules,
	}

	if len(w.TokenDomains) > 0 {
		webACLMap["TokenDomains"] = w.TokenDomains
	}

	if len(w.CustomResponseBodies) > 0 {
		var crb any
		if json.Unmarshal(w.CustomResponseBodies, &crb) == nil {
			webACLMap["CustomResponseBodies"] = crb
		}
	}

	if len(w.AssociationConfig) > 0 {
		var ac any
		if json.Unmarshal(w.AssociationConfig, &ac) == nil {
			webACLMap["AssociationConfig"] = ac
		}
	}

	if len(w.CaptchaConfig) > 0 {
		var cc any
		if json.Unmarshal(w.CaptchaConfig, &cc) == nil {
			webACLMap["CaptchaConfig"] = cc
		}
	}

	if len(w.ChallengeConfig) > 0 {
		var chc any
		if json.Unmarshal(w.ChallengeConfig, &chc) == nil {
			webACLMap["ChallengeConfig"] = chc
		}
	}

	return json.Marshal(map[string]any{
		"WebACL":     webACLMap,
		keyLockToken: w.LockToken,
	})
}

// updateWebACLRequest is the request body for UpdateWebACL.
type updateWebACLRequest struct {
	DefaultAction        json.RawMessage  `json:"DefaultAction"`
	VisibilityConfig     json.RawMessage  `json:"VisibilityConfig"`
	CustomResponseBodies json.RawMessage  `json:"CustomResponseBodies"`
	AssociationConfig    json.RawMessage  `json:"AssociationConfig"`
	CaptchaConfig        json.RawMessage  `json:"CaptchaConfig"`
	ChallengeConfig      json.RawMessage  `json:"ChallengeConfig"`
	ID                   string           `json:"Id"`
	Name                 string           `json:"Name"`
	Scope                string           `json:"Scope"`
	LockToken            string           `json:"LockToken"`
	Description          string           `json:"Description"`
	TokenDomains         []string         `json:"TokenDomains"`
	Rules                []map[string]any `json:"Rules"`
}

func (h *Handler) handleUpdateWebACL(ctx context.Context, body []byte) ([]byte, error) {
	var req updateWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	if err := validateVisibilityConfig(req.VisibilityConfig); err != nil {
		return nil, err
	}

	if err := validateDefaultAction(req.DefaultAction); err != nil {
		return nil, err
	}

	if err := validateRules(req.Rules); err != nil {
		return nil, err
	}

	w, err := h.Backend.UpdateWebACL(
		ctx,
		req.ID,
		req.Description,
		req.LockToken,
		req.DefaultAction,
		req.VisibilityConfig,
		req.Rules,
		req.TokenDomains,
		req.CustomResponseBodies,
		req.AssociationConfig,
		req.CaptchaConfig,
		req.ChallengeConfig,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: updated web ACL", "id", req.ID)

	return json.Marshal(map[string]string{
		keyNextLockToken: w.LockToken,
	})
}

// deleteWebACLRequest is the request body for DeleteWebACL.
type deleteWebACLRequest struct {
	ID        string `json:"Id"`
	Name      string `json:"Name"`
	Scope     string `json:"Scope"`
	LockToken string `json:"LockToken"`
}

func (h *Handler) handleDeleteWebACL(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteWebACLRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ID == "" {
		return nil, fmt.Errorf("%w: Id is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteWebACL(ctx, req.ID, req.LockToken); err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: deleted web ACL", "id", req.ID)

	return nil, nil
}

// handleListWebACLs handles the ListWebACLs request.
func (h *Handler) handleListWebACLs(ctx context.Context, body []byte) ([]byte, error) {
	return handleListResourceFamily(
		body,
		h.Backend.ListWebACLs(ctx),
		"WebACLs",
		func(w *WebACL) string { return w.Scope },
		func(w *WebACL) string { return w.Name },
		func(w *WebACL) map[string]string {
			return map[string]string{
				"Id":           w.ID,
				keyName:        w.Name,
				keyARN:         h.Backend.WebACLARN(w.Name, w.ID, w.Scope),
				keyLockToken:   w.LockToken,
				keyDescription: w.Description,
			}
		},
	)
}

// deleteFirewallManagerRuleGroupsRequest is the request body for DeleteFirewallManagerRuleGroups.
type deleteFirewallManagerRuleGroupsRequest struct {
	WebACLArn       string `json:"WebACLArn"`
	WebACLLockToken string `json:"WebACLLockToken"`
}

func (h *Handler) handleDeleteFirewallManagerRuleGroups(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteFirewallManagerRuleGroupsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WebACLArn == "" {
		return nil, fmt.Errorf("%w: WebACLArn is required", errInvalidRequest)
	}

	w, err := h.Backend.DeleteFirewallManagerRuleGroups(ctx, req.WebACLArn)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "wafv2: deleted firewall manager rule groups", "webACLArn", req.WebACLArn)

	return json.Marshal(map[string]string{
		"NextWebACLLockToken": w.LockToken,
	})
}

// webACLDispatchOps returns the WebACL-family operation dispatch entries. Each entry is a
// bound method value -- handleCreateWebACL et al. already match the dispatchFn signature,
// so no wrapper closure is needed.
func (h *Handler) webACLDispatchOps() map[string]dispatchFn {
	return map[string]dispatchFn{
		"CreateWebACL":                    h.handleCreateWebACL,
		"GetWebACL":                       h.handleGetWebACL,
		"UpdateWebACL":                    h.handleUpdateWebACL,
		"DeleteWebACL":                    h.handleDeleteWebACL,
		"ListWebACLs":                     h.handleListWebACLs,
		"DeleteFirewallManagerRuleGroups": h.handleDeleteFirewallManagerRuleGroups,
	}
}
