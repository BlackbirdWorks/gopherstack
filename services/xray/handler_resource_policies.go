package xray

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type deleteResourcePolicyInput struct {
	PolicyName       string `json:"PolicyName"`
	PolicyRevisionID string `json:"PolicyRevisionId"`
}

func (h *Handler) handleDeleteResourcePolicy(_ context.Context, body []byte) ([]byte, error) {
	var in deleteResourcePolicyInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.PolicyName == "" {
		return nil, fmt.Errorf("%w: PolicyName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteResourcePolicy(in.PolicyName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

type resourcePolicyView struct {
	PolicyName       string `json:"PolicyName"`
	PolicyDocument   string `json:"PolicyDocument"`
	PolicyRevisionID string `json:"PolicyRevisionId"`
}

func toResourcePolicyView(p *ResourcePolicy) resourcePolicyView {
	return resourcePolicyView{
		PolicyName:       p.PolicyName,
		PolicyDocument:   p.PolicyDocument,
		PolicyRevisionID: p.PolicyRevisionID,
	}
}

func (h *Handler) handleListResourcePolicies(_ context.Context, body []byte) ([]byte, error) {
	var in struct {
		NextToken  string `json:"NextToken"`
		MaxResults int    `json:"MaxResults"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &in)
	}

	policies := h.Backend.ListResourcePolicies()
	views := make([]resourcePolicyView, 0, len(policies))
	for i := range policies {
		views = append(views, toResourcePolicyView(&policies[i]))
	}

	pg := page.New(views, in.NextToken, in.MaxResults, defaultResourcePoliciesPageSize)
	resp := map[string]any{"ResourcePolicies": pg.Data, keyNextToken: pg.Next}

	return json.Marshal(resp)
}

type putResourcePolicyInput struct {
	PolicyName               string `json:"PolicyName"`
	PolicyDocument           string `json:"PolicyDocument"`
	PolicyRevisionID         string `json:"PolicyRevisionId"`
	BypassPolicyLockoutCheck bool   `json:"BypassPolicyLockoutCheck"`
}

func (h *Handler) handlePutResourcePolicy(_ context.Context, body []byte) ([]byte, error) {
	var in putResourcePolicyInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.PolicyName == "" {
		return nil, fmt.Errorf("%w: PolicyName is required", errInvalidRequest)
	}

	if in.PolicyDocument == "" {
		return nil, fmt.Errorf("%w: PolicyDocument is required", errInvalidRequest)
	}

	p, err := h.Backend.PutResourcePolicy(in.PolicyName, in.PolicyDocument, in.PolicyRevisionID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"ResourcePolicy": toResourcePolicyView(p),
	})
}
