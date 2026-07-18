package databrew

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

func parseRulesetOp(method, name string) string {
	switch method {
	case http.MethodPost:
		if name == "" {
			return opCreateRuleset
		}
	case http.MethodGet:
		if name == "" {
			return opListRulesets
		}

		return opDescribeRuleset
	case http.MethodPut:

		return opUpdateRuleset
	case http.MethodDelete:

		return opDeleteRuleset
	}

	return opUnknown
}

func (h *Handler) dispatchRuleset(ctx context.Context, action string, body []byte) ([]byte, bool, error) {
	switch action {
	case opCreateRuleset:
		r, e := h.handleCreateRuleset(ctx, body)

		return r, true, e
	case opDescribeRuleset:
		r, e := h.handleDescribeRuleset(ctx, body)

		return r, true, e
	case opListRulesets:
		r, e := h.handleListRulesets(ctx, body)

		return r, true, e
	case opUpdateRuleset:
		r, e := h.handleUpdateRuleset(ctx, body)

		return r, true, e
	case opDeleteRuleset:
		r, e := h.handleDeleteRuleset(ctx, body)

		return r, true, e
	}

	return nil, false, nil
}

func (h *Handler) handleCreateRuleset(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		Name        string            `json:"Name"`
		Description string            `json:"Description"`
		TargetArn   string            `json:"TargetArn"`
		Rules       []Rule            `json:"Rules"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	rs, err := h.Backend.CreateRuleset(ctx, req.Name, req.Description, req.TargetArn, req.Rules, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: rs.Name})
}

func (h *Handler) handleDescribeRuleset(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	rs, err := h.Backend.DescribeRuleset(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(rs)
}

func (h *Handler) handleListRulesets(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MaxResults string `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
		TargetArn  string `json:"TargetArn"`
	}
	_ = json.Unmarshal(body, &req)
	maxResults, _ := strconv.Atoi(req.MaxResults)

	rulesets, next := h.Backend.ListRulesets(ctx, maxResults, req.NextToken, req.TargetArn)

	return json.Marshal(map[string]any{"Rulesets": rulesets, nextTokenKey: next})
}

func (h *Handler) handleUpdateRuleset(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
		Rules       []Rule `json:"Rules"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateRuleset(ctx, req.Name, req.Description, req.Rules); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleDeleteRuleset(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.DeleteRuleset(ctx, req.Name); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}
