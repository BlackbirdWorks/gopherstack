package xray

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type updateIndexingRuleInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleUpdateIndexingRule(_ context.Context, body []byte) ([]byte, error) {
	var in updateIndexingRuleInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	rule, err := h.Backend.UpdateIndexingRule(in.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"IndexingRule": map[string]any{
			"Name":       rule.Name,
			"ModifiedAt": rule.ModifiedAt,
		},
	})
}

type indexingRuleView struct {
	Name       string  `json:"Name"`
	ModifiedAt float64 `json:"ModifiedAt"`
}

type getIndexingRulesInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

func (h *Handler) handleGetIndexingRules(_ context.Context, body []byte) ([]byte, error) {
	var in getIndexingRulesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	rules := h.Backend.GetIndexingRules()
	views := make([]indexingRuleView, 0, len(rules))

	for _, r := range rules {
		views = append(views, indexingRuleView{
			Name:       r.Name,
			ModifiedAt: float64(r.ModifiedAt.Unix()),
		})
	}

	pg := page.New(views, in.NextToken, int(in.MaxResults), defaultIndexingRulesPageSize)

	return json.Marshal(map[string]any{
		"IndexingRules": pg.Data,
		keyNextToken:    pg.Next,
	})
}
