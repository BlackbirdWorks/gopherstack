package xray

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// probabilisticRuleValueView is the wire shape for an indexing rule's Rule field:
// the tagged union {"Probabilistic": {...}} (real SDK type IndexingRuleValue /
// IndexingRuleValueUpdate) collapses to its only known member in gopherstack.
type probabilisticRuleValueUpdateView struct {
	DesiredSamplingPercentage float64 `json:"DesiredSamplingPercentage"`
}

type probabilisticRuleValueView struct {
	DesiredSamplingPercentage float64 `json:"DesiredSamplingPercentage"`
	ActualSamplingPercentage  float64 `json:"ActualSamplingPercentage"`
}

type indexingRuleValueUpdateInput struct {
	Probabilistic *probabilisticRuleValueUpdateView `json:"Probabilistic,omitempty"`
}

type updateIndexingRuleInput struct {
	Rule *indexingRuleValueUpdateInput `json:"Rule,omitempty"`
	Name string                        `json:"Name"`
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

	var update *ProbabilisticRuleValue

	if in.Rule != nil && in.Rule.Probabilistic != nil {
		update = &ProbabilisticRuleValue{DesiredSamplingPercentage: in.Rule.Probabilistic.DesiredSamplingPercentage}
	}

	rule, err := h.Backend.UpdateIndexingRule(in.Name, update)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"IndexingRule": toIndexingRuleView(rule),
	})
}

type indexingRuleView struct {
	Rule       map[string]any `json:"Rule,omitempty"`
	Name       string         `json:"Name"`
	ModifiedAt float64        `json:"ModifiedAt"`
}

func toIndexingRuleView(r *IndexingRule) indexingRuleView {
	v := indexingRuleView{
		Name:       r.Name,
		ModifiedAt: float64(r.ModifiedAt.Unix()),
	}

	if r.Rule != nil {
		v.Rule = map[string]any{
			"Probabilistic": probabilisticRuleValueView{
				DesiredSamplingPercentage: r.Rule.DesiredSamplingPercentage,
				ActualSamplingPercentage:  r.Rule.ActualSamplingPercentage,
			},
		}
	}

	return v
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
		views = append(views, toIndexingRuleView(r))
	}

	pg := page.New(views, in.NextToken, int(in.MaxResults), defaultIndexingRulesPageSize)

	return json.Marshal(map[string]any{
		"IndexingRules": pg.Data,
		keyNextToken:    pg.Next,
	})
}
