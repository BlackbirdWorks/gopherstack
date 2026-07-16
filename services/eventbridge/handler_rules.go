package eventbridge

import (
	"context"
	"encoding/json"
)

type deleteRuleInput struct {
	Name         string `json:"Name"`
	EventBusName string `json:"EventBusName"`
}

type listRulesInput struct {
	EventBusName string `json:"EventBusName"`
	NamePrefix   string `json:"NamePrefix"`
	NextToken    string `json:"NextToken"`
	Limit        int    `json:"Limit"`
}

type describeRuleInput struct {
	Name         string `json:"Name"`
	EventBusName string `json:"EventBusName"`
}

type enableRuleInput struct {
	Name         string `json:"Name"`
	EventBusName string `json:"EventBusName"`
}

type disableRuleInput struct {
	Name         string `json:"Name"`
	EventBusName string `json:"EventBusName"`
}

type putRuleOutput struct {
	RuleArn string `json:"RuleArn"`
}

type deleteRuleOutput struct{}

type listRulesOutput struct {
	NextToken string `json:"NextToken,omitempty"`
	Rules     []Rule `json:"Rules"`
}

type enableRuleOutput struct{}

type disableRuleOutput struct{}

func (h *Handler) ruleActions() map[string]actionFn {
	return map[string]actionFn{
		"PutRule": func(ctx context.Context, b []byte) (any, error) {
			var input PutRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			rule, err := h.Backend.PutRule(ctx, input)
			if err != nil {
				return nil, err
			}
			if len(input.Tags) > 0 {
				h.setTags(rule.Arn, input.Tags)
			}

			return &putRuleOutput{RuleArn: rule.Arn}, nil
		},
		"DeleteRule": func(ctx context.Context, b []byte) (any, error) {
			var input deleteRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			// Capture ARN before deletion so we can clean up tags.
			rule, _ := h.Backend.DescribeRule(ctx, input.Name, input.EventBusName)
			if err := h.Backend.DeleteRule(ctx, input.Name, input.EventBusName); err != nil {
				return nil, err
			}
			if rule != nil {
				h.clearResourceTags(rule.Arn)
			}

			return &deleteRuleOutput{}, nil
		},
		"ListRules": func(ctx context.Context, b []byte) (any, error) {
			var input listRulesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			rules, next, err := h.Backend.ListRules(
				ctx,
				input.EventBusName,
				input.NamePrefix,
				input.NextToken,
				input.Limit,
			)
			if err != nil {
				return nil, err
			}

			return &listRulesOutput{Rules: rules, NextToken: next}, nil
		},
		"DescribeRule": func(ctx context.Context, b []byte) (any, error) {
			var input describeRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeRule(ctx, input.Name, input.EventBusName)
		},
	}
}

// ruleQueryActions returns the ListRuleNamesByTarget and TestEventPattern actions.
func (h *Handler) ruleQueryActions() map[string]actionFn {
	return map[string]actionFn{
		"ListRuleNamesByTarget": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				EventBusName string `json:"EventBusName"`
				NextToken    string `json:"NextToken"`
				TargetArn    string `json:"TargetArn"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			names, next, err := h.Backend.ListRuleNamesByTarget(
				ctx,
				input.TargetArn,
				input.EventBusName,
				input.NextToken,
			)
			if err != nil {
				return nil, err
			}

			return &struct {
				NextToken string   `json:"NextToken,omitempty"`
				RuleNames []string `json:"RuleNames"`
			}{RuleNames: names, NextToken: next}, nil
		},
		"TestEventPattern": func(ctx context.Context, b []byte) (any, error) {
			var input struct {
				Event        string `json:"Event"`
				EventPattern string `json:"EventPattern"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			result, err := h.Backend.TestEventPattern(ctx, input.EventPattern, input.Event)
			if err != nil {
				return nil, err
			}

			return &struct {
				Result bool `json:"Result"`
			}{Result: result}, nil
		},
	}
}

func (h *Handler) ruleStateActions() map[string]actionFn {
	return map[string]actionFn{
		"EnableRule": func(ctx context.Context, b []byte) (any, error) {
			var input enableRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.EnableRule(ctx, input.Name, input.EventBusName); err != nil {
				return nil, err
			}

			return &enableRuleOutput{}, nil
		},
		"DisableRule": func(ctx context.Context, b []byte) (any, error) {
			var input disableRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DisableRule(ctx, input.Name, input.EventBusName); err != nil {
				return nil, err
			}

			return &disableRuleOutput{}, nil
		},
	}
}
