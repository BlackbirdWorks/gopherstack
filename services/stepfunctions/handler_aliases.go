package stepfunctions

import (
	"encoding/json"
)

type createStateMachineAliasInput struct {
	Name                 string               `json:"name"`
	StateMachineArn      string               `json:"stateMachineArn"`
	Description          string               `json:"description"`
	RoutingConfiguration []AliasRoutingConfig `json:"routingConfiguration"`
}

type updateStateMachineAliasInput struct {
	StateMachineAliasArn string               `json:"stateMachineAliasArn"`
	Description          string               `json:"description"`
	RoutingConfiguration []AliasRoutingConfig `json:"routingConfiguration"`
}

type deleteStateMachineAliasInput struct {
	StateMachineAliasArn string `json:"stateMachineAliasArn"`
}

type describeStateMachineAliasInput struct {
	StateMachineAliasArn string `json:"stateMachineAliasArn"`
}

type listStateMachineAliasesInput struct {
	StateMachineArn string `json:"stateMachineArn"`
	NextToken       string `json:"nextToken"`
	MaxResults      int    `json:"maxResults"`
}

type listStateMachineAliasesOutput struct {
	NextToken           string              `json:"nextToken,omitempty"`
	StateMachineAliases []StateMachineAlias `json:"stateMachineAliases"`
}

// ── RedriveExecution / DescribeStateMachineForExecution ───────────────────────

// aliasActions returns handler functions for state machine alias operations.
func (h *Handler) aliasActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateStateMachineAlias": func(b []byte) (any, error) {
			var input createStateMachineAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CreateStateMachineAlias(
				input.StateMachineArn, input.Name, input.Description, input.RoutingConfiguration,
			)
		},
		"UpdateStateMachineAlias": func(b []byte) (any, error) {
			var input updateStateMachineAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateStateMachineAlias(
				input.StateMachineAliasArn, input.Description, input.RoutingConfiguration,
			)
		},
		"DeleteStateMachineAlias": func(b []byte) (any, error) {
			var input deleteStateMachineAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteStateMachineAlias(input.StateMachineAliasArn); err != nil {
				return nil, err
			}

			return map[string]any{}, nil
		},
		"DescribeStateMachineAlias": func(b []byte) (any, error) {
			var input describeStateMachineAliasInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeStateMachineAlias(input.StateMachineAliasArn)
		},
		"ListStateMachineAliases": func(b []byte) (any, error) {
			var input listStateMachineAliasesInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			aliases, next, err := h.Backend.ListStateMachineAliases(
				input.StateMachineArn, input.NextToken, input.MaxResults,
			)
			if err != nil {
				return nil, err
			}

			return &listStateMachineAliasesOutput{
				StateMachineAliases: aliases,
				NextToken:           next,
			}, nil
		},
	}
}
