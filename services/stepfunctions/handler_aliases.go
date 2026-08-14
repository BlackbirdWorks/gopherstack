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
	NextToken           string                   `json:"nextToken,omitempty"`
	StateMachineAliases []stateMachineAliasEntry `json:"stateMachineAliases"`
}

// stateMachineAliasEntry mirrors AWS's alias response shapes on the wire.
// The domain StateMachineAlias tags its update timestamp "updatedDate" for
// stable snapshot persistence, but every real sfn alias response ("Describe",
// "List", the "updateDate" of "Update") names it "updateDate" -- so this view
// re-keys it at the wire boundary rather than at the persisted struct.
type stateMachineAliasEntry struct {
	StateMachineAliasArn string               `json:"stateMachineAliasArn"`
	Name                 string               `json:"name,omitempty"`
	Description          string               `json:"description,omitempty"`
	RoutingConfiguration []AliasRoutingConfig `json:"routingConfiguration,omitempty"`
	CreationDate         float64              `json:"creationDate"`
	UpdateDate           float64              `json:"updateDate,omitempty"`
}

func newStateMachineAliasEntry(a *StateMachineAlias) *stateMachineAliasEntry {
	return &stateMachineAliasEntry{
		StateMachineAliasArn: a.StateMachineAliasArn,
		Name:                 a.Name,
		Description:          a.Description,
		RoutingConfiguration: a.RoutingConfiguration,
		CreationDate:         a.CreationDate,
		UpdateDate:           a.UpdatedDate,
	}
}

// ── RedriveExecution / DescribeStateMachineForExecution ───────────────────────

// aliasActions returns handler functions for state machine alias operations.
func (h *Handler) aliasActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateStateMachineAlias":   h.handleCreateStateMachineAlias,
		"UpdateStateMachineAlias":   h.handleUpdateStateMachineAlias,
		"DeleteStateMachineAlias":   h.handleDeleteStateMachineAlias,
		"DescribeStateMachineAlias": h.handleDescribeStateMachineAlias,
		"ListStateMachineAliases":   h.handleListStateMachineAliases,
	}
}

func (h *Handler) handleCreateStateMachineAlias(b []byte) (any, error) {
	var input createStateMachineAliasInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	a, err := h.Backend.CreateStateMachineAlias(
		input.StateMachineArn, input.Name, input.Description, input.RoutingConfiguration,
	)
	if err != nil {
		return nil, err
	}

	return newStateMachineAliasEntry(a), nil
}

func (h *Handler) handleUpdateStateMachineAlias(b []byte) (any, error) {
	var input updateStateMachineAliasInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	a, err := h.Backend.UpdateStateMachineAlias(
		input.StateMachineAliasArn, input.Description, input.RoutingConfiguration,
	)
	if err != nil {
		return nil, err
	}

	return newStateMachineAliasEntry(a), nil
}

func (h *Handler) handleDeleteStateMachineAlias(b []byte) (any, error) {
	var input deleteStateMachineAliasInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	if err := h.Backend.DeleteStateMachineAlias(input.StateMachineAliasArn); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) handleDescribeStateMachineAlias(b []byte) (any, error) {
	var input describeStateMachineAliasInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	a, err := h.Backend.DescribeStateMachineAlias(input.StateMachineAliasArn)
	if err != nil {
		return nil, err
	}

	return newStateMachineAliasEntry(a), nil
}

func (h *Handler) handleListStateMachineAliases(b []byte) (any, error) {
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

	entries := make([]stateMachineAliasEntry, len(aliases))
	for i := range aliases {
		entries[i] = *newStateMachineAliasEntry(&aliases[i])
	}

	return &listStateMachineAliasesOutput{
		StateMachineAliases: entries,
		NextToken:           next,
	}, nil
}
