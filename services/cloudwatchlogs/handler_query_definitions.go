package cloudwatchlogs

import (
	"context"
	"encoding/json"
)

// --- PutQueryDefinition ---.
type putQueryDefinitionInput struct {
	Name              string           `json:"name"`
	QueryDefinitionID string           `json:"queryDefinitionId"`
	QueryString       string           `json:"queryString"`
	LogGroupNames     []string         `json:"logGroupNames"`
	Parameters        []QueryParameter `json:"parameters,omitempty"`
}

type putQueryDefinitionOutput struct {
	QueryDefinitionID string `json:"queryDefinitionId,omitempty"`
}

// --- DescribeQueryDefinitions ---.
type describeQueryDefinitionsInput struct {
	QueryDefinitionNamePrefix string `json:"queryDefinitionNamePrefix"`
	NextToken                 string `json:"nextToken"`
	MaxResults                int    `json:"maxResults"`
}

type describeQueryDefinitionsOutput struct {
	NextToken        string            `json:"nextToken,omitempty"`
	QueryDefinitions []QueryDefinition `json:"queryDefinitions"`
}

// --- DeleteQueryDefinition ---.
type deleteQueryDefinitionInput struct {
	QueryDefinitionID string `json:"queryDefinitionId"`
}

type deleteQueryDefinitionOutput struct {
	Success bool `json:"success"`
}

func (h *Handler) handlePutQueryDefinition(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input putQueryDefinitionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	id, err := h.Backend.PutQueryDefinition(
		input.Name, input.QueryString, input.QueryDefinitionID, input.LogGroupNames, input.Parameters,
	)
	if err != nil {
		return nil, err
	}

	return &putQueryDefinitionOutput{QueryDefinitionID: id}, nil
}

func (h *Handler) handleDescribeQueryDefinitions(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input describeQueryDefinitionsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	defs, next, err := h.Backend.DescribeQueryDefinitions(
		input.QueryDefinitionNamePrefix,
		input.MaxResults,
		input.NextToken,
	)
	if err != nil {
		return nil, err
	}

	return &describeQueryDefinitionsOutput{QueryDefinitions: defs, NextToken: next}, nil
}

func (h *Handler) handleDeleteQueryDefinition(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input deleteQueryDefinitionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.DeleteQueryDefinition(input.QueryDefinitionID); err != nil {
		return nil, err
	}

	return &deleteQueryDefinitionOutput{Success: true}, nil
}
