package stepfunctions

import (
	"encoding/json"
)

type publishStateMachineVersionInput struct {
	StateMachineArn string `json:"stateMachineArn"`
	Description     string `json:"description"`
	RevisionID      string `json:"revisionId"`
}

type describeStateMachineVersionInput struct {
	StateMachineVersionArn string `json:"stateMachineVersionArn"`
}

type deleteStateMachineVersionInput struct {
	StateMachineVersionArn string `json:"stateMachineVersionArn"`
}

type listStateMachineVersionsInput struct {
	StateMachineArn string `json:"stateMachineArn"`
	NextToken       string `json:"nextToken"`
	MaxResults      int    `json:"maxResults"`
}

type listStateMachineVersionsOutput struct {
	NextToken            string                `json:"nextToken,omitempty"`
	StateMachineVersions []StateMachineVersion `json:"stateMachineVersions"`
}

// versionActions returns handler functions for state machine version operations.
func (h *Handler) versionActions() map[string]actionFn {
	return map[string]actionFn{
		"PublishStateMachineVersion": func(b []byte) (any, error) {
			var input publishStateMachineVersionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.PublishStateMachineVersion(
				input.StateMachineArn,
				input.Description,
				input.RevisionID,
			)
		},
		"DescribeStateMachineVersion": func(b []byte) (any, error) {
			var input describeStateMachineVersionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeStateMachineVersion(input.StateMachineVersionArn)
		},
		"DeleteStateMachineVersion": func(b []byte) (any, error) {
			var input deleteStateMachineVersionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteStateMachineVersion(input.StateMachineVersionArn); err != nil {
				return nil, err
			}

			return map[string]any{}, nil
		},
		"ListStateMachineVersions": func(b []byte) (any, error) {
			var input listStateMachineVersionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			versions, next, err := h.Backend.ListStateMachineVersions(
				input.StateMachineArn, input.NextToken, input.MaxResults,
			)
			if err != nil {
				return nil, err
			}

			return &listStateMachineVersionsOutput{
				StateMachineVersions: versions,
				NextToken:            next,
			}, nil
		},
	}
}
