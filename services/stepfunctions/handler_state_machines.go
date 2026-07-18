package stepfunctions

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
)

type createStateMachineInput struct {
	TracingConfiguration    *TracingConfiguration    `json:"tracingConfiguration,omitempty"`
	LoggingConfiguration    *LoggingConfiguration    `json:"loggingConfiguration,omitempty"`
	EncryptionConfiguration *EncryptionConfiguration `json:"encryptionConfiguration,omitempty"`
	Name                    string                   `json:"name"`
	Definition              string                   `json:"definition"`
	RoleArn                 string                   `json:"roleArn"`
	Type                    string                   `json:"type"`
	Tags                    []sfnTagEntry            `json:"tags,omitempty"`
	Publish                 bool                     `json:"publish,omitempty"`
}

type deleteStateMachineInput struct {
	StateMachineArn string `json:"stateMachineArn"`
}

type updateStateMachineInput struct {
	TracingConfiguration    *TracingConfiguration    `json:"tracingConfiguration,omitempty"`
	LoggingConfiguration    *LoggingConfiguration    `json:"loggingConfiguration,omitempty"`
	EncryptionConfiguration *EncryptionConfiguration `json:"encryptionConfiguration,omitempty"`
	StateMachineArn         string                   `json:"stateMachineArn"`
	Definition              string                   `json:"definition"`
	RoleArn                 string                   `json:"roleArn"`
	Publish                 bool                     `json:"publish,omitempty"`
}

type listStateMachinesInput struct {
	NextToken  string `json:"nextToken"`
	MaxResults int    `json:"maxResults"`
}

type describeStateMachineInput struct {
	StateMachineArn string `json:"stateMachineArn"`
}

type createStateMachineOutput struct {
	StateMachineArn string  `json:"stateMachineArn"`
	CreationDate    float64 `json:"creationDate"`
}

type deleteStateMachineOutput struct{}

type listStateMachinesOutput struct {
	NextToken     string         `json:"nextToken,omitempty"`
	StateMachines []StateMachine `json:"stateMachines"`
}

type updateStateMachineOutput struct {
	UpdateDate float64 `json:"updateDate"`
}

// createStateMachineAction handles CreateStateMachine and applies tracing/logging/encryption
// configuration and inline tags when supplied in the request body.
func (h *Handler) createStateMachineAction(ctx context.Context, b []byte) (any, error) {
	var input createStateMachineInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	sm, err := h.Backend.CreateStateMachine(
		ctx,
		input.Name,
		input.Definition,
		input.RoleArn,
		input.Type,
	)
	if err != nil {
		return nil, err
	}

	if input.TracingConfiguration != nil || input.LoggingConfiguration != nil ||
		input.EncryptionConfiguration != nil {
		if cfgErr := h.Backend.SetStateMachineConfigurations(
			sm.StateMachineArn, input.TracingConfiguration, input.LoggingConfiguration, input.EncryptionConfiguration,
		); cfgErr != nil {
			return nil, cfgErr
		}
	}

	// Apply inline tags when provided.
	if len(input.Tags) > 0 {
		kv := make(map[string]string, len(input.Tags))
		for _, t := range input.Tags {
			kv[t.Key] = t.Value
		}
		h.setTags(sm.StateMachineArn, kv)
	}

	// When publish=true, immediately publish a version of the new state machine.
	if input.Publish {
		_, _ = h.Backend.PublishStateMachineVersion(sm.StateMachineArn, "", "")
	}

	return &createStateMachineOutput{
		StateMachineArn: sm.StateMachineArn,
		CreationDate:    sm.CreationDate,
	}, nil
}

// updateStateMachineAction handles UpdateStateMachine and applies tracing/logging/encryption
// configuration when supplied in the request body.
func (h *Handler) updateStateMachineAction(b []byte) (any, error) {
	var input updateStateMachineInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	if input.StateMachineArn == "" {
		return nil, fmt.Errorf("%w: stateMachineArn must not be empty", ErrValidation)
	}

	updateDate, err := h.Backend.UpdateStateMachine(
		input.StateMachineArn,
		input.Definition,
		input.RoleArn,
	)
	if err != nil {
		return nil, err
	}

	if input.TracingConfiguration != nil || input.LoggingConfiguration != nil ||
		input.EncryptionConfiguration != nil {
		if cfgErr := h.Backend.SetStateMachineConfigurations(
			input.StateMachineArn,
			input.TracingConfiguration,
			input.LoggingConfiguration,
			input.EncryptionConfiguration,
		); cfgErr != nil {
			return nil, cfgErr
		}
	}

	// When publish=true, immediately publish a version of the updated state machine.
	if input.Publish {
		_, _ = h.Backend.PublishStateMachineVersion(input.StateMachineArn, "", "")
	}

	return &updateStateMachineOutput{UpdateDate: updateDate}, nil
}

func (h *Handler) stateMachineActions() map[string]actionFn {
	m := map[string]actionFn{
		// CreateStateMachine and ListStateMachines are handled in dispatch (ctx-aware).
		"DeleteStateMachine": func(b []byte) (any, error) {
			var input deleteStateMachineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteStateMachine(input.StateMachineArn); err != nil {
				return nil, err
			}

			// Clean up tags for the deleted state machine.
			h.tagsMu.Lock("DeleteStateMachine")
			if t, ok := h.tags[input.StateMachineArn]; ok {
				t.Close()
				delete(h.tags, input.StateMachineArn)
			}
			h.tagsMu.Unlock()

			return &deleteStateMachineOutput{}, nil
		},
		"DescribeStateMachine": func(b []byte) (any, error) {
			var input describeStateMachineInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeStateMachine(input.StateMachineArn)
		},
		"UpdateStateMachine": h.updateStateMachineAction,
	}
	maps.Copy(m, h.stateMachineTagActions())
	maps.Copy(m, h.versionActions())
	maps.Copy(m, h.aliasActions())

	return m
}
