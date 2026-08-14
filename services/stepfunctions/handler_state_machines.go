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
	VersionDescription      string                   `json:"versionDescription,omitempty"`
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
	VersionDescription      string                   `json:"versionDescription,omitempty"`
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
	StateMachineArn string `json:"stateMachineArn"`
	// StateMachineVersionArn is set only when Publish=true ("If you do not
	// set the publish parameter to true, this field returns null value").
	StateMachineVersionArn string  `json:"stateMachineVersionArn,omitempty"`
	CreationDate           float64 `json:"creationDate"`
}

type deleteStateMachineOutput struct{}

type listStateMachinesOutput struct {
	NextToken     string                 `json:"nextToken,omitempty"`
	StateMachines []stateMachineListItem `json:"stateMachines"`
}

// stateMachineListItem mirrors AWS's StateMachineListItem, which -- unlike
// the full StateMachine shape DescribeStateMachine returns -- carries only
// the four fields below (types.go, sfn@v1.45.4): no definition, roleArn,
// status, revisionId, updatedDate, or tracing/logging/encryption config.
type stateMachineListItem struct {
	Name            string  `json:"name"`
	StateMachineArn string  `json:"stateMachineArn"`
	Type            string  `json:"type"`
	CreationDate    float64 `json:"creationDate"`
}

func newStateMachineListItem(sm *StateMachine) stateMachineListItem {
	return stateMachineListItem{
		CreationDate:    sm.CreationDate,
		Name:            sm.Name,
		StateMachineArn: sm.StateMachineArn,
		Type:            sm.Type,
	}
}

type updateStateMachineOutput struct {
	RevisionID             string  `json:"revisionId,omitempty"`
	StateMachineVersionArn string  `json:"stateMachineVersionArn,omitempty"`
	UpdateDate             float64 `json:"updateDate"`
}

// createStateMachineAction handles CreateStateMachine and applies tracing/logging/encryption
// configuration and inline tags when supplied in the request body.
func (h *Handler) createStateMachineAction(ctx context.Context, b []byte) (any, error) {
	var input createStateMachineInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	// AWS: "You can only set the description if the publish parameter is
	// set to true. Otherwise ... this API action throws ValidationException."
	if input.VersionDescription != "" && !input.Publish {
		return nil, fmt.Errorf(
			"%w: versionDescription requires publish to be true", ErrValidation,
		)
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

	out := &createStateMachineOutput{
		StateMachineArn: sm.StateMachineArn,
		CreationDate:    sm.CreationDate,
	}

	// When publish=true, immediately publish a version of the new state
	// machine and echo its ARN back (AWS: null unless publish=true).
	if input.Publish {
		v, pubErr := h.Backend.PublishStateMachineVersion(sm.StateMachineArn, input.VersionDescription, "")
		if pubErr != nil {
			return nil, pubErr
		}

		out.StateMachineVersionArn = v.StateMachineVersionArn
	}

	return out, nil
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

	if input.VersionDescription != "" && !input.Publish {
		return nil, fmt.Errorf(
			"%w: versionDescription requires publish to be true", ErrValidation,
		)
	}

	updateDate, revisionID, err := h.Backend.UpdateStateMachine(
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

	out := &updateStateMachineOutput{UpdateDate: updateDate, RevisionID: revisionID}

	// When publish=true, immediately publish a version of the updated state
	// machine and echo its ARN back.
	if input.Publish {
		v, pubErr := h.Backend.PublishStateMachineVersion(
			input.StateMachineArn, input.VersionDescription, revisionID,
		)
		if pubErr != nil {
			return nil, pubErr
		}

		out.StateMachineVersionArn = v.StateMachineVersionArn
	}

	return out, nil
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
