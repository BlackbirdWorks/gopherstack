package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"fmt"
)

type putTransformerInput struct {
	LogGroupIdentifier string           `json:"logGroupIdentifier"`
	TransformerConfig  []map[string]any `json:"transformerConfig"`
}

func (h *Handler) handlePutTransformer(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putTransformerInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.PutTransformer(in.LogGroupIdentifier, in.TransformerConfig); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

type getTransformerInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
}

func (h *Handler) handleGetTransformer(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in getTransformerInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		t, err := b.GetTransformer(in.LogGroupIdentifier)
		if err != nil {
			return nil, err
		}

		return map[string]any{
			completenessKeyLogGroupIdentifier: t.LogGroupIdentifier,
			"transformerConfig":               t.Processors,
		}, nil
	}

	return map[string]any{
		completenessKeyLogGroupIdentifier: in.LogGroupIdentifier,
		"transformerConfig":               []any{},
	}, nil
}

type deleteTransformerInput struct {
	LogGroupIdentifier string `json:"logGroupIdentifier"`
}

func (h *Handler) handleDeleteTransformer(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteTransformerInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteTransformer(in.LogGroupIdentifier); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

// handleTestTransformer applies the supplied transformer config to the supplied
// sample log event messages and returns the deterministically transformed
// results.
func (h *Handler) handleTestTransformer(
	_ context.Context,
	body []byte,
) (any, error) {
	var input struct {
		LogGroupIdentifier string           `json:"logGroupIdentifier"`
		TransformerConfig  []map[string]any `json:"transformerConfig"`
		LogEventMessages   []string         `json:"logEventMessages"`
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return nil, err
		}
	}

	if len(input.LogEventMessages) == 0 {
		return nil, fmt.Errorf("%w: logEventMessages is required", ErrValidation)
	}

	transformed := ApplyTransformer(input.LogEventMessages, input.TransformerConfig)

	return map[string]any{"transformedLogs": transformed}, nil
}
