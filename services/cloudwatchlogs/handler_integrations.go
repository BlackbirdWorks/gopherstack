package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"fmt"
)

type associateSourceToS3TableIntegrationInput struct {
	DataSource     map[string]any `json:"dataSource"`
	IntegrationArn string         `json:"integrationArn"`
}

type associateSourceToS3TableIntegrationOutput struct {
	Identifier string `json:"identifier,omitempty"`
}

func (h *Handler) handleAssociateSourceToS3TableIntegration(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input associateSourceToS3TableIntegrationInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	var dataSourceName, dataSourceType string
	if input.DataSource != nil {
		if v, ok := input.DataSource["name"].(string); ok {
			dataSourceName = v
		}
		if v, ok := input.DataSource["type"].(string); ok {
			dataSourceType = v
		}
	}

	id, err := h.Backend.AssociateSourceToS3TableIntegration(input.IntegrationArn, dataSourceName, dataSourceType)
	if err != nil {
		return nil, err
	}

	return &associateSourceToS3TableIntegrationOutput{Identifier: id}, nil
}

type putIntegrationInput struct {
	IntegrationName string `json:"integrationName"`
	IntegrationType string `json:"integrationType"`
}

func (h *Handler) handlePutIntegration(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putIntegrationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		ig, err := b.PutIntegration(in.IntegrationName, in.IntegrationType)
		if err != nil {
			return nil, err
		}

		return map[string]any{
			keyIntegrationName:   ig.Name,
			keyIntegrationStatus: ig.Status,
		}, nil
	}

	return map[string]any{
		keyIntegrationName:   in.IntegrationName,
		keyIntegrationStatus: completenessStatusActive,
	}, nil
}

type getIntegrationInput struct {
	IntegrationName string `json:"integrationName"`
}

func (h *Handler) handleGetIntegration(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in getIntegrationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		ig, err := b.GetIntegration(in.IntegrationName)
		if err != nil {
			return nil, err
		}

		return map[string]any{
			keyIntegrationName:   ig.Name,
			keyIntegrationType:   ig.Type,
			keyIntegrationStatus: ig.Status,
		}, nil
	}

	return map[string]any{
		keyIntegrationName:   "",
		keyIntegrationType:   "",
		keyIntegrationStatus: completenessStatusActive,
	}, nil
}

func (h *Handler) handleListIntegrations(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	if b := cwlBackend(h); b != nil {
		igs := b.ListIntegrations()
		out := make([]map[string]any, 0, len(igs))
		for _, ig := range igs {
			out = append(out, map[string]any{
				keyIntegrationName:   ig.Name,
				keyIntegrationType:   ig.Type,
				keyIntegrationStatus: ig.Status,
			})
		}

		return map[string]any{"integrationSummaries": out}, nil
	}

	return map[string]any{"integrationSummaries": []any{}}, nil
}

type deleteIntegrationInput struct {
	IntegrationName string `json:"integrationName"`
}

func (h *Handler) handleDeleteIntegration(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in deleteIntegrationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.DeleteIntegration(in.IntegrationName); err != nil {
			return nil, err
		}
	}

	return struct{}{}, nil
}

func (h *Handler) handleDisassociateSourceFromS3TableIntegration(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	return struct{}{}, nil
}

func (h *Handler) handleListSourcesForS3TableIntegration(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	return map[string]any{"sources": []any{}}, nil
}
