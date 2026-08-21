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

// openSearchResourceConfigInput mirrors types.OpenSearchResourceConfig
// (types.go:1977).
type openSearchResourceConfigInput struct {
	DataSourceRoleArn         *string  `json:"dataSourceRoleArn"`
	ApplicationArn            *string  `json:"applicationArn,omitempty"`
	KmsKeyArn                 *string  `json:"kmsKeyArn,omitempty"`
	RetentionDays             *int32   `json:"retentionDays"`
	DashboardViewerPrincipals []string `json:"dashboardViewerPrincipals"`
}

// resourceConfigInput mirrors the types.ResourceConfig union (types.go:2696),
// whose only member is openSearchResourceConfig -- unions serialize as a
// single-key object naming the active member under the JSON-RPC 1.1
// protocol (verified against awsAwsjson11_serializeDocumentResourceConfig,
// serializers.go).
type resourceConfigInput struct {
	OpenSearchResourceConfig *openSearchResourceConfigInput `json:"openSearchResourceConfig,omitempty"`
}

type putIntegrationInput struct {
	ResourceConfig  *resourceConfigInput `json:"resourceConfig"`
	IntegrationName string               `json:"integrationName"`
	IntegrationType string               `json:"integrationType"`
}

func (h *Handler) handlePutIntegration(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in putIntegrationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	// ResourceConfig is a required PutIntegrationInput member (verified
	// against validateOpPutIntegrationInput, validators.go) that the
	// pre-fix request never read at all.
	if in.ResourceConfig == nil {
		return nil, fmt.Errorf("%w: resourceConfig is required", ErrValidation)
	}

	osConfig, err := resourceConfigFromWire(in.ResourceConfig)
	if err != nil {
		return nil, err
	}

	if b := cwlBackend(h); b != nil {
		ig, putErr := b.PutIntegration(in.IntegrationName, in.IntegrationType, osConfig)
		if putErr != nil {
			return nil, putErr
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

// resourceConfigFromWire validates and converts a decoded resourceConfig
// wire object into the domain OpenSearchResourceConfig. DataSourceRoleArn/
// DashboardViewerPrincipals/RetentionDays are required whenever the
// OpenSearch branch is present (validateOpenSearchResourceConfig, validators.go).
func resourceConfigFromWire(in *resourceConfigInput) (*OpenSearchResourceConfig, error) {
	osIn := in.OpenSearchResourceConfig
	if osIn == nil {
		return nil, fmt.Errorf("%w: resourceConfig.openSearchResourceConfig is required", ErrValidation)
	}

	if osIn.DataSourceRoleArn == nil || *osIn.DataSourceRoleArn == "" {
		return nil, fmt.Errorf(
			"%w: resourceConfig.openSearchResourceConfig.dataSourceRoleArn is required", ErrValidation,
		)
	}

	if osIn.DashboardViewerPrincipals == nil {
		return nil, fmt.Errorf(
			"%w: resourceConfig.openSearchResourceConfig.dashboardViewerPrincipals is required", ErrValidation,
		)
	}

	if osIn.RetentionDays == nil {
		return nil, fmt.Errorf(
			"%w: resourceConfig.openSearchResourceConfig.retentionDays is required", ErrValidation,
		)
	}

	cfg := &OpenSearchResourceConfig{
		DataSourceRoleArn:         *osIn.DataSourceRoleArn,
		DashboardViewerPrincipals: osIn.DashboardViewerPrincipals,
		RetentionDays:             osIn.RetentionDays,
	}
	if osIn.ApplicationArn != nil {
		cfg.ApplicationArn = *osIn.ApplicationArn
	}

	if osIn.KmsKeyArn != nil {
		cfg.KmsKeyArn = *osIn.KmsKeyArn
	}

	return cfg, nil
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

type disassociateSourceFromS3TableIntegrationInput struct {
	Identifier string `json:"identifier"`
}

type disassociateSourceFromS3TableIntegrationOutput struct {
	Identifier string `json:"identifier,omitempty"`
}

func (h *Handler) handleDisassociateSourceFromS3TableIntegration(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var in disassociateSourceFromS3TableIntegrationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if err := h.Backend.DisassociateSourceFromS3TableIntegration(in.Identifier); err != nil {
		return nil, err
	}

	return &disassociateSourceFromS3TableIntegrationOutput{Identifier: in.Identifier}, nil
}

func (h *Handler) handleListSourcesForS3TableIntegration(
	ctx context.Context, //nolint:revive // existing issue.
	_ []byte,
) (any, error) {
	return map[string]any{"sources": []any{}}, nil
}
