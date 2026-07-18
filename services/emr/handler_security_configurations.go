package emr

import (
	"context"
)

// --- CreateSecurityConfiguration ---

type createSecurityConfigurationInput struct {
	Name                  string `json:"Name"`
	SecurityConfiguration string `json:"SecurityConfiguration"`
}

type createSecurityConfigurationOutput struct {
	Name string `json:"Name"`
	// CreationDateTime is epoch seconds (float64) -- the EMR awsjson1.1 wire
	// format; the real SDK deserializer parses it with
	// smithytime.ParseEpochSeconds and rejects RFC3339 strings.
	CreationDateTime float64 `json:"CreationDateTime"`
}

func (h *Handler) handleCreateSecurityConfiguration(
	ctx context.Context,
	in *createSecurityConfigurationInput,
) (*createSecurityConfigurationOutput, error) {
	sc, err := h.Backend.CreateSecurityConfiguration(ctx, in.Name, in.SecurityConfiguration)
	if err != nil {
		return nil, err
	}

	return &createSecurityConfigurationOutput{
		Name:             sc.Name,
		CreationDateTime: sc.CreationDateTime,
	}, nil
}

// --- DeleteSecurityConfiguration ---

type deleteSecurityConfigurationInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteSecurityConfiguration(
	ctx context.Context,
	in *deleteSecurityConfigurationInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteSecurityConfiguration(ctx, in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- DescribeSecurityConfiguration ---

type describeSecurityConfigurationInput struct {
	Name string `json:"Name"`
}

type describeSecurityConfigurationOutput struct {
	Name                  string `json:"Name"`
	SecurityConfiguration string `json:"SecurityConfiguration"`
	// CreationDateTime is epoch seconds (float64); see
	// createSecurityConfigurationOutput for why.
	CreationDateTime float64 `json:"CreationDateTime"`
}

func (h *Handler) handleDescribeSecurityConfiguration(
	ctx context.Context,
	in *describeSecurityConfigurationInput,
) (*describeSecurityConfigurationOutput, error) {
	sc, err := h.Backend.DescribeSecurityConfiguration(ctx, in.Name)
	if err != nil {
		return nil, err
	}

	return &describeSecurityConfigurationOutput{
		Name:                  sc.Name,
		SecurityConfiguration: sc.SecurityConfig,
		CreationDateTime:      sc.CreationDateTime,
	}, nil
}

// --- ListSecurityConfigurations ---

type listSecurityConfigurationsInput struct {
	Marker string `json:"Marker"`
}

type listSecurityConfigurationsOutput struct {
	Marker                 string                  `json:"Marker,omitempty"`
	SecurityConfigurations []SecurityConfigSummary `json:"SecurityConfigurations"`
}

func (h *Handler) handleListSecurityConfigurations(
	ctx context.Context,
	in *listSecurityConfigurationsInput,
) (*listSecurityConfigurationsOutput, error) {
	configs, nextMarker := h.Backend.ListSecurityConfigurations(ctx, in.Marker)

	return &listSecurityConfigurationsOutput{
		SecurityConfigurations: configs,
		Marker:                 nextMarker,
	}, nil
}
