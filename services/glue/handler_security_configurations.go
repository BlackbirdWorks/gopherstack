package glue

import (
	"context"
)

// createSecurityConfigurationInput holds input for CreateSecurityConfiguration.
type createSecurityConfigurationInput struct {
	Name                    string                  `json:"Name"`
	EncryptionConfiguration EncryptionConfiguration `json:"EncryptionConfiguration"`
}

// createSecurityConfigurationOutput holds the result for CreateSecurityConfiguration.
type createSecurityConfigurationOutput struct {
	Name      string  `json:"Name"`
	CreatedOn float64 `json:"CreatedOn"`
}

func (h *Handler) handleCreateSecurityConfiguration(
	_ context.Context,
	in *createSecurityConfigurationInput,
) (*createSecurityConfigurationOutput, error) {
	sc, err := h.Backend.CreateSecurityConfiguration(in.Name, in.EncryptionConfiguration)
	if err != nil {
		return nil, err
	}

	return &createSecurityConfigurationOutput{Name: sc.Name, CreatedOn: sc.CreatedTimeStamp}, nil
}

// deleteSecurityConfigurationInput holds input for DeleteSecurityConfiguration.
type deleteSecurityConfigurationInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteSecurityConfiguration(
	_ context.Context,
	in *deleteSecurityConfigurationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteSecurityConfiguration(in.Name)
}

// getSecurityConfigurationInput holds input for GetSecurityConfiguration.
type getSecurityConfigurationInput struct {
	Name string `json:"Name"`
}

// getSecurityConfigurationOutput holds the result for GetSecurityConfiguration.
type getSecurityConfigurationOutput struct {
	SecurityConfiguration *SecurityConfiguration `json:"SecurityConfiguration"`
}

func (h *Handler) handleGetSecurityConfiguration(
	_ context.Context,
	in *getSecurityConfigurationInput,
) (*getSecurityConfigurationOutput, error) {
	sc, err := h.Backend.GetSecurityConfiguration(in.Name)
	if err != nil {
		return nil, err
	}

	return &getSecurityConfigurationOutput{SecurityConfiguration: sc}, nil
}

// getSecurityConfigurationsInput holds input for GetSecurityConfigurations.
type getSecurityConfigurationsInput struct{}

// getSecurityConfigurationsOutput holds the result for GetSecurityConfigurations.
type getSecurityConfigurationsOutput struct {
	SecurityConfigurations []*SecurityConfiguration `json:"SecurityConfigurations"`
}

func (h *Handler) handleGetSecurityConfigurations(
	_ context.Context,
	_ *getSecurityConfigurationsInput,
) (*getSecurityConfigurationsOutput, error) {
	configs := h.Backend.ListSecurityConfigurations()
	if configs == nil {
		configs = []*SecurityConfiguration{}
	}

	return &getSecurityConfigurationsOutput{SecurityConfigurations: configs}, nil
}
