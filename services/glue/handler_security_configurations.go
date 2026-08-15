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

// defaultGetSecurityConfigurationsLimit is used when
// GetSecurityConfigurationsInput.MaxResults is unset.
const defaultGetSecurityConfigurationsLimit = 100

// getSecurityConfigurationsInput holds input for GetSecurityConfigurations.
type getSecurityConfigurationsInput struct {
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int32  `json:"MaxResults,omitempty"`
}

// getSecurityConfigurationsOutput holds the result for GetSecurityConfigurations.
type getSecurityConfigurationsOutput struct {
	NextToken              string                   `json:"NextToken,omitempty"`
	SecurityConfigurations []*SecurityConfiguration `json:"SecurityConfigurations"`
}

func (h *Handler) handleGetSecurityConfigurations(
	_ context.Context,
	in *getSecurityConfigurationsInput,
) (*getSecurityConfigurationsOutput, error) {
	configs := h.Backend.ListSecurityConfigurations()

	limit := int(in.MaxResults)
	if limit <= 0 {
		limit = defaultGetSecurityConfigurationsLimit
	}

	page, next := paginateSlice(configs, in.NextToken, limit)
	if page == nil {
		page = []*SecurityConfiguration{}
	}

	return &getSecurityConfigurationsOutput{SecurityConfigurations: page, NextToken: next}, nil
}
