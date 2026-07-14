package glue

import (
	"context"
)

// createIdentityCenterConfigurationInput holds input for CreateGlueIdentityCenterConfiguration.
type createIdentityCenterConfigurationInput struct {
	InstanceArn string `json:"InstanceArn,omitempty"`
}

func (h *Handler) handleCreateGlueIdentityCenterConfiguration(
	_ context.Context,
	in *createIdentityCenterConfigurationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.CreateGlueIdentityCenterConfiguration(in.InstanceArn)
}

// deleteIdentityCenterConfigurationInput holds input for DeleteGlueIdentityCenterConfiguration.
type deleteIdentityCenterConfigurationInput struct{}

func (h *Handler) handleDeleteGlueIdentityCenterConfiguration(
	_ context.Context,
	_ *deleteIdentityCenterConfigurationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteGlueIdentityCenterConfiguration()
}

// getIdentityCenterConfigurationInput holds input for GetGlueIdentityCenterConfiguration.
type getIdentityCenterConfigurationInput struct{}

// getIdentityCenterConfigurationOutput holds the result for GetGlueIdentityCenterConfiguration.
type getIdentityCenterConfigurationOutput struct {
	InstanceArn string `json:"InstanceArn"`
}

func (h *Handler) handleGetGlueIdentityCenterConfiguration(
	_ context.Context,
	_ *getIdentityCenterConfigurationInput,
) (*getIdentityCenterConfigurationOutput, error) {
	cfg, _ := h.Backend.GetGlueIdentityCenterConfiguration()
	if cfg == nil {
		return &getIdentityCenterConfigurationOutput{}, nil
	}

	return &getIdentityCenterConfigurationOutput{InstanceArn: cfg.InstanceARN}, nil
}

// updateIdentityCenterConfigurationInput holds input for UpdateGlueIdentityCenterConfiguration.
type updateIdentityCenterConfigurationInput struct {
	InstanceArn string `json:"InstanceArn,omitempty"`
}

func (h *Handler) handleUpdateGlueIdentityCenterConfiguration(
	_ context.Context,
	in *updateIdentityCenterConfigurationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateGlueIdentityCenterConfiguration(in.InstanceArn)
}
