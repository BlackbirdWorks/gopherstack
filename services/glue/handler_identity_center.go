package glue

import (
	"context"
)

// createIdentityCenterConfigurationInput holds input for CreateGlueIdentityCenterConfiguration.
type createIdentityCenterConfigurationInput struct {
	InstanceArn string `json:"InstanceArn,omitempty"`
}

// createIdentityCenterConfigurationOutput holds the result for
// CreateGlueIdentityCenterConfiguration. Real
// CreateGlueIdentityCenterConfigurationOutput carries ApplicationArn
// (confirmed against
// awsAwsjson11_deserializeOpDocumentCreateGlueIdentityCenterConfigurationOutput
// in the pinned glue SDK's deserializers.go), not an empty envelope.
type createIdentityCenterConfigurationOutput struct {
	ApplicationArn string `json:"ApplicationArn,omitempty"`
}

func (h *Handler) handleCreateGlueIdentityCenterConfiguration(
	_ context.Context,
	in *createIdentityCenterConfigurationInput,
) (*createIdentityCenterConfigurationOutput, error) {
	cfg, err := h.Backend.CreateGlueIdentityCenterConfiguration(in.InstanceArn)
	if err != nil {
		return nil, err
	}

	return &createIdentityCenterConfigurationOutput{ApplicationArn: cfg.ApplicationARN}, nil
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

// getIdentityCenterConfigurationOutput holds the result for
// GetGlueIdentityCenterConfiguration. Real
// GetGlueIdentityCenterConfigurationOutput also carries ApplicationArn
// (confirmed against
// awsAwsjson11_deserializeOpDocumentGetGlueIdentityCenterConfigurationOutput
// in the pinned glue SDK's deserializers.go), the same field
// CreateGlueIdentityCenterConfigurationOutput carries.
type getIdentityCenterConfigurationOutput struct {
	InstanceArn    string `json:"InstanceArn"`
	ApplicationArn string `json:"ApplicationArn,omitempty"`
}

func (h *Handler) handleGetGlueIdentityCenterConfiguration(
	_ context.Context,
	_ *getIdentityCenterConfigurationInput,
) (*getIdentityCenterConfigurationOutput, error) {
	cfg, _ := h.Backend.GetGlueIdentityCenterConfiguration()
	if cfg == nil {
		return &getIdentityCenterConfigurationOutput{}, nil
	}

	return &getIdentityCenterConfigurationOutput{
		InstanceArn:    cfg.InstanceARN,
		ApplicationArn: cfg.ApplicationARN,
	}, nil
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
