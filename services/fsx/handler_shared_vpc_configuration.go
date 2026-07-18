package fsx

import "context"

// --- DescribeSharedVpcConfiguration ---

type describeSharedVpcConfigurationInput struct{}

type describeSharedVpcConfigurationOutput struct {
	EnableSharedVpcOnFileSystemCreation string `json:"EnableSharedVpcOnFileSystemCreation"`
}

func (h *Handler) handleDescribeSharedVpcConfiguration(
	_ context.Context,
	_ *describeSharedVpcConfigurationInput,
) (*describeSharedVpcConfigurationOutput, error) {
	cfg, err := h.Backend.DescribeSharedVpcConfiguration()
	if err != nil {
		return nil, err
	}

	return &describeSharedVpcConfigurationOutput{
		EnableSharedVpcOnFileSystemCreation: cfg.EnableSharedVpcOnFileSystemCreation,
	}, nil
}

// --- UpdateSharedVpcConfiguration ---

type updateSharedVpcConfigurationOutput struct {
	EnableSharedVpcOnFileSystemCreation string `json:"EnableSharedVpcOnFileSystemCreation"`
}

func (h *Handler) handleUpdateSharedVpcConfiguration(
	_ context.Context,
	in *updateSharedVpcConfigurationInput,
) (*updateSharedVpcConfigurationOutput, error) {
	cfg, err := h.Backend.UpdateSharedVpcConfiguration(in)
	if err != nil {
		return nil, err
	}

	return &updateSharedVpcConfigurationOutput{
		EnableSharedVpcOnFileSystemCreation: cfg.EnableSharedVpcOnFileSystemCreation,
	}, nil
}
