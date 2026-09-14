package fsx

import "context"

// --- DescribeSharedVpcConfiguration ---

type describeSharedVpcConfigurationInput struct{}

type describeSharedVpcConfigurationOutput struct {
	EnableFsxRouteTableUpdatesFromParticipantAccounts string `json:"EnableFsxRouteTableUpdatesFromParticipantAccounts"`
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
		EnableFsxRouteTableUpdatesFromParticipantAccounts: cfg.EnableFsxRouteTableUpdatesFromParticipantAccounts,
	}, nil
}

// --- UpdateSharedVpcConfiguration ---

type updateSharedVpcConfigurationOutput struct {
	EnableFsxRouteTableUpdatesFromParticipantAccounts string `json:"EnableFsxRouteTableUpdatesFromParticipantAccounts"`
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
		EnableFsxRouteTableUpdatesFromParticipantAccounts: cfg.EnableFsxRouteTableUpdatesFromParticipantAccounts,
	}, nil
}
