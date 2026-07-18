package workspaces

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// buildAccountOps returns the map of account-level operations.
func (h *Handler) buildAccountOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"DescribeAccount":              service.WrapOp(h.handleDescribeAccount),
		"DescribeAccountModifications": service.WrapOp(h.handleDescribeAccountModifications),
		"ModifyAccount":                service.WrapOp(h.handleModifyAccount),
		"ListAvailableManagementCidrRanges": service.WrapOp(
			h.handleListAvailableManagementCidrRanges,
		),
	}
}

type describeAccountOutput struct {
	DedicatedTenancySupport             string `json:"DedicatedTenancySupport"`
	DedicatedTenancyManagementCidrRange string `json:"DedicatedTenancyManagementCidrRange,omitempty"`
}

func (h *Handler) handleDescribeAccount(
	_ context.Context,
	_ *emptyOutput,
) (*describeAccountOutput, error) {
	cfg := h.Backend.DescribeAccount()

	return &describeAccountOutput{
		DedicatedTenancySupport:             cfg.DedicatedTenancySupport,
		DedicatedTenancyManagementCidrRange: cfg.ManagementCidrRange,
	}, nil
}

type describeAccountModificationsOutput struct {
	NextToken            string `json:"NextToken,omitempty"`
	AccountModifications []any  `json:"AccountModifications"`
}

func (h *Handler) handleDescribeAccountModifications(
	_ context.Context, _ *emptyOutput,
) (*describeAccountModificationsOutput, error) {
	return &describeAccountModificationsOutput{AccountModifications: []any{}}, nil
}

type modifyAccountInput struct {
	DedicatedTenancyManagementCidrRange string `json:"DedicatedTenancyManagementCidrRange"`
	DedicatedTenancySupport             string `json:"DedicatedTenancySupport"`
}

func (h *Handler) handleModifyAccount(
	_ context.Context,
	req *modifyAccountInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.ModifyAccount(
		req.DedicatedTenancyManagementCidrRange, req.DedicatedTenancySupport,
	)
}

type listAvailableManagementCidrRangesInput struct {
	ManagementCidrRangeConstraint string `json:"ManagementCidrRangeConstraint"`
	NextToken                     string `json:"NextToken"`
	MaxResults                    int32  `json:"MaxResults"`
}

type listAvailableManagementCidrRangesOutput struct {
	NextToken            string   `json:"NextToken,omitempty"`
	ManagementCidrRanges []string `json:"ManagementCidrRanges"`
}

func (h *Handler) handleListAvailableManagementCidrRanges(
	_ context.Context, _ *listAvailableManagementCidrRangesInput,
) (*listAvailableManagementCidrRangesOutput, error) {
	return &listAvailableManagementCidrRangesOutput{
		ManagementCidrRanges: []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"},
	}, nil
}
