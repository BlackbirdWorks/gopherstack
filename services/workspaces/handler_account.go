package workspaces

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
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

// accountModificationResp mirrors the real AccountModification shape;
// StartTime is a wire-format epoch-seconds number.
type accountModificationResp struct {
	ModificationState                   string  `json:"ModificationState,omitempty"`
	DedicatedTenancySupport             string  `json:"DedicatedTenancySupport,omitempty"`
	DedicatedTenancyManagementCidrRange string  `json:"DedicatedTenancyManagementCidrRange,omitempty"`
	StartTime                           float64 `json:"StartTime,omitempty"`
}

type describeAccountModificationsInput struct {
	NextToken string `json:"NextToken"`
}

type describeAccountModificationsOutput struct {
	NextToken            string                    `json:"NextToken,omitempty"`
	AccountModifications []accountModificationResp `json:"AccountModifications"`
}

func (h *Handler) handleDescribeAccountModifications(
	_ context.Context, req *describeAccountModificationsInput,
) (*describeAccountModificationsOutput, error) {
	mods, nextToken, err := h.Backend.DescribeAccountModifications(req.NextToken)
	if err != nil {
		return nil, err
	}

	items := make([]accountModificationResp, 0, len(mods))
	for _, m := range mods {
		items = append(items, accountModificationResp{
			ModificationState:                   m.ModificationState,
			DedicatedTenancySupport:             m.DedicatedTenancySupport,
			DedicatedTenancyManagementCidrRange: m.DedicatedTenancyManagementCidrRange,
			StartTime:                           awstime.Epoch(m.StartTime),
		})
	}

	return &describeAccountModificationsOutput{
		AccountModifications: items,
		NextToken:            nextToken,
	}, nil
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
	_ context.Context, req *listAvailableManagementCidrRangesInput,
) (*listAvailableManagementCidrRangesOutput, error) {
	ranges, nextToken, err := h.Backend.ListAvailableManagementCidrRanges(
		req.ManagementCidrRangeConstraint, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	return &listAvailableManagementCidrRangesOutput{
		ManagementCidrRanges: ranges,
		NextToken:            nextToken,
	}, nil
}
