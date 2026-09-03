package emr

import (
	"context"
)

// --- ListReleaseLabels ---

type listReleaseLabelsInput struct {
	Filters    listReleaseLabelFilters `json:"Filters"`
	NextToken  string                  `json:"NextToken"`
	MaxResults int                     `json:"MaxResults"`
}

type listReleaseLabelFilters struct {
	Prefix      string `json:"Prefix"`
	Application string `json:"Application"`
}

type listReleaseLabelsOutput struct {
	NextToken     string   `json:"NextToken,omitempty"`
	ReleaseLabels []string `json:"ReleaseLabels"`
}

func (h *Handler) handleListReleaseLabels(
	ctx context.Context,
	in *listReleaseLabelsInput,
) (*listReleaseLabelsOutput, error) {
	labels, next := h.Backend.ListReleaseLabels(
		ctx, in.Filters.Prefix, in.Filters.Application, in.NextToken, in.MaxResults,
	)

	return &listReleaseLabelsOutput{ReleaseLabels: labels, NextToken: next}, nil
}

// --- DescribeReleaseLabel ---

type describeReleaseLabelInput struct {
	ReleaseLabel string `json:"ReleaseLabel"`
}

type describeReleaseLabelOutput struct {
	ReleaseLabel string                    `json:"ReleaseLabel"`
	Applications []ReleaseLabelApplication `json:"Applications,omitempty"`
}

func (h *Handler) handleDescribeReleaseLabel(
	ctx context.Context,
	in *describeReleaseLabelInput,
) (*describeReleaseLabelOutput, error) {
	rl, err := h.Backend.DescribeReleaseLabel(ctx, in.ReleaseLabel)
	if err != nil {
		return nil, err
	}

	return &describeReleaseLabelOutput{
		ReleaseLabel: rl.ReleaseLabel,
		Applications: rl.Applications,
	}, nil
}

// --- ListSupportedInstanceTypes ---

type listSupportedInstanceTypesInput struct {
	ReleaseLabel string `json:"ReleaseLabel"`
	Marker       string `json:"Marker"`
}

type listSupportedInstanceTypesOutput struct {
	Marker                 string                  `json:"Marker,omitempty"`
	SupportedInstanceTypes []SupportedInstanceType `json:"SupportedInstanceTypes"`
}

func (h *Handler) handleListSupportedInstanceTypes(
	ctx context.Context,
	in *listSupportedInstanceTypesInput,
) (*listSupportedInstanceTypesOutput, error) {
	types, nextMarker := h.Backend.ListSupportedInstanceTypes(ctx, in.ReleaseLabel, in.Marker)

	return &listSupportedInstanceTypesOutput{
		SupportedInstanceTypes: types,
		Marker:                 nextMarker,
	}, nil
}
