package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) handleCreateTerms(_ context.Context, in *createTermsInput) (*createTermsOutput, error) {
	t, err := h.Backend.CreateTerms(in.UserPoolID, "")
	if err != nil {
		return nil, err
	}

	return &createTermsOutput{Terms: &termsType{DefaultTermsAndConditions: t.Text}}, nil
}

func (h *Handler) handleDeleteTerms(_ context.Context, in *deleteTermsInput) (*deleteTermsOutput, error) {
	if err := h.Backend.DeleteTerms(in.UserPoolID); err != nil {
		return nil, err
	}

	return &deleteTermsOutput{}, nil
}

func (h *Handler) handleDescribeTerms(_ context.Context, in *describeTermsInput) (*describeTermsOutput, error) {
	t, err := h.Backend.DescribeTerms(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	return &describeTermsOutput{Terms: &termsType{DefaultTermsAndConditions: t.Text}}, nil
}

func (h *Handler) handleListTerms(_ context.Context, in *listTermsInput) (*listTermsOutput, error) {
	ts, err := h.Backend.ListTerms(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	out := make([]termsType, 0, len(ts))
	for _, t := range ts {
		out = append(out, termsType{DefaultTermsAndConditions: t.Text})
	}

	return &listTermsOutput{Terms: out}, nil
}

func (h *Handler) handleUpdateTerms(_ context.Context, in *updateTermsInput) (*updateTermsOutput, error) {
	t, err := h.Backend.UpdateTerms(in.UserPoolID, "")
	if err != nil {
		return nil, err
	}

	return &updateTermsOutput{Terms: &termsType{DefaultTermsAndConditions: t.Text}}, nil
}

func (h *Handler) termsOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateTerms":   service.WrapOp(h.handleCreateTerms),
		"DeleteTerms":   service.WrapOp(h.handleDeleteTerms),
		"DescribeTerms": service.WrapOp(h.handleDescribeTerms),
		"ListTerms":     service.WrapOp(h.handleListTerms),
		"UpdateTerms":   service.WrapOp(h.handleUpdateTerms),
	}
}
