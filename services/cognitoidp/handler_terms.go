package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func toTermsType(t *Terms) *termsType {
	return &termsType{
		ClientID:         t.ClientID,
		TermsID:          t.TermsID,
		UserPoolID:       t.UserPoolID,
		TermsName:        t.TermsName,
		Enforcement:      t.Enforcement,
		TermsSource:      t.TermsSource,
		Links:            t.Links,
		CreationDate:     awstime.Epoch(t.CreatedAt),
		LastModifiedDate: awstime.Epoch(t.LastModifiedAt),
	}
}

func toTermsDescriptionType(t *Terms) termsDescriptionType {
	return termsDescriptionType{
		TermsID:          t.TermsID,
		TermsName:        t.TermsName,
		Enforcement:      t.Enforcement,
		CreationDate:     awstime.Epoch(t.CreatedAt),
		LastModifiedDate: awstime.Epoch(t.LastModifiedAt),
	}
}

func (h *Handler) handleCreateTerms(_ context.Context, in *createTermsInput) (*createTermsOutput, error) {
	t, err := h.Backend.CreateTerms(in.UserPoolID, in.ClientID, in.TermsName, in.Enforcement, in.TermsSource, in.Links)
	if err != nil {
		return nil, err
	}

	return &createTermsOutput{Terms: toTermsType(t)}, nil
}

func (h *Handler) handleDeleteTerms(_ context.Context, in *deleteTermsInput) (*deleteTermsOutput, error) {
	if err := h.Backend.DeleteTerms(in.UserPoolID, in.TermsID); err != nil {
		return nil, err
	}

	return &deleteTermsOutput{}, nil
}

func (h *Handler) handleDescribeTerms(_ context.Context, in *describeTermsInput) (*describeTermsOutput, error) {
	t, err := h.Backend.DescribeTerms(in.UserPoolID, in.TermsID)
	if err != nil {
		return nil, err
	}

	return &describeTermsOutput{Terms: toTermsType(t)}, nil
}

func (h *Handler) handleListTerms(_ context.Context, in *listTermsInput) (*listTermsOutput, error) {
	limit, err := validateCognitoMaxResults(in.MaxResults)
	if err != nil {
		return nil, err
	}

	ts, token, err := h.Backend.ListTerms(in.UserPoolID, limit, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]termsDescriptionType, 0, len(ts))
	for _, t := range ts {
		out = append(out, toTermsDescriptionType(t))
	}

	return &listTermsOutput{Terms: out, NextToken: token}, nil
}

func (h *Handler) handleUpdateTerms(_ context.Context, in *updateTermsInput) (*updateTermsOutput, error) {
	t, err := h.Backend.UpdateTerms(in.UserPoolID, in.TermsID, in.Enforcement, in.TermsName, in.TermsSource, in.Links)
	if err != nil {
		return nil, err
	}

	return &updateTermsOutput{Terms: toTermsType(t)}, nil
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
