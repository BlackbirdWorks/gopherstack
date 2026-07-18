package transfer

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type createAgreementInput struct {
	ServerID         string              `json:"ServerId"`
	Description      string              `json:"Description"`
	LocalProfileID   string              `json:"LocalProfileId"`
	PartnerProfileID string              `json:"PartnerProfileId"`
	BaseDirectory    string              `json:"BaseDirectory"`
	AccessRole       string              `json:"AccessRole"`
	Status           string              `json:"Status,omitempty"`
	Tags             []map[string]string `json:"Tags"`
}

type createAgreementOutput struct {
	AgreementID string `json:"AgreementId"`
}

// agreementARN builds the ARN for a Transfer agreement.
func agreementARN(accountID, region, serverID, agreementID string) string {
	return arn.Build("transfer", region, accountID, "server/"+serverID+"/agreement/"+agreementID)
}

func (h *Handler) handleCreateAgreement(
	_ context.Context,
	in *createAgreementInput,
) (*createAgreementOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)

	ag, err := h.Backend.CreateAgreementFull(
		in.ServerID,
		in.Description,
		in.LocalProfileID,
		in.PartnerProfileID,
		in.BaseDirectory,
		in.AccessRole,
		in.Status,
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createAgreementOutput{AgreementID: ag.AgreementID}, nil
}

type deleteAgreementInput struct {
	ServerID    string `json:"ServerId"`
	AgreementID string `json:"AgreementId"`
}

func (h *Handler) handleDeleteAgreement(
	_ context.Context,
	in *deleteAgreementInput,
) (*struct{}, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.AgreementID == "" {
		return nil, fmt.Errorf("%w: AgreementId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteAgreement(in.ServerID, in.AgreementID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeAgreementInput struct {
	ServerID    string `json:"ServerId"`
	AgreementID string `json:"AgreementId"`
}

type describeAgreementOutput struct {
	Agreement map[string]any `json:"Agreement"`
}

func (h *Handler) handleDescribeAgreement(
	_ context.Context,
	in *describeAgreementInput,
) (*describeAgreementOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.AgreementID == "" {
		return nil, fmt.Errorf("%w: AgreementId is required", errInvalidRequest)
	}

	ag, err := h.Backend.DescribeAgreement(in.ServerID, in.AgreementID)
	if err != nil {
		return nil, err
	}

	return &describeAgreementOutput{
		Agreement: map[string]any{
			"AgreementId":       ag.AgreementID,
			"ServerId":          ag.ServerID,
			keyDescription:      ag.Description,
			keyStatus:           ag.Status,
			keyLocalProfileID:   ag.LocalProfileID,
			keyPartnerProfileID: ag.PartnerProfileID,
			"BaseDirectory":     ag.BaseDirectory,
			"AccessRole":        ag.AccessRole,
			keyArn:              agreementARN(ag.AccountID, ag.Region, ag.ServerID, ag.AgreementID),
			keyTags:             tagsToList(ag.Tags),
		},
	}, nil
}

type listAgreementsInput struct {
	ServerID   string `json:"ServerId"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listAgreementsOutput struct {
	NextToken  string           `json:"NextToken,omitempty"`
	Agreements []map[string]any `json:"Agreements"`
}

func (h *Handler) handleListAgreements(
	_ context.Context,
	in *listAgreementsInput,
) (*listAgreementsOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	items, err := h.Backend.ListAgreements(in.ServerID)
	if err != nil {
		return nil, err
	}

	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, ag := range page {
		out[i] = map[string]any{
			"AgreementId":       ag.AgreementID,
			keyArn:              agreementARN(ag.AccountID, ag.Region, ag.ServerID, ag.AgreementID),
			keyDescription:      ag.Description,
			keyStatus:           ag.Status,
			keyLocalProfileID:   ag.LocalProfileID,
			keyPartnerProfileID: ag.PartnerProfileID,
		}
	}

	return &listAgreementsOutput{Agreements: out, NextToken: next}, nil
}

type updateAgreementInput struct {
	ServerID    string `json:"ServerId"`
	AgreementID string `json:"AgreementId"`
	Description string `json:"Description"`
	Status      string `json:"Status"`
}

type updateAgreementOutput struct {
	AgreementID string `json:"AgreementId"`
}

func (h *Handler) handleUpdateAgreement(
	_ context.Context,
	in *updateAgreementInput,
) (*updateAgreementOutput, error) {
	if in.ServerID == "" {
		return nil, fmt.Errorf("%w: ServerId is required", errInvalidRequest)
	}

	if in.AgreementID == "" {
		return nil, fmt.Errorf("%w: AgreementId is required", errInvalidRequest)
	}

	ag, err := h.Backend.UpdateAgreement(in.ServerID, in.AgreementID, in.Description, in.Status)
	if err != nil {
		return nil, err
	}

	return &updateAgreementOutput{AgreementID: ag.AgreementID}, nil
}
