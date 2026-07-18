package cognitoidentity

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

type getIDInput struct {
	Logins         map[string]string `json:"Logins"`
	IdentityPoolID string            `json:"IdentityPoolId"`
	AccountID      string            `json:"AccountId"`
}

type getIDOutput struct {
	IdentityID string `json:"IdentityId"`
}

func (h *Handler) handleGetID(ctx context.Context, in *getIDInput) (*getIDOutput, error) {
	if in.IdentityPoolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	identity, err := h.Backend.GetID(ctx, in.IdentityPoolID, in.AccountID, in.Logins)
	if err != nil {
		return nil, err
	}

	return &getIDOutput{IdentityID: identity.IdentityID}, nil
}

type deleteIdentitiesInput struct {
	IdentityIDsToDelete []string `json:"IdentityIdsToDelete"`
}

type unprocessedIdentityIDOutput struct {
	ErrorCode  string `json:"ErrorCode"`
	IdentityID string `json:"IdentityId"`
}

type deleteIdentitiesOutput struct {
	UnprocessedIdentityIDs []unprocessedIdentityIDOutput `json:"UnprocessedIdentityIds"`
}

func (h *Handler) handleDeleteIdentities(
	ctx context.Context,
	in *deleteIdentitiesInput,
) (*deleteIdentitiesOutput, error) {
	if len(in.IdentityIDsToDelete) == 0 {
		return nil, fmt.Errorf("%w: IdentityIdsToDelete must not be empty", ErrInvalidParameter)
	}

	unprocessed, err := h.Backend.DeleteIdentities(ctx, in.IdentityIDsToDelete)
	if err != nil {
		return nil, err
	}

	out := make([]unprocessedIdentityIDOutput, 0, len(unprocessed))
	for _, u := range unprocessed {
		out = append(out, unprocessedIdentityIDOutput(u))
	}

	return &deleteIdentitiesOutput{UnprocessedIdentityIDs: out}, nil
}

type describeIdentityInput struct {
	IdentityID string `json:"IdentityId"`
}

type describeIdentityOutput struct {
	IdentityID       string   `json:"IdentityId"`
	Logins           []string `json:"Logins"`
	CreationDate     float64  `json:"CreationDate"`
	LastModifiedDate float64  `json:"LastModifiedDate"`
}

func (h *Handler) handleDescribeIdentity(
	ctx context.Context,
	in *describeIdentityInput,
) (*describeIdentityOutput, error) {
	if in.IdentityID == "" {
		return nil, fmt.Errorf("%w: IdentityId is required", ErrInvalidParameter)
	}

	desc, err := h.Backend.DescribeIdentity(ctx, in.IdentityID)
	if err != nil {
		return nil, err
	}

	logins := desc.Logins
	if logins == nil {
		logins = []string{}
	}

	return &describeIdentityOutput{
		IdentityID:       desc.IdentityID,
		Logins:           logins,
		CreationDate:     awstime.Epoch(desc.CreationDate),
		LastModifiedDate: awstime.Epoch(desc.LastModifiedDate),
	}, nil
}

type listIdentitiesInput struct {
	NextToken      string `json:"NextToken"`
	IdentityPoolID string `json:"IdentityPoolId"`
	MaxResults     int    `json:"MaxResults"`
	HideDisabled   bool   `json:"HideDisabled"`
}

type identityDescriptionOutput struct {
	IdentityID       string   `json:"IdentityId"`
	Logins           []string `json:"Logins"`
	CreationDate     float64  `json:"CreationDate"`
	LastModifiedDate float64  `json:"LastModifiedDate"`
}

type listIdentitiesOutput struct {
	IdentityPoolID string                      `json:"IdentityPoolId"`
	NextToken      string                      `json:"NextToken,omitempty"`
	Identities     []identityDescriptionOutput `json:"Identities"`
}

func (h *Handler) handleListIdentities(
	ctx context.Context,
	in *listIdentitiesInput,
) (*listIdentitiesOutput, error) {
	result, err := h.Backend.ListIdentities(ctx, in.IdentityPoolID, in.MaxResults, in.HideDisabled, in.NextToken)
	if err != nil {
		return nil, err
	}

	items := make([]identityDescriptionOutput, 0, len(result.Identities))
	for _, id := range result.Identities {
		logins := id.Logins
		if logins == nil {
			logins = []string{}
		}

		items = append(items, identityDescriptionOutput{
			IdentityID:       id.IdentityID,
			Logins:           logins,
			CreationDate:     awstime.Epoch(id.CreationDate),
			LastModifiedDate: awstime.Epoch(id.LastModifiedDate),
		})
	}

	return &listIdentitiesOutput{
		IdentityPoolID: result.IdentityPoolID,
		NextToken:      result.NextToken,
		Identities:     items,
	}, nil
}

type lookupDeveloperIdentityInput struct {
	NextToken               string `json:"NextToken"`
	IdentityPoolID          string `json:"IdentityPoolId"`
	IdentityID              string `json:"IdentityId"`
	DeveloperUserIdentifier string `json:"DeveloperUserIdentifier"`
	DeveloperProviderName   string `json:"DeveloperProviderName"`
	MaxResults              int    `json:"MaxResults"`
}

type lookupDeveloperIdentityOutput struct {
	IdentityID                  string   `json:"IdentityId"`
	NextToken                   string   `json:"NextToken,omitempty"`
	DeveloperUserIdentifierList []string `json:"DeveloperUserIdentifierList"`
}

func (h *Handler) handleLookupDeveloperIdentity(
	ctx context.Context,
	in *lookupDeveloperIdentityInput,
) (*lookupDeveloperIdentityOutput, error) {
	result, err := h.Backend.LookupDeveloperIdentity(
		ctx,
		in.IdentityPoolID,
		in.IdentityID,
		in.DeveloperUserIdentifier,
		in.DeveloperProviderName,
	)
	if err != nil {
		return nil, err
	}

	ids := result.DeveloperUserIdentifierList
	if ids == nil {
		ids = []string{}
	}

	return &lookupDeveloperIdentityOutput{
		IdentityID:                  result.IdentityID,
		DeveloperUserIdentifierList: ids,
	}, nil
}

type mergeDeveloperIdentitiesInput struct {
	SourceUserIdentifier      string `json:"SourceUserIdentifier"`
	DestinationUserIdentifier string `json:"DestinationUserIdentifier"`
	DeveloperProviderName     string `json:"DeveloperProviderName"`
	IdentityPoolID            string `json:"IdentityPoolId"`
}

type mergeDeveloperIdentitiesOutput struct {
	IdentityID string `json:"IdentityId"`
}

func (h *Handler) handleMergeDeveloperIdentities(
	ctx context.Context,
	in *mergeDeveloperIdentitiesInput,
) (*mergeDeveloperIdentitiesOutput, error) {
	identity, err := h.Backend.MergeDeveloperIdentities(
		ctx,
		in.SourceUserIdentifier,
		in.DestinationUserIdentifier,
		in.DeveloperProviderName,
		in.IdentityPoolID,
	)
	if err != nil {
		return nil, err
	}

	return &mergeDeveloperIdentitiesOutput{IdentityID: identity.IdentityID}, nil
}

type unlinkDeveloperIdentityInput struct {
	IdentityID              string `json:"IdentityId"`
	IdentityPoolID          string `json:"IdentityPoolId"`
	DeveloperProviderName   string `json:"DeveloperProviderName"`
	DeveloperUserIdentifier string `json:"DeveloperUserIdentifier"`
}

type unlinkDeveloperIdentityOutput struct{}

func (h *Handler) handleUnlinkDeveloperIdentity(
	ctx context.Context,
	in *unlinkDeveloperIdentityInput,
) (*unlinkDeveloperIdentityOutput, error) {
	if err := h.Backend.UnlinkDeveloperIdentity(
		ctx,
		in.IdentityID,
		in.IdentityPoolID,
		in.DeveloperProviderName,
		in.DeveloperUserIdentifier,
	); err != nil {
		return nil, err
	}

	return &unlinkDeveloperIdentityOutput{}, nil
}

type unlinkIdentityInput struct {
	IdentityID     string            `json:"IdentityId"`
	Logins         map[string]string `json:"Logins"`
	LoginsToRemove []string          `json:"LoginsToRemove"`
}

type unlinkIdentityOutput struct{}

func (h *Handler) handleUnlinkIdentity(
	ctx context.Context,
	in *unlinkIdentityInput,
) (*unlinkIdentityOutput, error) {
	if err := h.Backend.UnlinkIdentity(ctx, in.IdentityID, in.Logins, in.LoginsToRemove); err != nil {
		return nil, err
	}

	return &unlinkIdentityOutput{}, nil
}
