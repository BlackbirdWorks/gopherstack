package cognitoidp

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// identityProvidersPageSize is this backend's default page size for
// ListIdentityProviders; real AWS doesn't document an exact default, so this
// is chosen generously (larger than any realistic per-pool provider count)
// so pagination only activates when a caller explicitly requests a smaller
// MaxResults.
const identityProvidersPageSize = 100

func (h *Handler) handleAdminDisableProviderForUser(
	_ context.Context,
	in *adminDisableProviderForUserInput,
) (*adminDisableProviderForUserOutput, error) {
	if err := h.Backend.AdminDisableProviderForUser(in.UserPoolID); err != nil {
		return nil, err
	}

	return &adminDisableProviderForUserOutput{}, nil
}

func (h *Handler) handleCreateIdentityProviderFull(
	_ context.Context,
	in *createIdentityProviderFullInput,
) (*createIdentityProviderFullOutput, error) {
	idp, err := h.Backend.CreateIdentityProviderFull(
		in.UserPoolID, in.ProviderName, in.ProviderType,
		map[string]string(in.ProviderDetails),
		map[string]string(in.AttributeMapping),
		in.IdpIdentifiers,
	)
	if err != nil {
		return nil, err
	}

	return &createIdentityProviderFullOutput{IdentityProvider: toIdentityProviderJSON(idp)}, nil
}

func (h *Handler) handleUpdateIdentityProviderFull(
	_ context.Context,
	in *updateIdentityProviderFullInput,
) (*updateIdentityProviderFullOutput, error) {
	idp, err := h.Backend.UpdateIdentityProviderFull(
		in.UserPoolID, in.ProviderName,
		map[string]string(in.ProviderDetails),
		map[string]string(in.AttributeMapping),
		in.IdpIdentifiers,
	)
	if err != nil {
		return nil, err
	}

	return &updateIdentityProviderFullOutput{IdentityProvider: toIdentityProviderJSON(idp)}, nil
}

func (h *Handler) handleDescribeIdentityProviderFull(
	_ context.Context,
	in *describeIdentityProviderFullInput,
) (*describeIdentityProviderFullOutput, error) {
	idp, err := h.Backend.DescribeIdentityProvider(in.UserPoolID, in.ProviderName)
	if err != nil {
		return nil, err
	}

	return &describeIdentityProviderFullOutput{IdentityProvider: toIdentityProviderJSON(idp)}, nil
}

func (h *Handler) handleGetIdentityProviderByIdentifierFull(
	_ context.Context,
	in *getIdentityProviderByIdentFullInput,
) (*getIdentityProviderByIdentFullOutput, error) {
	idp, err := h.Backend.GetIdentityProviderByIdentifier(in.UserPoolID, in.IdpIdentifier)
	if err != nil {
		return nil, err
	}

	return &getIdentityProviderByIdentFullOutput{IdentityProvider: toIdentityProviderJSON(idp)}, nil
}

func (h *Handler) handleListIdentityProvidersFull(
	_ context.Context,
	in *listIdentityProvidersFullInput,
) (*listIdentityProvidersFullOutput, error) {
	idps, err := h.Backend.ListIdentityProviders(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	pg := page.New(idps, in.NextToken, in.MaxResults, identityProvidersPageSize)

	out := make([]identityProviderSummaryJSON, 0, len(pg.Data))

	for _, idp := range pg.Data {
		s := identityProviderSummaryJSON{
			ProviderName: idp.ProviderName,
			ProviderType: idp.ProviderType,
		}

		if !idp.CreatedAt.IsZero() {
			s.CreationDate = float64(idp.CreatedAt.Unix())
		}

		if !idp.LastModifiedAt.IsZero() {
			s.LastModifiedDate = float64(idp.LastModifiedAt.Unix())
		}

		out = append(out, s)
	}

	return &listIdentityProvidersFullOutput{Providers: out, NextToken: pg.Next}, nil
}

func toIdentityProviderJSON(idp *IdentityProvider) *identityProviderJSON {
	out := &identityProviderJSON{
		UserPoolID:       idp.UserPoolID,
		ProviderName:     idp.ProviderName,
		ProviderType:     idp.ProviderType,
		ProviderDetails:  idpProviderDetails(idp.ProviderDetails),
		AttributeMapping: idpAttributeMapping(idp.AttributeMapping),
		IdpIdentifiers:   idp.IdpIdentifiers,
	}

	if !idp.CreatedAt.IsZero() {
		out.CreationDate = float64(idp.CreatedAt.Unix())
	}

	if !idp.LastModifiedAt.IsZero() {
		out.LastModifiedDate = float64(idp.LastModifiedAt.Unix())
	}

	return out
}

// handleAdminLinkProviderForUser links the external provider identity (SourceUser) to the
// target native user (DestinationUser) and records the link in backend state. The
// destination native user is identified by its ProviderAttributeValue (the Cognito
// username), matching the AWS ProviderUserIdentifierType shape.
func (h *Handler) handleAdminLinkProviderForUser(
	_ context.Context,
	in *adminLinkProviderForUserInput,
) (*adminLinkProviderForUserOutput, error) {
	if in.DestinationUser == nil || in.SourceUser == nil {
		return nil, fmt.Errorf("%w: DestinationUser and SourceUser are required", ErrInvalidParameter)
	}

	if err := h.Backend.AdminLinkProviderForUser(
		in.UserPoolID,
		in.DestinationUser.ProviderAttributeValue,
		in.SourceUser.ProviderName,
		in.SourceUser.ProviderAttributeName,
		in.SourceUser.ProviderAttributeValue,
	); err != nil {
		return nil, err
	}

	return &adminLinkProviderForUserOutput{}, nil
}

func (h *Handler) handleCreateIdentityProvider(
	_ context.Context,
	in *createIdentityProviderInput,
) (*createIdentityProviderOutput, error) {
	idp, err := h.Backend.CreateIdentityProvider(in.UserPoolID, in.ProviderName, in.ProviderType, in.ProviderDetails)
	if err != nil {
		return nil, err
	}

	ts := float64(idp.CreatedAt.Unix())

	return &createIdentityProviderOutput{
		IdentityProvider: &identityProviderType{
			UserPoolID:       idp.UserPoolID,
			ProviderName:     idp.ProviderName,
			ProviderType:     idp.ProviderType,
			ProviderDetails:  idp.ProviderDetails,
			CreationDate:     &ts,
			LastModifiedDate: &ts,
		},
	}, nil
}

func (h *Handler) handleDeleteIdentityProvider(
	_ context.Context,
	in *deleteIdentityProviderInput,
) (*deleteIdentityProviderOutput, error) {
	if err := h.Backend.DeleteIdentityProvider(in.UserPoolID, in.ProviderName); err != nil {
		return nil, err
	}

	return &deleteIdentityProviderOutput{}, nil
}

func (h *Handler) handleDescribeIdentityProvider(
	_ context.Context,
	in *describeIdentityProviderInput,
) (*describeIdentityProviderOutput, error) {
	idp, err := h.Backend.DescribeIdentityProvider(in.UserPoolID, in.ProviderName)
	if err != nil {
		return nil, err
	}

	ts := float64(idp.CreatedAt.Unix())
	mod := float64(idp.LastModifiedAt.Unix())

	return &describeIdentityProviderOutput{
		IdentityProvider: &identityProviderType{
			UserPoolID:       idp.UserPoolID,
			ProviderName:     idp.ProviderName,
			ProviderType:     idp.ProviderType,
			ProviderDetails:  idp.ProviderDetails,
			CreationDate:     &ts,
			LastModifiedDate: &mod,
		},
	}, nil
}

func (h *Handler) handleGetIdentityProviderByIdentifier(
	_ context.Context,
	in *getIdentityProviderByIdentifierInput,
) (*getIdentityProviderByIdentifierOutput, error) {
	idp, err := h.Backend.GetIdentityProviderByIdentifier(in.UserPoolID, in.IdpIdentifier)
	if err != nil {
		return nil, err
	}

	ts := float64(idp.CreatedAt.Unix())

	return &getIdentityProviderByIdentifierOutput{
		IdentityProvider: &identityProviderType{
			UserPoolID:      idp.UserPoolID,
			ProviderName:    idp.ProviderName,
			ProviderType:    idp.ProviderType,
			ProviderDetails: idp.ProviderDetails,
			CreationDate:    &ts,
		},
	}, nil
}

func (h *Handler) handleListIdentityProviders(
	_ context.Context,
	in *listIdentityProvidersInput,
) (*listIdentityProvidersOutput, error) {
	idps, err := h.Backend.ListIdentityProviders(in.UserPoolID)
	if err != nil {
		return nil, err
	}

	summaries := make([]identityProviderSummary, 0, len(idps))
	for _, idp := range idps {
		summaries = append(summaries, identityProviderSummary{
			ProviderName: idp.ProviderName,
			ProviderType: idp.ProviderType,
		})
	}

	return &listIdentityProvidersOutput{Providers: summaries}, nil
}

func (h *Handler) handleUpdateIdentityProvider(
	_ context.Context,
	in *updateIdentityProviderInput,
) (*updateIdentityProviderOutput, error) {
	idp, err := h.Backend.UpdateIdentityProvider(in.UserPoolID, in.ProviderName, in.ProviderDetails)
	if err != nil {
		return nil, err
	}

	mod := float64(idp.LastModifiedAt.Unix())

	return &updateIdentityProviderOutput{
		IdentityProvider: &identityProviderType{
			UserPoolID:       idp.UserPoolID,
			ProviderName:     idp.ProviderName,
			ProviderType:     idp.ProviderType,
			ProviderDetails:  idp.ProviderDetails,
			LastModifiedDate: &mod,
		},
	}, nil
}

func (h *Handler) identityProvidersOpsA() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"AdminDisableProviderForUser": service.WrapOp(h.handleAdminDisableProviderForUser),
	}
}

func (h *Handler) identityProvidersOpsB() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"AdminLinkProviderForUser":        service.WrapOp(h.handleAdminLinkProviderForUser),
		"CreateIdentityProvider":          service.WrapOp(h.handleCreateIdentityProvider),
		"DeleteIdentityProvider":          service.WrapOp(h.handleDeleteIdentityProvider),
		"DescribeIdentityProvider":        service.WrapOp(h.handleDescribeIdentityProvider),
		"GetIdentityProviderByIdentifier": service.WrapOp(h.handleGetIdentityProviderByIdentifier),
		"ListIdentityProviders":           service.WrapOp(h.handleListIdentityProviders),
		"UpdateIdentityProvider":          service.WrapOp(h.handleUpdateIdentityProvider),
	}
}

func (h *Handler) identityProvidersOpsC() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opCreateIdentityProvider:     wrapAccuracy(h.handleCreateIdentityProviderFull),
		opUpdateIdentityProvider:     wrapAccuracy(h.handleUpdateIdentityProviderFull),
		opDescribeIdentityProvider:   wrapAccuracy(h.handleDescribeIdentityProviderFull),
		opGetIdentityProviderByIdent: wrapAccuracy(h.handleGetIdentityProviderByIdentifierFull),
		opListIdentityProviders:      wrapAccuracy(h.handleListIdentityProvidersFull),
	}
}
