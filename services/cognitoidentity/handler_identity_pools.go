package cognitoidentity

import (
	"context"
	"fmt"
)

// --- Request/Response types ---

type cognitoIdentityProviderInput struct {
	ProviderName         string `json:"ProviderName"`
	ClientID             string `json:"ClientId"`
	ServerSideTokenCheck bool   `json:"ServerSideTokenCheck"`
}

type identityPoolOutput struct {
	SupportedLoginProviders        map[string]string              `json:"SupportedLoginProviders,omitempty"`
	IdentityPoolTags               map[string]string              `json:"IdentityPoolTags,omitempty"`
	OpenIDConnectProviderARNs      []string                       `json:"OpenIdConnectProviderARNs,omitempty"`
	SamlProviderARNs               []string                       `json:"SamlProviderARNs,omitempty"`
	IdentityPoolID                 string                         `json:"IdentityPoolId"`
	IdentityPoolName               string                         `json:"IdentityPoolName"`
	DeveloperProviderName          string                         `json:"DeveloperProviderName,omitempty"`
	IdentityProviders              []cognitoIdentityProviderInput `json:"CognitoIdentityProviders,omitempty"`
	AllowUnauthenticatedIdentities bool                           `json:"AllowUnauthenticatedIdentities"`
	AllowClassicFlow               bool                           `json:"AllowClassicFlow,omitempty"`
}

type createIdentityPoolInput struct {
	SupportedLoginProviders        map[string]string              `json:"SupportedLoginProviders"`
	Tags                           map[string]string              `json:"IdentityPoolTags"`
	OpenIDConnectProviderARNs      []string                       `json:"OpenIdConnectProviderARNs"`
	SamlProviderARNs               []string                       `json:"SamlProviderARNs"`
	IdentityPoolName               string                         `json:"IdentityPoolName"`
	DeveloperProviderName          string                         `json:"DeveloperProviderName"`
	IdentityProviders              []cognitoIdentityProviderInput `json:"CognitoIdentityProviders"`
	AllowUnauthenticatedIdentities bool                           `json:"AllowUnauthenticatedIdentities"`
	AllowClassicFlow               bool                           `json:"AllowClassicFlow"`
}

func (h *Handler) handleCreateIdentityPool(
	ctx context.Context,
	in *createIdentityPoolInput,
) (*identityPoolOutput, error) {
	providers := toBackendProviders(in.IdentityProviders)

	pool, err := h.Backend.CreateIdentityPool(
		ctx,
		in.IdentityPoolName,
		in.AllowUnauthenticatedIdentities,
		in.AllowClassicFlow,
		in.DeveloperProviderName,
		providers,
		in.SupportedLoginProviders,
		in.Tags,
		&PoolExtendedConfig{
			OpenIDConnectProviderARNs: in.OpenIDConnectProviderARNs,
			SamlProviderARNs:          in.SamlProviderARNs,
		},
	)
	if err != nil {
		return nil, err
	}

	return toIdentityPoolOutput(pool), nil
}

type deleteIdentityPoolInput struct {
	IdentityPoolID string `json:"IdentityPoolId"`
}

type deleteIdentityPoolOutput struct{}

func (h *Handler) handleDeleteIdentityPool(
	ctx context.Context,
	in *deleteIdentityPoolInput,
) (*deleteIdentityPoolOutput, error) {
	if in.IdentityPoolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteIdentityPool(ctx, in.IdentityPoolID); err != nil {
		return nil, err
	}

	return &deleteIdentityPoolOutput{}, nil
}

type describeIdentityPoolInput struct {
	IdentityPoolID string `json:"IdentityPoolId"`
}

func (h *Handler) handleDescribeIdentityPool(
	ctx context.Context,
	in *describeIdentityPoolInput,
) (*identityPoolOutput, error) {
	if in.IdentityPoolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	pool, err := h.Backend.DescribeIdentityPool(ctx, in.IdentityPoolID)
	if err != nil {
		return nil, err
	}

	return toIdentityPoolOutput(pool), nil
}

type listIdentityPoolsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type identityPoolShortDescription struct {
	IdentityPoolID   string `json:"IdentityPoolId"`
	IdentityPoolName string `json:"IdentityPoolName"`
}

type listIdentityPoolsOutput struct {
	NextToken     string                         `json:"NextToken,omitempty"`
	IdentityPools []identityPoolShortDescription `json:"IdentityPools"`
}

func (h *Handler) handleListIdentityPools(
	ctx context.Context,
	in *listIdentityPoolsInput,
) (*listIdentityPoolsOutput, error) {
	pools, nextToken := h.Backend.ListIdentityPools(ctx, in.MaxResults, in.NextToken)

	items := make([]identityPoolShortDescription, 0, len(pools))
	for _, p := range pools {
		items = append(items, identityPoolShortDescription{
			IdentityPoolID:   p.IdentityPoolID,
			IdentityPoolName: p.IdentityPoolName,
		})
	}

	return &listIdentityPoolsOutput{IdentityPools: items, NextToken: nextToken}, nil
}

type updateIdentityPoolInput struct {
	SupportedLoginProviders        map[string]string              `json:"SupportedLoginProviders"`
	IdentityPoolTags               map[string]string              `json:"IdentityPoolTags"`
	OpenIDConnectProviderARNs      []string                       `json:"OpenIdConnectProviderARNs"`
	SamlProviderARNs               []string                       `json:"SamlProviderARNs"`
	IdentityPoolID                 string                         `json:"IdentityPoolId"`
	IdentityPoolName               string                         `json:"IdentityPoolName"`
	DeveloperProviderName          string                         `json:"DeveloperProviderName"`
	IdentityProviders              []cognitoIdentityProviderInput `json:"CognitoIdentityProviders"`
	AllowUnauthenticatedIdentities bool                           `json:"AllowUnauthenticatedIdentities"`
	AllowClassicFlow               bool                           `json:"AllowClassicFlow"`
}

func (h *Handler) handleUpdateIdentityPool(
	ctx context.Context,
	in *updateIdentityPoolInput,
) (*identityPoolOutput, error) {
	if in.IdentityPoolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	providers := toBackendProviders(in.IdentityProviders)

	pool, err := h.Backend.UpdateIdentityPool(
		ctx,
		in.IdentityPoolID,
		in.IdentityPoolName,
		in.AllowUnauthenticatedIdentities,
		in.AllowClassicFlow,
		in.DeveloperProviderName,
		providers,
		in.SupportedLoginProviders,
		in.IdentityPoolTags,
		&PoolExtendedConfig{
			OpenIDConnectProviderARNs: in.OpenIDConnectProviderARNs,
			SamlProviderARNs:          in.SamlProviderARNs,
		},
	)
	if err != nil {
		return nil, err
	}

	return toIdentityPoolOutput(pool), nil
}

// toBackendProviders converts handler-level provider inputs to backend provider structs.
func toBackendProviders(in []cognitoIdentityProviderInput) []IdentityProvider {
	if in == nil {
		return nil
	}

	out := make([]IdentityProvider, len(in))
	for i, p := range in {
		out[i] = IdentityProvider(p)
	}

	return out
}

// toIdentityPoolOutput converts a backend IdentityPool to the handler output struct.
func toIdentityPoolOutput(pool *IdentityPool) *identityPoolOutput {
	providers := make([]cognitoIdentityProviderInput, len(pool.IdentityProviders))
	for i, p := range pool.IdentityProviders {
		providers[i] = cognitoIdentityProviderInput(p)
	}

	return &identityPoolOutput{
		IdentityPoolID:                 pool.IdentityPoolID,
		IdentityPoolName:               pool.IdentityPoolName,
		DeveloperProviderName:          pool.DeveloperProviderName,
		AllowUnauthenticatedIdentities: pool.AllowUnauthenticatedIdentities,
		AllowClassicFlow:               pool.AllowClassicFlow,
		IdentityProviders:              providers,
		SupportedLoginProviders:        pool.SupportedLoginProviders,
		IdentityPoolTags:               pool.Tags,
		OpenIDConnectProviderARNs:      pool.OpenIDConnectProviderARNs,
		SamlProviderARNs:               pool.SamlProviderARNs,
	}
}
