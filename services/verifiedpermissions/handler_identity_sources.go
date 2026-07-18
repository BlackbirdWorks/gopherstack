package verifiedpermissions

import (
	"context"
	"fmt"
)

type cognitoGroupConfigJSON struct {
	GroupEntityType string `json:"groupEntityType,omitempty"`
}

type cognitoUserPoolConfigJSON struct {
	GroupConfiguration *cognitoGroupConfigJSON `json:"groupConfiguration,omitempty"`
	UserPoolArn        string                  `json:"userPoolArn"`
	// Issuer is a required response field (CognitoUserPoolConfigurationDetail/
	// Item.Issuer in the real SDK) that AWS derives from UserPoolArn; it is
	// never present on the request side, so omitempty keeps it silent there.
	Issuer    string   `json:"issuer,omitempty"`
	ClientIDs []string `json:"clientIds,omitempty"`
}

type oidcGroupConfigJSON struct {
	GroupClaim      string `json:"groupClaim,omitempty"`
	GroupEntityType string `json:"groupEntityType,omitempty"`
}

type oidcTokenSelectionJSON struct {
	IdentityTokenOnly *oidcIdentityTokenOnlyJSON `json:"identityTokenOnly,omitempty"`
	AccessTokenOnly   *oidcAccessTokenOnlyJSON   `json:"accessTokenOnly,omitempty"`
}

// oidcIdentityTokenOnlyJSON mirrors the real SDK's
// OpenIdConnectIdentityTokenConfiguration(Detail): the audience-restriction
// list for ID tokens is wire-named "clientIds", NOT "audiences" (that name is
// reserved for the accessTokenOnly variant below).
type oidcIdentityTokenOnlyJSON struct {
	PrincipalIDClaim string   `json:"principalIdClaim,omitempty"`
	ClientIDs        []string `json:"clientIds,omitempty"`
}

// oidcAccessTokenOnlyJSON mirrors the real SDK's
// OpenIdConnectAccessTokenConfiguration(Detail): the audience-restriction
// list for access tokens is wire-named "audiences".
type oidcAccessTokenOnlyJSON struct {
	PrincipalIDClaim string   `json:"principalIdClaim,omitempty"`
	Audiences        []string `json:"audiences,omitempty"`
}

type openIDConnectConfigJSON struct {
	GroupConfiguration *oidcGroupConfigJSON    `json:"groupConfiguration,omitempty"`
	TokenSelection     *oidcTokenSelectionJSON `json:"tokenSelection,omitempty"`
	Issuer             string                  `json:"issuer"`
	EntityIDPrefix     string                  `json:"entityIdPrefix,omitempty"`
}

type identitySourceConfigJSON struct {
	CognitoUserPool *cognitoUserPoolConfigJSON `json:"cognitoUserPoolConfiguration,omitempty"`
	OpenIDConnect   *openIDConnectConfigJSON   `json:"openIdConnectConfiguration,omitempty"`
}

type createIdentitySourceInput struct {
	PolicyStoreID       string                   `json:"policyStoreId"`
	PrincipalEntityType string                   `json:"principalEntityType"`
	Configuration       identitySourceConfigJSON `json:"configuration"`
	ClientToken         string                   `json:"clientToken,omitempty"`
}

type identitySourceOutput struct {
	IdentitySourceID    string                    `json:"identitySourceId"`
	PolicyStoreID       string                    `json:"policyStoreId"`
	PrincipalEntityType string                    `json:"principalEntityType"`
	Configuration       *identitySourceConfigJSON `json:"configuration,omitempty"`
	CreatedDate         string                    `json:"createdDate"`
	LastUpdatedDate     string                    `json:"lastUpdatedDate"`
}

func identitySourceToConfigJSON(is *IdentitySource) *identitySourceConfigJSON {
	if is.UserPoolArn != "" {
		cfg := &identitySourceConfigJSON{
			CognitoUserPool: &cognitoUserPoolConfigJSON{
				UserPoolArn: is.UserPoolArn,
				Issuer:      cognitoIssuerFromUserPoolArn(is.UserPoolArn),
				ClientIDs:   is.ClientIDs,
			},
		}

		if is.CognitoGroupConfig != nil {
			cfg.CognitoUserPool.GroupConfiguration = &cognitoGroupConfigJSON{
				GroupEntityType: is.CognitoGroupConfig.GroupEntityType,
			}
		}

		return cfg
	}

	if is.OpenIDIssuer != "" {
		cfg := &identitySourceConfigJSON{
			OpenIDConnect: &openIDConnectConfigJSON{
				Issuer:         is.OpenIDIssuer,
				EntityIDPrefix: is.EntityIDPrefix,
			},
		}

		if is.OIDCGroupConfig != nil {
			cfg.OpenIDConnect.GroupConfiguration = &oidcGroupConfigJSON{
				GroupClaim:      is.OIDCGroupConfig.GroupClaim,
				GroupEntityType: is.OIDCGroupConfig.GroupEntityType,
			}
		}

		if is.OIDCTokenSelection != nil {
			sel := is.OIDCTokenSelection
			tok := &oidcTokenSelectionJSON{}

			switch sel.TokenType {
			case "IDENTITY":
				tok.IdentityTokenOnly = &oidcIdentityTokenOnlyJSON{
					PrincipalIDClaim: sel.PrincipalIDClaim,
					ClientIDs:        sel.Audiences,
				}
			case "ACCESS":
				tok.AccessTokenOnly = &oidcAccessTokenOnlyJSON{
					PrincipalIDClaim: sel.PrincipalIDClaim,
					Audiences:        sel.Audiences,
				}
			}

			cfg.OpenIDConnect.TokenSelection = tok
		}

		return cfg
	}

	return nil
}

func identitySourceToOutput(is *IdentitySource) *identitySourceOutput {
	return &identitySourceOutput{
		IdentitySourceID:    is.IdentitySourceID,
		PolicyStoreID:       is.PolicyStoreID,
		PrincipalEntityType: is.PrincipalEntityType,
		Configuration:       identitySourceToConfigJSON(is),
		CreatedDate:         is.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate:     is.LastUpdated.UTC().Format(timeFormat),
	}
}

//nolint:nestif // identity source config union type
func configJSONToBackend(cfg identitySourceConfigJSON) IdentitySourceConfig {
	var out IdentitySourceConfig

	if cfg.CognitoUserPool != nil {
		out.UserPoolArn = cfg.CognitoUserPool.UserPoolArn
		out.ClientIDs = cfg.CognitoUserPool.ClientIDs

		if cfg.CognitoUserPool.GroupConfiguration != nil {
			out.CognitoGroupEntityType = cfg.CognitoUserPool.GroupConfiguration.GroupEntityType
		}
	} else if cfg.OpenIDConnect != nil {
		out.Issuer = cfg.OpenIDConnect.Issuer
		out.EntityIDPrefix = cfg.OpenIDConnect.EntityIDPrefix

		if cfg.OpenIDConnect.GroupConfiguration != nil {
			out.OIDCGroupClaim = cfg.OpenIDConnect.GroupConfiguration.GroupClaim
			out.OIDCGroupEntityType = cfg.OpenIDConnect.GroupConfiguration.GroupEntityType
		}

		if cfg.OpenIDConnect.TokenSelection != nil {
			if cfg.OpenIDConnect.TokenSelection.IdentityTokenOnly != nil {
				tok := cfg.OpenIDConnect.TokenSelection.IdentityTokenOnly
				out.TokenType = "IDENTITY"
				out.PrincipalIDClaim = tok.PrincipalIDClaim
				out.Audiences = tok.ClientIDs
			} else if cfg.OpenIDConnect.TokenSelection.AccessTokenOnly != nil {
				tok := cfg.OpenIDConnect.TokenSelection.AccessTokenOnly
				out.TokenType = "ACCESS"
				out.PrincipalIDClaim = tok.PrincipalIDClaim
				out.Audiences = tok.Audiences
			}
		}
	}

	return out
}

func (h *Handler) handleCreateIdentitySource(
	_ context.Context,
	in *createIdentitySourceInput,
) (*identitySourceOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.Configuration.CognitoUserPool == nil && in.Configuration.OpenIDConnect == nil {
		return nil, fmt.Errorf(
			"%w: configuration must contain cognitoUserPoolConfiguration or openIdConnectConfiguration",
			errInvalidRequest,
		)
	}

	if in.Configuration.CognitoUserPool != nil && in.Configuration.CognitoUserPool.UserPoolArn == "" {
		return nil, fmt.Errorf("%w: cognitoUserPoolConfiguration.userPoolArn is required", errInvalidRequest)
	}

	if in.Configuration.OpenIDConnect != nil && in.Configuration.OpenIDConnect.Issuer == "" {
		return nil, fmt.Errorf("%w: openIdConnectConfiguration.issuer is required", errInvalidRequest)
	}

	cfg := configJSONToBackend(in.Configuration)

	is, err := h.Backend.CreateIdentitySource(in.PolicyStoreID, in.PrincipalEntityType, cfg)
	if err != nil {
		return nil, err
	}

	return identitySourceToOutput(is), nil
}

type identitySourceIDInput struct {
	PolicyStoreID    string `json:"policyStoreId"`
	IdentitySourceID string `json:"identitySourceId"`
}

func (h *Handler) handleGetIdentitySource(
	_ context.Context,
	in *identitySourceIDInput,
) (*identitySourceOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.IdentitySourceID == "" {
		return nil, fmt.Errorf("%w: identitySourceId is required", errInvalidRequest)
	}

	is, err := h.Backend.GetIdentitySource(in.PolicyStoreID, in.IdentitySourceID)
	if err != nil {
		return nil, err
	}

	return identitySourceToOutput(is), nil
}

func (h *Handler) handleDeleteIdentitySource(_ context.Context, in *identitySourceIDInput) (*struct{}, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.IdentitySourceID == "" {
		return nil, fmt.Errorf("%w: identitySourceId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteIdentitySource(in.PolicyStoreID, in.IdentitySourceID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type identitySourceFilterJSON struct {
	PrincipalEntityType string `json:"principalEntityType,omitempty"`
}

type listIdentitySourcesInput struct {
	PolicyStoreID string                     `json:"policyStoreId"`
	NextToken     string                     `json:"nextToken,omitempty"`
	Filters       []identitySourceFilterJSON `json:"filters,omitempty"`
	MaxResults    int                        `json:"maxResults,omitempty"`
}

type listIdentitySourcesOutput struct {
	NextToken       string                 `json:"nextToken,omitempty"`
	IdentitySources []identitySourceOutput `json:"identitySources"`
}

func (h *Handler) handleListIdentitySources(
	_ context.Context,
	in *listIdentitySourcesInput,
) (*listIdentitySourcesOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	principalEntityTypes := make([]string, 0, len(in.Filters))

	for _, f := range in.Filters {
		if f.PrincipalEntityType != "" {
			principalEntityTypes = append(principalEntityTypes, f.PrincipalEntityType)
		}
	}

	sources, nextToken, err := h.Backend.ListIdentitySources(
		in.PolicyStoreID, in.NextToken, in.MaxResults, principalEntityTypes,
	)
	if err != nil {
		return nil, err
	}

	items := make([]identitySourceOutput, 0, len(sources))

	for i := range sources {
		items = append(items, *identitySourceToOutput(&sources[i]))
	}

	return &listIdentitySourcesOutput{IdentitySources: items, NextToken: nextToken}, nil
}

type updateIdentitySourceInput struct {
	UpdateConfiguration identitySourceConfigJSON `json:"updateConfiguration"`
	PolicyStoreID       string                   `json:"policyStoreId"`
	IdentitySourceID    string                   `json:"identitySourceId"`
	PrincipalEntityType string                   `json:"principalEntityType,omitempty"`
}

type updateIdentitySourceOutput struct {
	IdentitySourceID    string `json:"identitySourceId"`
	PolicyStoreID       string `json:"policyStoreId"`
	PrincipalEntityType string `json:"principalEntityType"`
	CreatedDate         string `json:"createdDate"`
	LastUpdatedDate     string `json:"lastUpdatedDate"`
}

func (h *Handler) handleUpdateIdentitySource(
	_ context.Context,
	in *updateIdentitySourceInput,
) (*updateIdentitySourceOutput, error) {
	if in.PolicyStoreID == "" {
		return nil, fmt.Errorf("%w: policyStoreId is required", errInvalidRequest)
	}

	if in.IdentitySourceID == "" {
		return nil, fmt.Errorf("%w: identitySourceId is required", errInvalidRequest)
	}

	if in.UpdateConfiguration.CognitoUserPool == nil && in.UpdateConfiguration.OpenIDConnect == nil {
		return nil, fmt.Errorf(
			"%w: updateConfiguration must contain cognitoUserPoolConfiguration or openIdConnectConfiguration",
			errInvalidRequest,
		)
	}

	if in.UpdateConfiguration.CognitoUserPool != nil && in.UpdateConfiguration.CognitoUserPool.UserPoolArn == "" {
		return nil, fmt.Errorf(
			"%w: updateConfiguration.cognitoUserPoolConfiguration.userPoolArn is required",
			errInvalidRequest,
		)
	}

	if in.UpdateConfiguration.OpenIDConnect != nil && in.UpdateConfiguration.OpenIDConnect.Issuer == "" {
		return nil, fmt.Errorf(
			"%w: updateConfiguration.openIdConnectConfiguration.issuer is required",
			errInvalidRequest,
		)
	}

	cfg := configJSONToBackend(in.UpdateConfiguration)

	is, err := h.Backend.UpdateIdentitySource(
		in.PolicyStoreID, in.IdentitySourceID, in.PrincipalEntityType, cfg,
	)
	if err != nil {
		return nil, err
	}

	return &updateIdentitySourceOutput{
		IdentitySourceID:    is.IdentitySourceID,
		PolicyStoreID:       is.PolicyStoreID,
		PrincipalEntityType: is.PrincipalEntityType,
		CreatedDate:         is.CreatedDate.UTC().Format(timeFormat),
		LastUpdatedDate:     is.LastUpdated.UTC().Format(timeFormat),
	}, nil
}
