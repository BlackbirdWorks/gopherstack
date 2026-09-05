package cognitoidp

import "time"

// IdentityProvider represents a federated identity provider attached to a user pool.
type IdentityProvider struct {
	ProviderDetails  map[string]string `json:"providerDetails,omitempty"`
	AttributeMapping map[string]string `json:"attributeMapping,omitempty"`
	CreatedAt        time.Time         `json:"createdAt"`
	LastModifiedAt   time.Time         `json:"lastModifiedAt"`
	UserPoolID       string            `json:"userPoolID,omitempty"`
	ProviderName     string            `json:"providerName,omitempty"`
	ProviderType     string            `json:"providerType,omitempty"`
	IdpIdentifiers   []string          `json:"idpIdentifiers,omitempty"`
}

// ProviderLink records a federated identity linked to a user via AdminLinkProviderForUser.
type ProviderLink struct {
	ProviderName           string `json:"providerName,omitempty"`
	ProviderAttributeName  string `json:"providerAttributeName,omitempty"`
	ProviderAttributeValue string `json:"providerAttributeValue,omitempty"`
}

type adminDisableProviderForUserInput struct {
	UserPoolID string                     `json:"UserPoolId,omitempty"`
	User       providerUserIdentifierType `json:"User"`
}

type adminDisableProviderForUserOutput struct{}

type (
	idpProviderDetails  map[string]string
	idpAttributeMapping map[string]string
)

type createIdentityProviderFullInput struct {
	UserPoolID       string              `json:"UserPoolId,omitempty"`
	ProviderName     string              `json:"ProviderName,omitempty"`
	ProviderType     string              `json:"ProviderType,omitempty"`
	ProviderDetails  idpProviderDetails  `json:"ProviderDetails"`
	AttributeMapping idpAttributeMapping `json:"AttributeMapping"`
	IdpIdentifiers   []string            `json:"IdpIdentifiers,omitempty"`
}

type identityProviderJSON struct {
	UserPoolID       string              `json:"UserPoolId,omitempty"`
	ProviderName     string              `json:"ProviderName,omitempty"`
	ProviderType     string              `json:"ProviderType,omitempty"`
	ProviderDetails  idpProviderDetails  `json:"ProviderDetails"`
	AttributeMapping idpAttributeMapping `json:"AttributeMapping"`
	IdpIdentifiers   []string            `json:"IdpIdentifiers,omitempty"`
	CreationDate     float64             `json:"CreationDate,omitempty"`
	LastModifiedDate float64             `json:"LastModifiedDate,omitempty"`
}

type createIdentityProviderFullOutput struct {
	IdentityProvider *identityProviderJSON `json:"IdentityProvider,omitempty"`
}

type updateIdentityProviderFullInput struct {
	UserPoolID       string              `json:"UserPoolId,omitempty"`
	ProviderName     string              `json:"ProviderName,omitempty"`
	ProviderDetails  idpProviderDetails  `json:"ProviderDetails"`
	AttributeMapping idpAttributeMapping `json:"AttributeMapping"`
	IdpIdentifiers   []string            `json:"IdpIdentifiers,omitempty"`
}

type updateIdentityProviderFullOutput struct {
	IdentityProvider *identityProviderJSON `json:"IdentityProvider,omitempty"`
}

type describeIdentityProviderFullInput struct {
	UserPoolID   string `json:"UserPoolId,omitempty"`
	ProviderName string `json:"ProviderName,omitempty"`
}

type describeIdentityProviderFullOutput struct {
	IdentityProvider *identityProviderJSON `json:"IdentityProvider,omitempty"`
}

type getIdentityProviderByIdentFullInput struct {
	UserPoolID    string `json:"UserPoolId,omitempty"`
	IdpIdentifier string `json:"IdpIdentifier,omitempty"`
}

type getIdentityProviderByIdentFullOutput struct {
	IdentityProvider *identityProviderJSON `json:"IdentityProvider,omitempty"`
}

type listIdentityProvidersFullInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

type listIdentityProvidersFullOutput struct {
	NextToken string                        `json:"NextToken,omitempty"`
	Providers []identityProviderSummaryJSON `json:"Providers"`
}

type identityProviderSummaryJSON struct {
	ProviderName     string  `json:"ProviderName,omitempty"`
	ProviderType     string  `json:"ProviderType,omitempty"`
	CreationDate     float64 `json:"CreationDate,omitempty"`
	LastModifiedDate float64 `json:"LastModifiedDate,omitempty"`
}

type adminLinkProviderForUserInput struct {
	DestinationUser *providerUserIdentifierType `json:"DestinationUser,omitempty"`
	SourceUser      *providerUserIdentifierType `json:"SourceUser,omitempty"`
	UserPoolID      string                      `json:"UserPoolId,omitempty"`
}

type adminLinkProviderForUserOutput struct{}

type deleteIdentityProviderInput struct {
	UserPoolID   string `json:"UserPoolId,omitempty"`
	ProviderName string `json:"ProviderName,omitempty"`
}

type deleteIdentityProviderOutput struct{}
