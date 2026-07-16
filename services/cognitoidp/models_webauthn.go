package cognitoidp

import "time"

// WebAuthnCredential represents a registered passkey/WebAuthn credential for a user.
type WebAuthnCredential struct {
	CreatedAt               time.Time `json:"createdAt"`
	CredentialID            string    `json:"credentialID,omitempty"`
	FriendlyName            string    `json:"friendlyName,omitempty"`
	RelyingPartyID          string    `json:"relyingPartyID,omitempty"`
	AuthenticatorAttachment string    `json:"authenticatorAttachment,omitempty"`
}

type completeWebAuthnRegistrationInput struct {
	Credential  map[string]any `json:"Credential,omitempty"`
	AccessToken string         `json:"AccessToken,omitempty"`
}

type completeWebAuthnRegistrationOutput struct{}

type deleteWebAuthnCredentialInput struct {
	AccessToken  string `json:"AccessToken,omitempty"`
	CredentialID string `json:"CredentialId,omitempty"`
}

type deleteWebAuthnCredentialOutput struct{}

type listWebAuthnCredentialsInput struct {
	AccessToken string `json:"AccessToken,omitempty"`
	NextToken   string `json:"NextToken,omitempty"`
	MaxResults  int    `json:"MaxResults,omitempty"`
}

type webAuthnCredentialDescriptionType struct {
	CredentialID            string  `json:"CredentialId,omitempty"`
	FriendlyName            string  `json:"FriendlyName,omitempty"`
	RelyingPartyID          string  `json:"RelyingPartyId,omitempty"`
	AuthenticatorAttachment string  `json:"AuthenticatorAttachment,omitempty"`
	CreatedAt               float64 `json:"CreatedAt,omitempty"`
}

type listWebAuthnCredentialsOutput struct {
	NextToken   string                              `json:"NextToken,omitempty"`
	Credentials []webAuthnCredentialDescriptionType `json:"Credentials,omitempty"`
}

type startWebAuthnRegistrationInput struct {
	AccessToken string `json:"AccessToken,omitempty"`
}

type startWebAuthnRegistrationOutput struct {
	CredentialCreationOptions map[string]any `json:"CredentialCreationOptions,omitempty"`
}
