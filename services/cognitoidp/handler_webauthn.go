package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// handleCompleteWebAuthnRegistration authenticates the access token and persists the
// supplied WebAuthn credential (id + authenticator attachment) on the user's account.
func (h *Handler) handleCompleteWebAuthnRegistration(
	_ context.Context,
	in *completeWebAuthnRegistrationInput,
) (*completeWebAuthnRegistrationOutput, error) {
	credentialID, _ := in.Credential["id"].(string)
	attachment, _ := in.Credential["authenticatorAttachment"].(string)

	if _, err := h.Backend.CompleteWebAuthnRegistration(in.AccessToken, credentialID, attachment); err != nil {
		return nil, err
	}

	return &completeWebAuthnRegistrationOutput{}, nil
}

func (h *Handler) handleDeleteWebAuthnCredential(
	_ context.Context,
	in *deleteWebAuthnCredentialInput,
) (*deleteWebAuthnCredentialOutput, error) {
	if err := h.Backend.DeleteWebAuthnCredential(in.AccessToken, in.CredentialID); err != nil {
		return nil, err
	}

	return &deleteWebAuthnCredentialOutput{}, nil
}

func (h *Handler) handleListWebAuthnCredentials(
	_ context.Context,
	in *listWebAuthnCredentialsInput,
) (*listWebAuthnCredentialsOutput, error) {
	limit, err := validateCognitoMaxResults(in.MaxResults)
	if err != nil {
		return nil, err
	}

	creds, token, err := h.Backend.ListWebAuthnCredentials(in.AccessToken, limit, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]webAuthnCredentialDescriptionType, 0, len(creds))
	for _, c := range creds {
		out = append(out, webAuthnCredentialDescriptionType{
			CredentialID:            c.CredentialID,
			FriendlyName:            c.FriendlyName,
			RelyingPartyID:          c.RelyingPartyID,
			AuthenticatorAttachment: c.AuthenticatorAttachment,
			CreatedAt:               float64(c.CreatedAt.Unix()),
		})
	}

	return &listWebAuthnCredentialsOutput{Credentials: out, NextToken: token}, nil
}

func (h *Handler) handleStartWebAuthnRegistration(
	_ context.Context,
	in *startWebAuthnRegistrationInput,
) (*startWebAuthnRegistrationOutput, error) {
	opts, err := h.Backend.StartWebAuthnRegistration(in.AccessToken)
	if err != nil {
		return nil, err
	}

	return &startWebAuthnRegistrationOutput{CredentialCreationOptions: opts}, nil
}

func (h *Handler) webauthnOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CompleteWebAuthnRegistration": service.WrapOp(h.handleCompleteWebAuthnRegistration),
		"DeleteWebAuthnCredential":     service.WrapOp(h.handleDeleteWebAuthnCredential),
		"ListWebAuthnCredentials":      service.WrapOp(h.handleListWebAuthnCredentials),
		"StartWebAuthnRegistration":    service.WrapOp(h.handleStartWebAuthnRegistration),
	}
}
