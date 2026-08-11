package cognitoidp

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"sort"
	"time"
)

const (
	webauthnChallengeLen   = 32
	webauthnUserHandleLen  = 16
	webauthnTimeoutMillis  = 60000
	webauthnRelyingPartyID = "localhost"
)

// StartWebAuthnRegistration returns real WebAuthn CredentialCreationOptions
// (rp, user, challenge, pubKeyCredParams) for the authenticated user to pass
// to the browser's navigator.credentials.create().
func (b *InMemoryBackend) StartWebAuthnRegistration(accessToken string) (map[string]any, error) {
	b.mu.RLock("StartWebAuthnRegistration")
	defer b.mu.RUnlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return nil, err
	}

	rpName := user.UserPoolID
	if pool, ok := b.pools.Get(user.UserPoolID); ok && pool.Name != "" {
		rpName = pool.Name
	}

	challenge := make([]byte, webauthnChallengeLen)
	if _, randErr := rand.Read(challenge); randErr != nil {
		return nil, fmt.Errorf("generating webauthn challenge: %w", randErr)
	}

	userHandle := make([]byte, webauthnUserHandleLen)
	if _, randErr := rand.Read(userHandle); randErr != nil {
		return nil, fmt.Errorf("generating webauthn user handle: %w", randErr)
	}

	enc := base64.RawURLEncoding

	const jsonKeyName = "name"

	return map[string]any{
		"rp": map[string]any{
			"id":        webauthnRelyingPartyID,
			jsonKeyName: rpName,
		},
		"user": map[string]any{
			"id":          enc.EncodeToString(userHandle),
			jsonKeyName:   user.Username,
			"displayName": user.Username,
		},
		"challenge": enc.EncodeToString(challenge),
		"pubKeyCredParams": []map[string]any{
			{"type": "public-key", "alg": -7},
			{"type": "public-key", "alg": -257},
		},
		"timeout":     webauthnTimeoutMillis,
		"attestation": "none",
		"authenticatorSelection": map[string]any{
			"userVerification": "preferred",
		},
	}, nil
}

// CompleteWebAuthnRegistration stores a WebAuthn credential for the authenticated
// user. transports is the browser-reported AuthenticatorAttestationResponse
// transports list (credential.response.transports), if the client included one.
func (b *InMemoryBackend) CompleteWebAuthnRegistration(
	accessToken, credentialID, authenticatorAttachment string, transports []string,
) (*WebAuthnCredential, error) {
	b.mu.Lock("CompleteWebAuthnRegistration")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return nil, err
	}

	if credentialID == "" {
		return nil, fmt.Errorf("%w: Credential.id is required", ErrInvalidParameter)
	}

	key := userStateKey(user.UserPoolID, user.Username)
	if b.webauthnCredentials[key] == nil {
		b.webauthnCredentials[key] = make(map[string]*WebAuthnCredential)
	}

	if transports == nil {
		transports = []string{}
	}

	cred := &WebAuthnCredential{
		CredentialID:            credentialID,
		FriendlyName:            fmt.Sprintf("Passkey %d", len(b.webauthnCredentials[key])+1),
		RelyingPartyID:          webauthnRelyingPartyID,
		AuthenticatorAttachment: authenticatorAttachment,
		AuthenticatorTransports: transports,
		CreatedAt:               time.Now(),
	}
	b.webauthnCredentials[key][credentialID] = cred

	cp := *cred

	return &cp, nil
}

// ListWebAuthnCredentials returns a page of WebAuthn credentials for the authenticated user.
func (b *InMemoryBackend) ListWebAuthnCredentials(
	accessToken string,
	limit int,
	nextToken string,
) ([]*WebAuthnCredential, string, error) {
	b.mu.RLock("ListWebAuthnCredentials")
	defer b.mu.RUnlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return nil, "", err
	}

	creds := b.webauthnCredentials[userStateKey(user.UserPoolID, user.Username)]
	all := make([]*WebAuthnCredential, 0, len(creds))

	for _, c := range creds {
		cp := *c
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].CredentialID < all[j].CredentialID })

	startIdx := 0

	if nextToken != "" {
		for i, c := range all {
			if c.CredentialID == nextToken {
				startIdx = i

				break
			}
		}
	}

	all = all[startIdx:]

	if limit <= 0 || limit >= len(all) {
		return all, "", nil
	}

	page := all[:limit]
	newToken := ""

	if limit < len(all) {
		newToken = all[limit].CredentialID
	}

	return page, newToken, nil
}

// DeleteWebAuthnCredential removes a WebAuthn credential for the authenticated user.
func (b *InMemoryBackend) DeleteWebAuthnCredential(accessToken, credentialID string) error {
	b.mu.Lock("DeleteWebAuthnCredential")
	defer b.mu.Unlock()

	user, err := b.findUserByAccessTokenLocked(accessToken)
	if err != nil {
		return err
	}

	key := userStateKey(user.UserPoolID, user.Username)
	if _, ok := b.webauthnCredentials[key][credentialID]; !ok {
		return fmt.Errorf("%w: credential %q not found", ErrWebAuthnCredentialNotFound, credentialID)
	}

	delete(b.webauthnCredentials[key], credentialID)

	return nil
}
