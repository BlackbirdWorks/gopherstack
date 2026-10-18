package cognitoidentity

import "crypto/rsa"

const identitySessionName = "CognitoIdentityCredentials"

// RoleAssumer mints credentials for a role from a Cognito OpenID token (STS AssumeRoleWithWebIdentity).
type RoleAssumer interface {
	AssumeRoleWithWebIdentity(roleARN, sessionName, webIdentityToken string) (*Credentials, error)
}

// TokenKeyResolver looks up the signing key of a login token issued by an emulator-hosted user pool.
// hosted is false when the issuer is not hosted by this emulator.
type TokenKeyResolver interface {
	ResolveTokenKey(issuer, kid string) (key *rsa.PublicKey, hosted bool, err error)
}

// SetRoleAssumer wires the STS seam used by GetCredentialsForIdentity.
func (b *InMemoryBackend) SetRoleAssumer(a RoleAssumer) {
	b.mu.Lock("SetRoleAssumer")
	defer b.mu.Unlock()

	b.assumer = a
}

// SetTokenKeyResolver wires signature verification for user-pool login tokens.
func (b *InMemoryBackend) SetTokenKeyResolver(r TokenKeyResolver) {
	b.mu.Lock("SetTokenKeyResolver")
	defer b.mu.Unlock()

	b.keyResolver = r
}
