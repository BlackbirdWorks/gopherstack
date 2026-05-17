package sts

// StorageBackend defines the STS service backend interface.
type StorageBackend interface {
	AssumeRole(input *AssumeRoleInput) (*AssumeRoleResponse, error)
	AssumeRoleWithSAML(input *AssumeRoleWithSAMLInput) (*AssumeRoleWithSAMLResponse, error)
	AssumeRoleWithWebIdentity(input *AssumeRoleWithWebIdentityInput) (*AssumeRoleWithWebIdentityResponse, error)
	AssumeRoot(input *AssumeRootInput) (*AssumeRootResponse, error)
	// GetCallerIdentity returns the caller's identity. sessionToken is the X-Amz-Security-Token
	// value; an empty string means long-term credentials are in use.
	GetCallerIdentity(accessKeyID, sessionToken string) (*GetCallerIdentityResponse, error)
	GetDelegatedAccessToken(input *GetDelegatedAccessTokenInput) (*GetDelegatedAccessTokenResponse, error)
	GetFederationToken(input *GetFederationTokenInput) (*GetFederationTokenResponse, error)
	GetSessionToken(input *GetSessionTokenInput) (*GetSessionTokenResponse, error)
	GetWebIdentityToken(input *GetWebIdentityTokenInput) (*GetWebIdentityTokenResponse, error)
	// ValidateSessionCredential looks up an active session by (accessKeyID, sessionToken) pair.
	// Returns the SessionInfo on match, ErrSessionNotFound when the key is unknown,
	// or ErrAccessDenied when the session token does not match the stored value.
	ValidateSessionCredential(accessKeyID, sessionToken string) (*SessionInfo, error)
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
