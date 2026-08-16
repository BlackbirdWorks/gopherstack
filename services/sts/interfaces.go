package sts

// StorageBackend defines the STS service backend interface.
type StorageBackend interface {
	AssumeRole(input *AssumeRoleInput) (*AssumeRoleResponse, error)
	AssumeRoleWithSAML(input *AssumeRoleWithSAMLInput) (*AssumeRoleWithSAMLResponse, error)
	AssumeRoleWithWebIdentity(
		input *AssumeRoleWithWebIdentityInput,
	) (*AssumeRoleWithWebIdentityResponse, error)
	AssumeRoot(input *AssumeRootInput) (*AssumeRootResponse, error)
	// GetCallerIdentity returns the caller's identity. sessionToken is the X-Amz-Security-Token
	// value; an empty string means long-term credentials are in use.
	GetCallerIdentity(accessKeyID, sessionToken string) (*GetCallerIdentityResponse, error)
	GetDelegatedAccessToken(
		input *GetDelegatedAccessTokenInput,
	) (*GetDelegatedAccessTokenResponse, error)
	GetFederationToken(input *GetFederationTokenInput) (*GetFederationTokenResponse, error)
	GetSessionToken(input *GetSessionTokenInput) (*GetSessionTokenResponse, error)
	GetWebIdentityToken(input *GetWebIdentityTokenInput) (*GetWebIdentityTokenResponse, error)
	// ValidateSessionCredential looks up an active session by (accessKeyID, sessionToken) pair.
	// Returns the SessionInfo on match, ErrSessionNotFound when the key is unknown,
	// or ErrAccessDenied when the session token does not match the stored value.
	ValidateSessionCredential(accessKeyID, sessionToken string) (*SessionInfo, error)
	// LookupSession returns the active SessionInfo for the given access key and optional
	// session token, or nil if no matching non-expired session exists.
	LookupSession(accessKeyID, sessionToken string) *SessionInfo
	// LookupUserArn returns the IAM user ARN for the given access key ID if a UserLookup is configured.
	LookupUserArn(accessKeyID string) (string, error)
	// IssueEncodedAuthorizationMessage encodes a plaintext message in the STS-proprietary
	// format that VerifyEncodedAuthorizationMessage can later authenticate.
	IssueEncodedAuthorizationMessage(decodedMsg string) string
	// VerifyEncodedAuthorizationMessage authenticates and decodes a message previously
	// issued by IssueEncodedAuthorizationMessage. Returns ErrInvalidAuthorizationMessage
	// when the encoded value was not issued by this backend.
	VerifyEncodedAuthorizationMessage(encoded string) (string, error)
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
