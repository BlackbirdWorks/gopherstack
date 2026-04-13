package sts

// StorageBackend defines the STS service backend interface.
type StorageBackend interface {
	AssumeRole(input *AssumeRoleInput) (*AssumeRoleResponse, error)
	AssumeRoleWithSAML(input *AssumeRoleWithSAMLInput) (*AssumeRoleWithSAMLResponse, error)
	AssumeRoleWithWebIdentity(input *AssumeRoleWithWebIdentityInput) (*AssumeRoleWithWebIdentityResponse, error)
	AssumeRoot(input *AssumeRootInput) (*AssumeRootResponse, error)
	GetCallerIdentity(accessKeyID string) (*GetCallerIdentityResponse, error)
	GetDelegatedAccessToken(input *GetDelegatedAccessTokenInput) (*GetDelegatedAccessTokenResponse, error)
	GetFederationToken(input *GetFederationTokenInput) (*GetFederationTokenResponse, error)
	GetSessionToken(input *GetSessionTokenInput) (*GetSessionTokenResponse, error)
	GetWebIdentityToken(input *GetWebIdentityTokenInput) (*GetWebIdentityTokenResponse, error)
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
