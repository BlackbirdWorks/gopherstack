package sso

import (
	"time"

	"github.com/blackbirdworks/gopherstack/services/ssooidc"
)

// TokenSource validates the bearer access tokens minted by the OIDC service.
type TokenSource interface {
	SessionForAccessToken(token string) (ssooidc.Session, error)
	RevokeSession(token string) error
}

// Entitlement is one permission set assigned to a user (directly or through a group) in an account.
type Entitlement struct {
	AccountID         string
	PermissionSetArn  string
	PermissionSetName string
	InstanceArn       string
}

// AccountInfo is the display data of an account.
type AccountInfo struct {
	Name  string
	Email string
}

// Credentials are short-lived role credentials.
type Credentials struct {
	Expiration      time.Time
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// Directory resolves entitlements and mints role credentials. Implementations look up
// sibling services lazily, at call time.
type Directory interface {
	Entitlements(subject ssooidc.Subject) ([]Entitlement, error)
	AccountInfo(accountID string) AccountInfo
	IssueCredentials(e Entitlement, subject ssooidc.Subject) (Credentials, error)
}
