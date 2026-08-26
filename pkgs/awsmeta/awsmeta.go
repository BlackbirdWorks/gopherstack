// Package awsmeta defines the Metadata struct that carries AWS request-scoped
// identity (account, region, partition, request ID) and a helper to populate
// it from an *http.Request.
//
// Storage on context.Context is delegated to pkgs/ctxval — this package
// exposes a single Key plus thin Set/Get/Region/Account wrappers so service
// backends pull metadata uniformly without each one defining its own key.
package awsmeta

import (
	"context"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/ctxval"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// PrincipalKind describes the type of AWS principal making a request.
type PrincipalKind string

const (
	// PrincipalKindUser represents an IAM user principal.
	PrincipalKindUser PrincipalKind = "User"
	// PrincipalKindAssumedRole represents an STS assumed-role session principal.
	PrincipalKindAssumedRole PrincipalKind = "AssumedRole"
	// PrincipalKindRole represents an IAM role principal.
	PrincipalKindRole PrincipalKind = "Role"
	// PrincipalKindRoot represents the AWS account root user principal.
	PrincipalKindRoot PrincipalKind = "Root"
	// PrincipalKindAnonymous represents an unauthenticated/anonymous caller.
	PrincipalKindAnonymous PrincipalKind = "Anonymous"
)

// Principal represents the resolved AWS identity of the caller.
type Principal struct {
	Kind           PrincipalKind `json:"kind"`
	Arn            string        `json:"arn"`
	UserName       string        `json:"userName,omitempty"`
	AccountID      string        `json:"accountID"`
	SessionName    string        `json:"sessionName,omitempty"`
	UserID         string        `json:"userID,omitempty"`
	SourceIdentity string        `json:"sourceIdentity,omitempty"`
}

// PrincipalResolver resolves an access key (and optional session token) to an AWS principal.
type PrincipalResolver interface {
	ResolvePrincipal(ctx context.Context, accessKeyID, sessionToken string) (*Principal, bool)
}

// PrincipalResolverChain is a slice of PrincipalResolvers evaluated in order.
type PrincipalResolverChain []PrincipalResolver

// ResolvePrincipal evaluates each resolver in the chain until one returns a principal.
func (c PrincipalResolverChain) ResolvePrincipal(
	ctx context.Context,
	accessKeyID, sessionToken string,
) (*Principal, bool) {
	for _, r := range c {
		if r == nil {
			continue
		}

		if p, ok := r.ResolvePrincipal(ctx, accessKeyID, sessionToken); ok && p != nil {
			return p, true
		}
	}

	return nil, false
}

// Metadata carries AWS request-scoped identity and routing fields.
type Metadata struct {
	// Principal is the resolved IAM/STS identity of the caller.
	Principal *Principal
	// Account is the 12-digit AWS account ID.
	Account string
	// Region is the AWS region (e.g. "us-east-1"). Empty for global services.
	Region string
	// Partition is the AWS partition (aws, aws-cn, aws-us-gov).
	Partition string
	// RequestID is the X-Amz-Request-Id correlation value.
	RequestID string
	// AccessKeyID is the AWS access key ID extracted from the SigV4 credential scope.
	AccessKeyID string
	// SecurityToken is the session token extracted from X-Amz-Security-Token.
	SecurityToken string
	// Service is the AWS service name extracted from the SigV4 credential scope.
	Service string
}

// DefaultAccount is the gopherstack default 12-digit account ID.
const DefaultAccount = "000000000000"

// DefaultPartition is the public AWS commercial partition.
const DefaultPartition = "aws"

// Key is the context key under which Metadata is stored. Exported so callers
// that want raw ctxval access (e.g. middleware that wraps Set with logging)
// can reuse it.
var Key = ctxval.NewKey[*Metadata]("awsmeta") //nolint:gochecknoglobals // existing issue.

// Set returns a child context carrying m. Passing nil is a no-op.
func Set(ctx context.Context, m *Metadata) context.Context {
	if m == nil {
		return ctx
	}

	return Key.Set(ctx, m)
}

// Get returns the metadata carried on ctx, or a *Metadata with default Account
// and Partition fields when none was set. The return is never nil so callers
// can dereference fields without a guard.
func Get(ctx context.Context) *Metadata {
	if m, ok := Key.Get(ctx); ok && m != nil {
		return m
	}

	return defaults()
}

// Region returns Get(ctx).Region.
func Region(ctx context.Context) string {
	return Get(ctx).Region
}

// Account returns Get(ctx).Account.
func Account(ctx context.Context) string {
	return Get(ctx).Account
}

// Partition returns Get(ctx).Partition.
func Partition(ctx context.Context) string {
	return Get(ctx).Partition
}

// AccessKeyID returns Get(ctx).AccessKeyID.
func AccessKeyID(ctx context.Context) string {
	return Get(ctx).AccessKeyID
}

// Service returns Get(ctx).Service.
func Service(ctx context.Context) string {
	return Get(ctx).Service
}

// GetPrincipal returns the Principal associated with ctx, or nil if unauthenticated/unresolved.
func GetPrincipal(ctx context.Context) *Principal {
	return Get(ctx).Principal
}

// CallerArn returns the ARN of the calling principal, or empty string.
func CallerArn(ctx context.Context) string {
	if p := Get(ctx).Principal; p != nil {
		return p.Arn
	}

	return ""
}

// UserName returns the username of the calling principal, or empty string.
func UserName(ctx context.Context) string {
	if p := Get(ctx).Principal; p != nil {
		return p.UserName
	}

	return ""
}

// UserID returns the unique user ID of the calling principal, or empty string.
func UserID(ctx context.Context) string {
	if p := Get(ctx).Principal; p != nil {
		return p.UserID
	}

	return ""
}

// FromRequest builds a Metadata from r. defaultRegion is applied when no
// region is derivable from the SigV4 scope. Always returns non-nil with
// Account and Partition populated.
func FromRequest(r *http.Request, defaultRegion string) *Metadata {
	if r == nil {
		m := defaults()
		m.Region = httputils.SanitizeHeaderString(defaultRegion)

		return m
	}

	m := &Metadata{
		Account:       DefaultAccount,
		Region:        httputils.ExtractRegionFromRequest(r, defaultRegion),
		Partition:     DefaultPartition,
		RequestID:     httputils.SanitizeHeaderString(r.Header.Get("X-Amz-Request-Id")),
		AccessKeyID:   httputils.ExtractAccessKeyFromRequest(r),
		SecurityToken: httputils.ExtractSecurityTokenFromRequest(r),
		Service:       httputils.ExtractServiceFromRequest(r),
	}

	if v := r.Header.Get("X-Amz-Account-Id"); v != "" {
		m.Account = httputils.SanitizeHeaderString(v)
	}

	return m
}

func defaults() *Metadata {
	return &Metadata{
		Account:   DefaultAccount,
		Partition: DefaultPartition,
	}
}
