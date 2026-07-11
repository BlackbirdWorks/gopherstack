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

// Metadata carries AWS request-scoped identity and routing fields.
type Metadata struct {
	// Account is the 12-digit AWS account ID.
	Account string
	// Region is the AWS region (e.g. "us-east-1"). Empty for global services.
	Region string
	// Partition is the AWS partition (aws, aws-cn, aws-us-gov).
	Partition string
	// RequestID is the X-Amz-Request-Id correlation value.
	RequestID string
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
		Account:   DefaultAccount,
		Region:    httputils.ExtractRegionFromRequest(r, defaultRegion),
		Partition: DefaultPartition,
		RequestID: httputils.SanitizeHeaderString(r.Header.Get("X-Amz-Request-Id")),
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
