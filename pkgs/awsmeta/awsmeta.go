// Package awsmeta carries AWS request metadata (account, region, partition,
// request ID) on context.Context for use by service backends.
//
// Handler middleware extracts metadata from the incoming HTTP request (SigV4
// auth header, X-Amz-* headers, configured defaults) and stashes it via Set.
// Service backends read it with Get to scope state per account/region without
// each service defining its own context key.
package awsmeta

import (
	"context"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// Metadata carries AWS request-scoped identity and routing fields.
type Metadata struct {
	// Account is the 12-digit AWS account ID. Defaults to "000000000000".
	Account string
	// Region is the AWS region (e.g. "us-east-1"). Empty for global services.
	Region string
	// Partition is the AWS partition (aws, aws-cn, aws-us-gov). Defaults to "aws".
	Partition string
	// RequestID is the X-Amz-Request-Id correlation value.
	RequestID string
}

type ctxKey struct{}

// DefaultAccount is the gopherstack default 12-digit account ID.
const DefaultAccount = "000000000000"

// DefaultPartition is the public AWS commercial partition.
const DefaultPartition = "aws"

// Set returns a child context carrying m. Service handlers/backends read it
// with Get. Passing nil is a no-op that returns ctx unchanged.
func Set(ctx context.Context, m *Metadata) context.Context {
	if m == nil {
		return ctx
	}

	return context.WithValue(ctx, ctxKey{}, m)
}

// Get returns the metadata carried on ctx, or a zero-value *Metadata with
// defaults filled in if none was set. The return is never nil so callers can
// dereference fields without a nil check.
func Get(ctx context.Context) *Metadata {
	if ctx == nil {
		return defaults()
	}

	if v, ok := ctx.Value(ctxKey{}).(*Metadata); ok && v != nil {
		return v
	}

	return defaults()
}

// Region is a convenience that returns Get(ctx).Region.
func Region(ctx context.Context) string {
	return Get(ctx).Region
}

// Account is a convenience that returns Get(ctx).Account.
func Account(ctx context.Context) string {
	return Get(ctx).Account
}

// Partition is a convenience that returns Get(ctx).Partition.
func Partition(ctx context.Context) string {
	return Get(ctx).Partition
}

// FromRequest builds a Metadata by extracting fields from r. defaultRegion is
// applied when no region can be derived from the SigV4 scope or X-Amz-Region
// headers; the returned Metadata always has non-empty Account and Partition.
func FromRequest(r *http.Request, defaultRegion string) *Metadata {
	if r == nil {
		m := defaults()
		m.Region = defaultRegion

		return m
	}

	m := &Metadata{
		Account:   DefaultAccount,
		Region:    httputils.ExtractRegionFromRequest(r, defaultRegion),
		Partition: DefaultPartition,
		RequestID: r.Header.Get("X-Amz-Request-Id"),
	}

	if v := r.Header.Get("X-Amz-Account-Id"); v != "" {
		m.Account = v
	}

	return m
}

// defaults returns a Metadata with safe defaults and no region set. Callers
// that observe Region == "" can decide whether the service is global or
// whether to fall back to a configured default.
func defaults() *Metadata {
	return &Metadata{
		Account:   DefaultAccount,
		Partition: DefaultPartition,
	}
}
