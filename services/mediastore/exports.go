package mediastore

import "context"

// ExportedContainer is a compatibility alias used by the dashboard package.
type ExportedContainer = Container

// WithRegion returns ctx carrying region for backend calls made outside a request.
func WithRegion(ctx context.Context, region string) context.Context {
	return context.WithValue(ctx, regionContextKey{}, region)
}
