package eventbridge

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// CreateAPIDestination creates a new API destination.
func (b *InMemoryBackend) CreateAPIDestination(ctx context.Context,
	input CreateAPIDestinationInput,
) (*APIDestination, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	if input.ConnectionArn == "" {
		return nil, fmt.Errorf("%w: ConnectionArn is required", ErrInvalidParameter)
	}

	if input.InvocationEndpoint == "" {
		return nil, fmt.Errorf("%w: InvocationEndpoint is required", ErrInvalidParameter)
	}

	if input.HTTPMethod == "" {
		return nil, fmt.Errorf("%w: HttpMethod is required", ErrInvalidParameter)
	}

	if !isValidHTTPMethod(input.HTTPMethod) {
		return nil, fmt.Errorf(
			"%w: HttpMethod must be one of GET, HEAD, POST, OPTIONS, PUT, DELETE, PATCH",
			ErrInvalidParameter,
		)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("CreateAPIDestination")
	defer b.mu.Unlock()

	if b.apiDestinationsTable(region).Has(input.Name) {
		return nil, fmt.Errorf(
			"%w: API destination %s already exists",
			ErrAlreadyExists,
			input.Name,
		)
	}

	now := time.Now()
	dst := &APIDestination{
		APIDestinationArn:            b.apiDestinationARN(input.Name),
		APIDestinationState:          stateActive,
		ConnectionArn:                input.ConnectionArn,
		CreationTime:                 now,
		Description:                  input.Description,
		HTTPMethod:                   input.HTTPMethod,
		InvocationEndpoint:           input.InvocationEndpoint,
		InvocationRateLimitPerSecond: input.InvocationRateLimitPerSecond,
		LastModifiedTime:             now,
		Name:                         input.Name,
	}
	b.apiDestinationsTable(region).Put(dst)

	cp := *dst

	return &cp, nil
}

// DeleteAPIDestination deletes an API destination.
func (b *InMemoryBackend) DeleteAPIDestination(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("DeleteAPIDestination")
	defer b.mu.Unlock()

	store := b.apiDestinationsTable(region)
	if !store.Has(name) {
		return fmt.Errorf("%w: API destination %s not found", ErrNotFound, name)
	}

	store.Delete(name)

	return nil
}

// DescribeAPIDestination returns a single API destination by name.
func (b *InMemoryBackend) DescribeAPIDestination(ctx context.Context, name string) (*APIDestination, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("DescribeAPIDestination")
	defer b.mu.RUnlock()

	dst, exists := b.apiDestinationsTable(region).Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: API destination %s not found", ErrNotFound, name)
	}

	cp := *dst

	return &cp, nil
}

// ListAPIDestinations returns API destinations optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListAPIDestinations(ctx context.Context,
	namePrefix, nextToken string,
) ([]APIDestination, string, error) {
	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListAPIDestinations")
	defer b.mu.RUnlock()

	store := b.apiDestinationsTable(region)
	all := make([]APIDestination, 0, store.Len())
	for _, d := range store.All() {
		if namePrefix == "" || strings.HasPrefix(d.Name, namePrefix) {
			all = append(all, *d)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// UpdateAPIDestination updates an existing API destination.
func (b *InMemoryBackend) UpdateAPIDestination(ctx context.Context,
	input UpdateAPIDestinationInput,
) (*APIDestination, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("UpdateAPIDestination")
	defer b.mu.Unlock()

	dst, exists := b.apiDestinationsTable(region).Get(input.Name)
	if !exists {
		return nil, fmt.Errorf("%w: API destination %s not found", ErrNotFound, input.Name)
	}

	if input.ConnectionArn != "" {
		dst.ConnectionArn = input.ConnectionArn
	}
	if input.Description != "" {
		dst.Description = input.Description
	}
	if input.HTTPMethod != "" {
		dst.HTTPMethod = input.HTTPMethod
	}
	if input.InvocationEndpoint != "" {
		dst.InvocationEndpoint = input.InvocationEndpoint
	}
	if input.InvocationRateLimitPerSecond > 0 {
		dst.InvocationRateLimitPerSecond = input.InvocationRateLimitPerSecond
	}
	dst.LastModifiedTime = time.Now()

	cp := *dst

	return &cp, nil
}

// AddAPIDestinationInternal adds an API destination directly for testing.
func (b *InMemoryBackend) AddAPIDestinationInternal(dst *APIDestination) {
	b.mu.Lock("AddAPIDestinationInternal")
	defer b.mu.Unlock()

	cp := *dst
	b.apiDestinationsTable(b.region).Put(&cp)
}

// isValidHTTPMethod reports whether method is a supported API Destination HTTP method.
func isValidHTTPMethod(method string) bool {
	validMethods := map[string]struct{}{
		"GET":     {},
		"HEAD":    {},
		"POST":    {},
		"OPTIONS": {},
		"PUT":     {},
		"DELETE":  {},
		"PATCH":   {},
	}
	_, ok := validMethods[strings.ToUpper(method)]

	return ok
}

// apiDestLimiter spaces requests to a single API destination so that no more
// than the configured rate per second are dispatched. next is the earliest
// time the next request may fire.
type apiDestLimiter struct {
	next time.Time
	mu   sync.Mutex
}

// ResolveAPIDestination resolves an API-destination ARN to the concrete
// invocation config plus the (un-masked) connection credentials used to sign
// the outbound request. It returns false if the destination does not exist.
// Reads use direct nil-safe map access under the read lock to avoid the
// lazy-init writes performed by the *Store accessors.
func (b *InMemoryBackend) ResolveAPIDestination(destARN string) (*ResolvedAPIDestination, bool) {
	region := arnRegion(destARN)
	if region == "" {
		region = b.region
	}
	name := arnResourceName(destARN, "api-destination")
	if name == "" {
		return nil, false
	}

	b.mu.RLock("ResolveAPIDestination")
	defer b.mu.RUnlock()

	destTable := b.apiDestinations[region]
	if destTable == nil {
		return nil, false
	}
	dest, ok := destTable.Get(name)
	if !ok {
		return nil, false
	}

	resolved := &ResolvedAPIDestination{
		HTTPMethod:         dest.HTTPMethod,
		Endpoint:           dest.InvocationEndpoint,
		RateLimitPerSecond: dest.InvocationRateLimitPerSecond,
	}

	connRegion := arnRegion(dest.ConnectionArn)
	if connRegion == "" {
		connRegion = region
	}
	connName := arnResourceName(dest.ConnectionArn, "connection")
	if connTable := b.connections[connRegion]; connTable != nil {
		if conn, connOK := connTable.Get(connName); connOK {
			applyConnectionAuthToResolved(resolved, conn)
		}
	}

	return resolved, true
}

// applyConnectionAuthToResolved copies a connection's (un-masked) auth into the
// resolved destination.
func applyConnectionAuthToResolved(resolved *ResolvedAPIDestination, conn *Connection) {
	resolved.AuthType = conn.AuthorizationType

	auth := conn.authSecret
	if auth == nil {
		return
	}

	if auth.BasicAuthParameters != nil {
		resolved.BasicUsername = auth.BasicAuthParameters.Username
		resolved.BasicPassword = auth.BasicAuthParameters.Password
	}
	if auth.APIKeyAuthParameters != nil {
		resolved.APIKeyName = auth.APIKeyAuthParameters.APIKeyName
		resolved.APIKeyValue = auth.APIKeyAuthParameters.APIKeyValue
	}
	if auth.OAuthParameters != nil {
		oauth := &ResolvedOAuth{
			AuthorizationEndpoint: auth.OAuthParameters.AuthorizationEndpoint,
			HTTPMethod:            auth.OAuthParameters.HTTPMethod,
		}
		if auth.OAuthParameters.ClientParameters != nil {
			oauth.ClientID = auth.OAuthParameters.ClientParameters.ClientID
			oauth.ClientSecret = auth.OAuthParameters.ClientParameters.ClientSecret
		}
		if hp := auth.OAuthParameters.OAuthHTTPParameters; hp != nil {
			oauth.HeaderParameters = hp.HeaderParameters
			oauth.QueryStringParameters = hp.QueryStringParameters
			oauth.BodyParameters = hp.BodyParameters
		}
		resolved.OAuth = oauth
	}
	if hp := auth.InvocationHTTPParameters; hp != nil {
		resolved.HeaderParameters = hp.HeaderParameters
		resolved.QueryStringParameters = hp.QueryStringParameters
		resolved.BodyParameters = hp.BodyParameters
	}
}

// WaitAPIDestinationRateLimit blocks until the destination's configured
// InvocationRateLimitPerSecond permits another request, or ctx is cancelled.
// A non-positive rate imposes no limit. Requests are spaced by 1s/rate so a
// burst of deliveries to the same destination is throttled to the target rate.
func (b *InMemoryBackend) WaitAPIDestinationRateLimit(
	ctx context.Context,
	destARN string,
	ratePerSecond int,
) {
	if ratePerSecond <= 0 {
		return
	}

	interval := time.Second / time.Duration(ratePerSecond)
	limAny, _ := b.apiDestLimiters.LoadOrStore(destARN, &apiDestLimiter{})
	lim, ok := limAny.(*apiDestLimiter)
	if !ok {
		return
	}

	lim.mu.Lock()
	now := time.Now()
	start := lim.next
	if start.Before(now) {
		start = now
	}
	lim.next = start.Add(interval)
	wait := time.Until(start)
	lim.mu.Unlock()

	if wait <= 0 {
		return
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

// arnFieldCount is the number of ":"-separated fields in an ARN
// (arn:partition:service:region:account:resource); the resource itself may
// contain further ":" so the split is bounded to this count.
const arnFieldCount = 6

// arnRegion returns the region field (index 3) of an ARN, or "" if malformed.
func arnRegion(a string) string {
	parts := strings.SplitN(a, ":", arnFieldCount)
	if len(parts) < arnFieldCount {
		return ""
	}

	return parts[3]
}

// arnResourceName extracts the resource name from an EventBridge ARN of the form
// arn:aws:events:region:account:<resourceType>/<name>[/<suffix>]. It returns ""
// if the ARN does not carry the expected resource type.
func arnResourceName(a, resourceType string) string {
	parts := strings.SplitN(a, ":", arnFieldCount)
	if len(parts) < arnFieldCount {
		return ""
	}
	resource := parts[arnFieldCount-1]
	prefix := resourceType + "/"
	if !strings.HasPrefix(resource, prefix) {
		return ""
	}
	rest := resource[len(prefix):]
	if name, _, found := strings.Cut(rest, "/"); found {
		return name
	}

	return rest
}
