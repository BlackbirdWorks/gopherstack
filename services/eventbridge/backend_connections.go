package eventbridge

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// CreateConnection creates a new connection.
func (b *InMemoryBackend) CreateConnection(ctx context.Context, input CreateConnectionInput) (*Connection, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	if input.AuthorizationType == "" {
		return nil, fmt.Errorf("%w: AuthorizationType is required", ErrInvalidParameter)
	}

	if !isValidConnectionAuthType(input.AuthorizationType) {
		return nil, fmt.Errorf(
			"%w: AuthorizationType must be one of API_KEY, BASIC, OAUTH_CLIENT_CREDENTIALS",
			ErrInvalidParameter,
		)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("CreateConnection")
	defer b.mu.Unlock()

	if b.connectionsTable(region).Has(input.Name) {
		return nil, fmt.Errorf("%w: connection %s already exists", ErrAlreadyExists, input.Name)
	}

	now := time.Now()
	conn := &Connection{
		ConnectionArn:     b.connectionARN(input.Name),
		AuthorizationType: input.AuthorizationType,
		AuthParameters:    maskConnectionAuthParameters(input.AuthParameters),
		authSecret:        cloneConnectionAuthParameters(input.AuthParameters),
		ConnectionState:   "AUTHORIZED",
		CreationTime:      now,
		Description:       input.Description,
		LastModifiedTime:  now,
		Name:              input.Name,
	}
	b.connectionsTable(region).Put(conn)

	cp := *conn

	return &cp, nil
}

// DeauthorizeConnection deauthorizes a connection.
func (b *InMemoryBackend) DeauthorizeConnection(ctx context.Context, name string) (*Connection, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("DeauthorizeConnection")
	defer b.mu.Unlock()

	conn, exists := b.connectionsTable(region).Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: connection %s not found", ErrNotFound, name)
	}

	conn.ConnectionState = "DEAUTHORIZED"

	cp := *conn

	return &cp, nil
}

// DeleteConnection deletes a connection.
func (b *InMemoryBackend) DeleteConnection(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	store := b.connectionsTable(region)
	if !store.Has(name) {
		return fmt.Errorf("%w: connection %s not found", ErrNotFound, name)
	}

	store.Delete(name)

	return nil
}

// DescribeConnection returns a single connection by name.
func (b *InMemoryBackend) DescribeConnection(ctx context.Context, name string) (*Connection, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("DescribeConnection")
	defer b.mu.RUnlock()

	conn, exists := b.connectionsTable(region).Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: connection %s not found", ErrNotFound, name)
	}

	cp := *conn

	return &cp, nil
}

// ListConnections returns connections optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListConnections(ctx context.Context,
	namePrefix, nextToken string,
) ([]Connection, string, error) {
	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListConnections")
	defer b.mu.RUnlock()

	store := b.connectionsTable(region)
	all := make([]Connection, 0, store.Len())
	for _, c := range store.All() {
		if namePrefix == "" || strings.HasPrefix(c.Name, namePrefix) {
			all = append(all, *c)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// UpdateConnection updates an existing connection.
func (b *InMemoryBackend) UpdateConnection(ctx context.Context, input UpdateConnectionInput) (*Connection, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("UpdateConnection")
	defer b.mu.Unlock()

	conn, exists := b.connectionsTable(region).Get(input.Name)
	if !exists {
		return nil, fmt.Errorf("%w: connection %s not found", ErrNotFound, input.Name)
	}

	if input.Description != "" {
		conn.Description = input.Description
	}
	if input.AuthorizationType != "" {
		conn.AuthorizationType = input.AuthorizationType
	}
	if input.AuthParameters != nil {
		conn.AuthParameters = maskConnectionAuthParameters(input.AuthParameters)
		conn.authSecret = cloneConnectionAuthParameters(input.AuthParameters)
	}
	conn.LastModifiedTime = time.Now()

	cp := *conn

	return &cp, nil
}

// AddConnectionInternal adds a connection directly for testing.
func (b *InMemoryBackend) AddConnectionInternal(conn *Connection) {
	b.mu.Lock("AddConnectionInternal")
	defer b.mu.Unlock()

	cp := *conn
	b.connectionsTable(b.region).Put(&cp)
}

// Connection authorization types accepted by CreateConnection and honoured by
// API-destination delivery.
const (
	connectionAuthAPIKey = "API_KEY"
	connectionAuthBasic  = "BASIC"
	connectionAuthOAuth  = "OAUTH_CLIENT_CREDENTIALS"
)

// isValidConnectionAuthType reports whether authType is a valid connection authorization type.
func isValidConnectionAuthType(authType string) bool {
	validAuthTypes := map[string]struct{}{
		connectionAuthAPIKey: {},
		connectionAuthBasic:  {},
		connectionAuthOAuth:  {},
	}
	_, ok := validAuthTypes[authType]

	return ok
}

// maskConnectionAuthParameters returns a copy of the auth parameters with
// secret values redacted, matching AWS behaviour where sensitive credentials
// are never returned in plaintext from Describe/List operations.
func maskConnectionAuthParameters(p *ConnectionAuthParameters) *ConnectionAuthParameters {
	if p == nil {
		return nil
	}

	masked := &ConnectionAuthParameters{}

	if p.BasicAuthParameters != nil {
		masked.BasicAuthParameters = &ConnectionBasicAuthParameters{
			Username: p.BasicAuthParameters.Username,
			// Password is intentionally omitted (masked).
		}
	}

	if p.APIKeyAuthParameters != nil {
		masked.APIKeyAuthParameters = &ConnectionAPIKeyAuthParameters{
			APIKeyName: p.APIKeyAuthParameters.APIKeyName,
			// APIKeyValue is intentionally omitted (masked).
		}
	}

	if p.OAuthParameters != nil {
		op := &ConnectionOAuthParameters{
			AuthorizationEndpoint: p.OAuthParameters.AuthorizationEndpoint,
			HTTPMethod:            p.OAuthParameters.HTTPMethod,
		}
		if p.OAuthParameters.ClientParameters != nil {
			op.ClientParameters = &ConnectionOAuthClientParameters{
				ClientID: p.OAuthParameters.ClientParameters.ClientID,
				// ClientSecret is intentionally omitted (masked).
			}
		}
		if p.OAuthParameters.OAuthHTTPParameters != nil {
			op.OAuthHTTPParameters = maskHTTPParameters(p.OAuthParameters.OAuthHTTPParameters)
		}
		masked.OAuthParameters = op
	}

	if p.InvocationHTTPParameters != nil {
		masked.InvocationHTTPParameters = maskHTTPParameters(p.InvocationHTTPParameters)
	}

	return masked
}

// maskHTTPParameters returns a copy of ConnectionHTTPParameters with secret
// values marked as IsValueSecret=true and Value cleared.
func maskHTTPParameters(p *ConnectionHTTPParameters) *ConnectionHTTPParameters {
	if p == nil {
		return nil
	}

	m := &ConnectionHTTPParameters{}

	for _, bp := range p.BodyParameters {
		mp := ConnectionBodyParameter{Key: bp.Key, IsValueSecret: bp.IsValueSecret}
		if !bp.IsValueSecret {
			mp.Value = bp.Value
		}
		m.BodyParameters = append(m.BodyParameters, mp)
	}

	for _, hp := range p.HeaderParameters {
		mp := ConnectionHeaderParameter{Key: hp.Key, IsValueSecret: hp.IsValueSecret}
		if !hp.IsValueSecret {
			mp.Value = hp.Value
		}
		m.HeaderParameters = append(m.HeaderParameters, mp)
	}

	for _, qp := range p.QueryStringParameters {
		mp := ConnectionQueryStringParameter{Key: qp.Key, IsValueSecret: qp.IsValueSecret}
		if !qp.IsValueSecret {
			mp.Value = qp.Value
		}
		m.QueryStringParameters = append(m.QueryStringParameters, mp)
	}

	return m
}

// cloneConnectionAuthParameters deep-copies auth parameters, preserving the
// plaintext secret values. It is used to retain the un-masked credentials on
// the stored connection for outbound API-destination signing, independent of
// the masked copy returned by Describe/List.
func cloneConnectionAuthParameters(p *ConnectionAuthParameters) *ConnectionAuthParameters {
	if p == nil {
		return nil
	}

	clone := &ConnectionAuthParameters{}

	if p.BasicAuthParameters != nil {
		bp := *p.BasicAuthParameters
		clone.BasicAuthParameters = &bp
	}
	if p.APIKeyAuthParameters != nil {
		ap := *p.APIKeyAuthParameters
		clone.APIKeyAuthParameters = &ap
	}
	if p.OAuthParameters != nil {
		op := &ConnectionOAuthParameters{
			AuthorizationEndpoint: p.OAuthParameters.AuthorizationEndpoint,
			HTTPMethod:            p.OAuthParameters.HTTPMethod,
		}
		if p.OAuthParameters.ClientParameters != nil {
			cp := *p.OAuthParameters.ClientParameters
			op.ClientParameters = &cp
		}
		op.OAuthHTTPParameters = cloneHTTPParameters(p.OAuthParameters.OAuthHTTPParameters)
		clone.OAuthParameters = op
	}
	clone.InvocationHTTPParameters = cloneHTTPParameters(p.InvocationHTTPParameters)

	return clone
}

// cloneHTTPParameters deep-copies custom HTTP body/header/query parameters,
// retaining any secret values verbatim.
func cloneHTTPParameters(p *ConnectionHTTPParameters) *ConnectionHTTPParameters {
	if p == nil {
		return nil
	}

	clone := &ConnectionHTTPParameters{}
	clone.BodyParameters = append(clone.BodyParameters, p.BodyParameters...)
	clone.HeaderParameters = append(clone.HeaderParameters, p.HeaderParameters...)
	clone.QueryStringParameters = append(clone.QueryStringParameters, p.QueryStringParameters...)

	return clone
}
