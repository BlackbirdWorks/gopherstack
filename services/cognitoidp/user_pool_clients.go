package cognitoidp

import (
	"fmt"
	"maps"
	"sort"
	"time"
)

// DeleteUserPoolClient removes the app client with the given client ID from the given pool.
// If userPoolID is empty the pool ownership check is skipped.
func (b *InMemoryBackend) DeleteUserPoolClient(userPoolID, clientID string) error {
	b.mu.Lock("DeleteUserPoolClient")
	defer b.mu.Unlock()

	client, ok := b.clients.Get(clientID)
	if !ok {
		return fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	if userPoolID != "" && client.UserPoolID != userPoolID {
		return fmt.Errorf("%w: client %q not found in pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	b.clients.Delete(clientID)

	// Clean up any refresh tokens issued by this client to prevent leaks.
	b.deleteRefreshTokensForClientAndUserIndexLocked(clientID)

	return nil
}

// ListUserPoolClients returns all app clients for the given user pool sorted by client name.
func (b *InMemoryBackend) ListUserPoolClients(userPoolID string) ([]*UserPoolClient, error) {
	b.mu.RLock("ListUserPoolClients")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolClients := b.clientsByPool.Get(userPoolID)
	out := make([]*UserPoolClient, 0, len(poolClients))

	for _, c := range poolClients {
		cp := *c
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ClientName < out[j].ClientName })

	return out, nil
}

// CreateUserPoolClient creates a new app client for the given user pool.
func (b *InMemoryBackend) CreateUserPoolClient(userPoolID, clientName string) (*UserPoolClient, error) {
	b.mu.Lock("CreateUserPoolClient")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	client := &UserPoolClient{
		ClientID:   randomAlphanumeric(clientIDLen),
		ClientName: clientName,
		UserPoolID: userPoolID,
		CreatedAt:  time.Now(),
	}

	b.clients.Put(client)

	cp := *client

	return &cp, nil
}

// DescribeUserPoolClient returns the app client with the given client ID.
func (b *InMemoryBackend) DescribeUserPoolClient(userPoolID, clientID string) (*UserPoolClient, error) {
	b.mu.RLock("DescribeUserPoolClient")
	defer b.mu.RUnlock()

	client, ok := b.clients.Get(clientID)
	if !ok {
		return nil, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	if client.UserPoolID != userPoolID {
		return nil, fmt.Errorf("%w: client %q not found in pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	cp := *client

	return &cp, nil
}

// maxExtraClientSecrets is the real API's documented cap of 2 active secrets
// per app client, minus the 1 slot the original CreateUserPoolClient/
// UpdateUserPoolClient(GenerateSecret) secret already occupies.
const maxExtraClientSecrets = 1

// AddUserPoolClientSecret generates and stores a new, independently
// ClientSecretId-keyed secret for the given app client, capped at
// maxExtraClientSecrets (mirrors AWS Cognito's documented 2-active-secrets
// limit; verified against the AddUserPoolClientSecret deserializer, which
// declares LimitExceededException).
func (b *InMemoryBackend) AddUserPoolClientSecret(userPoolID, clientID string) (*ClientSecretRecord, error) {
	b.mu.Lock("AddUserPoolClientSecret")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	client, ok := b.clients.Get(clientID)
	if !ok {
		return nil, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	if client.UserPoolID != userPoolID {
		return nil, fmt.Errorf("%w: client %q does not belong to pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	if len(client.ExtraClientSecrets) >= maxExtraClientSecrets {
		return nil, fmt.Errorf(
			"%w: client %q already has the maximum number of active secrets",
			ErrLimitExceeded,
			clientID,
		)
	}

	record := ClientSecretRecord{
		ClientSecretID:         randomAlphanumeric(clientSecretIDLen),
		ClientSecretValue:      randomAlphanumeric(clientSecretLen),
		ClientSecretCreateDate: time.Now(),
	}
	client.ExtraClientSecrets = append(client.ExtraClientSecrets, record)

	return &record, nil
}

// UpdateUserPoolClient updates mutable properties of an app client.
func (b *InMemoryBackend) UpdateUserPoolClient(userPoolID, clientID, clientName string) (*UserPoolClient, error) {
	b.mu.Lock("UpdateUserPoolClient")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	client, ok := b.clients.Get(clientID)
	if !ok {
		return nil, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	if client.UserPoolID != userPoolID {
		return nil, fmt.Errorf("%w: client %q does not belong to pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	if clientName != "" {
		client.ClientName = clientName
	}

	client.UpdatedAt = time.Now()
	cp := *client

	return &cp, nil
}

// AddUserPoolClientInternal seeds a user pool client directly into the backend.
// Intended for use in tests only.
func (b *InMemoryBackend) AddUserPoolClientInternal(client *UserPoolClient) {
	b.mu.Lock("AddUserPoolClientInternal")
	defer b.mu.Unlock()

	b.clients.Put(client)
}

// CreateUserPoolClientWithOpts creates an app client with full OAuth and flow configuration.
func (b *InMemoryBackend) CreateUserPoolClientWithOpts(
	userPoolID, clientName string,
	opts UserPoolClientOptions,
) (*UserPoolClient, error) {
	b.mu.Lock("CreateUserPoolClientWithOpts")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	preventUserExistenceErrors, err := normalizePreventUserExistenceErrors(opts.PreventUserExistenceErrors)
	if err != nil {
		return nil, err
	}

	flows := make([]string, len(opts.AllowedOAuthFlows))
	copy(flows, opts.AllowedOAuthFlows)
	scopes := make([]string, len(opts.AllowedOAuthScopes))
	copy(scopes, opts.AllowedOAuthScopes)
	explicitFlows := make([]string, len(opts.ExplicitAuthFlows))
	copy(explicitFlows, opts.ExplicitAuthFlows)

	callbackURLs := make([]string, len(opts.CallbackURLs))
	copy(callbackURLs, opts.CallbackURLs)
	logoutURLs := make([]string, len(opts.LogoutURLs))
	copy(logoutURLs, opts.LogoutURLs)
	supportedIDPs := make([]string, len(opts.SupportedIdentityProviders))
	copy(supportedIDPs, opts.SupportedIdentityProviders)

	var tvu map[string]string
	if opts.TokenValidityUnits != nil {
		tvu = maps.Clone(opts.TokenValidityUnits)
	}

	client := &UserPoolClient{
		ClientID:                        randomAlphanumeric(clientIDLen),
		ClientName:                      clientName,
		UserPoolID:                      userPoolID,
		CreatedAt:                       time.Now(),
		AllowedOAuthFlows:               flows,
		AllowedOAuthScopes:              scopes,
		ExplicitAuthFlows:               explicitFlows,
		CallbackURLs:                    callbackURLs,
		LogoutURLs:                      logoutURLs,
		SupportedIdentityProviders:      supportedIDPs,
		PreventUserExistenceErrors:      preventUserExistenceErrors,
		AccessTokenValidity:             opts.AccessTokenValidity,
		IDTokenValidity:                 opts.IDTokenValidity,
		RefreshTokenValidity:            opts.RefreshTokenValidity,
		TokenValidityUnits:              tvu,
		EnableTokenRevocation:           opts.EnableTokenRevocation,
		AllowedOAuthFlowsUserPoolClient: opts.AllowedOAuthFlowsUserPoolClient,
	}

	if opts.GenerateSecret {
		client.ClientSecret = randomAlphanumeric(clientSecretLen)
	}

	b.clients.Put(client)

	cp := *client

	return &cp, nil
}

// applyUserPoolClientListOpts copies each non-nil list field from opts onto client,
// leaving fields the caller omitted (nil) untouched. Split out of
// UpdateUserPoolClientWithOpts to keep that function's branching within lint limits.
func applyUserPoolClientListOpts(client *UserPoolClient, opts UserPoolClientOptions) {
	if opts.AllowedOAuthFlows != nil {
		flows := make([]string, len(opts.AllowedOAuthFlows))
		copy(flows, opts.AllowedOAuthFlows)
		client.AllowedOAuthFlows = flows
	}

	if opts.AllowedOAuthScopes != nil {
		scopes := make([]string, len(opts.AllowedOAuthScopes))
		copy(scopes, opts.AllowedOAuthScopes)
		client.AllowedOAuthScopes = scopes
	}

	if opts.ExplicitAuthFlows != nil {
		ef := make([]string, len(opts.ExplicitAuthFlows))
		copy(ef, opts.ExplicitAuthFlows)
		client.ExplicitAuthFlows = ef
	}

	if opts.CallbackURLs != nil {
		cb := make([]string, len(opts.CallbackURLs))
		copy(cb, opts.CallbackURLs)
		client.CallbackURLs = cb
	}

	if opts.LogoutURLs != nil {
		lo := make([]string, len(opts.LogoutURLs))
		copy(lo, opts.LogoutURLs)
		client.LogoutURLs = lo
	}

	if opts.SupportedIdentityProviders != nil {
		idps := make([]string, len(opts.SupportedIdentityProviders))
		copy(idps, opts.SupportedIdentityProviders)
		client.SupportedIdentityProviders = idps
	}
}

// UpdateUserPoolClientWithOpts updates app client fields including OAuth flows and scopes.
func (b *InMemoryBackend) UpdateUserPoolClientWithOpts(
	userPoolID, clientID, clientName string,
	opts UserPoolClientOptions,
) (*UserPoolClient, error) {
	b.mu.Lock("UpdateUserPoolClientWithOpts")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	client, ok := b.clients.Get(clientID)
	if !ok {
		return nil, fmt.Errorf("%w: client %q not found", ErrClientNotFound, clientID)
	}

	if client.UserPoolID != userPoolID {
		return nil, fmt.Errorf("%w: client %q does not belong to pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	if clientName != "" {
		client.ClientName = clientName
	}

	applyUserPoolClientListOpts(client, opts)

	if err := applyPreventUserExistenceErrorsUpdate(client, opts.PreventUserExistenceErrors); err != nil {
		return nil, err
	}

	client.EnableTokenRevocation = opts.EnableTokenRevocation
	client.AllowedOAuthFlowsUserPoolClient = opts.AllowedOAuthFlowsUserPoolClient
	if opts.AccessTokenValidity != 0 {
		client.AccessTokenValidity = opts.AccessTokenValidity
	}
	if opts.IDTokenValidity != 0 {
		client.IDTokenValidity = opts.IDTokenValidity
	}
	if opts.RefreshTokenValidity != 0 {
		client.RefreshTokenValidity = opts.RefreshTokenValidity
	}
	if opts.TokenValidityUnits != nil {
		client.TokenValidityUnits = maps.Clone(opts.TokenValidityUnits)
	}
	client.UpdatedAt = time.Now()
	cp := *client

	return &cp, nil
}

// ListUserPoolClientSecrets returns the ClientSecretId-keyed secrets added via
// AddUserPoolClientSecret for a client (up to maxExtraClientSecrets). The
// original CreateUserPoolClient/UpdateUserPoolClient(GenerateSecret) secret
// is not included -- it has no ClientSecretId (see ClientSecretRecord's doc
// comment).
func (b *InMemoryBackend) ListUserPoolClientSecrets(userPoolID, clientID string) ([]ClientSecretRecord, error) {
	b.mu.RLock("ListUserPoolClientSecrets")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	client, ok := b.clients.Get(clientID)
	if !ok || client.UserPoolID != userPoolID {
		return nil, fmt.Errorf("%w: client %q not found in pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	out := make([]ClientSecretRecord, len(client.ExtraClientSecrets))
	copy(out, client.ExtraClientSecrets)

	return out, nil
}

// DeleteUserPoolClientSecret removes the ClientSecretId-keyed secret matching
// clientSecretID from a pool client (added via AddUserPoolClientSecret).
func (b *InMemoryBackend) DeleteUserPoolClientSecret(userPoolID, clientID, clientSecretID string) error {
	b.mu.Lock("DeleteUserPoolClientSecret")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	client, ok := b.clients.Get(clientID)
	if !ok || client.UserPoolID != userPoolID {
		return fmt.Errorf("%w: client %q not found in pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	for i, s := range client.ExtraClientSecrets {
		if s.ClientSecretID == clientSecretID {
			client.ExtraClientSecrets = append(client.ExtraClientSecrets[:i], client.ExtraClientSecrets[i+1:]...)

			return nil
		}
	}

	return fmt.Errorf("%w: secret %q not found for client %q", ErrSecretNotFound, clientSecretID, clientID)
}
