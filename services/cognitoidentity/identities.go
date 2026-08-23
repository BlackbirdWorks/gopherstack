package cognitoidentity

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

// GetID returns an existing identity or creates a new one for the given pool and logins.
func (b *InMemoryBackend) GetID(
	ctx context.Context,
	poolID string,
	_ string,
	logins map[string]string,
) (*Identity, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("GetID")
	defer b.mu.Unlock()

	if poolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	pool, ok := b.poolGet(region, poolID)
	if !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	if len(logins) == 0 && !pool.AllowUnauthenticatedIdentities {
		return nil, fmt.Errorf(
			"%w: unauthenticated access is not supported for this identity pool",
			ErrNotAuthorized,
		)
	}

	// AWS GetId matches an existing identity if any of the provided (provider, token) pairs
	// already appear in the identity's logins. On a match the identity is updated with any
	// new login providers from the current request (provider-account linking).
	if existing := b.mergeExistingIdentity(region, poolID, logins); existing != nil {
		return existing, nil
	}

	// Create a new identity.
	identityID := region + ":" + uuid.New().String()
	now := time.Now()
	identity := &Identity{
		IdentityID:       identityID,
		IdentityPoolID:   poolID,
		Logins:           cloneStringMap(logins),
		CreatedAt:        now,
		LastModifiedDate: now,
		Enabled:          true,
		region:           region,
	}

	b.identityPut(identity)

	return cloneIdentity(identity), nil
}

// mergeExistingIdentity searches identitiesByPool[region][poolID] for an identity that shares any
// (provider, token) pair with logins, merges new providers into it, and returns a clone.
// Returns nil if no match is found. Must be called with b.mu held.
func (b *InMemoryBackend) mergeExistingIdentity(
	region, poolID string,
	logins map[string]string,
) *Identity {
	for _, identity := range b.identitiesInPool(region, poolID) {
		if !anyLoginMatches(identity.Logins, logins) {
			continue
		}

		updated := false

		for provider, token := range logins {
			if identity.Logins[provider] != token {
				if identity.Logins == nil {
					identity.Logins = make(map[string]string)
				}

				identity.Logins[provider] = token
				updated = true
			}
		}

		if updated {
			identity.LastModifiedDate = time.Now()
		}

		return cloneIdentity(identity)
	}

	return nil
}

// DeleteIdentities deletes the given identity IDs from the backend.
// Identities that do not exist are silently skipped.
// Returns a (possibly empty) list of IDs that could not be processed.
func (b *InMemoryBackend) DeleteIdentities(
	ctx context.Context,
	identityIDs []string,
) ([]UnprocessedIdentityID, error) {
	if len(identityIDs) > deleteIdentitiesMaxBatch {
		return nil, fmt.Errorf(
			"%w: DeleteIdentities accepts at most %d identities per call, got %d",
			ErrInvalidParameter, deleteIdentitiesMaxBatch, len(identityIDs),
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteIdentities")
	defer b.mu.Unlock()

	var unprocessed []UnprocessedIdentityID

	for _, id := range identityIDs {
		// Table.Delete is a documented no-op (reports false) when id is absent, matching
		// the original silent-skip behaviour; it also keeps the identitiesByPool index
		// consistent automatically, replacing the old by-hand slice rebuild.
		b.identityDelete(region, id)
	}

	return unprocessed, nil
}

// DescribeIdentity returns metadata about a specific federated identity.
func (b *InMemoryBackend) DescribeIdentity(
	ctx context.Context,
	identityID string,
) (*IdentityDescription, error) {
	if identityID == "" {
		return nil, fmt.Errorf("%w: IdentityId is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeIdentity")
	defer b.mu.RUnlock()

	identity, ok := b.identityGet(region, identityID)
	if !ok {
		return nil, fmt.Errorf("%w: identity %q not found", ErrIdentityPoolNotFound, identityID)
	}

	logins := make([]string, 0, len(identity.Logins))
	for provider := range identity.Logins {
		logins = append(logins, provider)
	}

	slices.Sort(logins)

	return &IdentityDescription{
		IdentityID:       identity.IdentityID,
		Logins:           logins,
		CreationDate:     identity.CreatedAt,
		LastModifiedDate: identity.LastModifiedDate,
	}, nil
}

// lookupOrCreateDeveloperIdentity finds an existing identity in poolID whose logins
// overlap with logins, merging any new providers into it.  If none is found, a new
// identity is created.  Must be called with b.mu held.
func (b *InMemoryBackend) lookupOrCreateDeveloperIdentity(
	region, poolID string,
	logins map[string]string,
) string {
	for _, identity := range b.identitiesInPool(region, poolID) {
		if anyLoginMatches(identity.Logins, logins) {
			if identity.Logins == nil {
				identity.Logins = make(map[string]string)
			}

			maps.Copy(identity.Logins, logins)

			identity.LastModifiedDate = time.Now()

			return identity.IdentityID
		}
	}

	newID := region + ":" + uuid.New().String()
	now := time.Now()
	identity := &Identity{
		IdentityID:       newID,
		IdentityPoolID:   poolID,
		Logins:           cloneStringMap(logins),
		CreatedAt:        now,
		LastModifiedDate: now,
		Enabled:          true,
		region:           region,
	}

	b.identityPut(identity)

	return newID
}

// ListIdentities returns identities associated with an identity pool, sorted by IdentityId.
// nextToken is an opaque cursor encoding the last-returned IdentityId for pagination.
func (b *InMemoryBackend) ListIdentities(
	ctx context.Context,
	poolID string,
	maxResults int,
	hideDisabled bool,
	nextToken string,
) (*ListIdentitiesResult, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	if maxResults < 1 || maxResults > listIdentitiesMaxResults {
		return nil, fmt.Errorf(
			"%w: MaxResults must be between 1 and %d",
			ErrInvalidParameter, listIdentitiesMaxResults,
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("ListIdentities")
	defer b.mu.RUnlock()

	if _, ok := b.poolGet(region, poolID); !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	poolIdentities := b.identitiesInPool(region, poolID)

	// Filter disabled identities (when requested) and sort by IdentityId. Builds a fresh
	// slice, so it never mutates the identitiesByPool index's own backing slice.
	sorted := filterAndSortIdentities(poolIdentities, hideDisabled)

	// Apply cursor: skip items up to and including the one with the cursor ID.
	startIdx := 0
	if nextToken != "" {
		for i, id := range sorted {
			if id.IdentityID == nextToken {
				startIdx = i + 1

				break
			}
		}
	}

	sorted = sorted[startIdx:]

	limit := len(sorted)
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	descriptions := make([]IdentityDescription, 0, limit)

	for i := range limit {
		identity := sorted[i]
		logins := make([]string, 0, len(identity.Logins))

		for provider := range identity.Logins {
			logins = append(logins, provider)
		}

		slices.Sort(logins)

		descriptions = append(descriptions, IdentityDescription{
			IdentityID:       identity.IdentityID,
			Logins:           logins,
			CreationDate:     identity.CreatedAt,
			LastModifiedDate: identity.LastModifiedDate,
		})
	}

	// Emit a cursor if there are more pages.
	var returnNextToken string
	if maxResults > 0 && len(sorted) > maxResults {
		returnNextToken = sorted[maxResults-1].IdentityID
	}

	return &ListIdentitiesResult{
		IdentityPoolID: poolID,
		Identities:     descriptions,
		NextToken:      returnNextToken,
	}, nil
}

// LookupDeveloperIdentity retrieves the identity associated with a developer user identifier
// or the list of developer user identifiers associated with an identity. Per AWS semantics,
// if you supply only one of IdentityId/DeveloperUserIdentifier, the other is looked up and
// returned; if you supply both, DeveloperUserIdentifier is verified against IdentityId and a
// ResourceConflictException is returned when they do not match.
func (b *InMemoryBackend) LookupDeveloperIdentity(
	ctx context.Context,
	poolID string,
	identityID string,
	developerUserIdentifier string,
	developerProviderName string,
) (*LookupDeveloperIdentityResult, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	if identityID == "" && developerUserIdentifier == "" {
		return nil, fmt.Errorf(
			"%w: either IdentityId or DeveloperUserIdentifier must be provided",
			ErrInvalidParameter,
		)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("LookupDeveloperIdentity")
	defer b.mu.RUnlock()

	if _, ok := b.poolGet(region, poolID); !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	var byID *Identity

	if identityID != "" {
		identity, ok := b.identityGet(region, identityID)
		if !ok {
			return nil, fmt.Errorf("%w: identity %q not found", ErrIdentityPoolNotFound, identityID)
		}

		if identity.IdentityPoolID != poolID {
			return nil, fmt.Errorf(
				"%w: identity %q not found in pool %q",
				ErrIdentityPoolNotFound,
				identityID,
				poolID,
			)
		}

		byID = identity
	}

	var byDeveloperID *Identity

	if developerUserIdentifier != "" {
		for _, identity := range b.identitiesInPool(region, poolID) {
			if v, ok := identity.Logins[developerProviderName]; ok && v == developerUserIdentifier {
				byDeveloperID = identity

				break
			}
		}

		if byDeveloperID == nil {
			return nil, fmt.Errorf(
				"%w: developer user identifier %q not found",
				ErrIdentityPoolNotFound,
				developerUserIdentifier,
			)
		}
	}

	identity, err := reconcileLookupMatch(byID, byDeveloperID)
	if err != nil {
		return nil, err
	}

	return &LookupDeveloperIdentityResult{
		IdentityID:                  identity.IdentityID,
		DeveloperUserIdentifierList: developerLoginsFrom(identity.Logins, developerProviderName),
	}, nil
}

// reconcileLookupMatch resolves LookupDeveloperIdentity's dual-lookup result: when only one
// of byID/byDeveloperID is non-nil, that one wins; when both are supplied they must refer to
// the same identity, per AWS's documented "DeveloperUserIdentifier will be matched against
// IdentityId... Otherwise, a ResourceConflictException is thrown" behavior.
func reconcileLookupMatch(byID, byDeveloperID *Identity) (*Identity, error) {
	switch {
	case byID != nil && byDeveloperID != nil:
		if byID.IdentityID != byDeveloperID.IdentityID {
			return nil, fmt.Errorf(
				"%w: developer user identifier does not match identity %q",
				ErrResourceConflict,
				byID.IdentityID,
			)
		}

		return byID, nil
	case byID != nil:
		return byID, nil
	default:
		return byDeveloperID, nil
	}
}

// developerLoginsFrom extracts developer user identifiers from a logins map.
// A developer login key is non-standard (not a well-known provider prefix).
// The result is always sorted for deterministic output.
func developerLoginsFrom(logins map[string]string, developerProviderName string) []string {
	ids := make([]string, 0, len(logins))

	if developerProviderName != "" {
		if v, ok := logins[developerProviderName]; ok {
			ids = append(ids, v)
		}

		return ids
	}

	for _, v := range logins {
		ids = append(ids, v)
	}

	slices.Sort(ids)

	return ids
}

// MergeDeveloperIdentities merges the source identity into the destination identity.
func (b *InMemoryBackend) MergeDeveloperIdentities(
	ctx context.Context,
	sourceUserID string,
	destUserID string,
	developerProviderName string,
	poolID string,
) (*Identity, error) {
	if poolID == "" {
		return nil, fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	if developerProviderName == "" {
		return nil, fmt.Errorf("%w: DeveloperProviderName is required", ErrInvalidParameter)
	}

	if sourceUserID == "" {
		return nil, fmt.Errorf("%w: SourceUserIdentifier is required", ErrInvalidParameter)
	}

	if destUserID == "" {
		return nil, fmt.Errorf("%w: DestinationUserIdentifier is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("MergeDeveloperIdentities")
	defer b.mu.Unlock()

	if _, ok := b.poolGet(region, poolID); !ok {
		return nil, fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	var sourceIdentity, destIdentity *Identity

	for _, identity := range b.identitiesInPool(region, poolID) {
		if v, ok := identity.Logins[developerProviderName]; ok {
			switch v {
			case sourceUserID:
				sourceIdentity = identity
			case destUserID:
				destIdentity = identity
			}
		}
	}

	if sourceIdentity == nil {
		return nil, fmt.Errorf(
			"%w: source developer user %q not found",
			ErrIdentityPoolNotFound,
			sourceUserID,
		)
	}

	if destIdentity == nil {
		return nil, fmt.Errorf(
			"%w: destination developer user %q not found",
			ErrIdentityPoolNotFound,
			destUserID,
		)
	}

	// Merge logins from source into destination.
	maps.Copy(destIdentity.Logins, sourceIdentity.Logins)
	destIdentity.LastModifiedDate = time.Now()

	// Remove source identity. Table.Delete keeps identitiesByPool consistent automatically.
	b.identityDelete(region, sourceIdentity.IdentityID)

	return cloneIdentity(destIdentity), nil
}

// UnlinkIdentity removes login providers from an identity after validating
// the supplied login tokens.
func (b *InMemoryBackend) UnlinkIdentity(
	ctx context.Context,
	identityID string,
	logins map[string]string,
	loginsToRemove []string,
) error {
	if identityID == "" {
		return fmt.Errorf("%w: IdentityId is required", ErrInvalidParameter)
	}

	if len(loginsToRemove) == 0 {
		return fmt.Errorf("%w: LoginsToRemove must not be empty", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("UnlinkIdentity")
	defer b.mu.Unlock()

	identity, ok := b.identityGet(region, identityID)
	if !ok {
		return fmt.Errorf("%w: identity %q not found", ErrIdentityPoolNotFound, identityID)
	}

	for _, providerName := range loginsToRemove {
		loginToken, hasLoginToken := logins[providerName]
		if !hasLoginToken {
			return fmt.Errorf(
				"%w: login token for provider %q is required",
				ErrInvalidParameter,
				providerName,
			)
		}

		identityToken, exists := identity.Logins[providerName]
		if !exists {
			return fmt.Errorf(
				"%w: provider %q is not linked to identity",
				ErrNotAuthorized,
				providerName,
			)
		}

		if identityToken != loginToken {
			return fmt.Errorf(
				"%w: invalid login token for provider %q",
				ErrNotAuthorized,
				providerName,
			)
		}

		delete(identity.Logins, providerName)
	}

	identity.LastModifiedDate = time.Now()

	return nil
}

// UnlinkDeveloperIdentity removes a developer-provider association from an identity.
func (b *InMemoryBackend) UnlinkDeveloperIdentity(
	ctx context.Context,
	identityID string,
	poolID string,
	developerProviderName string,
	developerUserIdentifier string,
) error {
	if identityID == "" {
		return fmt.Errorf("%w: IdentityId is required", ErrInvalidParameter)
	}

	if poolID == "" {
		return fmt.Errorf("%w: IdentityPoolId is required", ErrInvalidParameter)
	}

	if developerProviderName == "" {
		return fmt.Errorf("%w: DeveloperProviderName is required", ErrInvalidParameter)
	}

	if developerUserIdentifier == "" {
		return fmt.Errorf("%w: DeveloperUserIdentifier is required", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("UnlinkDeveloperIdentity")
	defer b.mu.Unlock()

	if _, ok := b.poolGet(region, poolID); !ok {
		return fmt.Errorf("%w: identity pool %q not found", ErrIdentityPoolNotFound, poolID)
	}

	identity, ok := b.identityGet(region, identityID)
	if !ok {
		return fmt.Errorf("%w: identity %q not found", ErrIdentityPoolNotFound, identityID)
	}

	if identity.IdentityPoolID != poolID {
		return fmt.Errorf(
			"%w: identity %q not found in pool %q",
			ErrIdentityPoolNotFound,
			identityID,
			poolID,
		)
	}

	existingUserIdentifier, ok := identity.Logins[developerProviderName]
	if !ok {
		return fmt.Errorf(
			"%w: provider %q is not linked to identity",
			ErrNotAuthorized,
			developerProviderName,
		)
	}

	if existingUserIdentifier != developerUserIdentifier {
		return fmt.Errorf(
			"%w: developer user identifier %q not linked to provider %q",
			ErrNotAuthorized,
			developerUserIdentifier,
			developerProviderName,
		)
	}

	delete(identity.Logins, developerProviderName)
	identity.LastModifiedDate = time.Now()

	return nil
}

// filterAndSortIdentities returns a new slice containing identities from src, optionally
// excluding disabled ones, sorted by IdentityID for deterministic output.
func filterAndSortIdentities(src []*Identity, hideDisabled bool) []*Identity {
	out := make([]*Identity, 0, len(src))

	for _, id := range src {
		if hideDisabled && !id.Enabled {
			continue
		}

		out = append(out, id)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].IdentityID < out[j].IdentityID
	})

	return out
}

// anyLoginMatches returns true if any (provider, token) pair in req also exists in stored.
// This implements AWS GetId semantics: an existing identity is returned when any of the
// requested login providers is already linked to it.
func anyLoginMatches(stored, req map[string]string) bool {
	for provider, token := range req {
		if stored[provider] == token {
			return true
		}
	}

	return false
}
