package workmail

import (
	"fmt"
	"sort"
	"time"
)

// --- Personal Access Tokens ---

// DeletePersonalAccessToken removes a personal access token.
func (b *InMemoryBackend) DeletePersonalAccessToken(orgID, tokenID string) error {
	b.mu.Lock("DeletePersonalAccessToken")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return fmt.Errorf("%w: organization %q not found", ErrOrganizationNotFound, orgID)
	}
	if !b.personalTokens.Delete(orgKey(orgID, tokenID)) {
		// DeletePersonalAccessToken's own error model declares no not-found
		// type for the token itself (only Organization*); no correct code
		// exists to send here (gopherstack-6flj/uox6 error-envelope sweep).
		return fmt.Errorf("%w: personal access token %q not found", ErrNotFound, tokenID)
	}

	return nil
}

// GetPersonalAccessTokenMetadata returns metadata for a personal access token.
func (b *InMemoryBackend) GetPersonalAccessTokenMetadata(
	orgID, tokenID string,
) (*PersonalAccessToken, error) {
	b.mu.RLock("GetPersonalAccessTokenMetadata")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrOrganizationNotFound, orgID)
	}
	tok, ok := b.personalTokens.Get(orgKey(orgID, tokenID))
	if !ok {
		return nil, fmt.Errorf("%w: personal access token %q not found", ErrResourceNotFound, tokenID)
	}

	return tok, nil
}

// ListPersonalAccessTokens lists personal access tokens, optionally filtered by userID.
func (b *InMemoryBackend) ListPersonalAccessTokens(
	orgID, userID string, maxResults int32, nextToken string,
) ([]*PersonalAccessToken, string, error) {
	b.mu.RLock("ListPersonalAccessTokens")
	defer b.mu.RUnlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, "", fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	all := make([]*PersonalAccessToken, 0)
	for _, tok := range b.personalTokensByOrg.Get(orgID) {
		if userID != "" && tok.UserID != userID {
			continue
		}
		all = append(all, tok)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].TokenID < all[j].TokenID })
	page, next := paginate(all, maxResults, nextToken)

	return page, next, nil
}

// CreatePersonalAccessToken creates a new personal access token (for testing).
func (b *InMemoryBackend) CreatePersonalAccessToken(
	orgID, userID, name string,
	scopes []string,
) (*PersonalAccessToken, error) {
	b.mu.Lock("CreatePersonalAccessToken")
	defer b.mu.Unlock()

	if _, ok := b.organizations.Get(orgID); !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}
	now := time.Now()
	tok := &PersonalAccessToken{
		TokenID:     newID(),
		UserID:      userID,
		Name:        name,
		Scopes:      scopes,
		DateCreated: now,
		ExpiresTime: now.Add(365 * 24 * time.Hour),
		orgID:       orgID,
	}
	b.personalTokens.Put(tok)

	return tok, nil
}
