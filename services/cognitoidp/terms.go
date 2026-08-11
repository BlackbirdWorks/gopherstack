package cognitoidp

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateTerms creates a terms-of-use or privacy-policy document for an app client.
func (b *InMemoryBackend) CreateTerms(
	userPoolID, clientID, termsName, enforcement, termsSource string,
	links map[string]string,
) (*Terms, error) {
	b.mu.Lock("CreateTerms")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	if clientID == "" {
		return nil, fmt.Errorf("%w: ClientId is required", ErrInvalidParameter)
	}

	if termsName == "" {
		return nil, fmt.Errorf("%w: TermsName is required", ErrInvalidParameter)
	}

	if enforcement == "" {
		return nil, fmt.Errorf("%w: Enforcement is required", ErrInvalidParameter)
	}

	if enforcement != TermsEnforcementNone {
		return nil, fmt.Errorf("%w: Enforcement must be %q", ErrInvalidParameter, TermsEnforcementNone)
	}

	if termsSource == "" {
		return nil, fmt.Errorf("%w: TermsSource is required", ErrInvalidParameter)
	}

	if termsSource != TermsSourceLink {
		return nil, fmt.Errorf("%w: TermsSource must be %q", ErrInvalidParameter, TermsSourceLink)
	}

	client, ok := b.clients.Get(clientID)
	if !ok || client.UserPoolID != userPoolID {
		return nil, fmt.Errorf("%w: client %q not found in pool %q", ErrClientNotFound, clientID, userPoolID)
	}

	for _, t := range b.termsByPool.Get(userPoolID) {
		if t.ClientID == clientID && t.TermsName == termsName {
			return nil, fmt.Errorf("%w: terms %q already exists for client %q", ErrTermsExists, termsName, clientID)
		}
	}

	now := time.Now()
	t := &Terms{
		TermsID:        uuid.New().String(),
		UserPoolID:     userPoolID,
		ClientID:       clientID,
		TermsName:      termsName,
		Enforcement:    enforcement,
		TermsSource:    termsSource,
		Links:          maps.Clone(links),
		CreatedAt:      now,
		LastModifiedAt: now,
	}
	b.terms.Put(t)

	cp := *t

	return &cp, nil
}

// DescribeTerms returns the requested terms documents by ID, scoped to the given pool.
func (b *InMemoryBackend) DescribeTerms(userPoolID, termsID string) (*Terms, error) {
	b.mu.RLock("DescribeTerms")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	t, ok := b.terms.Get(termsID)
	if !ok || t.UserPoolID != userPoolID {
		return nil, fmt.Errorf("%w: terms %q not found in pool %q", ErrTermsNotFound, termsID, userPoolID)
	}

	cp := *t
	cp.Links = maps.Clone(t.Links)

	return &cp, nil
}

// ListTerms returns a page of terms documents for a user pool.
func (b *InMemoryBackend) ListTerms(userPoolID string, limit int, nextToken string) ([]*Terms, string, error) {
	b.mu.RLock("ListTerms")
	defer b.mu.RUnlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, "", fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	poolTerms := b.termsByPool.Get(userPoolID)
	all := make([]*Terms, 0, len(poolTerms))

	for _, t := range poolTerms {
		cp := *t
		cp.Links = maps.Clone(t.Links)
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].TermsID < all[j].TermsID })

	pg := page.New(all, nextToken, limit, cognitoMaxResultsCap)

	return pg.Data, pg.Next, nil
}

// UpdateTerms modifies an existing terms document. Empty fields leave the current value unchanged.
func (b *InMemoryBackend) UpdateTerms(
	userPoolID, termsID, enforcement, termsName, termsSource string,
	links map[string]string,
) (*Terms, error) {
	b.mu.Lock("UpdateTerms")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return nil, fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	t, ok := b.terms.Get(termsID)
	if !ok || t.UserPoolID != userPoolID {
		return nil, fmt.Errorf("%w: terms %q not found in pool %q", ErrTermsNotFound, termsID, userPoolID)
	}

	if enforcement != "" && enforcement != TermsEnforcementNone {
		return nil, fmt.Errorf("%w: Enforcement must be %q", ErrInvalidParameter, TermsEnforcementNone)
	}

	if termsSource != "" && termsSource != TermsSourceLink {
		return nil, fmt.Errorf("%w: TermsSource must be %q", ErrInvalidParameter, TermsSourceLink)
	}

	if enforcement != "" {
		t.Enforcement = enforcement
	}

	if termsSource != "" {
		t.TermsSource = termsSource
	}

	if termsName != "" {
		t.TermsName = termsName
	}

	if links != nil {
		t.Links = maps.Clone(links)
	}

	t.LastModifiedAt = time.Now()

	cp := *t
	cp.Links = maps.Clone(t.Links)

	return &cp, nil
}

// DeleteTerms removes the terms documents with the given ID from the given pool.
func (b *InMemoryBackend) DeleteTerms(userPoolID, termsID string) error {
	b.mu.Lock("DeleteTerms")
	defer b.mu.Unlock()

	if _, ok := b.pools.Get(userPoolID); !ok {
		return fmt.Errorf("%w: pool %q not found", ErrUserPoolNotFound, userPoolID)
	}

	t, ok := b.terms.Get(termsID)
	if !ok || t.UserPoolID != userPoolID {
		return fmt.Errorf("%w: terms %q not found in pool %q", ErrTermsNotFound, termsID, userPoolID)
	}

	b.terms.Delete(termsID)

	return nil
}
