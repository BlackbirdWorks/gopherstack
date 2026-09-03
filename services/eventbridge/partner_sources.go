package eventbridge

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// CreatePartnerEventSource creates a new partner event source.
func (b *InMemoryBackend) CreatePartnerEventSource(ctx context.Context,
	name, account string,
) (*PartnerEventSource, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("CreatePartnerEventSource")
	defer b.mu.Unlock()

	if b.partnerSourcesTable(region).Has(name) {
		return nil, fmt.Errorf("%w: partner event source %s already exists", ErrAlreadyExists, name)
	}

	src := &PartnerEventSource{
		Arn:     b.partnerSourceARN(name),
		Name:    name,
		Account: account,
	}
	b.partnerSourcesTable(region).Put(src)

	// Mirror as a PENDING EventSource in the customer account — matches AWS
	// behaviour where creating a partner source causes it to appear in the
	// customer's ListEventSources as PENDING until they call ActivateEventSource.
	now := time.Now()
	esrc := &EventSource{
		Arn:          b.partnerSourceARN(name),
		CreatedBy:    name,
		CreationTime: now,
		Name:         name,
		State:        "PENDING",
	}
	b.eventSourcesTable(region).Put(esrc)

	cp := *src

	return &cp, nil
}

// DescribePartnerEventSource returns a single partner event source by name.
func (b *InMemoryBackend) DescribePartnerEventSource(ctx context.Context, name string) (*PartnerEventSource, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("DescribePartnerEventSource")
	defer b.mu.RUnlock()

	src, exists := b.partnerSourcesTable(region).Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: partner event source %s not found", ErrNotFound, name)
	}

	cp := *src

	return &cp, nil
}

// DeletePartnerEventSource deletes a partner event source.
func (b *InMemoryBackend) DeletePartnerEventSource(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("DeletePartnerEventSource")
	defer b.mu.Unlock()

	store := b.partnerSourcesTable(region)
	if !store.Has(name) {
		return fmt.Errorf("%w: partner event source %s not found", ErrNotFound, name)
	}

	store.Delete(name)

	return nil
}

// ListPartnerEventSources returns partner event sources optionally filtered by name prefix.
func (b *InMemoryBackend) ListPartnerEventSources(ctx context.Context,
	namePrefix, nextToken string, limit int,
) ([]PartnerEventSource, string, error) {
	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListPartnerEventSources")
	defer b.mu.RUnlock()

	store := b.partnerSourcesTable(region)
	all := make([]PartnerEventSource, 0, store.Len())
	for _, s := range store.All() {
		if namePrefix == "" || strings.HasPrefix(s.Name, namePrefix) {
			all = append(all, *s)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginateN(all, nextToken, limit)

	return page, outToken, nil
}

// ListPartnerEventSourceAccounts returns the account a partner event source
// was offered to, derived from CreatePartnerEventSource's already-tracked
// state (partnerSourcesTable's Account field) and the mirrored customer-side
// EventSource's CreationTime/ExpirationTime/State -- not fabricated
// cross-account data, since this emulator already records exactly this
// association. Real AWS can return multiple accounts per partner source name
// (a partner can offer one source to many accounts); this emulator models
// one name -> one account (see CreatePartnerEventSource), so at most one
// entry is ever returned, and Limit/NextToken are accepted but never needed.
func (b *InMemoryBackend) ListPartnerEventSourceAccounts(ctx context.Context,
	eventSourceName string,
) ([]PartnerEventSourceAccountInfo, error) {
	if eventSourceName == "" {
		return nil, fmt.Errorf("%w: EventSourceName is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListPartnerEventSourceAccounts")
	defer b.mu.RUnlock()

	src, exists := b.partnerSourcesTable(region).Get(eventSourceName)
	if !exists {
		return nil, fmt.Errorf("%w: partner event source %s not found", ErrNotFound, eventSourceName)
	}

	info := PartnerEventSourceAccountInfo{Account: src.Account, State: "PENDING"}

	if es, ok := b.eventSourcesTable(region).Get(eventSourceName); ok {
		info.CreationTime = es.CreationTime
		info.ExpirationTime = es.ExpirationTime
		info.State = es.State
	}

	return []PartnerEventSourceAccountInfo{info}, nil
}

// PutPartnerEvents records partner events (same as PutEvents but intended for partner sources).
func (b *InMemoryBackend) PutPartnerEvents(ctx context.Context, entries []EventEntry) ([]EventResultEntry, error) {
	return b.PutEvents(ctx, entries)
}

// AddPartnerSourceInternal adds a partner event source directly for testing.
func (b *InMemoryBackend) AddPartnerSourceInternal(src *PartnerEventSource) {
	b.mu.Lock("AddPartnerSourceInternal")
	defer b.mu.Unlock()

	cp := *src
	b.partnerSourcesTable(b.region).Put(&cp)
}
