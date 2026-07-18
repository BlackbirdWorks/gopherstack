package eventbridge

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// ActivateEventSource activates a partner event source.
func (b *InMemoryBackend) ActivateEventSource(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("ActivateEventSource")
	defer b.mu.Unlock()

	src, exists := b.eventSourcesTable(region).Get(name)
	if !exists {
		return fmt.Errorf("%w: event source %s not found", ErrNotFound, name)
	}

	src.State = stateActive

	return nil
}

// DeactivateEventSource deactivates a partner event source.
func (b *InMemoryBackend) DeactivateEventSource(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.Lock("DeactivateEventSource")
	defer b.mu.Unlock()

	src, exists := b.eventSourcesTable(region).Get(name)
	if !exists {
		return fmt.Errorf("%w: event source %s not found", ErrNotFound, name)
	}

	src.State = "INACTIVE"

	return nil
}

// DescribeEventSource returns a single event source by name.
func (b *InMemoryBackend) DescribeEventSource(ctx context.Context, name string) (*EventSource, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("DescribeEventSource")
	defer b.mu.RUnlock()

	src, exists := b.eventSourcesTable(region).Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: event source %s not found", ErrNotFound, name)
	}

	cp := *src

	return &cp, nil
}

// ListEventSources returns event sources optionally filtered by name prefix, with pagination.
func (b *InMemoryBackend) ListEventSources(ctx context.Context,
	namePrefix, nextToken string,
) ([]EventSource, string, error) {
	region := getRegionFromContext(ctx, b.region)

	b.mu.RLock("ListEventSources")
	defer b.mu.RUnlock()

	store := b.eventSourcesTable(region)
	all := make([]EventSource, 0, store.Len())
	for _, s := range store.All() {
		if namePrefix == "" || strings.HasPrefix(s.Name, namePrefix) {
			all = append(all, *s)
		}
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	page, outToken := paginate(all, nextToken)

	return page, outToken, nil
}

// AddEventSourceInternal adds an event source directly for testing.
func (b *InMemoryBackend) AddEventSourceInternal(src *EventSource) {
	b.mu.Lock("AddEventSourceInternal")
	defer b.mu.Unlock()

	cp := *src
	b.eventSourcesTable(b.region).Put(&cp)
}
