package rds

import (
	"fmt"
	"slices"
	"time"
)

// AddSourceIdentifierToSubscription adds a source identifier to an event notification subscription.
// If the subscription does not exist it is created automatically.
func (b *InMemoryBackend) AddSourceIdentifierToSubscription(
	subscriptionName, sourceIdentifier string,
) (*EventSubscription, error) {
	if subscriptionName == "" {
		return nil, fmt.Errorf("%w: SubscriptionName must not be empty", ErrInvalidParameter)
	}
	if sourceIdentifier == "" {
		return nil, fmt.Errorf("%w: SourceIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("AddSourceIdentifierToSubscription")
	defer b.mu.Unlock()

	sub, exists := b.eventSubscriptions.Get(subscriptionName)
	if !exists {
		sub = &EventSubscription{
			SubscriptionName: subscriptionName,
			Status:           subscriptionStatusActive,
			SourceIDs:        []string{},
		}
		b.eventSubscriptions.Put(sub)
	}

	if !slices.Contains(sub.SourceIDs, sourceIdentifier) {
		sub.SourceIDs = append(sub.SourceIDs, sourceIdentifier)
	}

	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)

	return &cp, nil
}

// RemoveSourceIdentifierFromSubscription removes a source identifier from an event subscription.
// Returns an error if the subscription does not exist.
func (b *InMemoryBackend) RemoveSourceIdentifierFromSubscription(
	subscriptionName, sourceIdentifier string,
) (*EventSubscription, error) {
	if subscriptionName == "" {
		return nil, fmt.Errorf("%w: SubscriptionName must not be empty", ErrInvalidParameter)
	}
	if sourceIdentifier == "" {
		return nil, fmt.Errorf("%w: SourceIdentifier must not be empty", ErrInvalidParameter)
	}

	b.mu.Lock("RemoveSourceIdentifierFromSubscription")
	defer b.mu.Unlock()

	sub, exists := b.eventSubscriptions.Get(subscriptionName)
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrEventSubscriptionNotFound, subscriptionName)
	}

	idx := slices.Index(sub.SourceIDs, sourceIdentifier)
	if idx >= 0 {
		sub.SourceIDs = slices.Delete(sub.SourceIDs, idx, idx+1)
	}

	cp := *sub
	cp.SourceIDs = make([]string, len(sub.SourceIDs))
	copy(cp.SourceIDs, sub.SourceIDs)

	return &cp, nil
}

// CreateEventSubscription creates a new event notification subscription.
func (b *InMemoryBackend) CreateEventSubscription(
	name, snsTopicARN, sourceType string,
	sourceIDs, eventCategories []string,
) (*EventSubscription, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: SubscriptionName is required", ErrInvalidParameter)
	}
	b.mu.Lock("CreateEventSubscription")
	defer b.mu.Unlock()
	if _, exists := b.eventSubscriptions.Get(name); exists {
		return nil, fmt.Errorf("%w: subscription %s already exists", ErrEventSubscriptionAlreadyExists, name)
	}
	ids := make([]string, len(sourceIDs))
	copy(ids, sourceIDs)
	cats := make([]string, len(eventCategories))
	copy(cats, eventCategories)
	sub := &EventSubscription{
		SubscriptionName:     name,
		SnsTopicArn:          snsTopicARN,
		EventSubscriptionArn: b.rdsARN("es", name),
		Status:               subscriptionStatusActive,
		SourceType:           sourceType,
		SourceIDs:            ids,
		EventCategories:      cats,
		Enabled:              true,
	}
	b.eventSubscriptions.Put(sub)
	cp := *sub

	return &cp, nil
}

// DeleteEventSubscription deletes the named event subscription.
func (b *InMemoryBackend) DeleteEventSubscription(name string) (*EventSubscription, error) {
	b.mu.Lock("DeleteEventSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptions.Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrEventSubscriptionNotFound, name)
	}
	cp := *sub
	b.eventSubscriptions.Delete(name)

	return &cp, nil
}

// DescribeEventSubscriptions returns all subscriptions or the named one.
func (b *InMemoryBackend) DescribeEventSubscriptions(name string) ([]EventSubscription, error) {
	b.mu.RLock("DescribeEventSubscriptions")
	defer b.mu.RUnlock()
	if name != "" {
		sub, exists := b.eventSubscriptions.Get(name)
		if !exists {
			return nil, fmt.Errorf("%w: subscription %s not found", ErrEventSubscriptionNotFound, name)
		}
		cp := *sub

		return []EventSubscription{cp}, nil
	}
	result := make([]EventSubscription, 0, b.eventSubscriptions.Len())
	for _, sub := range b.eventSubscriptions.All() {
		result = append(result, *sub)
	}

	return result, nil
}

// ModifyEventSubscription modifies an existing event subscription.
func (b *InMemoryBackend) ModifyEventSubscription(
	name, snsTopicARN, sourceType string,
	sourceIDs, eventCategories []string,
	enabled *bool,
) (*EventSubscription, error) {
	b.mu.Lock("ModifyEventSubscription")
	defer b.mu.Unlock()
	sub, exists := b.eventSubscriptions.Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: subscription %s not found", ErrEventSubscriptionNotFound, name)
	}
	if snsTopicARN != "" {
		sub.SnsTopicArn = snsTopicARN
	}
	if sourceType != "" {
		sub.SourceType = sourceType
	}
	if len(sourceIDs) > 0 {
		ids := make([]string, len(sourceIDs))
		copy(ids, sourceIDs)
		sub.SourceIDs = ids
	}
	if len(eventCategories) > 0 {
		cats := make([]string, len(eventCategories))
		copy(cats, eventCategories)
		sub.EventCategories = cats
	}
	if enabled != nil {
		sub.Enabled = *enabled
	}
	cp := *sub

	return &cp, nil
}

// DescribeEvents returns published events filtered by sourceID, sourceType, and/or duration minutes.
// Results are sorted by creation time ascending, matching AWS behaviour.
func (b *InMemoryBackend) DescribeEvents(sourceID, sourceType string, durationMinutes int) ([]Event, error) {
	b.mu.RLock("DescribeEvents")
	defer b.mu.RUnlock()
	var cutoff time.Time
	if durationMinutes > 0 {
		cutoff = time.Now().Add(-time.Duration(durationMinutes) * time.Minute)
	}
	result := make([]Event, 0, len(b.events))
	for _, ev := range b.events {
		if sourceID != "" && ev.SourceIdentifier != sourceID {
			continue
		}
		if sourceType != "" && ev.SourceType != sourceType {
			continue
		}
		if durationMinutes > 0 && ev.CreatedAt.Before(cutoff) {
			continue
		}
		result = append(result, ev)
	}
	slices.SortFunc(result, func(a, b Event) int {
		if a.CreatedAt.Before(b.CreatedAt) {
			return -1
		}
		if a.CreatedAt.After(b.CreatedAt) {
			return 1
		}

		return 0
	})

	return result, nil
}

// DescribeEventCategories returns event category names, optionally filtered by source type.
func (b *InMemoryBackend) DescribeEventCategories(_ string) ([]string, error) {
	return []string{
		"availability",
		"backup",
		"configuration change",
		"creation",
		"deletion",
		"failover",
		"failure",
		"maintenance",
		"notification",
		"recovery",
		"restoration",
	}, nil
}
