package dms

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// copyStringsOrEmpty returns a copy of src, guaranteeing a non-nil slice.
func copyStringsOrEmpty(src []string) []string {
	out := make([]string, len(src))
	copy(out, src)

	return out
}

// CreateEventSubscription creates a new event subscription.
func (b *InMemoryBackend) CreateEventSubscription(
	ctx context.Context,
	subscriptionName, snsTopicArn, sourceType string,
	sourceIDs, eventCategories []string,
	enabled bool,
	kv map[string]string,
) (*EventSubscription, error) {
	b.mu.Lock("CreateEventSubscription")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if b.eventSubscriptions.Has(regionKey(region, subscriptionName)) {
		return nil, fmt.Errorf(
			"%w: event subscription %s already exists",
			ErrAlreadyExists,
			subscriptionName,
		)
	}

	t := tags.New("dms.event-subscription." + subscriptionName + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	sourceIDsCopy := copyStringsOrEmpty(sourceIDs)
	eventCategoriesCopy := copyStringsOrEmpty(eventCategories)

	es := &EventSubscription{
		SubscriptionName: subscriptionName,
		SnsTopicArn:      snsTopicArn,
		SourceType:       sourceType,
		SourceIDsList:    sourceIDsCopy,
		EventCategories:  eventCategoriesCopy,
		Enabled:          enabled,
		Status:           statusActive,
		AccountID:        b.accountID,
		Region:           region,
		CreationTime:     time.Now().UTC(),
		Tags:             t,
	}
	b.eventSubscriptions.Put(es)
	cp := *es
	cp.SourceIDsList = copyStringsOrEmpty(es.SourceIDsList)
	cp.EventCategories = copyStringsOrEmpty(es.EventCategories)

	return &cp, nil
}

// appendEvent records a DMS operational event. Caller must hold b.mu.
func (b *InMemoryBackend) appendEvent(region, sourceID, sourceType, msg string, cats []string) {
	b.events[region] = append(b.events[region], &Event{
		SourceIdentifier: sourceID,
		SourceType:       sourceType,
		Message:          msg,
		EventCategories:  cats,
		Date:             time.Now().UTC().Format(time.RFC3339),
	})
}

// DescribeEvents returns all recorded DMS events for the request region.
func (b *InMemoryBackend) DescribeEvents(ctx context.Context) ([]*Event, error) {
	b.mu.RLock("DescribeEvents")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	list := b.events[region]
	result := make([]*Event, len(list))
	for i, e := range list {
		cp := *e
		result[i] = &cp
	}

	return result, nil
}

// AddEventSubscriptionInternal seeds an event subscription directly without HTTP.
func (b *InMemoryBackend) AddEventSubscriptionInternal(name, snsTopicArn string) {
	b.mu.Lock("AddEventSubscriptionInternal")
	defer b.mu.Unlock()
	t := tags.New("dms.event-subscription." + name + ".tags")
	es := &EventSubscription{
		SubscriptionName: name,
		SnsTopicArn:      snsTopicArn,
		Status:           statusActive,
		Enabled:          true,
		SourceIDsList:    []string{},
		EventCategories:  []string{},
		AccountID:        b.accountID,
		Region:           b.region,
		CreationTime:     time.Now().UTC(),
		Tags:             t,
	}
	b.eventSubscriptions.Put(es)
}

// DeleteEventSubscription deletes an event subscription by name.
func (b *InMemoryBackend) DeleteEventSubscription(ctx context.Context, name string) (*EventSubscription, error) {
	b.mu.Lock("DeleteEventSubscription")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	es, ok := b.eventSubscriptions.Get(regionKey(region, name))
	if !ok {
		return nil, fmt.Errorf("%w: event subscription %s not found", ErrNotFound, name)
	}

	cp := *es
	cp.SourceIDsList = copyStringsOrEmpty(es.SourceIDsList)
	cp.EventCategories = copyStringsOrEmpty(es.EventCategories)
	es.Tags.Close()
	b.eventSubscriptions.Delete(regionKey(region, name))

	return &cp, nil
}

// ModifyEventSubscription updates an event subscription.
func (b *InMemoryBackend) ModifyEventSubscription(
	ctx context.Context,
	name string,
	enabled *bool,
) (*EventSubscription, error) {
	b.mu.Lock("ModifyEventSubscription")
	defer b.mu.Unlock()

	es, ok := b.eventSubscriptions.Get(regionKey(getRegion(ctx, b.region), name))
	if !ok {
		return nil, fmt.Errorf("%w: event subscription %s not found", ErrNotFound, name)
	}

	if enabled != nil {
		es.Enabled = *enabled
	}

	cp := *es
	cp.SourceIDsList = copyStringsOrEmpty(es.SourceIDsList)
	cp.EventCategories = copyStringsOrEmpty(es.EventCategories)

	return &cp, nil
}

// DescribeEventSubscriptions returns all event subscriptions (optionally filtered by name).
func (b *InMemoryBackend) DescribeEventSubscriptions(ctx context.Context, name string) ([]*EventSubscription, error) {
	b.mu.RLock("DescribeEventSubscriptions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if name != "" {
		es, ok := b.eventSubscriptions.Get(regionKey(region, name))
		if !ok {
			return []*EventSubscription{}, nil
		}

		cp := *es
		cp.SourceIDsList = copyStringsOrEmpty(es.SourceIDsList)
		cp.EventCategories = copyStringsOrEmpty(es.EventCategories)

		return []*EventSubscription{&cp}, nil
	}

	items := b.eventSubscriptionsByRegion.Get(region)
	list := make([]*EventSubscription, 0, len(items))
	for _, es := range items {
		cp := *es
		cp.SourceIDsList = copyStringsOrEmpty(es.SourceIDsList)
		cp.EventCategories = copyStringsOrEmpty(es.EventCategories)
		list = append(list, &cp)
	}

	return list, nil
}
