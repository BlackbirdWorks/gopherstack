package appintegrations

import (
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/google/uuid"
)

// CreateEventIntegrationInput is the CreateEventIntegration request body.
type CreateEventIntegrationInput struct {
	Tags           map[string]string
	EventFilter    *EventFilter
	Name           string
	Description    string
	EventBridgeBus string
	ClientToken    string
}

func cloneEvent(e *EventIntegration) *EventIntegration {
	cp := *e
	cp.Tags = maps.Clone(e.Tags)

	return &cp
}

// CreateEventIntegration registers an event integration on an EventBridge bus.
func (b *InMemoryBackend) CreateEventIntegration(
	ctx context.Context, in *CreateEventIntegrationInput,
) (*EventIntegration, error) {
	if err := checkName("Name", in.Name); err != nil {
		return nil, err
	}

	if err := checkName("EventBridgeBus", in.EventBridgeBus); err != nil {
		return nil, err
	}

	if in.EventFilter == nil || len(in.EventFilter.Source) > maxNameLen+1 ||
		!sourcePattern.MatchString(in.EventFilter.Source) {
		return nil, invalidErr("EventFilter.Source must be a partner event source such as aws.partner/example")
	}

	if err := checkDescription(in.Description); err != nil {
		return nil, err
	}

	if err := checkTags(in.Tags); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateEventIntegration")
	defer b.mu.Unlock()

	if b.events.Has(in.Name) {
		return nil, duplicateErr("EventIntegration", in.Name)
	}

	if b.rules != nil && !b.rules.BusExists(ctx, in.EventBridgeBus) {
		return nil, invalidErr("EventBridge bus %q does not exist", in.EventBridgeBus)
	}

	e := &EventIntegration{
		Name:                in.Name,
		EventIntegrationArn: b.buildARN(ctx, "event-integration/"+in.Name),
		Description:         in.Description,
		EventBridgeBus:      in.EventBridgeBus,
		EventFilter:         *in.EventFilter,
		Tags:                maps.Clone(in.Tags),
	}
	b.events.Put(e)

	return cloneEvent(e), nil
}

// GetEventIntegration returns an event integration by name.
func (b *InMemoryBackend) GetEventIntegration(name string) (*EventIntegration, error) {
	b.mu.RLock("GetEventIntegration")
	defer b.mu.RUnlock()

	e, ok := b.events.Get(name)
	if !ok {
		return nil, notFoundErr("EventIntegration", name)
	}

	return cloneEvent(e), nil
}

// ListEventIntegrations pages through event integrations ordered by name.
func (b *InMemoryBackend) ListEventIntegrations(token string, maxResults int32) ([]EventIntegration, string, error) {
	b.mu.RLock("ListEventIntegrations")
	all := make([]EventIntegration, 0, b.events.Len())
	b.events.Range(func(e *EventIntegration) bool {
		all = append(all, *cloneEvent(e))

		return true
	})
	b.mu.RUnlock()

	slices.SortFunc(all, func(a, c EventIntegration) int { return strings.Compare(a.Name, c.Name) })

	return page(all, token, maxResults)
}

// UpdateEventIntegration changes the description of an event integration.
func (b *InMemoryBackend) UpdateEventIntegration(name string, description *string) error {
	if description != nil {
		if err := checkDescription(*description); err != nil {
			return err
		}
	}

	b.mu.Lock("UpdateEventIntegration")
	defer b.mu.Unlock()

	e, ok := b.events.Get(name)
	if !ok {
		return notFoundErr("EventIntegration", name)
	}

	if description != nil {
		e.Description = *description
	}

	return nil
}

// DeleteEventIntegration removes an event integration that has no associations.
func (b *InMemoryBackend) DeleteEventIntegration(name string) error {
	b.mu.Lock("DeleteEventIntegration")
	defer b.mu.Unlock()

	if !b.events.Has(name) {
		return notFoundErr("EventIntegration", name)
	}

	for _, a := range b.eventAssocs.All() {
		if a.EventIntegrationName == name {
			return invalidErr("EventIntegration %q still has associations", name)
		}
	}

	b.events.Delete(name)

	return nil
}

// CreateEventIntegrationAssociation links an event integration to a client and owns the EventBridge rule for it.
func (b *InMemoryBackend) CreateEventIntegrationAssociation(
	ctx context.Context, eventIntegration, clientID string, metadata map[string]string,
) (*EventIntegrationAssociation, error) {
	b.mu.Lock("CreateEventIntegrationAssociation")
	defer b.mu.Unlock()

	e, ok := b.events.Get(eventIntegration)
	if !ok {
		return nil, notFoundErr("EventIntegration", eventIntegration)
	}

	for _, a := range b.eventAssocs.All() {
		if a.EventIntegrationName == eventIntegration && a.ClientID == clientID {
			return nil, duplicateErr("EventIntegrationAssociation", clientID)
		}
	}

	id := uuid.NewString()
	rule := "AppIntegrations-" + id

	if b.rules != nil {
		if err := b.rules.PutRule(ctx, e.EventBridgeBus, rule, e.EventFilter.Source); err != nil {
			return nil, invalidErr("could not create the EventBridge rule: %v", err)
		}
	}

	a := &EventIntegrationAssociation{
		ClientAssociationMetadata:      maps.Clone(metadata),
		ClientID:                       clientID,
		EventBridgeRuleName:            rule,
		EventIntegrationAssociationArn: b.buildARN(ctx, "event-integration-association/"+id),
		EventIntegrationAssociationID:  id,
		EventIntegrationName:           eventIntegration,
	}
	b.eventAssocs.Put(a)

	cp := *a

	return &cp, nil
}

// DeleteEventIntegrationAssociation removes an association and its EventBridge rule.
func (b *InMemoryBackend) DeleteEventIntegrationAssociation(ctx context.Context, id string) error {
	b.mu.Lock("DeleteEventIntegrationAssociation")
	defer b.mu.Unlock()

	a, ok := b.eventAssocs.Get(id)
	if !ok {
		return notFoundErr("EventIntegrationAssociation", id)
	}

	if e, found := b.events.Get(a.EventIntegrationName); found && b.rules != nil {
		_ = b.rules.DeleteRule(ctx, e.EventBridgeBus, a.EventBridgeRuleName)
	}

	b.eventAssocs.Delete(id)

	return nil
}

// ListEventIntegrationAssociations pages through the associations of one event integration.
func (b *InMemoryBackend) ListEventIntegrationAssociations(
	name, token string, maxResults int32,
) ([]EventIntegrationAssociation, string, error) {
	b.mu.RLock("ListEventIntegrationAssociations")

	if !b.events.Has(name) {
		b.mu.RUnlock()

		return nil, "", notFoundErr("EventIntegration", name)
	}

	var all []EventIntegrationAssociation

	for _, a := range b.eventAssocs.All() {
		if a.EventIntegrationName == name {
			all = append(all, *a)
		}
	}

	b.mu.RUnlock()

	slices.SortFunc(all, func(a, c EventIntegrationAssociation) int {
		return strings.Compare(a.EventIntegrationAssociationID, c.EventIntegrationAssociationID)
	})

	return page(all, token, maxResults)
}
