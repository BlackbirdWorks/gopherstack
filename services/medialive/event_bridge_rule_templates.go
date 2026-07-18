package medialive

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- EventBridge Rule Template Group operations ---

func (b *InMemoryBackend) findEBRuleTemplateGroup(
	identifier string,
) (*storedEventBridgeRuleTemplateGroup, bool) {
	for _, g := range b.ebRuleTemplateGroups.All() {
		if g.ID == identifier || g.Arn == identifier || g.Name == identifier {
			return g, true
		}
	}

	return nil, false
}

// CreateEventBridgeRuleTemplateGroup creates a new EB rule template group.
func (b *InMemoryBackend) CreateEventBridgeRuleTemplateGroup(
	name, description string, tags map[string]string,
) (*EventBridgeRuleTemplateGroup, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}
	id := newID()
	now := time.Now().UTC()
	g := &storedEventBridgeRuleTemplateGroup{
		Tags: copyTags(
			tags,
		), Arn: b.ebRuleTemplateGroupARN(id), ID: id, Name: name, Description: description,
		CreatedAt: now, ModifiedAt: now,
	}
	b.mu.Lock("CreateEventBridgeRuleTemplateGroup")
	defer b.mu.Unlock()
	b.ebRuleTemplateGroups.Put(g)

	return g.toGroup(), nil
}

// GetEventBridgeRuleTemplateGroup returns an EB rule template group.
func (b *InMemoryBackend) GetEventBridgeRuleTemplateGroup(
	identifier string,
) (*EventBridgeRuleTemplateGroup, error) {
	b.mu.RLock("GetEventBridgeRuleTemplateGroup")
	defer b.mu.RUnlock()
	g, ok := b.findEBRuleTemplateGroup(identifier)
	if !ok {
		return nil, fmt.Errorf(
			"%w: eventbridge rule template group %s not found",
			ErrNotFound,
			identifier,
		)
	}

	return g.toGroup(), nil
}

// ListEventBridgeRuleTemplateGroups returns all EB rule template groups.
func (b *InMemoryBackend) ListEventBridgeRuleTemplateGroups(
	maxResults int,
	nextToken string,
) ([]*EventBridgeRuleTemplateGroup, string, error) {
	b.mu.RLock("ListEventBridgeRuleTemplateGroups")
	defer b.mu.RUnlock()
	all := b.ebRuleTemplateGroups.All()
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	pg := page.New(all, nextToken, maxResults, defaultMaxResults)
	result := make([]*EventBridgeRuleTemplateGroup, 0, len(pg.Data))
	for _, g := range pg.Data {
		result = append(result, g.toGroup())
	}

	return result, pg.Next, nil
}

// UpdateEventBridgeRuleTemplateGroup updates an EB rule template group.
func (b *InMemoryBackend) UpdateEventBridgeRuleTemplateGroup(
	identifier, name, description string,
) (*EventBridgeRuleTemplateGroup, error) {
	b.mu.Lock("UpdateEventBridgeRuleTemplateGroup")
	defer b.mu.Unlock()
	g, ok := b.findEBRuleTemplateGroup(identifier)
	if !ok {
		return nil, fmt.Errorf(
			"%w: eventbridge rule template group %s not found",
			ErrNotFound,
			identifier,
		)
	}
	if name != "" {
		g.Name = name
	}
	if description != "" {
		g.Description = description
	}
	g.ModifiedAt = time.Now().UTC()

	return g.toGroup(), nil
}

// DeleteEventBridgeRuleTemplateGroup deletes an EB rule template group.
func (b *InMemoryBackend) DeleteEventBridgeRuleTemplateGroup(identifier string) error {
	b.mu.Lock("DeleteEventBridgeRuleTemplateGroup")
	defer b.mu.Unlock()
	g, ok := b.findEBRuleTemplateGroup(identifier)
	if !ok {
		return fmt.Errorf(
			"%w: eventbridge rule template group %s not found",
			ErrNotFound,
			identifier,
		)
	}
	b.ebRuleTemplateGroups.Delete(g.ID)

	return nil
}

// --- EventBridge Rule Template operations ---

func (b *InMemoryBackend) findEBRuleTemplate(
	identifier string,
) (*storedEventBridgeRuleTemplate, bool) {
	for _, t := range b.ebRuleTemplates.All() {
		if t.ID == identifier || t.Arn == identifier || t.Name == identifier {
			return t, true
		}
	}

	return nil, false
}

// CreateEventBridgeRuleTemplate creates a new EB rule template.
func (b *InMemoryBackend) CreateEventBridgeRuleTemplate(
	name, description, groupIdentifier, eventType string,
	eventTargets []EventBridgeRuleTemplateTarget,
	tags map[string]string,
) (*EventBridgeRuleTemplate, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name required", ErrInvalidParameter)
	}
	groupID := groupIdentifier
	b.mu.Lock("CreateEventBridgeRuleTemplate")
	defer b.mu.Unlock()
	if g, ok := b.findEBRuleTemplateGroup(groupIdentifier); ok {
		groupID = g.ID
	}
	targets := make([]EventBridgeRuleTemplateTarget, len(eventTargets))
	copy(targets, eventTargets)
	id := newID()
	now := time.Now().UTC()
	t := &storedEventBridgeRuleTemplate{
		Tags: copyTags(
			tags,
		), EventTargets: targets, Arn: b.ebRuleTemplateARN(id), ID: id, Name: name,
		Description: description, GroupID: groupID, GroupIdentifier: groupIdentifier, EventType: eventType,
		CreatedAt: now, ModifiedAt: now,
	}
	b.ebRuleTemplates.Put(t)

	return t.toTemplate(), nil
}

// GetEventBridgeRuleTemplate returns an EB rule template.
func (b *InMemoryBackend) GetEventBridgeRuleTemplate(
	identifier string,
) (*EventBridgeRuleTemplate, error) {
	b.mu.RLock("GetEventBridgeRuleTemplate")
	defer b.mu.RUnlock()
	t, ok := b.findEBRuleTemplate(identifier)
	if !ok {
		return nil, fmt.Errorf(
			"%w: eventbridge rule template %s not found",
			ErrNotFound,
			identifier,
		)
	}

	return t.toTemplate(), nil
}

// ListEventBridgeRuleTemplates returns all EB rule templates.
func (b *InMemoryBackend) ListEventBridgeRuleTemplates(
	maxResults int,
	nextToken string,
) ([]*EventBridgeRuleTemplate, string, error) {
	b.mu.RLock("ListEventBridgeRuleTemplates")
	defer b.mu.RUnlock()
	all := b.ebRuleTemplates.All()
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	pg := page.New(all, nextToken, maxResults, defaultMaxResults)
	result := make([]*EventBridgeRuleTemplate, 0, len(pg.Data))
	for _, t := range pg.Data {
		result = append(result, t.toTemplate())
	}

	return result, pg.Next, nil
}

// UpdateEventBridgeRuleTemplate updates an EB rule template.
func (b *InMemoryBackend) UpdateEventBridgeRuleTemplate(
	identifier, name, description, groupIdentifier, eventType string,
	eventTargets []EventBridgeRuleTemplateTarget,
) (*EventBridgeRuleTemplate, error) {
	b.mu.Lock("UpdateEventBridgeRuleTemplate")
	defer b.mu.Unlock()
	t, ok := b.findEBRuleTemplate(identifier)
	if !ok {
		return nil, fmt.Errorf(
			"%w: eventbridge rule template %s not found",
			ErrNotFound,
			identifier,
		)
	}
	if name != "" {
		t.Name = name
	}
	if description != "" {
		t.Description = description
	}
	if groupIdentifier != "" {
		t.GroupIdentifier = groupIdentifier
		g, found := b.findEBRuleTemplateGroup(groupIdentifier)
		if found {
			t.GroupID = g.ID
		} else {
			t.GroupID = groupIdentifier
		}
	}
	if eventType != "" {
		t.EventType = eventType
	}
	if eventTargets != nil {
		t.EventTargets = make([]EventBridgeRuleTemplateTarget, len(eventTargets))
		copy(t.EventTargets, eventTargets)
	}
	t.ModifiedAt = time.Now().UTC()

	return t.toTemplate(), nil
}

// DeleteEventBridgeRuleTemplate deletes an EB rule template.
func (b *InMemoryBackend) DeleteEventBridgeRuleTemplate(identifier string) error {
	b.mu.Lock("DeleteEventBridgeRuleTemplate")
	defer b.mu.Unlock()
	t, ok := b.findEBRuleTemplate(identifier)
	if !ok {
		return fmt.Errorf("%w: eventbridge rule template %s not found", ErrNotFound, identifier)
	}
	b.ebRuleTemplates.Delete(t.ID)

	return nil
}
