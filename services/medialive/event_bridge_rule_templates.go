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

// ListEventBridgeRuleTemplateGroups returns EB rule template groups
// referenced by signalMapIdentifier (when set; api_op_
// ListEventBridgeRuleTemplateGroups.go's SignalMapIdentifier, matched
// against the signal map's eventBridgeRuleTemplateGroupIds), each annotated
// with its live templateCount (see EventBridgeRuleTemplateGroupSummary's
// doc comment). Shares listTemplateGroups (cloudwatch_alarm_templates.go)
// with its CloudWatch counterpart.
//
//nolint:dupl // mirrors the CloudWatch equivalent; logic is shared via listTemplateGroups
func (b *InMemoryBackend) ListEventBridgeRuleTemplateGroups(
	maxResults int,
	nextToken string,
	signalMapIdentifier string,
) ([]*EventBridgeRuleTemplateGroupSummary, string, error) {
	b.mu.RLock("ListEventBridgeRuleTemplateGroups")
	defer b.mu.RUnlock()

	result, next := listTemplateGroups(
		b, b.ebRuleTemplateGroups.All(), maxResults, nextToken, signalMapIdentifier,
		func(sm *storedSignalMap) []string { return sm.EventBridgeRuleTemplateGroupIDs },
		func(g *storedEventBridgeRuleTemplateGroup) string { return g.ID },
		func(g *storedEventBridgeRuleTemplateGroup) string { return g.Arn },
		func(g *storedEventBridgeRuleTemplateGroup) string { return g.Name },
		func(g *storedEventBridgeRuleTemplateGroup) *EventBridgeRuleTemplateGroupSummary {
			return &EventBridgeRuleTemplateGroupSummary{
				EventBridgeRuleTemplateGroup: *g.toGroup(),
				TemplateCount:                b.countEBRuleTemplatesForGroup(g.ID),
			}
		},
	)

	return result, next, nil
}

// countEBRuleTemplatesForGroup returns the number of EventBridge rule
// templates belonging to groupID. Caller must already hold b.mu (Lock or
// RLock).
func (b *InMemoryBackend) countEBRuleTemplatesForGroup(groupID string) int32 {
	var n int32

	for _, t := range b.ebRuleTemplates.All() {
		if t.GroupID == groupID {
			n++
		}
	}

	return n
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
	delete(b.tags, g.Arn)

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

// ListEventBridgeRuleTemplates returns all EB rule templates using the real
// List Summary shape (eventTargetCount, not the full eventTargets array --
// see EventBridgeRuleTemplateSummary's doc comment).
// ListEventBridgeRuleTemplates returns EB rule templates constrained by
// groupIdentifier and/or signalMapIdentifier when set, same semantics as
// ListCloudWatchAlarmTemplates' equivalent filters.
func (b *InMemoryBackend) ListEventBridgeRuleTemplates(
	maxResults int,
	nextToken string,
	groupIdentifier, signalMapIdentifier string,
) ([]*EventBridgeRuleTemplateSummary, string, error) {
	b.mu.RLock("ListEventBridgeRuleTemplates")
	defer b.mu.RUnlock()
	all := b.ebRuleTemplates.All()

	if groupIdentifier != "" {
		groupID := groupIdentifier
		if g, ok := b.findEBRuleTemplateGroup(groupIdentifier); ok {
			groupID = g.ID
		}

		filtered := make([]*storedEventBridgeRuleTemplate, 0, len(all))

		for _, t := range all {
			if t.GroupID == groupID {
				filtered = append(filtered, t)
			}
		}

		all = filtered
	}

	if signalMapIdentifier != "" {
		sm, ok := b.findSignalMap(signalMapIdentifier)
		filtered := make([]*storedEventBridgeRuleTemplate, 0, len(all))

		if ok {
			for _, t := range all {
				g, gok := b.findEBRuleTemplateGroup(t.GroupID)
				if gok && groupMatchesIdentifierList(g.ID, g.Arn, g.Name, sm.EventBridgeRuleTemplateGroupIDs) {
					filtered = append(filtered, t)
				}
			}
		}

		all = filtered
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	pg := page.New(all, nextToken, maxResults, defaultMaxResults)
	result := make([]*EventBridgeRuleTemplateSummary, 0, len(pg.Data))
	for _, t := range pg.Data {
		tmpl := t.toTemplate()
		result = append(result, &EventBridgeRuleTemplateSummary{
			EventBridgeRuleTemplate: *tmpl,
			//nolint:gosec // G115: target count is bounded by request-body size, never near int32 max
			EventTargetCount: int32(len(tmpl.EventTargets)),
		})
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
	delete(b.tags, t.Arn)

	return nil
}
