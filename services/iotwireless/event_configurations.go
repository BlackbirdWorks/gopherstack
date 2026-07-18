package iotwireless

import (
	"cmp"
	"slices"
	"strings"
)

// GetEventConfigurationByResourceTypes returns the account-wide default event
// configuration. Returns an empty (zero-value) document if never configured.
func (b *InMemoryBackend) GetEventConfigurationByResourceTypes() *EventConfigDoc {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.eventConfigDefault == nil {
		return &EventConfigDoc{}
	}

	cp := *b.eventConfigDefault

	return &cp
}

// UpdateEventConfigurationByResourceTypes replaces the account-wide default
// event configuration.
func (b *InMemoryBackend) UpdateEventConfigurationByResourceTypes(doc *EventConfigDoc) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cp := *doc
	b.eventConfigDefault = &cp
}

// GetResourceEventConfiguration returns the stored event configuration for a
// specific resource identifier, if any.
func (b *InMemoryBackend) GetResourceEventConfiguration(identifier string) (*ResourceEventConfigEntry, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	e, ok := b.resourceEventConfigs.Get(identifier)
	if !ok {
		return nil, false
	}

	cp := *e

	return &cp, true
}

// UpdateResourceEventConfiguration stores the event configuration for a
// specific resource identifier.
func (b *InMemoryBackend) UpdateResourceEventConfiguration(
	identifier, identifierType, partnerType string, doc *EventConfigDoc,
) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.resourceEventConfigs.Put(&ResourceEventConfigEntry{
		Identifier:     identifier,
		IdentifierType: identifierType,
		PartnerType:    partnerType,
		Config:         *doc,
	})
}

// ListEventConfigurations returns all stored per-resource event
// configurations, optionally filtered by resource type (matched as a prefix
// against the stored IdentifierType, e.g. "WirelessDevice" matches
// "WirelessDeviceId").
func (b *InMemoryBackend) ListEventConfigurations(resourceType string) []*ResourceEventConfigEntry {
	b.mu.RLock()
	defer b.mu.RUnlock()

	all := b.resourceEventConfigs.All()
	result := make([]*ResourceEventConfigEntry, 0, len(all))

	for _, e := range all {
		if resourceType != "" && !strings.HasPrefix(e.IdentifierType, resourceType) {
			continue
		}

		cp := *e
		result = append(result, &cp)
	}

	slices.SortFunc(result, func(a, b *ResourceEventConfigEntry) int {
		return cmp.Compare(a.Identifier, b.Identifier)
	})

	return result
}
