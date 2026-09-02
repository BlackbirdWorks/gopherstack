package iotwireless

import (
	"cmp"
	"slices"
)

// GetEventConfigurationByResourceTypes returns the account-wide default event
// configuration. Returns an empty (zero-value) document if never configured.
func (b *InMemoryBackend) GetEventConfigurationByResourceTypes() *EventConfigDoc {
	b.mu.RLock("GetEventConfigurationByResourceTypes")
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
	b.mu.Lock("UpdateEventConfigurationByResourceTypes")
	defer b.mu.Unlock()

	cp := *doc
	b.eventConfigDefault = &cp
}

// GetResourceEventConfiguration returns the stored event configuration for a
// specific resource identifier, if any.
func (b *InMemoryBackend) GetResourceEventConfiguration(identifier string) (*ResourceEventConfigEntry, bool) {
	b.mu.RLock("GetResourceEventConfiguration")
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
	b.mu.Lock("UpdateResourceEventConfiguration")
	defer b.mu.Unlock()

	b.resourceEventConfigs.Put(&ResourceEventConfigEntry{
		Identifier:     identifier,
		IdentifierType: identifierType,
		PartnerType:    partnerType,
		Config:         *doc,
	})
}

// eventResourceTypeIdentifierTypes maps ListEventConfigurationsInput's
// ResourceType enum (enums.go: EventNotificationResourceType --
// SidewalkAccount|WirelessDevice|WirelessGateway) to the IdentifierType
// values (enums.go: IdentifierType) that identify a resource of that type.
// A wireless device may be identified by its WirelessDeviceId or, for
// LoRaWAN devices, its DevEui; a wireless gateway likewise by
// WirelessGatewayId or GatewayEui; a Sidewalk account only by
// PartnerAccountId (EventNotificationPartnerType has exactly one legal
// value, "Sidewalk", so PartnerAccountId is unambiguous). These are NOT the
// same enum and do not share a common prefix -- a string-prefix match
// against IdentifierType silently excludes DevEui/GatewayEui entirely and
// never matches SidewalkAccount at all.
func eventResourceTypeIdentifierTypes(resourceType string) []string {
	switch resourceType {
	case "WirelessDevice":
		return []string{"WirelessDeviceId", "DevEui"}
	case "WirelessGateway":
		return []string{"WirelessGatewayId", "GatewayEui"}
	case "SidewalkAccount":
		return []string{"PartnerAccountId"}
	default:
		return nil
	}
}

// ListEventConfigurations returns all stored per-resource event
// configurations, optionally filtered by resource type.
func (b *InMemoryBackend) ListEventConfigurations(resourceType string) []*ResourceEventConfigEntry {
	b.mu.RLock("ListEventConfigurations")
	defer b.mu.RUnlock()

	all := b.resourceEventConfigs.All()
	result := make([]*ResourceEventConfigEntry, 0, len(all))

	for _, e := range all {
		if resourceType != "" && !slices.Contains(eventResourceTypeIdentifierTypes(resourceType), e.IdentifierType) {
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
