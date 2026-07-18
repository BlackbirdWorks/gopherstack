package sesv2

import (
	"fmt"
	"time"
)

// EventDestination represents an event destination for a configuration set.
type EventDestination struct {
	CreatedAt            time.Time `json:"createdAt"`
	Name                 string    `json:"name"`
	ConfigurationSetName string    `json:"configurationSetName"`
	MatchingEventTypes   []string  `json:"matchingEventTypes"`
	Enabled              bool      `json:"enabled"`
}

// CreateConfigurationSetEventDestination adds an event destination to a configuration set.
func (b *InMemoryBackend) CreateConfigurationSetEventDestination(
	configSetName, destName string,
	enabled bool,
	matchingEventTypes []string,
) (*EventDestination, error) {
	b.mu.Lock("CreateConfigurationSetEventDestination")
	defer b.mu.Unlock()

	if !b.configurationSets.Has(configSetName) {
		return nil, fmt.Errorf("%w: configuration set %s not found", ErrNotFound, configSetName)
	}

	key := eventDestinationKey(configSetName, destName)
	if b.eventDestinations.Has(key) {
		return nil, fmt.Errorf(
			"%w: event destination %s already exists in config set %s",
			ErrAlreadyExists, destName, configSetName,
		)
	}

	types := make([]string, len(matchingEventTypes))
	copy(types, matchingEventTypes)

	dest := &EventDestination{
		Name:                 destName,
		ConfigurationSetName: configSetName,
		Enabled:              enabled,
		MatchingEventTypes:   types,
		CreatedAt:            time.Now(),
	}
	b.eventDestinations.Put(dest)

	cp := *dest

	return &cp, nil
}

// GetConfigurationSetEventDestinations retrieves event destinations for a config set.
func (b *InMemoryBackend) GetConfigurationSetEventDestinations(
	configSetName string,
) ([]*EventDestination, error) {
	b.mu.RLock("GetConfigurationSetEventDestinations")
	defer b.mu.RUnlock()

	if !b.configurationSets.Has(configSetName) {
		return nil, fmt.Errorf("%w: configuration set %s not found", ErrNotFound, configSetName)
	}

	dests := b.eventDestinationsByConfigSet.Get(configSetName)
	out := make([]*EventDestination, 0, len(dests))

	for _, d := range dests {
		cp := *d
		out = append(out, &cp)
	}

	return out, nil
}

// DeleteConfigurationSetEventDestination removes an event destination.
func (b *InMemoryBackend) DeleteConfigurationSetEventDestination(
	configSetName, destName string,
) error {
	b.mu.Lock("DeleteConfigurationSetEventDestination")
	defer b.mu.Unlock()

	if !b.configurationSets.Has(configSetName) {
		return fmt.Errorf("%w: configuration set %s not found", ErrNotFound, configSetName)
	}

	key := eventDestinationKey(configSetName, destName)
	if !b.eventDestinations.Has(key) {
		return fmt.Errorf(
			"%w: event destination %s not found in %s",
			ErrNotFound,
			destName,
			configSetName,
		)
	}

	b.eventDestinations.Delete(key)

	return nil
}

// UpdateConfigurationSetEventDestination updates an event destination.
func (b *InMemoryBackend) UpdateConfigurationSetEventDestination(
	configSetName, destName string,
	enabled bool,
	matchingEventTypes []string,
) error {
	b.mu.Lock("UpdateConfigurationSetEventDestination")
	defer b.mu.Unlock()

	if !b.configurationSets.Has(configSetName) {
		return fmt.Errorf("%w: configuration set %s not found", ErrNotFound, configSetName)
	}

	dest, ok := b.eventDestinations.Get(eventDestinationKey(configSetName, destName))
	if !ok {
		return fmt.Errorf(
			"%w: event destination %s not found in %s",
			ErrNotFound,
			destName,
			configSetName,
		)
	}

	dest.Enabled = enabled

	types := make([]string, len(matchingEventTypes))
	copy(types, matchingEventTypes)
	dest.MatchingEventTypes = types

	return nil
}
