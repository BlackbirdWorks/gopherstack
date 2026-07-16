package iot

import (
	"fmt"
	"slices"
)

// GetThingShadow returns the shadow for a thing (classic or named).
func (b *InMemoryBackend) GetThingShadow(thingName, shadowName string) (*ThingShadow, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.things.Has(thingName) {
		return nil, fmt.Errorf("%w: %s", ErrThingNotFound, thingName)
	}

	key := shadowKey{thingName: thingName, shadowName: shadowName}
	s, ok := b.shadows[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", ErrShadowNotFound, thingName, shadowName)
	}

	cp := *s

	return &cp, nil
}

// UpdateThingShadow creates or updates the shadow for a thing.
func (b *InMemoryBackend) UpdateThingShadow(thingName, shadowName string, state map[string]any) (*ThingShadow, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.things.Has(thingName) {
		return nil, fmt.Errorf("%w: %s", ErrThingNotFound, thingName)
	}

	key := shadowKey{thingName: thingName, shadowName: shadowName}
	existing := b.shadows[key]

	version := int64(1)
	if existing != nil {
		version = existing.Version + 1
	}

	s := &ThingShadow{
		State:   state,
		Version: version,
	}
	b.shadows[key] = s

	cp := *s

	return &cp, nil
}

// DeleteThingShadow deletes the shadow for a thing.
func (b *InMemoryBackend) DeleteThingShadow(thingName, shadowName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.things.Has(thingName) {
		return fmt.Errorf("%w: %s", ErrThingNotFound, thingName)
	}

	key := shadowKey{thingName: thingName, shadowName: shadowName}
	if _, ok := b.shadows[key]; !ok {
		return fmt.Errorf("%w: %s/%s", ErrShadowNotFound, thingName, shadowName)
	}

	delete(b.shadows, key)

	return nil
}

// ListNamedShadowsForThing returns all named shadow names for a thing.
func (b *InMemoryBackend) ListNamedShadowsForThing(thingName string) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.things.Has(thingName) {
		return nil, fmt.Errorf("%w: %s", ErrThingNotFound, thingName)
	}

	var names []string

	for k := range b.shadows {
		if k.thingName == thingName && k.shadowName != "" {
			names = append(names, k.shadowName)
		}
	}

	slices.Sort(names)

	return names, nil
}
