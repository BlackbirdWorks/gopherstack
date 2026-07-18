package opsworks

import (
	"time"

	"github.com/google/uuid"
)

// CreateApp creates a new app in a stack.
func (b *InMemoryBackend) CreateApp(stackID, name, appType string) (*App, error) {
	if name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateApp")
	defer b.mu.Unlock()

	if !b.stacks.Has(stackID) {
		return nil, ErrStackNotFound
	}

	id := uuid.NewString()
	now := time.Now().UTC()

	a := &storedApp{
		CreatedAt: now,
		StackID:   stackID,
		AppID:     id,
		Arn:       b.appARN(id),
		Name:      name,
		Type:      appType,
	}
	b.apps.Put(a)

	return a.toApp(), nil
}

// DescribeApps returns apps filtered by stack and/or app IDs.
func (b *InMemoryBackend) DescribeApps(stackID string, appIDs []string) ([]*App, error) {
	b.mu.RLock("DescribeApps")
	defer b.mu.RUnlock()

	if len(appIDs) > 0 {
		result := make([]*App, 0, len(appIDs))
		for _, id := range appIDs {
			a, ok := b.apps.Get(id)
			if !ok {
				return nil, ErrAppNotFound
			}
			result = append(result, a.toApp())
		}

		return result, nil
	}

	source := stackScoped(stackID, b.apps.All, b.appsByStack.Get)

	result := make([]*App, 0, len(source))
	for _, a := range source {
		result = append(result, a.toApp())
	}

	return result, nil
}

// UpdateApp updates an app's name.
func (b *InMemoryBackend) UpdateApp(appID, name string) error {
	b.mu.Lock("UpdateApp")
	defer b.mu.Unlock()

	a, ok := b.apps.Get(appID)
	if !ok {
		return ErrAppNotFound
	}

	if name != "" {
		a.Name = name
	}

	return nil
}

// DeleteApp deletes an app.
func (b *InMemoryBackend) DeleteApp(appID string) error {
	b.mu.Lock("DeleteApp")
	defer b.mu.Unlock()

	if !b.apps.Delete(appID) {
		return ErrAppNotFound
	}

	return nil
}
