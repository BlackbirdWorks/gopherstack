package mediastore

import (
	"context"
	"encoding/json/jsontext"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// PutLifecyclePolicy stores a lifecycle policy for a container.
func (b *InMemoryBackend) PutLifecyclePolicy(ctx context.Context, name, policy string) error {
	if !jsontext.Value([]byte(policy)).IsValid(jsonx.V1()) {
		return ErrInvalidPolicy
	}

	region := regionFromContext(ctx)

	b.advanceContainerStates(time.Now())

	b.mu.Lock("PutLifecyclePolicy")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	if err := requireActive(c); err != nil {
		return err
	}

	c.LifecyclePolicy = policy

	return nil
}

// GetLifecyclePolicy retrieves the lifecycle policy for a container.
func (b *InMemoryBackend) GetLifecyclePolicy(ctx context.Context, name string) (string, error) {
	region := regionFromContext(ctx)

	b.advanceContainerStates(time.Now())

	b.mu.RLock("GetLifecyclePolicy")
	defer b.mu.RUnlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return "", ErrContainerNotFound
	}

	if err := requireActive(c); err != nil {
		return "", err
	}

	if c.LifecyclePolicy == "" {
		return "", ErrLifecyclePolicyNotFound
	}

	return c.LifecyclePolicy, nil
}

// DeleteLifecyclePolicy removes the lifecycle policy from a container.
func (b *InMemoryBackend) DeleteLifecyclePolicy(ctx context.Context, name string) error {
	region := regionFromContext(ctx)

	b.advanceContainerStates(time.Now())

	b.mu.Lock("DeleteLifecyclePolicy")
	defer b.mu.Unlock()

	c, exists := b.getContainer(region, name)
	if !exists {
		return ErrContainerNotFound
	}

	if err := requireActive(c); err != nil {
		return err
	}

	if c.LifecyclePolicy == "" {
		return ErrLifecyclePolicyNotFound
	}

	c.LifecyclePolicy = ""

	return nil
}
