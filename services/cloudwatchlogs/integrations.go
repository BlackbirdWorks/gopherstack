package cloudwatchlogs

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// AssociateSourceToS3TableIntegration associates a data source with an S3 table integration.
// Returns a unique identifier for the association.
func (b *InMemoryBackend) AssociateSourceToS3TableIntegration(
	integrationArn, _, _ string,
) (string, error) {
	if integrationArn == "" {
		return "", fmt.Errorf("%w: integrationArn is required", ErrValidation)
	}

	id := uuid.New().String()

	b.mu.Lock("AssociateSourceToS3TableIntegration")
	defer b.mu.Unlock()

	b.s3TableIntegrations.Put(&s3TableIntegrationEntry{ID: id, IntegrationArn: integrationArn})

	return id, nil
}

// PutIntegration creates or updates an integration.
func (b *InMemoryBackend) PutIntegration(name, integrationType string) (*CWLIntegration, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: integrationName is required", ErrValidation)
	}

	b.mu.Lock("PutIntegration")
	defer b.mu.Unlock()

	ig := CWLIntegration{
		Name:      name,
		Type:      integrationType,
		Status:    completenessStatusActive,
		CreatedAt: time.Now().UTC(),
	}
	stored := ig
	b.integrations.Put(&stored)

	return &ig, nil
}

// GetIntegration returns an integration by name.
func (b *InMemoryBackend) GetIntegration(name string) (*CWLIntegration, error) {
	b.mu.RLock("GetIntegration")
	defer b.mu.RUnlock()

	ig, ok := b.integrations.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: integration %q not found", ErrIntegrationNotFound, name)
	}
	cp := *ig

	return &cp, nil
}

// ListIntegrations returns all integrations sorted by name.
func (b *InMemoryBackend) ListIntegrations() []CWLIntegration {
	b.mu.RLock("ListIntegrations")
	defer b.mu.RUnlock()

	out := make([]CWLIntegration, 0, b.integrations.Len())
	for _, ig := range b.integrations.All() {
		out = append(out, *ig)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// DeleteIntegration removes an integration by name.
func (b *InMemoryBackend) DeleteIntegration(name string) error {
	b.mu.Lock("DeleteIntegration")
	defer b.mu.Unlock()

	if !b.integrations.Delete(name) {
		return fmt.Errorf("%w: integration %q not found", ErrIntegrationNotFound, name)
	}

	return nil
}
