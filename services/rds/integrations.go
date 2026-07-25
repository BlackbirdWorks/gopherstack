package rds

import (
	"fmt"
	"slices"
	"time"
)

// CreateIntegration creates a new zero-ETL integration.
func (b *InMemoryBackend) CreateIntegration(
	name, sourceARN, targetARN, kmsKeyID, dataFilter, description string,
) (*Integration, error) {
	b.mu.Lock("CreateIntegration")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: IntegrationName is required", ErrInvalidParameter)
	}
	if _, exists := b.integrations.Get(name); exists {
		return nil, fmt.Errorf("%w: %s", ErrIntegrationAlreadyExists, name)
	}

	intg := &Integration{
		IntegrationArn: fmt.Sprintf(
			"arn:aws:rds:%s:%s:integration:%s",
			b.region,
			b.accountID,
			name,
		),
		IntegrationName:        name,
		SourceArn:              sourceARN,
		TargetArn:              targetARN,
		KmsKeyID:               kmsKeyID,
		DataFilter:             dataFilter,
		IntegrationDescription: description,
		Status:                 integrationStatusActive,
		CreatedAt:              time.Now(),
	}
	b.integrations.Put(intg)
	cp := *intg

	return &cp, nil
}

// DeleteIntegration deletes an integration by name or ARN identifier.
func (b *InMemoryBackend) DeleteIntegration(identifier string) (*Integration, error) {
	b.mu.Lock("DeleteIntegration")
	defer b.mu.Unlock()

	for _, intg := range b.integrations.All() {
		if intg.IntegrationName == identifier || intg.IntegrationArn == identifier {
			cp := *intg
			cp.Status = integrationStatusDeleting
			b.integrations.Delete(intg.IntegrationName)
			// Cascade-clean tags now that Integration surfaces Tags on the
			// wire (see handler_integrations.go's toXMLIntegration): without
			// this, b.tags[intg.IntegrationArn] would be an unreachable
			// ghost entry that grows unboundedly across create/delete
			// cycles, exactly the leak class this emulator's other
			// resources' delete paths already guard against.
			delete(b.tags, intg.IntegrationArn)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrIntegrationNotFound, identifier)
}

// DescribeIntegrations returns integrations, optionally filtered by identifier.
func (b *InMemoryBackend) DescribeIntegrations(identifier string) ([]Integration, error) {
	b.mu.RLock("DescribeIntegrations")
	defer b.mu.RUnlock()

	result := make([]Integration, 0, b.integrations.Len())
	for _, intg := range b.integrations.All() {
		if identifier != "" && intg.IntegrationName != identifier &&
			intg.IntegrationArn != identifier {
			continue
		}
		result = append(result, *intg)
	}

	if identifier != "" && len(result) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrIntegrationNotFound, identifier)
	}

	slices.SortFunc(result, func(a, b Integration) int {
		if a.IntegrationName < b.IntegrationName {
			return -1
		}
		if a.IntegrationName > b.IntegrationName {
			return 1
		}

		return 0
	})

	return result, nil
}

// ModifyIntegration modifies an integration's description or data filter.
func (b *InMemoryBackend) ModifyIntegration(identifier, dataFilter, description string) (*Integration, error) {
	b.mu.Lock("ModifyIntegration")
	defer b.mu.Unlock()

	for _, intg := range b.integrations.All() {
		if intg.IntegrationName == identifier || intg.IntegrationArn == identifier {
			if dataFilter != "" {
				intg.DataFilter = dataFilter
			}
			if description != "" {
				intg.IntegrationDescription = description
			}
			cp := *intg

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrIntegrationNotFound, identifier)
}

const (
	integrationStatusActive   = "active"
	integrationStatusDeleting = instanceStatusDeleting
)
