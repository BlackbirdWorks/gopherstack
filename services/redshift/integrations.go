package redshift

import "fmt"

// CreateIntegration creates a new zero-ETL integration.
func (b *InMemoryBackend) CreateIntegration(
	integrationName, sourceArn, targetArn, kmsKeyID, description string,
) (*Integration, error) {
	if integrationName == "" {
		return nil, fmt.Errorf("%w: IntegrationName is required", ErrInvalidParameter)
	}

	if sourceArn == "" {
		return nil, fmt.Errorf("%w: SourceArn is required", ErrInvalidParameter)
	}

	if targetArn == "" {
		return nil, fmt.Errorf("%w: TargetArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateIntegration")
	defer b.mu.Unlock()

	if _, exists := b.integrations.Get(integrationName); exists {
		return nil, fmt.Errorf("%w: integration %s already exists", ErrIntegrationAlreadyExists, integrationName)
	}

	arn := "arn:aws:redshift:" + b.region + ":" + b.accountID + ":integration/" + integrationName
	ig := &Integration{
		IntegrationArn:  arn,
		IntegrationName: integrationName,
		SourceArn:       sourceArn,
		TargetArn:       targetArn,
		KmsKeyID:        kmsKeyID,
		Description:     description,
		Status:          endpointStatusActive,
	}
	b.integrations.Put(ig)

	cp := *ig

	return &cp, nil
}

// DeleteIntegration deletes the named integration.
func (b *InMemoryBackend) DeleteIntegration(integrationArn string) (*Integration, error) {
	if integrationArn == "" {
		return nil, fmt.Errorf("%w: IntegrationArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteIntegration")
	defer b.mu.Unlock()

	for _, ig := range b.integrations.All() {
		if ig.IntegrationArn == integrationArn || ig.IntegrationName == integrationArn {
			cp := *ig
			b.integrations.Delete(ig.IntegrationName)

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: integration %s not found", ErrIntegrationNotFound, integrationArn)
}

// DescribeIntegrations returns integrations, optionally filtered by ARN.
func (b *InMemoryBackend) DescribeIntegrations(integrationArn string) ([]Integration, error) {
	b.mu.RLock("DescribeIntegrations")
	defer b.mu.RUnlock()

	if integrationArn != "" {
		for _, ig := range b.integrations.All() {
			if ig.IntegrationArn == integrationArn {
				cp := *ig

				return []Integration{cp}, nil
			}
		}

		return nil, fmt.Errorf("%w: integration %s not found", ErrIntegrationNotFound, integrationArn)
	}

	result := make([]Integration, 0, b.integrations.Len())

	for _, ig := range b.integrations.All() {
		result = append(result, *ig)
	}

	return result, nil
}

// ModifyIntegration updates the description of an integration.
func (b *InMemoryBackend) ModifyIntegration(integrationArn, description string) (*Integration, error) {
	if integrationArn == "" {
		return nil, fmt.Errorf("%w: IntegrationArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyIntegration")
	defer b.mu.Unlock()

	for _, ig := range b.integrations.All() {
		if ig.IntegrationArn == integrationArn {
			ig.Description = description
			cp := *ig

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: integration %s not found", ErrIntegrationNotFound, integrationArn)
}
