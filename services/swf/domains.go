package swf

import "fmt"

// RegisterDomain registers a new SWF domain with the given retention period.
// retention must be "0"-"90" or "NONE" (empty defaults to "NONE").
func (b *InMemoryBackend) RegisterDomain(name, description, retention string) error {
	if name == "" {
		return fmt.Errorf("%w: name is required", ErrValidation)
	}
	if retention == "" {
		retention = retentionNone
	}
	if err := validateRetention(retention); err != nil {
		return err
	}

	b.mu.Lock("RegisterDomain")
	defer b.mu.Unlock()

	if d, ok := b.domains.Get(name); ok {
		if d.Status == statusDeprecated {
			return fmt.Errorf("%w: %s", ErrDeprecated, name)
		}

		return fmt.Errorf("%w: %s", ErrAlreadyExists, name)
	}

	b.domains.Put(&Domain{
		Name:                                   name,
		Description:                            description,
		Status:                                 statusRegistered,
		Arn:                                    domainARN(defaultRegion, defaultAccountID, name),
		WorkflowExecutionRetentionPeriodInDays: retention,
	})

	return nil
}

// ListDomains returns all domains with the given registrationStatus.
// An empty status returns all domains.
func (b *InMemoryBackend) ListDomains(registrationStatus string) ([]Domain, error) {
	if err := validateRegistrationStatus(registrationStatus); err != nil {
		return nil, err
	}

	b.mu.RLock("ListDomains")
	defer b.mu.RUnlock()

	out := make([]Domain, 0, b.domains.Len())
	for _, d := range b.domains.All() {
		if registrationStatus == "" || d.Status == registrationStatus {
			out = append(out, *d)
		}
	}

	return out, nil
}

// DescribeDomain returns the details of a registered SWF domain.
func (b *InMemoryBackend) DescribeDomain(name string) (*Domain, error) {
	b.mu.RLock("DescribeDomain")
	defer b.mu.RUnlock()

	d, ok := b.domains.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	cp := *d

	return &cp, nil
}

// DeprecateDomain marks a domain as deprecated.
func (b *InMemoryBackend) DeprecateDomain(name string) error {
	b.mu.Lock("DeprecateDomain")
	defer b.mu.Unlock()

	d, ok := b.domains.Get(name)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	if d.Status == statusDeprecated {
		return fmt.Errorf("%w: %s", ErrDeprecated, name)
	}
	d.Status = statusDeprecated

	return nil
}

// UndeprecateDomain re-activates a deprecated domain.
func (b *InMemoryBackend) UndeprecateDomain(name string) error {
	b.mu.Lock("UndeprecateDomain")
	defer b.mu.Unlock()

	d, ok := b.domains.Get(name)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	if d.Status == statusRegistered {
		return fmt.Errorf("%w: %s", ErrAlreadyExists, name)
	}
	d.Status = statusRegistered

	return nil
}
