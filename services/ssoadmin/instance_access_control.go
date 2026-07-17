package ssoadmin

import (
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// validateACAAttributes validates the AccessControlAttribute list per AWS limits.
func validateACAAttributes(attrs []AccessControlAttribute) error {
	if len(attrs) > maxACAAttributes {
		return fmt.Errorf("%w: AccessControlAttributes exceeds maximum of %d",
			awserr.ErrInvalidParameter, maxACAAttributes)
	}
	for _, a := range attrs {
		if len(a.Key) > maxACAKeyLen {
			return fmt.Errorf("%w: AccessControlAttribute key exceeds maximum length of %d",
				awserr.ErrInvalidParameter, maxACAKeyLen)
		}
		if a.Key != "" && !acaKeyRe.MatchString(a.Key) {
			return fmt.Errorf("%w: AccessControlAttribute key contains invalid characters", awserr.ErrInvalidParameter)
		}
		if len(a.Value.Source) > maxACASourceItems {
			return fmt.Errorf("%w: AccessControlAttribute source list exceeds maximum of %d items",
				awserr.ErrInvalidParameter, maxACASourceItems)
		}
		if len(a.Value.Source) == 0 {
			return fmt.Errorf("%w: AccessControlAttribute source list must have at least one item",
				awserr.ErrInvalidParameter)
		}
		for _, s := range a.Value.Source {
			if len(s) > maxACASourceItemLen {
				return fmt.Errorf("%w: AccessControlAttribute source item exceeds maximum length of %d",
					awserr.ErrInvalidParameter, maxACASourceItemLen)
			}
		}
	}

	return nil
}

// copyAccessControlAttributes returns a deep copy of an AccessControlAttribute slice.
func copyAccessControlAttributes(attrs []AccessControlAttribute) []AccessControlAttribute {
	if attrs == nil {
		return nil
	}
	result := make([]AccessControlAttribute, len(attrs))
	for i, a := range attrs {
		result[i] = AccessControlAttribute{
			Key: a.Key,
			Value: AccessControlAttributeValue{
				Source: slices.Clone(a.Value.Source),
			},
		}
	}

	return result
}

// CreateInstanceAccessControlAttributeConfiguration creates ABAC configuration for an SSO instance.
// Returns ConflictException if a configuration already exists (use Update to modify).
func (b *InMemoryBackend) CreateInstanceAccessControlAttributeConfiguration(
	instanceArn string,
	attributes []AccessControlAttribute,
) error {
	b.mu.Lock("CreateInstanceAccessControlAttributeConfiguration")
	defer b.mu.Unlock()

	if err := validateACAAttributes(attributes); err != nil {
		return err
	}

	if !b.instances.Has(instanceArn) {
		return ErrInstanceNotFound
	}
	if b.instanceACAs.Has(instanceArn) {
		return ErrACAAlreadyExists
	}
	b.instanceACAs.Put(&ABACConfig{
		InstanceArn:             instanceArn,
		AccessControlAttributes: copyAccessControlAttributes(attributes),
		Status:                  abacStatusCreationInProgress,
	})

	return nil
}

// DescribeInstanceAccessControlAttributeConfiguration returns ABAC configuration for an instance.
func (b *InMemoryBackend) DescribeInstanceAccessControlAttributeConfiguration(
	instanceArn string,
) (*ABACConfig, error) {
	b.mu.Lock("DescribeInstanceAccessControlAttributeConfiguration")
	defer b.mu.Unlock()

	if !b.instances.Has(instanceArn) {
		return nil, ErrInstanceNotFound
	}
	cfg, ok := b.instanceACAs.Get(instanceArn)
	if !ok {
		return nil, ErrRequestNotFound
	}
	// Lazily transition CREATION_IN_PROGRESS → ENABLED on first describe.
	if cfg.Status == abacStatusCreationInProgress {
		cfg.Status = abacStatusEnabled
	}

	return &ABACConfig{
		AccessControlAttributes: copyAccessControlAttributes(cfg.AccessControlAttributes),
		Status:                  cfg.Status,
		StatusReason:            cfg.StatusReason,
	}, nil
}

// DeleteInstanceAccessControlAttributeConfiguration deletes ABAC configuration for an instance.
func (b *InMemoryBackend) DeleteInstanceAccessControlAttributeConfiguration(instanceArn string) error {
	b.mu.Lock("DeleteInstanceAccessControlAttributeConfiguration")
	defer b.mu.Unlock()

	if !b.instances.Has(instanceArn) {
		return ErrInstanceNotFound
	}
	if !b.instanceACAs.Has(instanceArn) {
		return ErrRequestNotFound
	}
	b.instanceACAs.Delete(instanceArn)

	return nil
}

// UpdateInstanceAccessControlAttributeConfiguration updates the ABAC config for an instance.
func (b *InMemoryBackend) UpdateInstanceAccessControlAttributeConfiguration(
	instanceArn string,
	attributes []AccessControlAttribute,
) error {
	b.mu.Lock("UpdateInstanceAccessControlAttributeConfiguration")
	defer b.mu.Unlock()

	if err := validateACAAttributes(attributes); err != nil {
		return err
	}

	if !b.instances.Has(instanceArn) {
		return ErrInstanceNotFound
	}
	existing, _ := b.instanceACAs.Get(instanceArn)
	status := abacStatusEnabled
	if existing != nil {
		status = existing.Status
	}
	b.instanceACAs.Put(&ABACConfig{
		InstanceArn:             instanceArn,
		AccessControlAttributes: copyAccessControlAttributes(attributes),
		Status:                  status,
	})

	return nil
}
