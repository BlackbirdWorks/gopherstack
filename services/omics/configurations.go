package omics

import (
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ────────────────────────────────────────────────────────────────────────────
// Configuration
// ────────────────────────────────────────────────────────────────────────────

// CreateConfiguration creates a configuration. runConfigurations is a
// required CreateConfigurationInput member (verified against
// validateOpCreateConfigurationInput, validators.go) that the pre-fix
// request never read at all.
func (b *InMemoryBackend) CreateConfiguration(
	name, description string, runConfigurations *ConfigurationRunConfigurations, tags map[string]string,
) (*Configuration, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if runConfigurations == nil {
		return nil, fmt.Errorf("%w: runConfigurations is required", ErrValidation)
	}

	b.mu.Lock("CreateConfiguration")
	defer b.mu.Unlock()

	if b.configurations.Has(name) {
		return nil, fmt.Errorf("%w: configuration %s already exists", ErrAlreadyExists, name)
	}

	id := uuid.NewString()
	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	cfg := &Configuration{
		Name:              name,
		Description:       description,
		CreationTime:      time.Now().UTC(),
		ARN:               arn.Build("omics", b.defaultRegion, b.accountID, "configuration/"+id),
		UUID:              id,
		Status:            statusActive,
		Tags:              tagsCopy,
		RunConfigurations: runConfigurations,
	}
	b.configurations.Put(cfg)

	result := *cfg

	return &result, nil
}

// DeleteConfiguration deletes a configuration.
func (b *InMemoryBackend) DeleteConfiguration(name string) error {
	b.mu.Lock("DeleteConfiguration")
	defer b.mu.Unlock()

	if !b.configurations.Has(name) {
		return fmt.Errorf("%w: configuration %s not found", ErrNotFound, name)
	}

	b.configurations.Delete(name)

	return nil
}

// GetConfiguration retrieves a configuration.
func (b *InMemoryBackend) GetConfiguration(name string) (*Configuration, error) {
	b.mu.RLock("GetConfiguration")
	defer b.mu.RUnlock()

	cfg, ok := b.configurations.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: configuration %s not found", ErrNotFound, name)
	}

	result := *cfg

	return &result, nil
}

// ListConfigurations lists configurations.
func (b *InMemoryBackend) ListConfigurations(
	maxResults int,
	nextToken string,
) ([]*Configuration, string, error) {
	b.mu.RLock("ListConfigurations")
	defer b.mu.RUnlock()

	all := b.configurations.All()
	names := make([]string, 0, len(all))

	for _, cfg := range all {
		names = append(names, cfg.Name)
	}

	result, outToken := paginatedCopies(names, nextToken, maxResults, b.configurations.Get)

	return result, outToken, nil
}

// ────────────────────────────────────────────────────────────────────────────
// S3 Access Policy
// ────────────────────────────────────────────────────────────────────────────

// PutS3AccessPolicy stores an S3 access policy.
func (b *InMemoryBackend) PutS3AccessPolicy(s3AccessPointARN, policy string) error {
	b.mu.Lock("PutS3AccessPolicy")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	b.s3AccessPolicies.Put(&S3AccessPolicy{
		S3AccessPointARN: s3AccessPointARN,
		Policy:           policy,
		UpdateTime:       &now,
	})

	return nil
}

// GetS3AccessPolicy retrieves an S3 access policy.
func (b *InMemoryBackend) GetS3AccessPolicy(s3AccessPointARN string) (*S3AccessPolicy, error) {
	b.mu.RLock("GetS3AccessPolicy")
	defer b.mu.RUnlock()

	p, ok := b.s3AccessPolicies.Get(s3AccessPointARN)
	if !ok {
		return nil, fmt.Errorf(
			"%w: S3 access policy for %s not found",
			ErrNotFound,
			s3AccessPointARN,
		)
	}

	result := *p

	return &result, nil
}

// DeleteS3AccessPolicy deletes an S3 access policy.
func (b *InMemoryBackend) DeleteS3AccessPolicy(s3AccessPointARN string) error {
	b.mu.Lock("DeleteS3AccessPolicy")
	defer b.mu.Unlock()

	if !b.s3AccessPolicies.Has(s3AccessPointARN) {
		return fmt.Errorf("%w: S3 access policy for %s not found", ErrNotFound, s3AccessPointARN)
	}

	b.s3AccessPolicies.Delete(s3AccessPointARN)

	return nil
}
