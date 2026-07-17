package mediapackage

import (
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreatePackagingConfiguration creates a new packaging configuration.
func (b *InMemoryBackend) CreatePackagingConfiguration(
	id, packagingGroupID, description string,
	tags map[string]string,
) (*PackagingConfiguration, error) {
	b.mu.Lock("CreatePackagingConfiguration")
	defer b.mu.Unlock()

	if id == "" {
		return nil, fmt.Errorf("%w: id required", ErrInvalidParameter)
	}
	if b.packagingConfigurations.Has(id) {
		return nil, ErrConflict
	}

	t := make(map[string]string, len(tags))
	maps.Copy(t, tags)

	pc := &storedPackagingConfiguration{
		Tags:             t,
		ARN:              b.buildPackagingConfigARN(id),
		ID:               id,
		PackagingGroupID: packagingGroupID,
		Description:      description,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
	}
	b.packagingConfigurations.Put(pc)

	return pc.toPackagingConfiguration(), nil
}

// DescribePackagingConfiguration returns a packaging configuration by ID.
func (b *InMemoryBackend) DescribePackagingConfiguration(id string) (*PackagingConfiguration, error) {
	b.mu.RLock("DescribePackagingConfiguration")
	defer b.mu.RUnlock()

	pc, ok := b.packagingConfigurations.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: packagingConfiguration %s not found", ErrNotFound, id)
	}

	return pc.toPackagingConfiguration(), nil
}

// DeletePackagingConfiguration removes a packaging configuration.
func (b *InMemoryBackend) DeletePackagingConfiguration(id string) error {
	b.mu.Lock("DeletePackagingConfiguration")
	defer b.mu.Unlock()

	if !b.packagingConfigurations.Has(id) {
		return fmt.Errorf("%w: packagingConfiguration %s not found", ErrNotFound, id)
	}
	b.packagingConfigurations.Delete(id)

	return nil
}

// ListPackagingConfigurations returns all packaging configurations.
func (b *InMemoryBackend) ListPackagingConfigurations(
	maxResults int,
	nextToken string,
) ([]*PackagingConfiguration, string, error) {
	b.mu.RLock("ListPackagingConfigurations")
	defer b.mu.RUnlock()

	all := b.packagingConfigurations.Snapshot()

	p := page.New(all, nextToken, maxResults, defaultMaxResults)

	result := make([]*PackagingConfiguration, 0, len(p.Data))
	for _, pc := range p.Data {
		result = append(result, pc.toPackagingConfiguration())
	}

	return result, p.Next, nil
}
