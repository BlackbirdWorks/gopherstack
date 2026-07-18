package translate

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) parallelDataARN(name string) string {
	return arn.Build("translate", b.region, b.accountID, "parallel-data/"+name)
}

// CreateParallelData creates a new parallel data resource.
func (b *InMemoryBackend) CreateParallelData(
	name, description string,
	cfg *ParallelDataConfig,
	encKey *EncryptionKey,
	tags map[string]string,
) (*ParallelData, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.parallelData.Has(name) {
		return nil, fmt.Errorf("%w: parallel data %q already exists", ErrConflict, name)
	}

	now := time.Now().UTC()
	resourceARN := b.parallelDataARN(name)

	pd := &ParallelData{
		ARN:                resourceARN,
		Name:               name,
		Description:        description,
		ParallelDataConfig: cfg,
		EncryptionKey:      encKey,
		Tags:               tags,
		CreatedAt:          now,
		LastUpdatedAt:      now,
		Status:             "ACTIVE",
		SourceLanguage:     "en",
	}
	b.parallelData.Put(pd)

	if tags != nil {
		b.tags[resourceARN] = copyMap(tags)
	}

	return pd, nil
}

// GetParallelData retrieves a parallel data resource by name.
func (b *InMemoryBackend) GetParallelData(name string) (*ParallelData, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	pd, ok := b.parallelData.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: parallel data %q not found", ErrNotFound, name)
	}

	return pd, nil
}

// UpdateParallelData updates an existing parallel data resource.
func (b *InMemoryBackend) UpdateParallelData(
	name, description string,
	cfg *ParallelDataConfig,
) (*ParallelData, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pd, ok := b.parallelData.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: parallel data %q not found", ErrNotFound, name)
	}

	pd.Description = description
	if cfg != nil {
		pd.ParallelDataConfig = cfg
	}

	pd.LastUpdatedAt = time.Now().UTC()

	return pd, nil
}

// DeleteParallelData removes a parallel data resource by name.
func (b *InMemoryBackend) DeleteParallelData(name string) (*ParallelData, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pd, ok := b.parallelData.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: parallel data %q not found", ErrNotFound, name)
	}

	resourceARN := b.parallelDataARN(name)
	b.parallelData.Delete(name)
	delete(b.tags, resourceARN)

	return pd, nil
}

// ListParallelData returns a paginated list of parallel data resources.
func (b *InMemoryBackend) ListParallelData(maxResults int, nextToken string) ([]*ParallelData, string) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := sortedNames(b.parallelData.All(), func(pd *ParallelData) string { return pd.Name })

	return paginate(names, func(n string) *ParallelData { return tableGet(b.parallelData, n) }, maxResults, nextToken)
}
