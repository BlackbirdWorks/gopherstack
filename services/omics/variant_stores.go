package omics

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ────────────────────────────────────────────────────────────────────────────
// VariantStore
// ────────────────────────────────────────────────────────────────────────────

// CreateVariantStore creates a new variant store.
func (b *InMemoryBackend) CreateVariantStore(
	name string,
	reference map[string]any,
	tags map[string]string,
) (*VariantStore, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("CreateVariantStore")
	defer b.mu.Unlock()

	if b.variantStores.Has(name) {
		return nil, fmt.Errorf("%w: variant store %s already exists", ErrAlreadyExists, name)
	}

	now := time.Now().UTC()
	vs := &VariantStore{
		ID:           newID(),
		Name:         name,
		Reference:    reference,
		Status:       statusCreating,
		Tags:         copyTags(tags),
		CreationTime: now,
		UpdateTime:   now,
	}
	vs.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "variantStore/"+name)
	b.variantStores.Put(vs)

	if tags != nil {
		b.tags[vs.Arn] = copyTags(tags)
	}

	result := *vs

	return &result, nil
}

// DeleteVariantStore deletes a variant store.
func (b *InMemoryBackend) DeleteVariantStore(name string) (*VariantStore, error) {
	b.mu.Lock("DeleteVariantStore")
	defer b.mu.Unlock()

	vs, ok := b.variantStores.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: variant store %s not found", ErrNotFound, name)
	}

	delete(b.tags, vs.Arn)
	b.variantStores.Delete(name)

	result := *vs
	result.Status = statusDeleting

	return &result, nil
}

// GetVariantStore retrieves a variant store, advancing CREATING→ACTIVE on
// first poll (real VariantStoreCreatedWaiter clients poll until Status ==
// ACTIVE).
func (b *InMemoryBackend) GetVariantStore(name string) (*VariantStore, error) {
	b.mu.Lock("GetVariantStore")
	defer b.mu.Unlock()

	vs, ok := b.variantStores.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: variant store %s not found", ErrNotFound, name)
	}

	if vs.Status == statusCreating {
		vs.pollCount++
		if vs.pollCount >= 1 {
			vs.Status = statusActive
		}
	}

	result := *vs

	return &result, nil
}

// ListVariantStores lists variant stores.
func (b *InMemoryBackend) ListVariantStores(
	maxResults int,
	nextToken string,
) ([]*VariantStore, string, error) {
	b.mu.RLock("ListVariantStores")
	defer b.mu.RUnlock()

	all := b.variantStores.All()
	names := make([]string, 0, len(all))

	for _, vs := range all {
		names = append(names, vs.Name)
	}

	result, outToken := paginatedCopies(names, nextToken, maxResults, b.variantStores.Get)

	return result, outToken, nil
}

// UpdateVariantStore updates a variant store.
func (b *InMemoryBackend) UpdateVariantStore(name, description string) (*VariantStore, error) {
	b.mu.Lock("UpdateVariantStore")
	defer b.mu.Unlock()

	vs, ok := b.variantStores.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: variant store %s not found", ErrNotFound, name)
	}

	if description != "" {
		vs.Description = description
	}

	vs.UpdateTime = time.Now().UTC()
	result := *vs

	return &result, nil
}

// StartVariantImportJob starts a variant import job.
func (b *InMemoryBackend) StartVariantImportJob(
	destinationName, roleARN string,
	items []VariantImportItem,
) (*VariantImportJob, error) {
	b.mu.Lock("StartVariantImportJob")
	defer b.mu.Unlock()

	if !b.variantStores.Has(destinationName) {
		return nil, fmt.Errorf("%w: variant store %s not found", ErrNotFound, destinationName)
	}

	now := time.Now().UTC()
	job := &VariantImportJob{
		ID:              newID(),
		DestinationName: destinationName,
		RoleARN:         roleARN,
		Items:           items,
		Status:          statusCompleted,
		CreationTime:    now,
		CompletionTime:  &now,
	}
	b.variantImportJobs.Put(job)

	result := *job

	return &result, nil
}

// GetVariantImportJob retrieves a variant import job.
func (b *InMemoryBackend) GetVariantImportJob(jobID string) (*VariantImportJob, error) {
	b.mu.RLock("GetVariantImportJob")
	defer b.mu.RUnlock()

	job, ok := b.variantImportJobs.Get(jobID)
	if !ok {
		return nil, fmt.Errorf("%w: variant import job %s not found", ErrNotFound, jobID)
	}

	result := *job

	return &result, nil
}

// ListVariantImportJobs lists variant import jobs.
func (b *InMemoryBackend) ListVariantImportJobs(
	maxResults int,
	nextToken string,
) ([]*VariantImportJob, string, error) {
	b.mu.RLock("ListVariantImportJobs")
	defer b.mu.RUnlock()

	all := b.variantImportJobs.All()
	ids := make([]string, 0, len(all))

	for _, j := range all {
		ids = append(ids, j.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.variantImportJobs.Get)

	return result, outToken, nil
}

// CancelVariantImportJob cancels a variant import job.
func (b *InMemoryBackend) CancelVariantImportJob(jobID string) error {
	b.mu.Lock("CancelVariantImportJob")
	defer b.mu.Unlock()

	job, ok := b.variantImportJobs.Get(jobID)
	if !ok {
		return fmt.Errorf("%w: variant import job %s not found", ErrNotFound, jobID)
	}

	job.Status = statusCancelled

	return nil
}
