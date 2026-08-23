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
//
// sseConfig is real CreateVariantStoreInput's optional "sseConfig"
// (api_op_CreateVariantStore.go) -- previously not even read by the
// handler, despite GetVariantStoreOutput/VariantStoreItem requiring it on
// every response (types.go); the same passthrough-map convention
// AnnotationStore already uses (gopherstack-r80d batch 7).
func (b *InMemoryBackend) CreateVariantStore(
	name string,
	reference, sseConfig map[string]any,
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
		SseConfig:    sseConfig,
		Status:       statusCreating,
		Tags:         copyTags(tags),
		CreationTime: now,
		UpdateTime:   now,
	}
	vs.StoreArn = arn.Build("omics", b.defaultRegion, b.accountID, "variantStore/"+name)
	b.variantStores.Put(vs)

	if tags != nil {
		b.tags[vs.StoreArn] = copyTags(tags)
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

	delete(b.tags, vs.StoreArn)
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

// ListVariantStores lists variant stores, optionally filtered by status
// and/or a specific set of store ids (real AWS ListVariantStoresInput body
// "filter"/"ids", omics@v1.49.5 serializers.go:7543).
func (b *InMemoryBackend) ListVariantStores(
	filter *StoreStatusFilter,
	ids0 []string,
	maxResults int,
	nextToken string,
) ([]*VariantStore, string, error) {
	b.mu.RLock("ListVariantStores")
	defer b.mu.RUnlock()

	idSet := stringSet(ids0)
	all := b.variantStores.All()
	names := make([]string, 0, len(all))

	for _, vs := range all {
		if !storeMatchesFilter(vs.Status, vs.ID, filter, idSet) {
			continue
		}

		names = append(names, vs.Name)
	}

	result, outToken := paginatedCopies(names, nextToken, maxResults, b.variantStores.Get)

	return result, outToken, nil
}

// newVariantStoreSummary converts a persisted store record into the real
// ListVariantStoresOutput element shape (see VariantStoreSummary's doc
// comment for why List and Get differ).
func newVariantStoreSummary(vs *VariantStore) VariantStoreSummary {
	return VariantStoreSummary{
		CreationTime:   vs.CreationTime,
		UpdateTime:     vs.UpdateTime,
		Reference:      vs.Reference,
		SseConfig:      vs.SseConfig,
		StoreArn:       vs.StoreArn,
		ID:             vs.ID,
		Name:           vs.Name,
		Description:    vs.Description,
		Status:         vs.Status,
		StatusMessage:  vs.StatusMessage,
		StoreSizeBytes: vs.StoreSizeBytes,
	}
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

// variantImportItemDetails converts the real StartVariantImportJobInput item
// shape (VariantImportItem, source only) into the real
// GetVariantImportJobOutput item shape (VariantImportItemDetail, jobStatus +
// source), stamping every item with the job's own status. This backend
// completes import jobs synchronously in one step, so that status is each
// item's true final state, not a guess.
func variantImportItemDetails(items []VariantImportItem, status string) []VariantImportItemDetail {
	details := make([]VariantImportItemDetail, 0, len(items))
	for _, item := range items {
		details = append(details, VariantImportItemDetail{Source: item.Source, JobStatus: status})
	}

	return details
}

// newVariantImportJobSummary converts a persisted job record into the real
// ListVariantImportJobsOutput element shape (see VariantImportJobSummary's
// doc comment for why List and Get differ).
func newVariantImportJobSummary(job *VariantImportJob) VariantImportJobSummary {
	return VariantImportJobSummary{
		CreationTime:         job.CreationTime,
		CompletionTime:       job.CompletionTime,
		UpdateTime:           job.UpdateTime,
		AnnotationFields:     job.AnnotationFields,
		ID:                   job.ID,
		DestinationName:      job.DestinationName,
		RoleARN:              job.RoleARN,
		Status:               job.Status,
		RunLeftNormalization: job.RunLeftNormalization,
	}
}

// StartVariantImportJob starts a variant import job. annotationFields and
// runLeftNormalization are real optional StartVariantImportJobInput members
// (serializers.go:8737-8767) that were previously dropped on the floor -- the
// handler never read them at all. Unlike annotation import jobs, variant
// import jobs have no formatOptions or versionName field in the real API.
func (b *InMemoryBackend) StartVariantImportJob(
	destinationName, roleARN string,
	items []VariantImportItem,
	annotationFields map[string]string,
	runLeftNormalization bool,
) (*VariantImportJob, error) {
	b.mu.Lock("StartVariantImportJob")
	defer b.mu.Unlock()

	if !b.variantStores.Has(destinationName) {
		return nil, fmt.Errorf("%w: variant store %s not found", ErrNotFound, destinationName)
	}

	now := time.Now().UTC()
	status := statusCompleted
	job := &VariantImportJob{
		ID:                   newID(),
		DestinationName:      destinationName,
		RoleARN:              roleARN,
		Items:                variantImportItemDetails(items, status),
		AnnotationFields:     annotationFields,
		RunLeftNormalization: runLeftNormalization,
		Status:               status,
		CreationTime:         now,
		CompletionTime:       &now,
		UpdateTime:           now,
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

// ListVariantImportJobs lists variant import jobs, optionally filtered by
// status/storeName and/or a specific set of job ids (real AWS
// ListVariantImportJobsInput body "filter"/"ids").
func (b *InMemoryBackend) ListVariantImportJobs(
	filter *ImportJobFilter,
	ids0 []string,
	maxResults int,
	nextToken string,
) ([]*VariantImportJob, string, error) {
	b.mu.RLock("ListVariantImportJobs")
	defer b.mu.RUnlock()

	idSet := stringSet(ids0)
	all := b.variantImportJobs.All()
	ids := make([]string, 0, len(all))

	for _, j := range all {
		if !importJobMatchesFilter(j.Status, j.DestinationName, filter, idSet, j.ID) {
			continue
		}

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
