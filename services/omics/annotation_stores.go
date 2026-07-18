package omics

import (
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ────────────────────────────────────────────────────────────────────────────
// AnnotationStore
// ────────────────────────────────────────────────────────────────────────────

// CreateAnnotationStore creates a new annotation store.
func (b *InMemoryBackend) CreateAnnotationStore(
	name, storeFormat string,
	reference, sseConfig, storeOptions map[string]any,
	tags map[string]string,
) (*AnnotationStore, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("CreateAnnotationStore")
	defer b.mu.Unlock()

	if b.annotationStores.Has(name) {
		return nil, fmt.Errorf("%w: annotation store %s already exists", ErrAlreadyExists, name)
	}

	now := time.Now().UTC()
	as := &AnnotationStore{
		ID:           newID(),
		Name:         name,
		StoreFormat:  storeFormat,
		Reference:    reference,
		SseConfig:    sseConfig,
		StoreOptions: storeOptions,
		Status:       statusCreating,
		Tags:         copyTags(tags),
		CreationTime: now,
		UpdateTime:   now,
	}
	as.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "annotationStore/"+name)
	b.annotationStores.Put(as)

	if tags != nil {
		b.tags[as.Arn] = copyTags(tags)
	}

	result := *as

	return &result, nil
}

// DeleteAnnotationStore deletes an annotation store.
func (b *InMemoryBackend) DeleteAnnotationStore(name string) (*AnnotationStore, error) {
	b.mu.Lock("DeleteAnnotationStore")
	defer b.mu.Unlock()

	as, ok := b.annotationStores.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	delete(b.tags, as.Arn)
	b.annotationStores.Delete(name)

	for _, v := range slices.Clone(b.annotationVersionsByStore.Get(name)) {
		b.annotationVersions.Delete(parentKey(name, v.VersionName))
	}

	result := *as
	result.Status = statusDeleting

	return &result, nil
}

// GetAnnotationStore retrieves an annotation store, advancing CREATING→ACTIVE
// on first poll (real AnnotationStoreCreatedWaiter clients poll until Status
// == ACTIVE).
func (b *InMemoryBackend) GetAnnotationStore(name string) (*AnnotationStore, error) {
	b.mu.Lock("GetAnnotationStore")
	defer b.mu.Unlock()

	as, ok := b.annotationStores.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	if as.Status == statusCreating {
		as.pollCount++
		if as.pollCount >= 1 {
			as.Status = statusActive
		}
	}

	result := *as

	return &result, nil
}

// ListAnnotationStores lists annotation stores.
func (b *InMemoryBackend) ListAnnotationStores(
	maxResults int,
	nextToken string,
) ([]*AnnotationStore, string, error) {
	b.mu.RLock("ListAnnotationStores")
	defer b.mu.RUnlock()

	all := b.annotationStores.All()
	names := make([]string, 0, len(all))

	for _, as := range all {
		names = append(names, as.Name)
	}

	result, outToken := paginatedCopies(names, nextToken, maxResults, b.annotationStores.Get)

	return result, outToken, nil
}

// UpdateAnnotationStore updates an annotation store.
func (b *InMemoryBackend) UpdateAnnotationStore(
	name, description string,
) (*AnnotationStore, error) {
	b.mu.Lock("UpdateAnnotationStore")
	defer b.mu.Unlock()

	as, ok := b.annotationStores.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	if description != "" {
		as.Description = description
	}

	as.UpdateTime = time.Now().UTC()
	result := *as

	return &result, nil
}

// StartAnnotationImportJob starts an annotation import job.
func (b *InMemoryBackend) StartAnnotationImportJob(
	destinationName, roleARN string,
	items []AnnotationImportItem,
) (*AnnotationImportJob, error) {
	b.mu.Lock("StartAnnotationImportJob")
	defer b.mu.Unlock()

	if !b.annotationStores.Has(destinationName) {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, destinationName)
	}

	now := time.Now().UTC()
	job := &AnnotationImportJob{
		ID:              newID(),
		DestinationName: destinationName,
		RoleARN:         roleARN,
		Items:           items,
		Status:          statusCompleted,
		CreationTime:    now,
		CompletionTime:  &now,
	}
	b.annotationImportJobs.Put(job)

	result := *job

	return &result, nil
}

// GetAnnotationImportJob retrieves an annotation import job.
func (b *InMemoryBackend) GetAnnotationImportJob(jobID string) (*AnnotationImportJob, error) {
	b.mu.RLock("GetAnnotationImportJob")
	defer b.mu.RUnlock()

	job, ok := b.annotationImportJobs.Get(jobID)
	if !ok {
		return nil, fmt.Errorf("%w: annotation import job %s not found", ErrNotFound, jobID)
	}

	result := *job

	return &result, nil
}

// ListAnnotationImportJobs lists annotation import jobs.
func (b *InMemoryBackend) ListAnnotationImportJobs(
	maxResults int,
	nextToken string,
) ([]*AnnotationImportJob, string, error) {
	b.mu.RLock("ListAnnotationImportJobs")
	defer b.mu.RUnlock()

	all := b.annotationImportJobs.All()
	ids := make([]string, 0, len(all))

	for _, j := range all {
		ids = append(ids, j.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.annotationImportJobs.Get)

	return result, outToken, nil
}

// CancelAnnotationImportJob cancels an annotation import job.
func (b *InMemoryBackend) CancelAnnotationImportJob(jobID string) error {
	b.mu.Lock("CancelAnnotationImportJob")
	defer b.mu.Unlock()

	job, ok := b.annotationImportJobs.Get(jobID)
	if !ok {
		return fmt.Errorf("%w: annotation import job %s not found", ErrNotFound, jobID)
	}

	job.Status = statusCancelled

	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// AnnotationStoreVersion
// ────────────────────────────────────────────────────────────────────────────

// CreateAnnotationStoreVersion creates a version of an annotation store.
func (b *InMemoryBackend) CreateAnnotationStoreVersion(
	name, versionName, description string,
	tags map[string]string,
) (*AnnotationStoreVersion, error) {
	b.mu.Lock("CreateAnnotationStoreVersion")
	defer b.mu.Unlock()

	as, ok := b.annotationStores.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	if b.annotationVersions.Has(parentKey(name, versionName)) {
		return nil, fmt.Errorf(
			"%w: annotation store version %s already exists",
			ErrAlreadyExists,
			versionName,
		)
	}

	now := time.Now().UTC()
	v := &AnnotationStoreVersion{
		StoreID:      as.ID,
		StoreName:    name,
		VersionName:  versionName,
		Description:  description,
		Status:       statusActive,
		Tags:         copyTags(tags),
		CreationTime: now,
		UpdateTime:   now,
	}
	v.Arn = arn.Build(
		"omics",
		b.defaultRegion,
		b.accountID,
		fmt.Sprintf("annotationStore/%s/version/%s", name, versionName),
	)
	b.annotationVersions.Put(v)

	if tags != nil {
		b.tags[v.Arn] = copyTags(tags)
	}

	result := *v

	return &result, nil
}

// DeleteAnnotationStoreVersions deletes one or more annotation store versions.
func (b *InMemoryBackend) DeleteAnnotationStoreVersions(
	name string,
	versionNames []string,
) ([]VersionDeleteError, error) {
	b.mu.Lock("DeleteAnnotationStoreVersions")
	defer b.mu.Unlock()

	if !b.annotationStores.Has(name) {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	var errs []VersionDeleteError

	for _, vn := range versionNames {
		v, ok := b.annotationVersions.Get(parentKey(name, vn))
		if !ok {
			errs = append(errs, VersionDeleteError{
				VersionName: vn,
				Code:        errResourceNotFound,
				Message:     fmt.Sprintf("version %s not found", vn),
			})

			continue
		}

		delete(b.tags, v.Arn)
		b.annotationVersions.Delete(parentKey(name, vn))
	}

	return errs, nil
}

// GetAnnotationStoreVersion retrieves an annotation store version.
func (b *InMemoryBackend) GetAnnotationStoreVersion(
	name, versionName string,
) (*AnnotationStoreVersion, error) {
	b.mu.RLock("GetAnnotationStoreVersion")
	defer b.mu.RUnlock()

	if !b.annotationStores.Has(name) {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	v, ok := b.annotationVersions.Get(parentKey(name, versionName))
	if !ok {
		return nil, fmt.Errorf(
			"%w: annotation store version %s not found",
			ErrNotFound,
			versionName,
		)
	}

	result := *v

	return &result, nil
}

// ListAnnotationStoreVersions lists versions of an annotation store.
func (b *InMemoryBackend) ListAnnotationStoreVersions(
	name string,
	maxResults int,
	nextToken string,
) ([]*AnnotationStoreVersion, string, error) {
	b.mu.RLock("ListAnnotationStoreVersions")
	defer b.mu.RUnlock()

	if !b.annotationStores.Has(name) {
		return nil, "", fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	group := b.annotationVersionsByStore.Get(name)
	names := make([]string, 0, len(group))

	for _, v := range group {
		names = append(names, v.VersionName)
	}

	result, outToken := paginatedCopies(names, nextToken, maxResults, func(id string) (*AnnotationStoreVersion, bool) {
		return b.annotationVersions.Get(parentKey(name, id))
	})

	return result, outToken, nil
}

// UpdateAnnotationStoreVersion updates an annotation store version.
func (b *InMemoryBackend) UpdateAnnotationStoreVersion(
	name, versionName, description string,
) (*AnnotationStoreVersion, error) {
	b.mu.Lock("UpdateAnnotationStoreVersion")
	defer b.mu.Unlock()

	if !b.annotationStores.Has(name) {
		return nil, fmt.Errorf("%w: annotation store %s not found", ErrNotFound, name)
	}

	v, ok := b.annotationVersions.Get(parentKey(name, versionName))
	if !ok {
		return nil, fmt.Errorf(
			"%w: annotation store version %s not found",
			ErrNotFound,
			versionName,
		)
	}

	if description != "" {
		v.Description = description
	}

	v.UpdateTime = time.Now().UTC()
	result := *v

	return &result, nil
}
