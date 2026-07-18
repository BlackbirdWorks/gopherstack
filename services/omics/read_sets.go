package omics

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ────────────────────────────────────────────────────────────────────────────
// ReadSet
// ────────────────────────────────────────────────────────────────────────────

// BatchDeleteReadSet deletes multiple read sets.
func (b *InMemoryBackend) BatchDeleteReadSet(
	sequenceStoreID string,
	ids []string,
) ([]ReadSetBatchError, error) {
	b.mu.Lock("BatchDeleteReadSet")
	defer b.mu.Unlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	var errs []ReadSetBatchError

	for _, id := range ids {
		rs, ok := b.readSets.Get(parentKey(sequenceStoreID, id))
		if !ok {
			errs = append(errs, ReadSetBatchError{
				ID:      id,
				Code:    errResourceNotFound,
				Message: fmt.Sprintf("read set %s not found", id),
			})

			continue
		}

		delete(b.tags, rs.Arn)
		b.readSets.Delete(parentKey(sequenceStoreID, id))
	}

	return errs, nil
}

// GetReadSetMetadata retrieves read set metadata.
func (b *InMemoryBackend) GetReadSetMetadata(sequenceStoreID, id string) (*ReadSetMetadata, error) {
	b.mu.RLock("GetReadSetMetadata")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	rs, ok := b.readSets.Get(parentKey(sequenceStoreID, id))
	if !ok {
		return nil, fmt.Errorf("%w: read set %s not found", ErrNotFound, id)
	}

	result := *rs

	return &result, nil
}

// ListReadSets lists read sets in a sequence store.
func (b *InMemoryBackend) ListReadSets(
	sequenceStoreID string,
	filter *ReadSetFilter,
	maxResults int,
	nextToken string,
) ([]*ReadSetMetadata, string, error) {
	b.mu.RLock("ListReadSets")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	group := b.readSetsByStore.Get(sequenceStoreID)
	ids := make([]string, 0, len(group))

	for _, rs := range group {
		if filter != nil {
			if filter.Name != "" && rs.Name != filter.Name {
				continue
			}

			if filter.Status != "" && rs.Status != filter.Status {
				continue
			}
		}

		ids = append(ids, rs.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, func(id string) (*ReadSetMetadata, bool) {
		return b.readSets.Get(parentKey(sequenceStoreID, id))
	})

	return result, outToken, nil
}

// StartReadSetActivationJob creates a read set activation job.
func (b *InMemoryBackend) StartReadSetActivationJob(
	sequenceStoreID string,
	sources []ReadSetActivationJobSource,
) (*ReadSetActivationJob, error) {
	b.mu.Lock("StartReadSetActivationJob")
	defer b.mu.Unlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	job := &ReadSetActivationJob{
		ID:              newID(),
		SequenceStoreID: sequenceStoreID,
		Sources:         sources,
		Status:          statusCompleted,
		CreationTime:    time.Now().UTC(),
	}
	now := time.Now().UTC()
	job.CompletionTime = &now
	b.readSetActivationJobs.Put(job)

	result := *job

	return &result, nil
}

// GetReadSetActivationJob retrieves a read set activation job.
func (b *InMemoryBackend) GetReadSetActivationJob(
	sequenceStoreID, jobID string,
) (*ReadSetActivationJob, error) {
	b.mu.RLock("GetReadSetActivationJob")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	job, ok := b.readSetActivationJobs.Get(parentKey(sequenceStoreID, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: activation job %s not found", ErrNotFound, jobID)
	}

	result := *job

	return &result, nil
}

// ListReadSetActivationJobs lists read set activation jobs.
func (b *InMemoryBackend) ListReadSetActivationJobs(
	sequenceStoreID string,
	maxResults int,
	nextToken string,
) ([]*ReadSetActivationJob, string, error) {
	b.mu.RLock("ListReadSetActivationJobs")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	group := b.readSetActivationJobsByStore.Get(sequenceStoreID)
	ids := make([]string, 0, len(group))

	for _, j := range group {
		ids = append(ids, j.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, func(id string) (*ReadSetActivationJob, bool) {
		return b.readSetActivationJobs.Get(parentKey(sequenceStoreID, id))
	})

	return result, outToken, nil
}

// StartReadSetExportJob creates a read set export job.
func (b *InMemoryBackend) StartReadSetExportJob(
	sequenceStoreID, destination string,
	sources []ReadSetExportJobSource,
) (*ReadSetExportJob, error) {
	b.mu.Lock("StartReadSetExportJob")
	defer b.mu.Unlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	job := &ReadSetExportJob{
		ID:              newID(),
		SequenceStoreID: sequenceStoreID,
		Destination:     destination,
		Sources:         sources,
		Status:          statusCompleted,
		CreationTime:    time.Now().UTC(),
	}
	now := time.Now().UTC()
	job.CompletionTime = &now
	b.readSetExportJobs.Put(job)

	result := *job

	return &result, nil
}

// GetReadSetExportJob retrieves a read set export job.
func (b *InMemoryBackend) GetReadSetExportJob(
	sequenceStoreID, jobID string,
) (*ReadSetExportJob, error) {
	b.mu.RLock("GetReadSetExportJob")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	job, ok := b.readSetExportJobs.Get(parentKey(sequenceStoreID, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: export job %s not found", ErrNotFound, jobID)
	}

	result := *job

	return &result, nil
}

// ListReadSetExportJobs lists read set export jobs.
func (b *InMemoryBackend) ListReadSetExportJobs(
	sequenceStoreID string,
	maxResults int,
	nextToken string,
) ([]*ReadSetExportJob, string, error) {
	b.mu.RLock("ListReadSetExportJobs")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	group := b.readSetExportJobsByStore.Get(sequenceStoreID)
	ids := make([]string, 0, len(group))

	for _, j := range group {
		ids = append(ids, j.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, func(id string) (*ReadSetExportJob, bool) {
		return b.readSetExportJobs.Get(parentKey(sequenceStoreID, id))
	})

	return result, outToken, nil
}

// StartReadSetImportJob creates a read set import job.
func (b *InMemoryBackend) StartReadSetImportJob(
	sequenceStoreID, roleARN string,
	sources []ReadSetImportJobSource,
) (*ReadSetImportJob, error) {
	b.mu.Lock("StartReadSetImportJob")
	defer b.mu.Unlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	job := &ReadSetImportJob{
		ID:              newID(),
		SequenceStoreID: sequenceStoreID,
		RoleARN:         roleARN,
		Sources:         sources,
		Status:          statusCompleted,
		CreationTime:    time.Now().UTC(),
	}
	now := time.Now().UTC()
	job.CompletionTime = &now

	// Create read set entries for each source
	for _, src := range sources {
		rsID := newID()
		rs := &ReadSetMetadata{
			ID:              rsID,
			SequenceStoreID: sequenceStoreID,
			Arn: arn.Build(
				"omics",
				b.defaultRegion,
				b.accountID,
				fmt.Sprintf("sequenceStore/%s/readSet/%s", sequenceStoreID, rsID),
			),
			Name:         src.Name,
			Description:  src.Description,
			SequenceType: src.SourceFileType,
			SubjectID:    src.SubjectID,
			SampleID:     src.SampleID,
			ReferenceARN: src.ReferenceARN,
			Status:       statusActive,
			CreationTime: time.Now().UTC(),
		}
		b.readSets.Put(rs)
	}

	b.readSetImportJobs.Put(job)

	result := *job

	return &result, nil
}

// GetReadSetImportJob retrieves a read set import job.
func (b *InMemoryBackend) GetReadSetImportJob(
	sequenceStoreID, jobID string,
) (*ReadSetImportJob, error) {
	b.mu.RLock("GetReadSetImportJob")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	job, ok := b.readSetImportJobs.Get(parentKey(sequenceStoreID, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: import job %s not found", ErrNotFound, jobID)
	}

	result := *job

	return &result, nil
}

// ListReadSetImportJobs lists read set import jobs.
func (b *InMemoryBackend) ListReadSetImportJobs(
	sequenceStoreID string,
	maxResults int,
	nextToken string,
) ([]*ReadSetImportJob, string, error) {
	b.mu.RLock("ListReadSetImportJobs")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	group := b.readSetImportJobsByStore.Get(sequenceStoreID)
	ids := make([]string, 0, len(group))

	for _, j := range group {
		ids = append(ids, j.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, func(id string) (*ReadSetImportJob, bool) {
		return b.readSetImportJobs.Get(parentKey(sequenceStoreID, id))
	})

	return result, outToken, nil
}

// ────────────────────────────────────────────────────────────────────────────
// Multipart Upload
// ────────────────────────────────────────────────────────────────────────────

// CreateMultipartReadSetUpload creates a multipart read set upload.
func (b *InMemoryBackend) CreateMultipartReadSetUpload(
	sequenceStoreID, name, sequenceType string,
	tags map[string]string,
) (*MultipartReadSetUpload, error) {
	b.mu.Lock("CreateMultipartReadSetUpload")
	defer b.mu.Unlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	upload := &MultipartReadSetUpload{
		UploadID:        newID(),
		SequenceStoreID: sequenceStoreID,
		Name:            name,
		SequenceType:    sequenceType,
		Tags:            copyTags(tags),
		Status:          "IN_PROGRESS",
		CreationTime:    time.Now().UTC(),
	}
	b.multipartUploads.Put(upload)
	b.uploadParts[sequenceStoreID][upload.UploadID] = nil
	b.uploadPartData[sequenceStoreID][upload.UploadID] = make(map[string]map[int][]byte)

	result := *upload

	return &result, nil
}

// AbortMultipartReadSetUpload aborts a multipart read set upload.
func (b *InMemoryBackend) AbortMultipartReadSetUpload(sequenceStoreID, uploadID string) error {
	b.mu.Lock("AbortMultipartReadSetUpload")
	defer b.mu.Unlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	if !b.multipartUploads.Has(parentKey(sequenceStoreID, uploadID)) {
		return fmt.Errorf("%w: upload %s not found", ErrNotFound, uploadID)
	}

	b.multipartUploads.Delete(parentKey(sequenceStoreID, uploadID))
	delete(b.uploadParts[sequenceStoreID], uploadID)
	delete(b.uploadPartData[sequenceStoreID], uploadID)

	return nil
}

// CompleteMultipartReadSetUpload completes a multipart read set upload.
func (b *InMemoryBackend) CompleteMultipartReadSetUpload(
	sequenceStoreID, uploadID string,
) (*ReadSetMetadata, error) {
	b.mu.Lock("CompleteMultipartReadSetUpload")
	defer b.mu.Unlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	upload, ok := b.multipartUploads.Get(parentKey(sequenceStoreID, uploadID))
	if !ok {
		return nil, fmt.Errorf("%w: upload %s not found", ErrNotFound, uploadID)
	}

	rsID := newID()
	rs := &ReadSetMetadata{
		ID:              rsID,
		SequenceStoreID: sequenceStoreID,
		Arn: arn.Build(
			"omics",
			b.defaultRegion,
			b.accountID,
			fmt.Sprintf("sequenceStore/%s/readSet/%s", sequenceStoreID, rsID),
		),
		Name:         upload.Name,
		SequenceType: upload.SequenceType,
		Status:       statusActive,
		CreationTime: time.Now().UTC(),
		Tags:         maps.Clone(upload.Tags),
	}
	b.readSets.Put(rs)

	// Concatenate stored part bytes (SOURCE1 then SOURCE2, in part-number order) as the read set body.
	partData := b.uploadPartData[sequenceStoreID][uploadID]
	var combined []byte
	for _, src := range []string{"SOURCE1", "SOURCE2"} {
		srcParts := partData[src]
		partNums := make([]int, 0, len(srcParts))
		for n := range srcParts {
			partNums = append(partNums, n)
		}
		sort.Ints(partNums)
		for _, n := range partNums {
			combined = append(combined, srcParts[n]...)
		}
	}
	b.readSetBytes[sequenceStoreID][rsID] = combined

	b.multipartUploads.Delete(parentKey(sequenceStoreID, uploadID))
	delete(b.uploadParts[sequenceStoreID], uploadID)
	delete(b.uploadPartData[sequenceStoreID], uploadID)

	result := *rs

	return &result, nil
}

// ListMultipartReadSetUploads lists in-progress multipart uploads.
func (b *InMemoryBackend) ListMultipartReadSetUploads(
	sequenceStoreID string,
	maxResults int,
	nextToken string,
) ([]*MultipartReadSetUpload, string, error) {
	b.mu.RLock("ListMultipartReadSetUploads")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	group := b.multipartUploadsByStore.Get(sequenceStoreID)
	ids := make([]string, 0, len(group))

	for _, u := range group {
		ids = append(ids, u.UploadID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, func(id string) (*MultipartReadSetUpload, bool) {
		return b.multipartUploads.Get(parentKey(sequenceStoreID, id))
	})

	return result, outToken, nil
}

// ListReadSetUploadParts lists parts for a multipart read set upload.
func (b *InMemoryBackend) ListReadSetUploadParts(
	sequenceStoreID, uploadID string,
	maxResults int,
	nextToken string,
) ([]*ReadSetUploadPart, string, error) {
	b.mu.RLock("ListReadSetUploadParts")
	defer b.mu.RUnlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return nil, "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	if !b.multipartUploads.Has(parentKey(sequenceStoreID, uploadID)) {
		return nil, "", fmt.Errorf("%w: upload %s not found", ErrNotFound, uploadID)
	}

	parts := b.uploadParts[sequenceStoreID][uploadID]

	if maxResults <= 0 || maxResults > maxPageSize {
		maxResults = maxPageSize
	}

	start := 0

	if nextToken != "" {
		for i, p := range parts {
			if strconv.Itoa(p.PartNumber) == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+maxResults, len(parts))
	page := parts[start:end]

	var outToken string
	if end < len(parts) {
		outToken = strconv.Itoa(parts[end].PartNumber)
	}

	result := make([]*ReadSetUploadPart, len(page))

	for i, p := range page {
		cp := *p
		result[i] = &cp
	}

	return result, outToken, nil
}

// UploadReadSetPart stores binary data for a single part and returns its SHA256 checksum.
func (b *InMemoryBackend) UploadReadSetPart(
	sequenceStoreID, uploadID string,
	partNumber int,
	partSource string,
	data []byte,
) (string, error) {
	b.mu.Lock("UploadReadSetPart")
	defer b.mu.Unlock()

	if !b.sequenceStores.Has(sequenceStoreID) {
		return "", fmt.Errorf("%w: sequence store %s not found", ErrNotFound, sequenceStoreID)
	}

	if !b.multipartUploads.Has(parentKey(sequenceStoreID, uploadID)) {
		return "", fmt.Errorf("%w: upload %s not found", ErrNotFound, uploadID)
	}

	if b.uploadPartData[sequenceStoreID][uploadID] == nil {
		b.uploadPartData[sequenceStoreID][uploadID] = make(map[string]map[int][]byte)
	}
	if b.uploadPartData[sequenceStoreID][uploadID][partSource] == nil {
		b.uploadPartData[sequenceStoreID][uploadID][partSource] = make(map[int][]byte)
	}
	b.uploadPartData[sequenceStoreID][uploadID][partSource][partNumber] = data

	// Update or append the upload part metadata.
	parts := b.uploadParts[sequenceStoreID][uploadID]
	found := false
	for _, p := range parts {
		if p.PartNumber == partNumber && p.Source == partSource {
			p.PartSize = int64(len(data))
			p.LastUpdatedTime = time.Now().UTC()
			found = true

			break
		}
	}

	if !found {
		parts = append(parts, &ReadSetUploadPart{
			PartNumber:      partNumber,
			Source:          partSource,
			PartSize:        int64(len(data)),
			LastUpdatedTime: time.Now().UTC(),
		})
		b.uploadParts[sequenceStoreID][uploadID] = parts
	}

	sum := sha256.Sum256(data)

	return hex.EncodeToString(sum[:]), nil
}

// GetReadSetBytes returns the stored binary body for a read set.
func (b *InMemoryBackend) GetReadSetBytes(sequenceStoreID, id string) ([]byte, error) {
	b.mu.RLock("GetReadSetBytes")
	defer b.mu.RUnlock()

	if !b.readSets.Has(parentKey(sequenceStoreID, id)) {
		return nil, fmt.Errorf("%w: read set %s not found", ErrNotFound, id)
	}

	return b.readSetBytes[sequenceStoreID][id], nil
}
