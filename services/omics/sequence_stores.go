package omics

import (
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ────────────────────────────────────────────────────────────────────────────
// SequenceStore
// ────────────────────────────────────────────────────────────────────────────

// sequenceStoreDefaultETagAlgorithm is the ETag algorithm family CreateSequenceStore
// applies when the caller omits eTagAlgorithmFamily (CreateSequenceStoreRequest
// documentation, botocore omics service-2.json).
const sequenceStoreDefaultETagAlgorithm = "MD5up"

// CreateSequenceStore creates a new sequence store.
func (b *InMemoryBackend) CreateSequenceStore(in CreateSequenceStoreInput) (*SequenceStore, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("CreateSequenceStore")
	defer b.mu.Unlock()

	eTagAlgorithmFamily := in.ETagAlgorithmFamily
	if eTagAlgorithmFamily == "" {
		eTagAlgorithmFamily = sequenceStoreDefaultETagAlgorithm
	}

	var s3Access map[string]any
	if in.AccessLogLocation != "" {
		s3Access = map[string]any{"accessLogLocation": in.AccessLogLocation}
	}

	now := time.Now().UTC()
	ss := &SequenceStore{
		ID:                     newID(),
		Name:                   in.Name,
		Description:            in.Description,
		Status:                 statusActive,
		Tags:                   copyTags(in.Tags),
		CreationTime:           now,
		UpdateTime:             now,
		ETagAlgorithm:          eTagAlgorithmFamily,
		S3Access:               s3Access,
		SseConfig:              in.SseConfig,
		FallbackLocation:       in.FallbackLocation,
		PropagatedSetLevelTags: slices.Clone(in.PropagatedSetLevelTags),
	}
	ss.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "sequenceStore/"+ss.ID)

	b.sequenceStores.Put(ss)
	b.uploadParts[ss.ID] = make(map[string][]*ReadSetUploadPart)
	b.uploadPartData[ss.ID] = make(map[string]map[string]map[int][]byte)
	b.readSetBytes[ss.ID] = make(map[string][]byte)

	if in.Tags != nil {
		b.tags[ss.Arn] = copyTags(in.Tags)
	}

	result := *ss

	return &result, nil
}

// DeleteSequenceStore deletes a sequence store by ID. Real AWS only permits
// this when the store contains no read sets
// (api_op_DeleteSequenceStore.go: "You can only delete a sequence store when
// it does not contain any read sets. Use the BatchDeleteReadSet API
// operation to ensure that all read sets in the sequence store are
// deleted.").
func (b *InMemoryBackend) DeleteSequenceStore(id string) error {
	b.mu.Lock("DeleteSequenceStore")
	defer b.mu.Unlock()

	ss, ok := b.sequenceStores.Get(id)
	if !ok {
		return fmt.Errorf("%w: sequence store %s not found", ErrNotFound, id)
	}

	if readSets := b.readSetsByStore.Get(id); len(readSets) > 0 {
		return fmt.Errorf(
			"%w: sequence store %s still contains %d read set(s), delete them first",
			ErrInvalidState, id, len(readSets),
		)
	}

	delete(b.tags, ss.Arn)
	b.sequenceStores.Delete(id)

	for _, rs := range slices.Clone(b.readSetsByStore.Get(id)) {
		b.readSets.Delete(parentKey(id, rs.ID))
	}

	for _, j := range slices.Clone(b.readSetActivationJobsByStore.Get(id)) {
		b.readSetActivationJobs.Delete(parentKey(id, j.ID))
	}

	for _, j := range slices.Clone(b.readSetExportJobsByStore.Get(id)) {
		b.readSetExportJobs.Delete(parentKey(id, j.ID))
	}

	for _, j := range slices.Clone(b.readSetImportJobsByStore.Get(id)) {
		b.readSetImportJobs.Delete(parentKey(id, j.ID))
	}

	for _, u := range slices.Clone(b.multipartUploadsByStore.Get(id)) {
		b.multipartUploads.Delete(parentKey(id, u.UploadID))
	}

	delete(b.uploadParts, id)
	delete(b.uploadPartData, id)
	delete(b.readSetBytes, id)

	return nil
}

// GetSequenceStore retrieves a sequence store by ID.
func (b *InMemoryBackend) GetSequenceStore(id string) (*SequenceStore, error) {
	b.mu.RLock("GetSequenceStore")
	defer b.mu.RUnlock()

	ss, ok := b.sequenceStores.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, id)
	}

	result := *ss

	return &result, nil
}

// ListSequenceStores lists sequence stores.
func (b *InMemoryBackend) ListSequenceStores(
	filter *SequenceStoreFilter,
	maxResults int,
	nextToken string,
) ([]*SequenceStore, string, error) {
	b.mu.RLock("ListSequenceStores")
	defer b.mu.RUnlock()

	all := b.sequenceStores.All()
	ids := make([]string, 0, len(all))

	for _, ss := range all {
		if !filter.matches(ss) {
			continue
		}

		ids = append(ids, ss.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.sequenceStores.Get)

	return result, outToken, nil
}

// UpdateSequenceStore updates a sequence store's name and description.
func (b *InMemoryBackend) UpdateSequenceStore(id string, in UpdateSequenceStoreInput) (*SequenceStore, error) {
	b.mu.Lock("UpdateSequenceStore")
	defer b.mu.Unlock()

	ss, ok := b.sequenceStores.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: sequence store %s not found", ErrNotFound, id)
	}

	if in.Name != "" {
		ss.Name = in.Name
	}

	if in.Description != "" {
		ss.Description = in.Description
	}

	if in.FallbackLocation != nil {
		ss.FallbackLocation = *in.FallbackLocation
	}

	if in.PropagatedSetLevelTags != nil {
		ss.PropagatedSetLevelTags = slices.Clone(in.PropagatedSetLevelTags)
	}

	if in.AccessLogLocation != nil {
		ss.S3Access = map[string]any{"accessLogLocation": *in.AccessLogLocation}
	}

	ss.UpdateTime = time.Now().UTC()
	result := *ss

	return &result, nil
}
