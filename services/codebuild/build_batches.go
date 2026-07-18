package codebuild

import (
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) buildBatchARN(projectName, batchID string) string {
	return arn.Build("codebuild", b.region, b.accountID, "build-batch/"+projectName+":"+batchID)
}

// AddBuildBatchInternal seeds a BuildBatch directly into the backend (test helper).
func (b *InMemoryBackend) AddBuildBatchInternal(bb *BuildBatch) {
	b.mu.Lock("AddBuildBatchInternal")
	defer b.mu.Unlock()

	b.buildBatches.Put(bb)
}

// BatchGetBuildBatches returns build batches by ID. Missing IDs are returned separately.
func (b *InMemoryBackend) BatchGetBuildBatches(ids []string) ([]*BuildBatch, []string) {
	b.mu.RLock("BatchGetBuildBatches")
	defer b.mu.RUnlock()

	found := make([]*BuildBatch, 0, len(ids))
	notFound := make([]string, 0, len(ids))

	for _, id := range ids {
		if bb, ok := b.buildBatches.Get(id); ok {
			out := *bb
			found = append(found, &out)
		} else {
			notFound = append(notFound, id)
		}
	}

	return found, notFound
}

// ListBuildBatches returns all build batch IDs in sorted order.
func (b *InMemoryBackend) ListBuildBatches() []string {
	b.mu.RLock("ListBuildBatches")
	defer b.mu.RUnlock()

	items := b.buildBatches.Snapshot()
	ids := make([]string, len(items))

	for i, bb := range items {
		ids[i] = bb.ID
	}

	return ids
}

// StartBuildBatch creates a new build batch for a project.
func (b *InMemoryBackend) StartBuildBatch(projectName string) (*BuildBatch, error) {
	b.mu.Lock("StartBuildBatch")
	defer b.mu.Unlock()

	if !b.projects.Has(projectName) {
		return nil, ErrNotFound
	}

	batchID := uuid.NewString()
	id := projectName + ":" + batchID
	bb := &BuildBatch{
		ID:               id,
		Arn:              b.buildBatchARN(projectName, batchID),
		ProjectName:      projectName,
		BuildBatchStatus: buildStatusInProgress,
		StartTime:        float64(time.Now().Unix()),
	}
	b.buildBatches.Put(bb)

	out := *bb

	return &out, nil
}

// DeleteBuildBatch removes a build batch by ID.
func (b *InMemoryBackend) DeleteBuildBatch(id string) error {
	b.mu.Lock("DeleteBuildBatch")
	defer b.mu.Unlock()

	if !b.buildBatches.Delete(id) {
		return ErrNotFound
	}

	return nil
}

// RetryBuildBatch creates a new build batch with the same project as an existing one.
func (b *InMemoryBackend) RetryBuildBatch(id string) (*BuildBatch, error) {
	b.mu.Lock("RetryBuildBatch")
	defer b.mu.Unlock()

	existing, ok := b.buildBatches.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	projectName := existing.ProjectName
	batchID := uuid.NewString()
	newID := projectName + ":" + batchID
	bb := &BuildBatch{
		ID:               newID,
		Arn:              b.buildBatchARN(projectName, batchID),
		ProjectName:      projectName,
		BuildBatchStatus: buildStatusInProgress,
		StartTime:        float64(time.Now().Unix()),
	}
	b.buildBatches.Put(bb)

	out := *bb

	return &out, nil
}

// StopBuildBatch marks a build batch as STOPPED.
func (b *InMemoryBackend) StopBuildBatch(id string) (*BuildBatch, error) {
	b.mu.Lock("StopBuildBatch")
	defer b.mu.Unlock()

	bb, ok := b.buildBatches.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	bb.BuildBatchStatus = buildStatusStopped
	bb.EndTime = float64(time.Now().Unix())
	out := *bb

	return &out, nil
}

// ListBuildBatchesForProject returns all batch IDs for a project in sorted order.
func (b *InMemoryBackend) ListBuildBatchesForProject(projectName string) ([]string, error) {
	b.mu.RLock("ListBuildBatchesForProject")
	defer b.mu.RUnlock()

	if !b.projects.Has(projectName) {
		return nil, ErrNotFound
	}

	group := b.buildBatchesByProject.Get(projectName)
	ids := make([]string, len(group))

	for i, bb := range group {
		ids[i] = bb.ID
	}

	sort.Strings(ids)

	return ids, nil
}
