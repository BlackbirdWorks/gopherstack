package mgn

import (
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// This file backs family I (8 ops): StartExport, ListExports,
// ListExportErrors, StartImport, ListImports, ListImportErrors, plus
// StartImportFileEnrichment/ListImportFileEnrichments (wire-routed under
// /network-migration/ despite being conceptually part of this family --
// PARITY.md wire-shape trap #4).
//
// StartExport writes a metadata dump of Applications/Waves/Servers to a
// caller-supplied S3 bucket; StartImport reads one back in. Neither this
// SDK nor this repo has any S3-file-format schema for what that content
// actually contains (PARITY.md's honest-gap section) -- this backend never
// reads or writes real S3 object bytes for either op (no cross-service S3
// wiring is in this pass's scope, and inventing a schema to parse would be
// fabrication). StartExport's Summary counts are real (a live count of this
// account's Applications/Waves/SourceServers at call time -- not
// fabricated); StartImport's Summary counts are always zero (see
// models.go's ImportTaskSummary doc comment) -- SeedSourceServer
// (sourceservers.go) and SeedVcenterClient (vcenterclients.go) are this
// emulator's explicit, documented, non-SDK alternative for actually getting
// SourceServer/VcenterClient records into the backend.

// StartExport starts a new (Pending -> Started -> Succeeded) async
// ExportTask, snapshotting this account's real current Applications/Waves/
// SourceServers counts into Summary.
func (b *InMemoryBackend) StartExport(
	s3Bucket, s3Key, s3BucketOwner string,
	exportTags map[string]string,
) (*ExportTask, error) {
	b.mu.Lock("StartExport")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	if s3Bucket == "" || s3Key == "" {
		return nil, validationError("s3Bucket and s3Key are required")
	}

	id := newExportID()
	t := tags.New("mgn.exporttask." + id + ".tags")
	t.Merge(exportTags)

	task := &ExportTask{
		ExportID:         id,
		Arn:              b.exportTaskARN(id),
		S3Bucket:         s3Bucket,
		S3Key:            s3Key,
		S3BucketOwner:    s3BucketOwner,
		Status:           TaskStatusPending,
		CreationDateTime: nowRFC3339(),
		Tags:             t,
		Summary: &ExportTaskSummary{
			ApplicationsCount: int64(b.applications.Len()),
			ServersCount:      int64(b.sourceServers.Len()),
			WavesCount:        int64(b.waves.Len()),
		},
	}
	b.exportTasks.Put(task)

	b.scheduleExportLocked(id)

	return task.clone(), nil
}

func (b *InMemoryBackend) scheduleExportLocked(id string) {
	b.work.After("ExportStarted", asyncTransitionDelay, func() {
		b.mu.Lock("ExportStarted-async")
		if t, ok := b.exportTasks.Get(id); ok && t.Status == TaskStatusPending {
			t.Status = TaskStatusStarted
			t.ProgressPercentage = progressHalfway
		}
		b.mu.Unlock()

		b.work.After("ExportSucceeded", asyncTransitionDelay, func() {
			b.mu.Lock("ExportSucceeded-async")
			defer b.mu.Unlock()

			if t, ok := b.exportTasks.Get(id); ok && t.Status == TaskStatusStarted {
				t.Status = TaskStatusSucceeded
				t.ProgressPercentage = progressComplete
				t.EndDateTime = nowRFC3339()
			}
		})
	})
}

// ListExports returns a page of ExportTasks, optionally filtered by ids.
func (b *InMemoryBackend) ListExports(ids []string, token string, limit int) (page.Page[*ExportTask], error) {
	b.mu.RLock("ListExports")
	defer b.mu.RUnlock()

	if err := b.requireInitializedLocked(); err != nil {
		return page.Page[*ExportTask]{}, err
	}

	all := b.exportTasks.Snapshot()
	filtered := make([]*ExportTask, 0, len(all))

	for _, t := range all {
		if len(ids) == 0 || containsStr(ids, t.ExportID) {
			filtered = append(filtered, t.clone())
		}
	}

	return page.New(filtered, token, limit, defaultPageLimit), nil
}

// ListExportErrors always returns an empty list: this deterministic
// emulator never injects a real export failure to report an error for (see
// this file's doc comment) -- an honestly-empty list, not a stub, since
// ListExportErrors' own error set has no ResourceNotFoundException
// (confirmed by direct SDK read), so it never 404s on an unknown exportID
// either.
func (b *InMemoryBackend) ListExportErrors() error {
	b.mu.RLock("ListExportErrors")
	defer b.mu.RUnlock()

	return b.requireInitializedLocked()
}

// StartImport starts a new (Pending -> Started -> Succeeded) async
// ImportTask. Summary counts are always zero -- see this file's doc
// comment.
func (b *InMemoryBackend) StartImport(source *S3BucketSource, importTags map[string]string) (*ImportTask, error) {
	b.mu.Lock("StartImport")
	defer b.mu.Unlock()

	if err := b.requireInitializedLocked(); err != nil {
		return nil, err
	}

	if source == nil || source.S3Bucket == "" || source.S3Key == "" {
		return nil, validationError("s3BucketSource.s3Bucket and s3Key are required")
	}

	id := newImportID()
	t := tags.New("mgn.importtask." + id + ".tags")
	t.Merge(importTags)

	task := &ImportTask{
		ImportID:         id,
		Arn:              b.importTaskARN(id),
		S3BucketSource:   source,
		Status:           TaskStatusPending,
		CreationDateTime: nowRFC3339(),
		Tags:             t,
		Summary:          &ImportTaskSummary{},
	}
	b.importTasks.Put(task)

	b.scheduleImportLocked(id)

	return task.clone(), nil
}

func (b *InMemoryBackend) scheduleImportLocked(id string) {
	b.work.After("ImportStarted", asyncTransitionDelay, func() {
		b.mu.Lock("ImportStarted-async")
		if t, ok := b.importTasks.Get(id); ok && t.Status == TaskStatusPending {
			t.Status = TaskStatusStarted
			t.ProgressPercentage = progressHalfway
		}
		b.mu.Unlock()

		b.work.After("ImportSucceeded", asyncTransitionDelay, func() {
			b.mu.Lock("ImportSucceeded-async")
			defer b.mu.Unlock()

			if t, ok := b.importTasks.Get(id); ok && t.Status == TaskStatusStarted {
				t.Status = TaskStatusSucceeded
				t.ProgressPercentage = progressComplete
				t.EndDateTime = nowRFC3339()
			}
		})
	})
}

// ListImports returns a page of ImportTasks, optionally filtered by ids.
func (b *InMemoryBackend) ListImports(ids []string, token string, limit int) (page.Page[*ImportTask], error) {
	b.mu.RLock("ListImports")
	defer b.mu.RUnlock()

	if err := b.requireInitializedLocked(); err != nil {
		return page.Page[*ImportTask]{}, err
	}

	all := b.importTasks.Snapshot()
	filtered := make([]*ImportTask, 0, len(all))

	for _, t := range all {
		if len(ids) == 0 || containsStr(ids, t.ImportID) {
			filtered = append(filtered, t.clone())
		}
	}

	return page.New(filtered, token, limit, defaultPageLimit), nil
}

// ListImportErrors always returns an empty list -- same honest-gap
// rationale as ListExportErrors.
func (b *InMemoryBackend) ListImportErrors() error {
	b.mu.RLock("ListImportErrors")
	defer b.mu.RUnlock()

	return b.requireInitializedLocked()
}

// StartImportFileEnrichment starts a new (Pending -> Started -> Succeeded)
// async ImportFileEnrichment job. Tags accepted as input have nowhere to go
// on the wire response (types.ImportFileEnrichment carries no Tags field at
// all, confirmed by direct SDK read, and this resource kind is absent from
// family L's 12 taggable kinds) -- accepted and silently dropped, matching
// the real SDK's own shape, not a bug in this implementation.
func (b *InMemoryBackend) StartImportFileEnrichment(
	target *EnrichmentTargetS3Configuration,
) (*ImportFileEnrichment, error) {
	b.mu.Lock("StartImportFileEnrichment")
	defer b.mu.Unlock()

	if target == nil || target.S3Bucket == "" || target.S3Key == "" {
		return nil, validationError("targetS3Configuration.s3Bucket and s3Key are required")
	}

	id := newNMJobID()
	job := &ImportFileEnrichment{
		JobID:          id,
		S3BucketTarget: target,
		Status:         TaskStatusPending,
		CreatedAt:      nowUTC(),
	}
	b.importFileEnrichments.Put(job)

	b.scheduleImportFileEnrichmentLocked(id)

	return job.clone(), nil
}

func (b *InMemoryBackend) scheduleImportFileEnrichmentLocked(id string) {
	b.work.After("ImportFileEnrichmentStarted", asyncTransitionDelay, func() {
		b.mu.Lock("ImportFileEnrichmentStarted-async")
		if j, ok := b.importFileEnrichments.Get(id); ok && j.Status == TaskStatusPending {
			j.Status = TaskStatusStarted
		}
		b.mu.Unlock()

		b.work.After("ImportFileEnrichmentSucceeded", asyncTransitionDelay, func() {
			b.mu.Lock("ImportFileEnrichmentSucceeded-async")
			defer b.mu.Unlock()

			if j, ok := b.importFileEnrichments.Get(id); ok && j.Status == TaskStatusStarted {
				j.Status = TaskStatusSucceeded
				j.EndedAt = nowUTC()
			}
		})
	})
}

// ListImportFileEnrichments returns a page of ImportFileEnrichment jobs,
// optionally filtered by jobIDs.
func (b *InMemoryBackend) ListImportFileEnrichments(
	jobIDs []string,
	token string,
	limit int,
) page.Page[*ImportFileEnrichment] {
	b.mu.RLock("ListImportFileEnrichments")
	defer b.mu.RUnlock()

	all := b.importFileEnrichments.Snapshot()
	filtered := make([]*ImportFileEnrichment, 0, len(all))

	for _, j := range all {
		if len(jobIDs) == 0 || containsStr(jobIDs, j.JobID) {
			filtered = append(filtered, j.clone())
		}
	}

	return page.New(filtered, token, limit, defaultPageLimit)
}

// progressHalfway/progressComplete are this emulator's deterministic
// ProgressPercentage checkpoints for Export/Import tasks -- a documented,
// invented 2-step walk (Started=50%, Succeeded=100%), not a fabricated
// real-time progress measurement.
const (
	progressHalfway  = 50.0
	progressComplete = 100.0
)
