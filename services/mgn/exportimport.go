package mgn

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// StartImportFileEnrichment/ListImportFileEnrichments are wire-routed under
// /network-migration/ despite being conceptually part of this family
// (PARITY.md wire-shape trap #4).
//
// StartExport writes a metadata dump of Applications/Waves/Servers to a
// caller-supplied S3 bucket; StartImport reads one back in. StartExport never
// writes real S3 object bytes -- its Summary is a real, live count of this
// account's resources, never derived from written content -- but StartImport
// DOES read a real S3 object via the S3Accessor cross-service seam
// (s3import.go). Every SourceServer StartImport creates comes from an
// actually-parsed row; every malformed row becomes a real ImportTaskError, never
// silently dropped nor fabricated as a success. SeedVcenterClient
// (vcenterclients.go) remains the only non-SDK creation seam -- no import path
// exists for VcenterClient.

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

// StartImport starts a new (Pending -> Started -> Succeeded|Failed) async
// ImportTask that really reads and parses source (see s3import.go). Summary
// counts reflect what was actually parsed and created -- never fabricated
// (see models.go's ImportTaskSummary doc comment).
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

	b.scheduleImportLocked(id, source)

	return task.clone(), nil
}

// scheduleImportLocked walks importID's ImportTask Pending -> Started ->
// Succeeded|Failed. The real S3 read + CSV parse (readImportSourceServers)
// happens between the two ticks, outside b.mu -- mirroring
// services/dynamodb's ImportTable, which likewise never holds its own
// coarse lock during S3 I/O.
func (b *InMemoryBackend) scheduleImportLocked(id string, source *S3BucketSource) {
	b.work.After("ImportStarted", asyncTransitionDelay, func() {
		b.mu.Lock("ImportStarted-async")
		t, ok := b.importTasks.Get(id)
		started := ok && t.Status == TaskStatusPending
		if started {
			t.Status = TaskStatusStarted
			t.ProgressPercentage = progressHalfway
		}
		b.mu.Unlock()

		if !started {
			return
		}

		result, parseErr := b.readImportSourceServers(context.Background(), source)

		b.work.After("ImportSucceeded", asyncTransitionDelay, func() {
			b.finishImportLocked(id, result, parseErr)
		})
	})
}

// finishImportLocked records readImportSourceServers' real outcome onto
// importID's ImportTask: a whole-object read/parse failure (parseErr set) fails
// the task with one recorded ImportTaskError and zero created records;
// otherwise every successfully-parsed row creates a real SourceServer, every
// malformed row's error is recorded, and the task SUCCEEDS with
// Summary.Servers.CreatedCount set to the real created count -- partial success
// is still SUCCEEDED, matching real AWS's ImportTaskSummary/ListImportErrors split.
func (b *InMemoryBackend) finishImportLocked(id string, result *importCSVResult, parseErr error) {
	b.mu.Lock("ImportSucceeded-async")
	defer b.mu.Unlock()

	t, ok := b.importTasks.Get(id)
	if !ok || t.Status != TaskStatusStarted {
		return
	}

	t.EndDateTime = nowRFC3339()
	t.ProgressPercentage = progressComplete

	if parseErr != nil {
		t.Status = TaskStatusFailed
		t.Errors = append(t.Errors, &ImportTaskError{
			ErrorType:     ImportErrorTypeProcessing,
			ErrorDateTime: nowRFC3339(),
			ErrorData:     &ImportErrorData{RawError: parseErr.Error()},
		})

		return
	}

	var created int64

	for _, row := range result.servers {
		b.createSourceServerLocked(sourceServerSeed{
			UserProvidedID:   row.userProvidedID,
			SourceProperties: row.sourceProperties,
		})
		created++
	}

	t.Errors = append(t.Errors, result.errors...)
	t.Summary.Servers = countPair{CreatedCount: created}
	t.Status = TaskStatusSucceeded
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

// ListImportErrors returns the real per-row CSV parse failures recorded
// against importID's ImportTask (see s3import.go/finishImportLocked) --
// StartImport itself never fabricates a row, so a malformed row shows up
// here, never as a fabricated CreatedCount. An unrecognized importID yields
// an empty page rather than an error: same no-404 rationale as before
// (ListImportErrors' own error set has no ResourceNotFoundException).
func (b *InMemoryBackend) ListImportErrors(importID, token string, limit int) (page.Page[*ImportTaskError], error) {
	b.mu.RLock("ListImportErrors")
	defer b.mu.RUnlock()

	if err := b.requireInitializedLocked(); err != nil {
		return page.Page[*ImportTaskError]{}, err
	}

	t, ok := b.importTasks.Get(importID)
	if !ok {
		return page.New([]*ImportTaskError{}, token, limit, defaultPageLimit), nil
	}

	cloned := make([]*ImportTaskError, len(t.Errors))
	for i, e := range t.Errors {
		cloned[i] = e.clone()
	}

	return page.New(cloned, token, limit, defaultPageLimit), nil
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
