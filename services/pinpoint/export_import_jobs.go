package pinpoint

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// CreateExportJob creates a new Pinpoint export job for an application.
func (b *InMemoryBackend) CreateExportJob(
	region, accountID, appID string,
	req createExportJobRequest,
) (*ExportJob, error) {
	b.mu.Lock("CreateExportJob")
	defer b.mu.Unlock()

	if _, ok := b.apps.Get(appID); !ok {
		return nil, ErrAppNotFound
	}

	id := uuid.NewString()
	jobARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("apps/%s/jobs/export/%s", appID, id))

	j := &ExportJob{
		ARN:           jobARN,
		ApplicationID: appID,
		ID:            id,
		RoleArn:       req.RoleArn,
		S3UrlPrefix:   req.S3UrlPrefix,
		JobStatus:     jobStatusCreated,
		CreationDate:  nowRFC3339(),
	}

	b.exportJobs.Put(j)

	cp := *j

	return &cp, nil
}

// CreateImportJob creates a new Pinpoint import job for an application.
// It also materialises an associated IMPORT-type Segment so that callers can
// reference the segment by ID after the job completes (AWS behaviour).
func (b *InMemoryBackend) CreateImportJob(
	region, accountID, appID string,
	req createImportJobRequest,
) (*ImportJob, error) {
	b.mu.Lock("CreateImportJob")
	defer b.mu.Unlock()

	if _, ok := b.apps.Get(appID); !ok {
		return nil, ErrAppNotFound
	}

	id := uuid.NewString()
	jobARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("apps/%s/jobs/import/%s", appID, id))
	now := nowRFC3339()

	j := &ImportJob{
		ARN:           jobARN,
		ApplicationID: appID,
		ID:            id,
		RoleArn:       req.RoleArn,
		S3Url:         req.S3Url,
		Format:        req.Format,
		JobStatus:     jobStatusCreated,
		CreationDate:  now,
	}

	b.importJobs.Put(j)

	// Materialise an IMPORT-type segment so Terraform/clients can look it up.
	segName := req.SegmentName
	if segName == "" {
		segName = "import-" + id
	}

	segID := uuid.NewString()
	segARN := arn.Build("mobiletargeting", region, accountID, fmt.Sprintf("apps/%s/segments/%s", appID, segID))

	importDef := map[string]any{
		"Format":  req.Format,
		"RoleArn": req.RoleArn,
		"S3Url":   req.S3Url,
		"Size":    0,
	}

	seg := &Segment{
		ApplicationID:    appID,
		ARN:              segARN,
		ID:               segID,
		Name:             segName,
		SegmentType:      segmentTypeImport,
		ImportDefinition: importDef,
		CreationDate:     now,
		LastModifiedDate: now,
		Version:          1,
	}

	b.segments.Put(seg)
	b.arnIndex[segARN] = seg

	versionKey := appID + "/" + segID
	b.segmentVersions[versionKey] = []*Segment{cloneSegment(seg)}

	j.SegmentID = segID

	cp := *j

	return &cp, nil
}

// GetExportJob retrieves an export job by ID.
func (b *InMemoryBackend) GetExportJob(appID, jobID string) (*ExportJob, error) {
	b.mu.RLock("GetExportJob")
	defer b.mu.RUnlock()

	j, ok := b.exportJobs.Get(jobID)
	if !ok || j.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	cp := *j

	return &cp, nil
}

// GetExportJobs returns all export jobs for an app.
func (b *InMemoryBackend) GetExportJobs(appID string) ([]*ExportJob, error) {
	b.mu.RLock("GetExportJobs")
	defer b.mu.RUnlock()

	var jobs []*ExportJob

	for _, j := range b.exportJobs.All() {
		if j.ApplicationID == appID {
			cp := *j
			jobs = append(jobs, &cp)
		}
	}

	return jobs, nil
}

// GetImportJob retrieves an import job by ID.
func (b *InMemoryBackend) GetImportJob(appID, jobID string) (*ImportJob, error) {
	b.mu.RLock("GetImportJob")
	defer b.mu.RUnlock()

	j, ok := b.importJobs.Get(jobID)
	if !ok || j.ApplicationID != appID {
		return nil, ErrAppNotFound
	}

	cp := *j

	return &cp, nil
}

// GetImportJobs returns all import jobs for an app.
func (b *InMemoryBackend) GetImportJobs(appID string) ([]*ImportJob, error) {
	b.mu.RLock("GetImportJobs")
	defer b.mu.RUnlock()

	var jobs []*ImportJob

	for _, j := range b.importJobs.All() {
		if j.ApplicationID == appID {
			cp := *j
			jobs = append(jobs, &cp)
		}
	}

	return jobs, nil
}

// GetSegmentExportJobs returns all export jobs for a segment.
func (b *InMemoryBackend) GetSegmentExportJobs(appID, _ string) ([]*ExportJob, error) {
	return b.GetExportJobs(appID)
}

// GetSegmentImportJobs returns import jobs associated with a specific segment.
func (b *InMemoryBackend) GetSegmentImportJobs(appID, segmentID string) ([]*ImportJob, error) {
	b.mu.RLock("GetSegmentImportJobs")
	defer b.mu.RUnlock()

	var jobs []*ImportJob

	for _, j := range b.importJobs.All() {
		if j.ApplicationID == appID && j.SegmentID == segmentID {
			cp := *j
			jobs = append(jobs, &cp)
		}
	}

	return jobs, nil
}
