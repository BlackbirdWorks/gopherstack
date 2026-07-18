package macie2

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/google/uuid"
)

// CreateClassificationJob creates a new classification job.
func (b *InMemoryBackend) CreateClassificationJob(
	name, description, jobType, clientToken string,
	s3JobDefinition, scheduleFrequency map[string]any,
	tags map[string]string,
	samplingPercentage int32,
	initialRun bool,
) (string, string, error) {
	b.mu.Lock("CreateClassificationJob")
	defer b.mu.Unlock()

	id := uuid.New().String()
	jobArn := arn.Build("macie2", b.region, b.accountID, fmt.Sprintf("classification-job/%s", id))
	now := time.Now().UTC()

	status := "RUNNING"
	if jobType == "SCHEDULED" {
		status = "IDLE"
	}

	pct := samplingPercentage
	if pct == 0 {
		pct = 100
	}

	b.classificationJobs.Put(&ClassificationJob{
		JobID:              id,
		Arn:                jobArn,
		Name:               name,
		Description:        description,
		JobType:            jobType,
		JobStatus:          status,
		CreatedAt:          now,
		S3JobDefinition:    s3JobDefinition,
		ScheduleFrequency:  scheduleFrequency,
		Tags:               maps.Clone(tags),
		SamplingPercentage: pct,
		InitialRun:         initialRun,
		ClientToken:        clientToken,
	})

	return id, jobArn, nil
}

// DescribeClassificationJob returns a classification job by ID.
func (b *InMemoryBackend) DescribeClassificationJob(jobID string) (*ClassificationJob, error) {
	b.mu.RLock("DescribeClassificationJob")
	defer b.mu.RUnlock()

	job, ok := b.classificationJobs.Get(jobID)
	if !ok {
		return nil, ErrClassificationJobNotFound
	}

	cp := *job
	cp.Tags = maps.Clone(job.Tags)

	return &cp, nil
}

// ListClassificationJobs returns summaries of all classification jobs.
func (b *InMemoryBackend) ListClassificationJobs(
	_ map[string]any, _ int, _ string,
) ([]*ClassificationJobSummary, string, error) {
	b.mu.RLock("ListClassificationJobs")
	defer b.mu.RUnlock()

	jobs := b.classificationJobs.All()
	result := make([]*ClassificationJobSummary, 0, len(jobs))

	for _, job := range jobs {
		result = append(result, &ClassificationJobSummary{
			JobID:       job.JobID,
			Name:        job.Name,
			Description: job.Description,
			JobType:     job.JobType,
			JobStatus:   job.JobStatus,
			CreatedAt:   job.CreatedAt,
			LastRunTime: job.LastRunTime,
			Tags:        maps.Clone(job.Tags),
		})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })

	return result, "", nil
}

// UpdateClassificationJob updates a job's status.
func (b *InMemoryBackend) UpdateClassificationJob(jobID, status string) error {
	b.mu.Lock("UpdateClassificationJob")
	defer b.mu.Unlock()

	job, ok := b.classificationJobs.Get(jobID)
	if !ok {
		return ErrClassificationJobNotFound
	}

	job.JobStatus = status

	return nil
}

// GetClassificationExportConfiguration returns the export config.
func (b *InMemoryBackend) GetClassificationExportConfiguration() (*ClassificationExportConfig, error) {
	b.mu.RLock("GetClassificationExportConfiguration")
	defer b.mu.RUnlock()

	if b.classExportConfig == nil {
		return &ClassificationExportConfig{}, nil
	}

	cp := *b.classExportConfig

	return &cp, nil
}

// PutClassificationExportConfiguration stores the export config.
func (b *InMemoryBackend) PutClassificationExportConfiguration(cfg *ClassificationExportConfig) error {
	b.mu.Lock("PutClassificationExportConfiguration")
	defer b.mu.Unlock()

	if cfg == nil {
		b.classExportConfig = nil

		return nil
	}

	cp := *cfg
	b.classExportConfig = &cp

	return nil
}

const defaultScopeName = "Default classification scope"

func (b *InMemoryBackend) ensureDefaultScope() {
	if b.classScopes.Len() > 0 {
		return
	}

	id := uuid.New().String()
	now := time.Now().UTC()
	b.classScopes.Put(&ClassificationScope{
		ID:        id,
		Name:      defaultScopeName,
		S3:        &ClassificationScopeS3{},
		CreatedAt: now,
		UpdatedAt: now,
	})
}

// GetClassificationScope returns a classification scope by ID.
func (b *InMemoryBackend) GetClassificationScope(scopeID string) (*ClassificationScope, error) {
	b.mu.Lock("GetClassificationScope")
	defer b.mu.Unlock()

	b.ensureDefaultScope()

	scope, ok := b.classScopes.Get(scopeID)
	if !ok {
		return nil, ErrClassificationScopeNotFound
	}

	cp := *scope

	return &cp, nil
}

// ListClassificationScopes returns all classification scopes.
func (b *InMemoryBackend) ListClassificationScopes() ([]*ClassificationScopeSummary, error) {
	b.mu.Lock("ListClassificationScopes")
	defer b.mu.Unlock()

	b.ensureDefaultScope()

	scopes := b.classScopes.All()
	result := make([]*ClassificationScopeSummary, 0, len(scopes))

	for _, s := range scopes {
		result = append(result, &ClassificationScopeSummary{ID: s.ID, Name: s.Name})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })

	return result, nil
}

// UpdateClassificationScope updates a scope's S3 settings.
func (b *InMemoryBackend) UpdateClassificationScope(scopeID string, s3 *ClassificationScopeS3) error {
	b.mu.Lock("UpdateClassificationScope")
	defer b.mu.Unlock()

	scope, ok := b.classScopes.Get(scopeID)
	if !ok {
		return ErrClassificationScopeNotFound
	}

	if s3 != nil {
		scope.S3 = s3
	}

	scope.UpdatedAt = time.Now().UTC()

	return nil
}
