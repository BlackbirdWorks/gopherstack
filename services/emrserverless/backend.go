package emrserverless

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// ApplicationStateCreating is the state when an application is being created.
const ApplicationStateCreating = "CREATING"

// ApplicationStateCreated is the state when an application has been created.
const ApplicationStateCreated = "CREATED"

// ApplicationStateStarting is the state when an application is starting.
const ApplicationStateStarting = "STARTING"

// ApplicationStateStarted is the state when an application is running.
const ApplicationStateStarted = "STARTED"

// ApplicationStateStopping is the state when an application is stopping.
const ApplicationStateStopping = "STOPPING"

// ApplicationStateStopped is the state when an application has stopped.
const ApplicationStateStopped = "STOPPED"

// ApplicationStateTerminated is the state when an application is terminated.
const ApplicationStateTerminated = "TERMINATED"

// ApplicationStateTerminatedWithError is the state when an application terminated with errors.
const ApplicationStateTerminatedWithError = "TERMINATED_WITH_ERRORS"

// JobRunStateSubmitted is the state when a job run has been submitted.
const JobRunStateSubmitted = "SUBMITTED"

// JobRunStatePending is the state when a job run is pending.
const JobRunStatePending = "PENDING"

// JobRunStateScheduled is the state when a job run is scheduled.
const JobRunStateScheduled = "SCHEDULED"

// JobRunStateRunning is the state when a job run is running.
const JobRunStateRunning = "RUNNING"

// JobRunStateSuccess is the state when a job run completed successfully.
const JobRunStateSuccess = "SUCCESS"

// JobRunStateFailed is the state when a job run has failed.
const JobRunStateFailed = "FAILED"

// JobRunStateCancelling is the state when a job run is being cancelled.
const JobRunStateCancelling = "CANCELLING"

// JobRunStateCancelled is the state when a job run has been cancelled.
const JobRunStateCancelled = "CANCELLED"

const (
	idChars  = "abcdefghijklmnopqrstuvwxyz0123456789"
	idLength = 10
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
)

// Application represents an EMR Serverless application.
type Application struct {
	Tags          map[string]string `json:"tags,omitempty"`
	CreatedAt     time.Time         `json:"createdAt"`
	UpdatedAt     time.Time         `json:"updatedAt"`
	ApplicationID string            `json:"applicationID"`
	Arn           string            `json:"arn"`
	Name          string            `json:"name"`
	Type          string            `json:"type"`
	ReleaseLabel  string            `json:"releaseLabel"`
	State         string            `json:"state"`
}

// JobRun represents an EMR Serverless job run.
type JobRun struct {
	Tags             map[string]string `json:"tags,omitempty"`
	CreatedAt        time.Time         `json:"createdAt"`
	UpdatedAt        time.Time         `json:"updatedAt"`
	ApplicationID    string            `json:"applicationID"`
	JobRunID         string            `json:"jobRunID"`
	Arn              string            `json:"arn"`
	Name             string            `json:"name"`
	State            string            `json:"state"`
	ExecutionRoleArn string            `json:"executionRoleArn"`
}

// InMemoryBackend stores EMR Serverless state in memory.
type InMemoryBackend struct {
	applications map[string]*Application
	// jobRuns maps applicationID -> jobRunID -> JobRun.
	jobRuns   map[string]map[string]*JobRun
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		applications: make(map[string]*Application),
		jobRuns:      make(map[string]map[string]*JobRun),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("emrserverless"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// newID generates a cryptographically random 10-character lowercase alphanumeric ID.
func newID() string {
	chars := []byte(idChars)
	charCount := uint64(len(chars))
	result := make([]byte, idLength)

	for i := range result {
		var v [8]byte
		_, _ = rand.Read(v[:])
		result[i] = chars[binary.BigEndian.Uint64(v[:])%charCount]
	}

	return string(result)
}

func (b *InMemoryBackend) applicationARN(applicationID string) string {
	return arn.Build("emr-serverless", b.region, b.accountID, "/applications/"+applicationID)
}

func (b *InMemoryBackend) jobRunARN(applicationID, jobRunID string) string {
	return arn.Build("emr-serverless", b.region, b.accountID,
		fmt.Sprintf("/applications/%s/jobruns/%s", applicationID, jobRunID))
}

// CreateApplication creates a new EMR Serverless application.
func (b *InMemoryBackend) CreateApplication(
	name, appType, releaseLabel string,
	tags map[string]string,
) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	for _, app := range b.applications {
		if app.Name == name {
			return nil, fmt.Errorf("%w: application %s already exists", ErrAlreadyExists, name)
		}
	}

	id := newID()
	now := time.Now().UTC()

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	app := &Application{
		ApplicationID: id,
		Arn:           b.applicationARN(id),
		Name:          name,
		Type:          appType,
		ReleaseLabel:  releaseLabel,
		State:         ApplicationStateCreated,
		CreatedAt:     now,
		UpdatedAt:     now,
		Tags:          tagsCopy,
	}
	b.applications[id] = app
	cp := *app

	return &cp, nil
}

// GetApplication retrieves an application by ID.
func (b *InMemoryBackend) GetApplication(id string) (*Application, error) {
	b.mu.RLock("GetApplication")
	defer b.mu.RUnlock()

	app, ok := b.applications[id]
	if !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, id)
	}

	cp := *app

	return &cp, nil
}

// ListApplications returns all applications.
func (b *InMemoryBackend) ListApplications() []*Application {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	list := make([]*Application, 0, len(b.applications))

	for _, app := range b.applications {
		cp := *app
		list = append(list, &cp)
	}

	return list
}

// UpdateApplication applies a mutating function to an application.
func (b *InMemoryBackend) UpdateApplication(id string, update func(*Application)) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	app, ok := b.applications[id]
	if !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, id)
	}

	update(app)
	app.UpdatedAt = time.Now().UTC()
	cp := *app

	return &cp, nil
}

// DeleteApplication removes an application.
func (b *InMemoryBackend) DeleteApplication(id string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	if _, ok := b.applications[id]; !ok {
		return fmt.Errorf("%w: application %s not found", ErrNotFound, id)
	}

	delete(b.applications, id)
	delete(b.jobRuns, id)

	return nil
}

// StartApplication transitions an application to STARTED state.
func (b *InMemoryBackend) StartApplication(id string) error {
	b.mu.Lock("StartApplication")
	defer b.mu.Unlock()

	app, ok := b.applications[id]
	if !ok {
		return fmt.Errorf("%w: application %s not found", ErrNotFound, id)
	}

	app.State = ApplicationStateStarted
	app.UpdatedAt = time.Now().UTC()

	return nil
}

// StopApplication transitions an application to STOPPED state.
func (b *InMemoryBackend) StopApplication(id string) error {
	b.mu.Lock("StopApplication")
	defer b.mu.Unlock()

	app, ok := b.applications[id]
	if !ok {
		return fmt.Errorf("%w: application %s not found", ErrNotFound, id)
	}

	app.State = ApplicationStateStopped
	app.UpdatedAt = time.Now().UTC()

	return nil
}

// StartJobRun creates and starts a new job run.
func (b *InMemoryBackend) StartJobRun(
	applicationID, executionRoleArn, name string,
	tags map[string]string,
) (*JobRun, error) {
	b.mu.Lock("StartJobRun")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, applicationID)
	}

	jobRunID := newID()
	now := time.Now().UTC()

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	jr := &JobRun{
		ApplicationID:    applicationID,
		JobRunID:         jobRunID,
		Arn:              b.jobRunARN(applicationID, jobRunID),
		Name:             name,
		State:            JobRunStateRunning,
		ExecutionRoleArn: executionRoleArn,
		CreatedAt:        now,
		UpdatedAt:        now,
		Tags:             tagsCopy,
	}

	if b.jobRuns[applicationID] == nil {
		b.jobRuns[applicationID] = make(map[string]*JobRun)
	}

	b.jobRuns[applicationID][jobRunID] = jr
	cp := *jr

	return &cp, nil
}

// GetJobRun retrieves a job run by application ID and job run ID.
func (b *InMemoryBackend) GetJobRun(applicationID, jobRunID string) (*JobRun, error) {
	b.mu.RLock("GetJobRun")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, applicationID)
	}

	runs, ok := b.jobRuns[applicationID]
	if !ok {
		return nil, fmt.Errorf("%w: job run %s not found", ErrNotFound, jobRunID)
	}

	jr, ok := runs[jobRunID]
	if !ok {
		return nil, fmt.Errorf("%w: job run %s not found", ErrNotFound, jobRunID)
	}

	cp := *jr

	return &cp, nil
}

// ListJobRuns returns all job runs for an application.
func (b *InMemoryBackend) ListJobRuns(applicationID string) ([]*JobRun, error) {
	b.mu.RLock("ListJobRuns")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, applicationID)
	}

	runs := b.jobRuns[applicationID]
	list := make([]*JobRun, 0, len(runs))

	for _, jr := range runs {
		cp := *jr
		list = append(list, &cp)
	}

	return list, nil
}

// CancelJobRun cancels a job run.
func (b *InMemoryBackend) CancelJobRun(applicationID, jobRunID string) (*JobRun, error) {
	b.mu.Lock("CancelJobRun")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, applicationID)
	}

	runs, ok := b.jobRuns[applicationID]
	if !ok {
		return nil, fmt.Errorf("%w: job run %s not found", ErrNotFound, jobRunID)
	}

	jr, ok := runs[jobRunID]
	if !ok {
		return nil, fmt.Errorf("%w: job run %s not found", ErrNotFound, jobRunID)
	}

	jr.State = JobRunStateCancelled
	jr.UpdatedAt = time.Now().UTC()
	cp := *jr

	return &cp, nil
}

// ListTagsForResource returns tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tags, ok := b.findTagsByARN(resourceARN)
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	out := make(map[string]string, len(tags))
	maps.Copy(out, tags)

	return out, nil
}

// TagResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing, ok := b.findTagsByARN(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	maps.Copy(existing, tags)

	return nil
}

// UntagResource removes tags from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	existing, ok := b.findTagsByARN(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	for _, k := range tagKeys {
		delete(existing, k)
	}

	return nil
}

// findTagsByARN looks up the tags map for a resource by ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findTagsByARN(resourceARN string) (map[string]string, bool) {
	for _, app := range b.applications {
		if app.Arn == resourceARN {
			return app.Tags, true
		}
	}

	for _, runs := range b.jobRuns {
		for _, jr := range runs {
			if jr.Arn == resourceARN {
				return jr.Tags, true
			}
		}
	}

	return nil, false
}
