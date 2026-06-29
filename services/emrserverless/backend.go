package emrserverless

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"maps"
	"sort"
	"strconv"
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
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrInvalidState is returned when an operation is not valid for the resource's current state.
	ErrInvalidState = awserr.New("RequestFailedException", awserr.ErrInvalidParameter)
)

// Application represents an EMR Serverless application.
type Application struct {
	Tags          map[string]string `json:"tags,omitempty"`
	CreatedAt     time.Time         `json:"createdAt"`
	UpdatedAt     time.Time         `json:"updatedAt"`
	ApplicationID string            `json:"applicationId"`
	Arn           string            `json:"arn"`
	Name          string            `json:"name"`
	Type          string            `json:"type"`
	ReleaseLabel  string            `json:"releaseLabel"`
	Architecture  string            `json:"architecture,omitempty"`
	State         string            `json:"state"`
}

// JobRun represents an EMR Serverless job run.
type JobRun struct {
	Tags             map[string]string `json:"tags,omitempty"`
	CreatedAt        time.Time         `json:"createdAt"`
	UpdatedAt        time.Time         `json:"updatedAt"`
	ApplicationID    string            `json:"applicationId"`
	JobRunID         string            `json:"jobRunId"`
	Arn              string            `json:"arn"`
	Name             string            `json:"name"`
	State            string            `json:"state"`
	ExecutionRoleArn string            `json:"executionRoleArn"`
	Mode             string            `json:"mode,omitempty"`
	ReleaseLabel     string            `json:"releaseLabel,omitempty"`
	StateDetails     string            `json:"stateDetails,omitempty"`
}

// InMemoryBackend stores EMR Serverless state in memory.
type InMemoryBackend struct {
	applications    map[string]*Application
	applicationARNs map[string]string    // application ARN → applicationID
	jobRunARNs      map[string][2]string // job run ARN → {applicationID, jobRunID}
	sessionARNs     map[string][2]string // session ARN → {applicationID, sessionID}
	// jobRuns maps applicationID -> jobRunID -> JobRun.
	jobRuns       map[string]map[string]*JobRun
	sessions      map[string]map[string]*Session
	sessionTokens map[string]map[string]string
	mu            *lockmetrics.RWMutex
	accountID     string
	region        string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		applications:    make(map[string]*Application),
		applicationARNs: make(map[string]string),
		jobRunARNs:      make(map[string][2]string),
		sessionARNs:     make(map[string][2]string),
		jobRuns:         make(map[string]map[string]*JobRun),
		sessions:        make(map[string]map[string]*Session),
		sessionTokens:   make(map[string]map[string]string),
		accountID:       accountID,
		region:          region,
		mu:              lockmetrics.New("emrserverless"),
	}
}

// Reset clears all backend state, returning it to the initial empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.applications = make(map[string]*Application)
	b.applicationARNs = make(map[string]string)
	b.jobRunARNs = make(map[string][2]string)
	b.sessionARNs = make(map[string][2]string)
	b.jobRuns = make(map[string]map[string]*JobRun)
	b.sessions = make(map[string]map[string]*Session)
	b.sessionTokens = make(map[string]map[string]string)
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

func (b *InMemoryBackend) sessionARN(applicationID, sessionID string) string {
	return arn.Build("emr-serverless", b.region, b.accountID,
		fmt.Sprintf("/applications/%s/sessions/%s", applicationID, sessionID))
}

// CreateApplication creates a new EMR Serverless application.
func (b *InMemoryBackend) CreateApplication(
	name, appType, releaseLabel, architecture string,
	tags map[string]string,
) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if appType == "" {
		return nil, fmt.Errorf("%w: type is required", ErrValidation)
	}

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
		Architecture:  architecture,
		State:         ApplicationStateCreated,
		CreatedAt:     now,
		UpdatedAt:     now,
		Tags:          tagsCopy,
	}
	b.applications[id] = app
	b.applicationARNs[app.Arn] = id

	return cloneApplication(app), nil
}

// GetApplication retrieves an application by ID.
func (b *InMemoryBackend) GetApplication(id string) (*Application, error) {
	b.mu.RLock("GetApplication")
	defer b.mu.RUnlock()

	app, ok := b.applications[id]
	if !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, id)
	}

	return cloneApplication(app), nil
}

// ListApplications returns paginated applications, optionally filtered by state.
func (b *InMemoryBackend) ListApplications(
	nextToken string, maxResults int, states ...string,
) ([]*Application, string) {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	list := make([]*Application, 0, len(b.applications))

	for _, app := range b.applications {
		list = append(list, cloneApplication(app))
	}

	if len(states) > 0 {
		stateSet := make(map[string]struct{}, len(states))
		for _, s := range states {
			stateSet[s] = struct{}{}
		}
		filtered := list[:0]
		for _, app := range list {
			if _, ok := stateSet[app.State]; ok {
				filtered = append(filtered, app)
			}
		}
		list = filtered
	}

	sort.Slice(list, func(i, j int) bool {
		if list[i].CreatedAt.Equal(list[j].CreatedAt) {
			return list[i].ApplicationID < list[j].ApplicationID
		}

		return list[i].CreatedAt.Before(list[j].CreatedAt)
	})

	page, token := emrPaginate(list, nextToken, maxResults)

	return page, token
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

	return cloneApplication(app), nil
}

// DeleteApplication removes an application.
// It rejects the request if the application is in STARTED or STARTING state.
func (b *InMemoryBackend) DeleteApplication(id string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	app, ok := b.applications[id]
	if !ok {
		return fmt.Errorf("%w: application %s not found", ErrNotFound, id)
	}

	switch app.State {
	case ApplicationStateStarted, ApplicationStateStarting, ApplicationStateStopping, ApplicationStateCreating:
		return fmt.Errorf(
			"%w: application %s must be stopped before deletion (current state: %s)",
			ErrInvalidState, id, app.State,
		)
	}

	delete(b.applicationARNs, app.Arn)

	// Clean up job run ARN index entries for this application's job runs.
	for _, jr := range b.jobRuns[id] {
		delete(b.jobRunARNs, jr.Arn)
	}
	for _, session := range b.sessions[id] {
		delete(b.sessionARNs, session.Arn)
	}

	delete(b.applications, id)
	delete(b.jobRuns, id)
	delete(b.sessions, id)
	delete(b.sessionTokens, id)

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

	switch app.State {
	case ApplicationStateStarted:
		return fmt.Errorf("%w: application %s is already in STARTED state", ErrInvalidState, id)
	case ApplicationStateTerminated, ApplicationStateTerminatedWithError:
		return fmt.Errorf(
			"%w: application %s cannot be started from state %s",
			ErrInvalidState, id, app.State,
		)
	case ApplicationStateStarting, ApplicationStateStopping:
		return fmt.Errorf(
			"%w: application %s cannot be started from state %s",
			ErrInvalidState, id, app.State,
		)
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

	switch app.State {
	case ApplicationStateStopped, ApplicationStateTerminated, ApplicationStateTerminatedWithError:
		return fmt.Errorf("%w: application %s is already in %s state", ErrInvalidState, id, app.State)
	}

	app.State = ApplicationStateStopped
	app.UpdatedAt = time.Now().UTC()

	return nil
}

// StartJobRun creates and starts a new job run.
func (b *InMemoryBackend) StartJobRun(
	applicationID, executionRoleArn, name, mode string,
	tags map[string]string,
) (*JobRun, error) {
	b.mu.Lock("StartJobRun")
	defer b.mu.Unlock()

	if executionRoleArn == "" {
		return nil, fmt.Errorf("%w: executionRoleArn is required", ErrValidation)
	}

	app, ok := b.applications[applicationID]
	if !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, applicationID)
	}

	if mode == "" {
		mode = "BATCH"
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
		State:            JobRunStateSubmitted,
		ExecutionRoleArn: executionRoleArn,
		Mode:             mode,
		ReleaseLabel:     app.ReleaseLabel,
		CreatedAt:        now,
		UpdatedAt:        now,
		Tags:             tagsCopy,
	}

	if b.jobRuns[applicationID] == nil {
		b.jobRuns[applicationID] = make(map[string]*JobRun)
	}

	b.jobRuns[applicationID][jobRunID] = jr
	b.jobRunARNs[jr.Arn] = [2]string{applicationID, jobRunID}

	return cloneJobRun(jr), nil
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

	return cloneJobRun(jr), nil
}

// ListJobRuns returns paginated job runs for an application, optionally filtered by state.
func (b *InMemoryBackend) ListJobRuns(
	applicationID, nextToken string, maxResults int, states ...string,
) ([]*JobRun, string, error) {
	b.mu.RLock("ListJobRuns")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, "", fmt.Errorf("%w: application %s not found", ErrNotFound, applicationID)
	}

	runs := b.jobRuns[applicationID]
	list := make([]*JobRun, 0, len(runs))

	for _, jr := range runs {
		list = append(list, cloneJobRun(jr))
	}

	if len(states) > 0 {
		stateSet := make(map[string]struct{}, len(states))
		for _, s := range states {
			stateSet[s] = struct{}{}
		}
		filtered := list[:0]
		for _, jr := range list {
			if _, ok := stateSet[jr.State]; ok {
				filtered = append(filtered, jr)
			}
		}
		list = filtered
	}

	sort.Slice(list, func(i, j int) bool {
		if list[i].CreatedAt.Equal(list[j].CreatedAt) {
			return list[i].JobRunID < list[j].JobRunID
		}

		return list[i].CreatedAt.After(list[j].CreatedAt)
	})

	page, token := emrPaginate(list, nextToken, maxResults)

	return page, token, nil
}

// isTerminalJobRunState returns true if the given state prevents cancellation.
func isTerminalJobRunState(state string) bool {
	return state == JobRunStateSuccess || state == JobRunStateFailed || state == JobRunStateCancelled
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

	if isTerminalJobRunState(jr.State) {
		return nil, fmt.Errorf(
			"%w: job run %s cannot be cancelled from state %s",
			ErrInvalidState, jobRunID, jr.State,
		)
	}

	jr.State = JobRunStateCancelled
	jr.StateDetails = "Job run cancelled by user request"
	jr.UpdatedAt = time.Now().UTC()

	return cloneJobRun(jr), nil
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

// findTagsByARN looks up the tags map for a resource by ARN using O(1) index lookups.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findTagsByARN(resourceARN string) (map[string]string, bool) {
	if id, ok := b.applicationARNs[resourceARN]; ok {
		return b.applications[id].Tags, true
	}

	if key, ok := b.jobRunARNs[resourceARN]; ok {
		runs, runsOK := b.jobRuns[key[0]]
		if !runsOK {
			return nil, false
		}
		jr, jrOK := runs[key[1]]
		if !jrOK {
			return nil, false
		}

		return jr.Tags, true
	}

	if key, ok := b.sessionARNs[resourceARN]; ok {
		sessions, sessionsOK := b.sessions[key[0]]
		if !sessionsOK {
			return nil, false
		}
		session, sessionOK := sessions[key[1]]
		if !sessionOK {
			return nil, false
		}

		return session.Tags, true
	}

	return nil, false
}

// JobRunAttemptSummary represents a single attempt of a job run.
type JobRunAttemptSummary struct {
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
	JobCreatedAt  time.Time `json:"jobCreatedAt"`
	ApplicationID string    `json:"applicationId"`
	Arn           string    `json:"arn"`
	CreatedBy     string    `json:"createdBy"`
	ExecutionRole string    `json:"executionRole"`
	ID            string    `json:"id"`
	ReleaseLabel  string    `json:"releaseLabel"`
	State         string    `json:"state"`
	StateDetails  string    `json:"stateDetails"`
	Name          string    `json:"name"`
	Type          string    `json:"type"`
	Attempt       int32     `json:"attempt"`
}

// GetDashboardForJobRun returns a dashboard URL for a job run.
func (b *InMemoryBackend) GetDashboardForJobRun(applicationID, jobRunID string) (string, error) {
	b.mu.RLock("GetDashboardForJobRun")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationID]; !ok {
		return "", fmt.Errorf("%w: application %s not found", ErrNotFound, applicationID)
	}

	runs, ok := b.jobRuns[applicationID]
	if !ok {
		return "", fmt.Errorf("%w: job run %s not found", ErrNotFound, jobRunID)
	}

	if _, exists := runs[jobRunID]; !exists {
		return "", fmt.Errorf("%w: job run %s not found", ErrNotFound, jobRunID)
	}

	url := fmt.Sprintf("https://console.aws.amazon.com/emr-serverless/home?region=%s#/applications/%s/jobruns/%s",
		b.region, applicationID, jobRunID)

	return url, nil
}

// ListJobRunAttempts returns paginated attempt summaries for a job run.
func (b *InMemoryBackend) ListJobRunAttempts(
	applicationID, jobRunID, nextToken string,
	maxResults int,
) ([]*JobRunAttemptSummary, string, error) {
	b.mu.RLock("ListJobRunAttempts")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, "", fmt.Errorf("%w: application %s not found", ErrNotFound, applicationID)
	}

	runs, ok := b.jobRuns[applicationID]
	if !ok {
		return nil, "", fmt.Errorf("%w: job run %s not found", ErrNotFound, jobRunID)
	}

	jr, ok := runs[jobRunID]
	if !ok {
		return nil, "", fmt.Errorf("%w: job run %s not found", ErrNotFound, jobRunID)
	}

	// The in-memory backend synthesises a single attempt (attempt 0) from the job
	// run itself.  Fields that are not tracked by the backend (ReleaseLabel,
	// StateDetails, CreatedBy) use sensible placeholders or empty values.
	attempt := &JobRunAttemptSummary{
		ApplicationID: jr.ApplicationID,
		Arn:           jr.Arn,
		CreatedAt:     jr.CreatedAt,
		UpdatedAt:     jr.UpdatedAt,
		JobCreatedAt:  jr.CreatedAt,
		// CreatedBy is set to the execution role ARN as a best-effort substitute;
		// the in-memory backend does not record the IAM principal that submitted the run.
		CreatedBy:     jr.ExecutionRoleArn,
		ExecutionRole: jr.ExecutionRoleArn,
		ID:            jr.JobRunID,
		// ReleaseLabel is not stored on JobRun in this backend.
		ReleaseLabel: "",
		State:        jr.State,
		// StateDetails are not tracked in the in-memory backend.
		StateDetails: "",
		Name:         jr.Name,
		// Attempt index starts at 0; the backend does not track retries.
		Attempt: 0,
	}

	all := []*JobRunAttemptSummary{attempt}
	page, token := emrPaginate(all, nextToken, maxResults)

	return page, token, nil
}

// cloneApplication returns a deep copy of an Application with its Tags map cloned.
// The returned copy always has a non-nil Tags map.
func cloneApplication(app *Application) *Application {
	cp := *app
	cp.Tags = make(map[string]string, len(app.Tags))
	maps.Copy(cp.Tags, app.Tags)

	return &cp
}

// cloneJobRun returns a deep copy of a JobRun with its Tags map cloned.
// The returned copy always has a non-nil Tags map.
func cloneJobRun(jr *JobRun) *JobRun {
	cp := *jr
	cp.Tags = make(map[string]string, len(jr.Tags))
	maps.Copy(cp.Tags, jr.Tags)

	return &cp
}

// AddApplicationInternal directly inserts an Application into the backend without
// going through the HTTP layer.  Intended for test seeding only.
func (b *InMemoryBackend) AddApplicationInternal(app *Application) {
	b.mu.Lock("AddApplicationInternal")
	defer b.mu.Unlock()

	if app.Tags == nil {
		app.Tags = make(map[string]string)
	}

	b.applications[app.ApplicationID] = app
	b.applicationARNs[app.Arn] = app.ApplicationID

	if b.jobRuns[app.ApplicationID] == nil {
		b.jobRuns[app.ApplicationID] = make(map[string]*JobRun)
	}
}

// AddJobRunInternal directly inserts a JobRun into the backend without going through
// the HTTP layer.  The application must already exist.  Intended for test seeding only.
func (b *InMemoryBackend) AddJobRunInternal(jr *JobRun) {
	b.mu.Lock("AddJobRunInternal")
	defer b.mu.Unlock()

	if jr.Tags == nil {
		jr.Tags = make(map[string]string)
	}

	if b.jobRuns[jr.ApplicationID] == nil {
		b.jobRuns[jr.ApplicationID] = make(map[string]*JobRun)
	}

	b.jobRuns[jr.ApplicationID][jr.JobRunID] = jr
	b.jobRunARNs[jr.Arn] = [2]string{jr.ApplicationID, jr.JobRunID}
}

// emrPaginate applies token-based pagination to a sorted slice of pointers.
func emrPaginate[T any](all []*T, nextToken string, maxResults int) ([]*T, string) {
	const defaultLimit = 100

	startIdx := 0
	if nextToken != "" {
		if idx, err := strconv.Atoi(nextToken); err == nil && idx >= 0 {
			startIdx = idx
		}
	}

	if startIdx >= len(all) {
		return []*T{}, ""
	}

	limit := defaultLimit
	if maxResults > 0 {
		limit = maxResults
	}

	end := startIdx + limit

	var outToken string
	if end < len(all) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken
}
