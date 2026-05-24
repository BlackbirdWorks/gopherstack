package comprehend

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

var (
	// ErrNotFound is returned when a requested Comprehend resource is absent.
	ErrNotFound = errors.New("ResourceNotFoundException")
	// ErrConflict is returned when a named Comprehend resource already exists.
	ErrConflict = errors.New("ResourceInUseException")
	// ErrValidation is returned for invalid request values.
	ErrValidation = errors.New("InvalidRequestException")
)

const (
	statusSubmitted     = "SUBMITTED"
	statusInProgress    = "IN_PROGRESS"
	statusCompleted     = "COMPLETED"
	statusFailed        = "FAILED"
	statusStopRequested = "STOP_REQUESTED"
	statusStopped       = "STOPPED"
	statusTrained       = "TRAINED"
	statusReady         = "READY"
	statusActive        = "ACTIVE"
	defaultLanguageCode = "en"
	defaultScore        = 0.99
	failedMarker        = "[fail]"
)

// Tag is a Comprehend resource tag.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// Job represents an asynchronous document-analysis job.
type Job struct {
	SubmitTime              time.Time
	EndTime                 time.Time
	JobID                   string
	JobArn                  string
	JobName                 string
	JobType                 string
	JobStatus               string
	LanguageCode            string
	FailureReason           string
	InputDataConfig         map[string]any
	OutputDataConfig        map[string]any
	DataAccessRoleArn       string
	DocumentClassifierArn   string
	EntityRecognizerArn     string
	TargetEventTypes        []string
	polls                   int
	stopRequested           bool
	shouldFail              bool
}

// Resource stores a Comprehend trainable or hosted resource.
type Resource struct {
	CreatedAt       time.Time
	UpdatedAt       time.Time
	Name            string
	Arn             string
	Type            string
	Status          string
	VersionName     string
	ModelArn        string
	FlywheelArn     string
	EndpointArn     string
	DatasetArn      string
	Configuration   map[string]any
	FailureReason   string
}

// FlywheelIteration represents one model training iteration.
type FlywheelIteration struct {
	CreationTime      time.Time
	EndTime           time.Time
	FlywheelArn       string
	FlywheelIterationID string
	FlywheelIterationStatus string
	Message           string
	polls             int
}

// InMemoryBackend stores Comprehend state safely for concurrent requests.
type InMemoryBackend struct {
	jobs       map[string]*Job
	resources  map[string]*Resource
	iterations map[string]*FlywheelIteration
	tags       map[string]map[string]string
	accountID  string
	region     string
	mu         sync.RWMutex
}

// NewInMemoryBackend creates a configured Comprehend backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		jobs:       make(map[string]*Job),
		resources:  make(map[string]*Resource),
		iterations: make(map[string]*FlywheelIteration),
		tags:       make(map[string]map[string]string),
		accountID:  accountID,
		region:     region,
	}
}

// Region returns configured AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored Comprehend resources.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.jobs = make(map[string]*Job)
	b.resources = make(map[string]*Resource)
	b.iterations = make(map[string]*FlywheelIteration)
	b.tags = make(map[string]map[string]string)
}

// StartJob submits an analysis job with AWS-style initial status.
func (b *InMemoryBackend) StartJob(jobType, name string, values map[string]any) (*Job, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: JobName is required", ErrValidation)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, job := range b.jobs {
		if job.JobType == jobType && job.JobName == name {
			return nil, fmt.Errorf("%w: job %q already exists", ErrConflict, name)
		}
	}

	id := uuid.NewString()
	job := &Job{
		JobID:                 id,
		JobArn:                arn.Build("comprehend", b.region, b.accountID, jobType+"/"+id),
		JobName:               name,
		JobType:               jobType,
		JobStatus:             statusSubmitted,
		LanguageCode:          stringValue(values, "LanguageCode", defaultLanguageCode),
		DataAccessRoleArn:     stringValue(values, "DataAccessRoleArn", ""),
		DocumentClassifierArn: stringValue(values, "DocumentClassifierArn", ""),
		EntityRecognizerArn:   stringValue(values, "EntityRecognizerArn", ""),
		InputDataConfig:       mapValue(values, "InputDataConfig"),
		OutputDataConfig:      mapValue(values, "OutputDataConfig"),
		TargetEventTypes:      stringSliceValue(values, "TargetEventTypes"),
		SubmitTime:            time.Now().UTC(),
		shouldFail:            strings.Contains(strings.ToLower(name), failedMarker),
	}
	b.jobs[id] = job

	return cloneJob(job), nil
}

// DescribeJob retrieves and advances a submitted job through its lifecycle.
func (b *InMemoryBackend) DescribeJob(id, jobType string) (*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	job, ok := b.jobs[id]
	if !ok || job.JobType != jobType {
		return nil, fmt.Errorf("%w: job %q", ErrNotFound, id)
	}

	advanceJob(job)

	return cloneJob(job), nil
}

// StopJob starts cancellation of an active job.
func (b *InMemoryBackend) StopJob(id, jobType string) (*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	job, ok := b.jobs[id]
	if !ok || job.JobType != jobType {
		return nil, fmt.Errorf("%w: job %q", ErrNotFound, id)
	}

	if job.JobStatus == statusSubmitted || job.JobStatus == statusInProgress {
		job.JobStatus = statusStopRequested
		job.stopRequested = true
	}

	return cloneJob(job), nil
}

// ListJobs returns jobs for one operation family in stable submission order.
func (b *InMemoryBackend) ListJobs(jobType string) []*Job {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*Job, 0, len(b.jobs))
	for _, job := range b.jobs {
		if job.JobType == jobType {
			out = append(out, cloneJob(job))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SubmitTime.Before(out[j].SubmitTime) })

	return out
}

func advanceJob(job *Job) {
	switch job.JobStatus {
	case statusSubmitted:
		job.JobStatus = statusInProgress
	case statusInProgress:
		if job.shouldFail {
			job.JobStatus = statusFailed
			job.FailureReason = "simulated analysis failure"
		} else {
			job.JobStatus = statusCompleted
		}
		job.EndTime = time.Now().UTC()
	case statusStopRequested:
		job.JobStatus = statusStopped
		job.EndTime = time.Now().UTC()
	}
	job.polls++
}

// CreateResource creates a stateful Comprehend resource and optionally attaches tags.
func (b *InMemoryBackend) CreateResource(
	resourceType, name, versionName string,
	values map[string]any,
	tags []Tag,
) (*Resource, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	resourceArn := b.resourceARN(resourceType, name, versionName)
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.resources[resourceArn]; ok {
		return nil, fmt.Errorf("%w: resource %q already exists", ErrConflict, name)
	}

	now := time.Now().UTC()
	resource := &Resource{
		CreatedAt:     now,
		UpdatedAt:     now,
		Name:          name,
		Arn:           resourceArn,
		Type:          resourceType,
		Status:        initialResourceStatus(resourceType),
		VersionName:   versionName,
		ModelArn:      stringValue(values, "ModelArn", ""),
		FlywheelArn:   stringValue(values, "FlywheelArn", ""),
		Configuration: cloneMap(values),
	}
	switch resourceType {
	case "endpoint":
		resource.EndpointArn = resourceArn
	case "dataset":
		resource.DatasetArn = resourceArn
	}
	b.resources[resourceArn] = resource
	b.tags[resourceArn] = tagsMap(tags)

	return cloneResource(resource), nil
}

// GetResource finds resource by ARN.
func (b *InMemoryBackend) GetResource(resourceArn, resourceType string) (*Resource, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	resource, ok := b.resources[resourceArn]
	if !ok || resource.Type != resourceType {
		return nil, fmt.Errorf("%w: resource %q", ErrNotFound, resourceArn)
	}

	return cloneResource(resource), nil
}

// ListResources returns resources of one type.
func (b *InMemoryBackend) ListResources(resourceType string) []*Resource {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*Resource, 0, len(b.resources))
	for _, resource := range b.resources {
		if resource.Type == resourceType {
			out = append(out, cloneResource(resource))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Arn < out[j].Arn })

	return out
}

// UpdateResource changes stored configuration of a mutable resource.
func (b *InMemoryBackend) UpdateResource(resourceArn, resourceType string, values map[string]any) (*Resource, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	resource, ok := b.resources[resourceArn]
	if !ok || resource.Type != resourceType {
		return nil, fmt.Errorf("%w: resource %q", ErrNotFound, resourceArn)
	}

	maps.Copy(resource.Configuration, cloneMap(values))
	resource.UpdatedAt = time.Now().UTC()

	return cloneResource(resource), nil
}

// DeleteResource removes a stored resource and its tags.
func (b *InMemoryBackend) DeleteResource(resourceArn, resourceType string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	resource, ok := b.resources[resourceArn]
	if !ok || resource.Type != resourceType {
		return fmt.Errorf("%w: resource %q", ErrNotFound, resourceArn)
	}
	delete(b.resources, resourceArn)
	delete(b.tags, resourceArn)

	return nil
}

// StartFlywheelIteration creates an asynchronous flywheel iteration.
func (b *InMemoryBackend) StartFlywheelIteration(flywheelArn string) (*FlywheelIteration, error) {
	if _, err := b.GetResource(flywheelArn, "flywheel"); err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	id := uuid.NewString()
	iteration := &FlywheelIteration{
		CreationTime:            time.Now().UTC(),
		FlywheelArn:             flywheelArn,
		FlywheelIterationID:     id,
		FlywheelIterationStatus: statusSubmitted,
	}
	b.iterations[id] = iteration

	return cloneIteration(iteration), nil
}

// GetFlywheelIteration returns and advances an iteration.
func (b *InMemoryBackend) GetFlywheelIteration(id string) (*FlywheelIteration, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	iteration, ok := b.iterations[id]
	if !ok {
		return nil, fmt.Errorf("%w: iteration %q", ErrNotFound, id)
	}
	if iteration.FlywheelIterationStatus == statusSubmitted {
		iteration.FlywheelIterationStatus = statusInProgress
	} else if iteration.FlywheelIterationStatus == statusInProgress {
		iteration.FlywheelIterationStatus = statusCompleted
		iteration.EndTime = time.Now().UTC()
	}
	iteration.polls++

	return cloneIteration(iteration), nil
}

// ListFlywheelIterations lists iterations belonging to one flywheel.
func (b *InMemoryBackend) ListFlywheelIterations(flywheelArn string) []*FlywheelIteration {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]*FlywheelIteration, 0, len(b.iterations))
	for _, iteration := range b.iterations {
		if iteration.FlywheelArn == flywheelArn {
			out = append(out, cloneIteration(iteration))
		}
	}

	return out
}

// TagResource adds or replaces tags on an existing resource.
func (b *InMemoryBackend) TagResource(resourceArn string, tags []Tag) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	current, ok := b.tags[resourceArn]
	if !ok {
		return fmt.Errorf("%w: resource %q", ErrNotFound, resourceArn)
	}
	for _, tag := range tags {
		current[tag.Key] = tag.Value
	}

	return nil
}

// UntagResource removes keys from an existing resource.
func (b *InMemoryBackend) UntagResource(resourceArn string, keys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	current, ok := b.tags[resourceArn]
	if !ok {
		return fmt.Errorf("%w: resource %q", ErrNotFound, resourceArn)
	}
	for _, key := range keys {
		delete(current, key)
	}

	return nil
}

// ListTags returns sorted resource tags.
func (b *InMemoryBackend) ListTags(resourceArn string) ([]Tag, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	current, ok := b.tags[resourceArn]
	if !ok {
		return nil, fmt.Errorf("%w: resource %q", ErrNotFound, resourceArn)
	}
	keys := make([]string, 0, len(current))
	for key := range current {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]Tag, 0, len(keys))
	for _, key := range keys {
		out = append(out, Tag{Key: key, Value: current[key]})
	}

	return out, nil
}

func (b *InMemoryBackend) resourceARN(resourceType, name, version string) string {
	resource := resourceType + "/" + name
	if version != "" {
		resource += "/version/" + version
	}

	return arn.Build("comprehend", b.region, b.accountID, resource)
}

func initialResourceStatus(resourceType string) string {
	switch resourceType {
	case "endpoint":
		return statusActive
	case "flywheel", "dataset":
		return statusReady
	default:
		return statusTrained
	}
}

func tagsMap(tags []Tag) map[string]string {
	out := make(map[string]string, len(tags))
	for _, tag := range tags {
		out[tag.Key] = tag.Value
	}

	return out
}

func stringValue(values map[string]any, key, fallback string) string {
	value, ok := values[key].(string)
	if !ok || value == "" {
		return fallback
	}

	return value
}

func mapValue(values map[string]any, key string) map[string]any {
	value, _ := values[key].(map[string]any)

	return cloneMap(value)
}

func stringSliceValue(values map[string]any, key string) []string {
	raw, _ := values[key].([]any)
	out := make([]string, 0, len(raw))
	for _, value := range raw {
		if text, ok := value.(string); ok {
			out = append(out, text)
		}
	}

	return out
}

func cloneMap(source map[string]any) map[string]any {
	out := make(map[string]any, len(source))
	maps.Copy(out, source)

	return out
}

func cloneJob(job *Job) *Job {
	copy := *job
	copy.InputDataConfig = cloneMap(job.InputDataConfig)
	copy.OutputDataConfig = cloneMap(job.OutputDataConfig)
	copy.TargetEventTypes = append([]string(nil), job.TargetEventTypes...)

	return &copy
}

func cloneResource(resource *Resource) *Resource {
	copy := *resource
	copy.Configuration = cloneMap(resource.Configuration)

	return &copy
}

func cloneIteration(iteration *FlywheelIteration) *FlywheelIteration {
	copy := *iteration

	return &copy
}
