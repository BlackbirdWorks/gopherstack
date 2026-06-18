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
	statusSubmitted                  = "SUBMITTED"
	statusInProgress                 = "IN_PROGRESS"
	statusCompleted                  = "COMPLETED"
	statusFailed                     = "FAILED"
	statusStopRequested              = "STOP_REQUESTED"
	statusStopped                    = "STOPPED"
	statusTrained                    = "TRAINED"
	statusReady                      = "READY"
	statusActive                     = "ACTIVE"
	defaultLanguageCode              = "en"
	defaultScore                     = 0.99
	failedMarker                     = "[fail]"
	resourceTypeEndpoint             = "endpoint"
	resourceTypeFlywheel             = "flywheel"
	resourceTypeDataset              = "dataset"
	resourceTypeDocClassifier        = "document-classifier"
	resourceTypeDocClassifierVersion = "document-classifier-version"
	resourceTypeEntityRecognizer     = "entity-recognizer"
	resourceTypeEntityRecognizerVer  = "entity-recognizer-version"
)

// Tag is a Comprehend resource tag.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// Job represents an asynchronous document-analysis job.
type Job struct {
	SubmitTime            time.Time
	EndTime               time.Time
	JobID                 string
	JobArn                string
	JobName               string
	JobType               string
	JobStatus             string
	LanguageCode          string
	FailureReason         string
	InputDataConfig       map[string]any
	OutputDataConfig      map[string]any
	DataAccessRoleArn     string
	DocumentClassifierArn string
	EntityRecognizerArn   string
	TargetEventTypes      []string
	polls                 int
	stopRequested         bool
	shouldFail            bool
}

// Resource stores a Comprehend trainable or hosted resource.
type Resource struct {
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Name          string
	Arn           string
	Type          string
	Status        string
	VersionName   string
	ModelArn      string
	FlywheelArn   string
	EndpointArn   string
	DatasetArn    string
	Configuration map[string]any
	FailureReason string
}

// FlywheelIteration represents one model training iteration.
type FlywheelIteration struct {
	CreationTime            time.Time
	EndTime                 time.Time
	FlywheelArn             string
	FlywheelIterationID     string
	FlywheelIterationStatus string
	Message                 string
	polls                   int
}

// InMemoryBackend stores Comprehend state safely for concurrent requests.
type InMemoryBackend struct {
	jobs            map[string]*Job
	resources       map[string]*Resource
	iterations      map[string]*FlywheelIteration
	tags            map[string]map[string]string
	policies        map[string]string
	policyRevisions map[string]string
	accountID       string
	region          string
	mu              sync.RWMutex
}

// NewInMemoryBackend creates a configured Comprehend backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		jobs:            make(map[string]*Job),
		resources:       make(map[string]*Resource),
		iterations:      make(map[string]*FlywheelIteration),
		tags:            make(map[string]map[string]string),
		policies:        make(map[string]string),
		policyRevisions: make(map[string]string),
		accountID:       accountID,
		region:          region,
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
	b.policies = make(map[string]string)
	b.policyRevisions = make(map[string]string)
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
		LanguageCode:          stringValue(values, fieldLanguageCode, defaultLanguageCode),
		DataAccessRoleArn:     stringValue(values, "DataAccessRoleArn", ""),
		DocumentClassifierArn: stringValue(values, fieldDocumentClassifierARN, ""),
		EntityRecognizerArn:   stringValue(values, fieldEntityRecognizerARN, ""),
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

// StopJob starts cancellation of an active job. AWS returns InvalidRequestException
// when the job is already in a terminal state (COMPLETED, FAILED, STOPPED, STOP_REQUESTED).
func (b *InMemoryBackend) StopJob(id, jobType string) (*Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	job, ok := b.jobs[id]
	if !ok || job.JobType != jobType {
		return nil, fmt.Errorf("%w: job %q", ErrNotFound, id)
	}

	switch job.JobStatus {
	case statusSubmitted, statusInProgress:
		job.JobStatus = statusStopRequested
		job.stopRequested = true
	default:
		return nil, fmt.Errorf("%w: job %q cannot be stopped in status %s", ErrValidation, id, job.JobStatus)
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
		FlywheelArn:   stringValue(values, fieldFlywheelARN, ""),
		Configuration: cloneMap(values),
	}
	switch resourceType {
	case resourceTypeEndpoint:
		resource.EndpointArn = resourceArn
	case resourceTypeDataset:
		resource.DatasetArn = resourceArn
	}
	b.resources[resourceArn] = resource
	b.tags[resourceArn] = tagsMap(tags)

	return cloneResource(resource), nil
}

// GetResource finds resource by ARN. For classifier and recognizer types, each
// Describe call advances training lifecycle: SUBMITTED → IN_PROGRESS → TRAINED
// (or FAILED when the name contains "[fail]"). This mirrors AWS async training.
func (b *InMemoryBackend) GetResource(resourceArn, resourceType string) (*Resource, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	resource, ok := b.resources[resourceArn]
	if !ok || resource.Type != resourceType {
		return nil, fmt.Errorf("%w: resource %q", ErrNotFound, resourceArn)
	}

	advanceTrainingResource(resource)

	return cloneResource(resource), nil
}

// ListResources returns resources of one type. For classifier and recognizer
// types, listing advances the async training lifecycle one step (mirroring a
// status poll), consistent with how Describe advances it. This lets a
// create→describe→list→delete flow reach a deletable (TRAINED) state.
func (b *InMemoryBackend) ListResources(resourceType string) []*Resource {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make([]*Resource, 0, len(b.resources))
	for _, resource := range b.resources {
		if resource.Type == resourceType {
			advanceTrainingResource(resource)
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

// DeleteResource removes a stored resource and its tags. AWS returns
// ResourceInUseException when deleting a classifier or recognizer that is
// still training (status SUBMITTED or IN_PROGRESS).
func (b *InMemoryBackend) DeleteResource(resourceArn, resourceType string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	resource, ok := b.resources[resourceArn]
	if !ok || resource.Type != resourceType {
		return fmt.Errorf("%w: resource %q", ErrNotFound, resourceArn)
	}

	if isTrainingResourceType(resource.Type) &&
		(resource.Status == statusSubmitted || resource.Status == statusInProgress) {
		return fmt.Errorf(
			"%w: resource %q cannot be deleted in status %s",
			ErrConflict, resourceArn, resource.Status,
		)
	}

	delete(b.resources, resourceArn)
	delete(b.tags, resourceArn)

	return nil
}

// StartFlywheelIteration creates an asynchronous flywheel iteration.
func (b *InMemoryBackend) StartFlywheelIteration(flywheelArn string) (*FlywheelIteration, error) {
	if _, err := b.GetResource(flywheelArn, resourceTypeFlywheel); err != nil {
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
	switch iteration.FlywheelIterationStatus {
	case statusSubmitted:
		iteration.FlywheelIterationStatus = statusInProgress
	case statusInProgress:
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

// PutResourcePolicy saves a resource policy.
func (b *InMemoryBackend) PutResourcePolicy(resourceArn, policy, expectedRevision string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if expectedRevision != "" && b.policyRevisions[resourceArn] != expectedRevision {
		return "", fmt.Errorf("%w: policy revision mismatch", ErrConflict)
	}

	revision := uuid.NewString()
	b.policies[resourceArn] = policy
	b.policyRevisions[resourceArn] = revision

	return revision, nil
}

// GetResourcePolicy retrieves a resource policy.
func (b *InMemoryBackend) GetResourcePolicy(resourceArn string) (string, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	policy, ok := b.policies[resourceArn]
	if !ok {
		return "", "", fmt.Errorf("%w: resource policy not found for %q", ErrNotFound, resourceArn)
	}

	return policy, b.policyRevisions[resourceArn], nil
}

// DeleteResourcePolicy removes a resource policy.
func (b *InMemoryBackend) DeleteResourcePolicy(resourceArn, expectedRevision string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if expectedRevision != "" && b.policyRevisions[resourceArn] != expectedRevision {
		return fmt.Errorf("%w: policy revision mismatch", ErrConflict)
	}
	if _, ok := b.policies[resourceArn]; !ok {
		return fmt.Errorf("%w: resource policy not found for %q", ErrNotFound, resourceArn)
	}

	delete(b.policies, resourceArn)
	delete(b.policyRevisions, resourceArn)

	return nil
}

// StopTrainingResource sets a trainable resource's status to STOPPED.
func (b *InMemoryBackend) StopTrainingResource(resourceArn, resourceType string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	resource, ok := b.resources[resourceArn]
	if !ok || resource.Type != resourceType {
		return fmt.Errorf("%w: resource %q", ErrNotFound, resourceArn)
	}

	if resource.Status == statusSubmitted || resource.Status == statusInProgress {
		resource.Status = statusStopped
		resource.UpdatedAt = time.Now().UTC()
	}

	return nil
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
	case resourceTypeEndpoint:
		return statusActive
	case resourceTypeFlywheel, resourceTypeDataset:
		return statusReady
	case resourceTypeDocClassifier, resourceTypeDocClassifierVersion,
		resourceTypeEntityRecognizer, resourceTypeEntityRecognizerVer:
		// Emulator skips async training; classifiers/recognizers are immediately TRAINED.
		// The real AWS provider waits minutes before polling, causing CI timeouts if we
		// start at SUBMITTED and require multiple poll cycles.
		return statusTrained
	default:
		return statusSubmitted
	}
}

// isTrainingResourceType reports whether rType goes through async training.
func isTrainingResourceType(rType string) bool {
	switch rType {
	case resourceTypeDocClassifier, resourceTypeDocClassifierVersion,
		resourceTypeEntityRecognizer, resourceTypeEntityRecognizerVer:
		return true
	}

	return false
}

// advanceTrainingResource steps a classifier/recognizer one lifecycle state
// forward on each Describe call: SUBMITTED → IN_PROGRESS → TRAINED (or FAILED).
func advanceTrainingResource(resource *Resource) {
	if !isTrainingResourceType(resource.Type) {
		return
	}
	switch resource.Status {
	case statusSubmitted:
		resource.Status = statusInProgress
		resource.UpdatedAt = time.Now().UTC()
	case statusInProgress:
		if strings.Contains(strings.ToLower(resource.Name), failedMarker) {
			resource.Status = statusFailed
		} else {
			resource.Status = statusTrained
		}
		resource.UpdatedAt = time.Now().UTC()
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
	cloned := *job
	cloned.InputDataConfig = cloneMap(job.InputDataConfig)
	cloned.OutputDataConfig = cloneMap(job.OutputDataConfig)
	cloned.TargetEventTypes = append([]string(nil), job.TargetEventTypes...)

	return &cloned
}

func cloneResource(resource *Resource) *Resource {
	cloned := *resource
	cloned.Configuration = cloneMap(resource.Configuration)

	return &cloned
}

func cloneIteration(iteration *FlywheelIteration) *FlywheelIteration {
	cloned := *iteration

	return &cloned
}
