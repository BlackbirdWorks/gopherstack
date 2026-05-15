package elastictranscoder

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ResourceInUseException", awserr.ErrConflict)
	// ErrValidation is returned when request parameters fail validation.
	ErrValidation = errors.New("ValidationException")
)

// PipelineNotifications holds SNS topic ARNs for pipeline event notifications.
type PipelineNotifications struct {
	Completed   string `json:"Completed,omitempty"`
	Error       string `json:"Error,omitempty"`
	Progressing string `json:"Progressing,omitempty"`
	Warning     string `json:"Warning,omitempty"`
}

// Pipeline represents an Elastic Transcoder pipeline.
type Pipeline struct {
	CreationTime  time.Time              `json:"creationTime,omitzero"`
	Tags          map[string]string      `json:"Tags,omitempty"`
	Notifications *PipelineNotifications `json:"Notifications,omitempty"`
	Name          string                 `json:"Name"`
	ID            string                 `json:"Id"`
	ARN           string                 `json:"Arn"`
	InputBucket   string                 `json:"InputBucket"`
	OutputBucket  string                 `json:"OutputBucket,omitempty"`
	Role          string                 `json:"Role"`
	Status        string                 `json:"Status"`
	AccountID     string                 `json:"accountId,omitempty"`
	Region        string                 `json:"region,omitempty"`
}

// TestRoleResult holds the result of a TestRole operation.
type TestRoleResult struct {
	Success  string   `json:"Success"`
	Messages []string `json:"Messages"`
}

// Preset represents an Elastic Transcoder preset.
type Preset struct {
	Tags        map[string]string `json:"Tags,omitempty"`
	Name        string            `json:"Name"`
	ID          string            `json:"Id"`
	ARN         string            `json:"Arn"`
	Description string            `json:"Description,omitempty"`
	Container   string            `json:"Container"`
	Type        string            `json:"Type"`
}

// Job represents an Elastic Transcoder job.
type Job struct {
	CreationTime time.Time         `json:"creationTime,omitzero"`
	Tags         map[string]string `json:"Tags,omitempty"`
	ID           string            `json:"Id"`
	ARN          string            `json:"Arn"`
	PipelineID   string            `json:"PipelineId"`
	Status       string            `json:"Status"`
}

// InMemoryBackend is the in-memory store for Elastic Transcoder resources.
type InMemoryBackend struct {
	pipelines       map[string]*Pipeline
	presets         map[string]*Preset
	jobs            map[string]*Job
	pipelinesByName map[string]string // pipeline name → ID
	pipelinesByARN  map[string]string // pipeline ARN → ID
	presetsByARN    map[string]string // preset ARN → ID
	jobsByARN       map[string]string // job ARN → ID
	mu              *lockmetrics.RWMutex
	accountID       string
	region          string
}

// NewInMemoryBackend creates a new in-memory Elastic Transcoder backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		pipelines:       make(map[string]*Pipeline),
		presets:         make(map[string]*Preset),
		jobs:            make(map[string]*Job),
		pipelinesByName: make(map[string]string),
		pipelinesByARN:  make(map[string]string),
		presetsByARN:    make(map[string]string),
		jobsByARN:       make(map[string]string),
		accountID:       accountID,
		region:          region,
		mu:              lockmetrics.New("elastictranscoder"),
	}
}

// pipelineCopy returns a deep copy of a Pipeline, duplicating all pointer/map fields.
func pipelineCopy(p *Pipeline) *Pipeline {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)

	if p.Notifications != nil {
		n := *p.Notifications
		cp.Notifications = &n
	}

	return &cp
}

// presetCopy returns a deep copy of a Preset.
func presetCopy(p *Preset) *Preset {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)

	return &cp
}

// jobCopy returns a deep copy of a Job.
func jobCopy(j *Job) *Job {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)

	return &cp
}

// sortedTagKeys returns the keys of a string map in sorted order.
func sortedTagKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return keys
}

// Reset clears all backend state, returning the backend to an empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.pipelines = make(map[string]*Pipeline)
	b.presets = make(map[string]*Preset)
	b.jobs = make(map[string]*Job)
	b.pipelinesByName = make(map[string]string)
	b.pipelinesByARN = make(map[string]string)
	b.presetsByARN = make(map[string]string)
	b.jobsByARN = make(map[string]string)
}

// AddPipelineInternal directly inserts a pipeline into the backend.
// It is intended for use in tests and should not be called from production code.
func (b *InMemoryBackend) AddPipelineInternal(p *Pipeline) {
	b.mu.Lock("AddPipelineInternal")
	defer b.mu.Unlock()

	cp := pipelineCopy(p)
	b.pipelines[cp.ID] = cp
	b.pipelinesByName[cp.Name] = cp.ID
	b.pipelinesByARN[cp.ARN] = cp.ID
}

// AddPresetInternal directly inserts a preset into the backend.
// It is intended for use in tests and should not be called from production code.
func (b *InMemoryBackend) AddPresetInternal(p *Preset) {
	b.mu.Lock("AddPresetInternal")
	defer b.mu.Unlock()

	cp := presetCopy(p)
	b.presets[cp.ID] = cp
	b.presetsByARN[cp.ARN] = cp.ID
}

// AddJobInternal directly inserts a job into the backend.
// It is intended for use in tests and should not be called from production code.
func (b *InMemoryBackend) AddJobInternal(j *Job) {
	b.mu.Lock("AddJobInternal")
	defer b.mu.Unlock()

	cp := jobCopy(j)
	b.jobs[cp.ID] = cp
	b.jobsByARN[cp.ARN] = cp.ID
}

// Region returns the region configured for this backend.
func (b *InMemoryBackend) Region() string { return b.region }

// CreatePipeline creates a new pipeline.
func (b *InMemoryBackend) CreatePipeline(name, inputBucket, outputBucket, role string) (*Pipeline, error) {
	b.mu.Lock("CreatePipeline")
	defer b.mu.Unlock()

	if _, exists := b.pipelinesByName[name]; exists {
		return nil, fmt.Errorf("%w: pipeline %s already exists", ErrAlreadyExists, name)
	}

	id := uuid.NewString()
	pipelineARN := arn.Build("elastictranscoder", b.region, b.accountID, "pipeline/"+id)
	p := &Pipeline{
		ID:           id,
		ARN:          pipelineARN,
		Name:         name,
		InputBucket:  inputBucket,
		OutputBucket: outputBucket,
		Role:         role,
		Status:       "Active",
		AccountID:    b.accountID,
		Region:       b.region,
		CreationTime: time.Now().UTC(),
		Tags:         make(map[string]string),
	}
	b.pipelines[id] = p
	b.pipelinesByName[name] = id
	b.pipelinesByARN[pipelineARN] = id

	return pipelineCopy(p), nil
}

// ReadPipeline returns a pipeline by ID.
func (b *InMemoryBackend) ReadPipeline(id string) (*Pipeline, error) {
	b.mu.RLock("ReadPipeline")
	defer b.mu.RUnlock()

	p, ok := b.pipelines[id]
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %s not found", ErrNotFound, id)
	}

	return pipelineCopy(p), nil
}

// ListPipelines returns all pipelines sorted by name.
func (b *InMemoryBackend) ListPipelines() []*Pipeline {
	b.mu.RLock("ListPipelines")
	defer b.mu.RUnlock()

	list := make([]*Pipeline, 0, len(b.pipelines))
	for _, p := range b.pipelines {
		list = append(list, pipelineCopy(p))
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdatePipeline updates a pipeline's name, input bucket, output bucket, and role.
func (b *InMemoryBackend) UpdatePipeline(id, name, inputBucket, outputBucket, role string) (*Pipeline, error) {
	b.mu.Lock("UpdatePipeline")
	defer b.mu.Unlock()

	p, ok := b.pipelines[id]
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %s not found", ErrNotFound, id)
	}

	if name != "" && name != p.Name {
		if _, exists := b.pipelinesByName[name]; exists {
			return nil, fmt.Errorf("%w: pipeline name %s already exists", ErrAlreadyExists, name)
		}

		delete(b.pipelinesByName, p.Name)
		p.Name = name
		b.pipelinesByName[name] = id
	}

	if inputBucket != "" {
		p.InputBucket = inputBucket
	}

	if outputBucket != "" {
		p.OutputBucket = outputBucket
	}

	if role != "" {
		p.Role = role
	}

	return pipelineCopy(p), nil
}

// DeletePipeline removes a pipeline by ID.
func (b *InMemoryBackend) DeletePipeline(id string) error {
	b.mu.Lock("DeletePipeline")
	defer b.mu.Unlock()

	p, ok := b.pipelines[id]
	if !ok {
		return fmt.Errorf("%w: pipeline %s not found", ErrNotFound, id)
	}
	delete(b.pipelinesByName, p.Name)
	delete(b.pipelinesByARN, p.ARN)
	delete(b.pipelines, id)

	return nil
}

// CreatePreset creates a new preset.
func (b *InMemoryBackend) CreatePreset(name, description, container string) (*Preset, error) {
	b.mu.Lock("CreatePreset")
	defer b.mu.Unlock()

	id := uuid.NewString()
	presetARN := arn.Build("elastictranscoder", b.region, b.accountID, "preset/"+id)
	p := &Preset{
		ID:          id,
		ARN:         presetARN,
		Name:        name,
		Description: description,
		Container:   container,
		Type:        "Custom",
		Tags:        make(map[string]string),
	}
	b.presets[id] = p
	b.presetsByARN[presetARN] = id

	return presetCopy(p), nil
}

// ReadPreset returns a preset by ID.
func (b *InMemoryBackend) ReadPreset(id string) (*Preset, error) {
	b.mu.RLock("ReadPreset")
	defer b.mu.RUnlock()

	p, ok := b.presets[id]
	if !ok {
		return nil, fmt.Errorf("%w: preset %s not found", ErrNotFound, id)
	}

	return presetCopy(p), nil
}

// ListPresets returns all presets sorted by name.
func (b *InMemoryBackend) ListPresets() []*Preset {
	b.mu.RLock("ListPresets")
	defer b.mu.RUnlock()

	list := make([]*Preset, 0, len(b.presets))
	for _, p := range b.presets {
		list = append(list, presetCopy(p))
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// DeletePreset removes a preset by ID.
func (b *InMemoryBackend) DeletePreset(id string) error {
	b.mu.Lock("DeletePreset")
	defer b.mu.Unlock()

	p, ok := b.presets[id]
	if !ok {
		return fmt.Errorf("%w: preset %s not found", ErrNotFound, id)
	}
	delete(b.presetsByARN, p.ARN)
	delete(b.presets, id)

	return nil
}

// CreateJob creates a new transcoding job on the given pipeline.
func (b *InMemoryBackend) CreateJob(pipelineID string) (*Job, error) {
	b.mu.Lock("CreateJob")
	defer b.mu.Unlock()

	if _, ok := b.pipelines[pipelineID]; !ok {
		return nil, fmt.Errorf("%w: pipeline %s not found", ErrNotFound, pipelineID)
	}

	id := uuid.NewString()
	jobARN := arn.Build("elastictranscoder", b.region, b.accountID, "job/"+id)
	j := &Job{
		ID:           id,
		ARN:          jobARN,
		PipelineID:   pipelineID,
		Status:       "Progressing",
		CreationTime: time.Now().UTC(),
		Tags:         make(map[string]string),
	}
	b.jobs[id] = j
	b.jobsByARN[jobARN] = id

	return jobCopy(j), nil
}

// ReadJob returns a job by ID.
func (b *InMemoryBackend) ReadJob(id string) (*Job, error) {
	b.mu.RLock("ReadJob")
	defer b.mu.RUnlock()

	j, ok := b.jobs[id]
	if !ok {
		return nil, fmt.Errorf("%w: job %s not found", ErrNotFound, id)
	}

	return jobCopy(j), nil
}

// ListJobsByPipeline returns all jobs for a given pipeline sorted by ID.
func (b *InMemoryBackend) ListJobsByPipeline(pipelineID string) []*Job {
	b.mu.RLock("ListJobsByPipeline")
	defer b.mu.RUnlock()

	list := make([]*Job, 0, len(b.jobs))
	for _, j := range b.jobs {
		if j.PipelineID == pipelineID {
			list = append(list, jobCopy(j))
		}
	}
	sort.Slice(list, func(i, k int) bool { return list[i].ID < list[k].ID })

	return list
}

// CancelJob removes a job by ID.
func (b *InMemoryBackend) CancelJob(id string) error {
	b.mu.Lock("CancelJob")
	defer b.mu.Unlock()

	j, ok := b.jobs[id]
	if !ok {
		return fmt.Errorf("%w: job %s not found", ErrNotFound, id)
	}
	delete(b.jobsByARN, j.ARN)
	delete(b.jobs, id)

	return nil
}

// AddTagsToResource adds or updates tags on a resource identified by ARN.
func (b *InMemoryBackend) AddTagsToResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("AddTagsToResource")
	defer b.mu.Unlock()

	if id, ok := b.pipelinesByARN[resourceARN]; ok {
		if b.pipelines[id].Tags == nil {
			b.pipelines[id].Tags = make(map[string]string)
		}
		maps.Copy(b.pipelines[id].Tags, tags)

		return nil
	}

	if id, ok := b.presetsByARN[resourceARN]; ok {
		if b.presets[id].Tags == nil {
			b.presets[id].Tags = make(map[string]string)
		}
		maps.Copy(b.presets[id].Tags, tags)

		return nil
	}

	if id, ok := b.jobsByARN[resourceARN]; ok {
		if b.jobs[id].Tags == nil {
			b.jobs[id].Tags = make(map[string]string)
		}
		maps.Copy(b.jobs[id].Tags, tags)

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// RemoveTagsFromResource removes tag keys from a resource identified by ARN.
func (b *InMemoryBackend) RemoveTagsFromResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("RemoveTagsFromResource")
	defer b.mu.Unlock()

	if id, ok := b.pipelinesByARN[resourceARN]; ok {
		for _, k := range tagKeys {
			delete(b.pipelines[id].Tags, k)
		}

		return nil
	}

	if id, ok := b.presetsByARN[resourceARN]; ok {
		for _, k := range tagKeys {
			delete(b.presets[id].Tags, k)
		}

		return nil
	}

	if id, ok := b.jobsByARN[resourceARN]; ok {
		for _, k := range tagKeys {
			delete(b.jobs[id].Tags, k)
		}

		return nil
	}

	return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// ListJobsByStatus returns all jobs with the given status sorted by ID.
func (b *InMemoryBackend) ListJobsByStatus(status string) []*Job {
	b.mu.RLock("ListJobsByStatus")
	defer b.mu.RUnlock()

	list := make([]*Job, 0, len(b.jobs))
	for _, j := range b.jobs {
		if j.Status == status {
			list = append(list, jobCopy(j))
		}
	}
	sort.Slice(list, func(i, k int) bool { return list[i].ID < list[k].ID })

	return list
}

// TestRole verifies that the given role can access the specified buckets.
// In this in-memory implementation it always returns success.
func (b *InMemoryBackend) TestRole(role, inputBucket, _ string) (*TestRoleResult, error) {
	if role == "" || inputBucket == "" {
		return nil, fmt.Errorf("%w: Role and InputBucket are required", ErrValidation)
	}

	return &TestRoleResult{
		Messages: []string{},
		Success:  "true",
	}, nil
}

// UpdatePipelineNotifications updates the SNS notification settings for a pipeline.
func (b *InMemoryBackend) UpdatePipelineNotifications(
	id string,
	notifications *PipelineNotifications,
) (*Pipeline, error) {
	b.mu.Lock("UpdatePipelineNotifications")
	defer b.mu.Unlock()

	p, ok := b.pipelines[id]
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %s not found", ErrNotFound, id)
	}

	p.Notifications = notifications

	return pipelineCopy(p), nil
}

// UpdatePipelineStatus updates the active/paused status of a pipeline.
// Valid values are "Active" and "Paused".
func (b *InMemoryBackend) UpdatePipelineStatus(id, status string) (*Pipeline, error) {
	b.mu.Lock("UpdatePipelineStatus")
	defer b.mu.Unlock()

	p, ok := b.pipelines[id]
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %s not found", ErrNotFound, id)
	}

	if status != "Active" && status != "Paused" {
		return nil, fmt.Errorf("%w: Status must be Active or Paused, got %q", ErrValidation, status)
	}

	p.Status = status

	return pipelineCopy(p), nil
}

// ListTagsForResource returns the tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if id, ok := b.pipelinesByARN[resourceARN]; ok {
		return maps.Clone(b.pipelines[id].Tags), nil
	}

	if id, ok := b.presetsByARN[resourceARN]; ok {
		return maps.Clone(b.presets[id].Tags), nil
	}

	if id, ok := b.jobsByARN[resourceARN]; ok {
		return maps.Clone(b.jobs[id].Tags), nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}
