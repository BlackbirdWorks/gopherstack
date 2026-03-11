package elastictranscoder

import (
	"fmt"
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
)

// Pipeline represents an Elastic Transcoder pipeline.
type Pipeline struct {
	CreationTime time.Time `json:"creationTime,omitzero"`
	Name         string    `json:"Name"`
	ID           string    `json:"Id"`
	ARN          string    `json:"Arn"`
	InputBucket  string    `json:"InputBucket"`
	OutputBucket string    `json:"OutputBucket,omitempty"`
	Role         string    `json:"Role"`
	Status       string    `json:"Status"`
	AccountID    string    `json:"accountId,omitempty"`
	Region       string    `json:"region,omitempty"`
}

// Preset represents an Elastic Transcoder preset.
type Preset struct {
	Name        string `json:"Name"`
	ID          string `json:"Id"`
	ARN         string `json:"Arn"`
	Description string `json:"Description,omitempty"`
	Container   string `json:"Container"`
	Type        string `json:"Type"`
}

// Job represents an Elastic Transcoder job.
type Job struct {
	CreationTime time.Time `json:"creationTime,omitzero"`
	ID           string    `json:"Id"`
	ARN          string    `json:"Arn"`
	PipelineID   string    `json:"PipelineId"`
	Status       string    `json:"Status"`
}

// InMemoryBackend is the in-memory store for Elastic Transcoder resources.
type InMemoryBackend struct {
	pipelines map[string]*Pipeline
	presets   map[string]*Preset
	jobs      map[string]*Job
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new in-memory Elastic Transcoder backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		pipelines: make(map[string]*Pipeline),
		presets:   make(map[string]*Preset),
		jobs:      make(map[string]*Job),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("elastictranscoder"),
	}
}

// Region returns the region configured for this backend.
func (b *InMemoryBackend) Region() string { return b.region }

// CreatePipeline creates a new pipeline.
func (b *InMemoryBackend) CreatePipeline(name, inputBucket, outputBucket, role string) (*Pipeline, error) {
	b.mu.Lock("CreatePipeline")
	defer b.mu.Unlock()

	for _, existing := range b.pipelines {
		if existing.Name == name {
			return nil, fmt.Errorf("%w: pipeline %s already exists", ErrAlreadyExists, name)
		}
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
	}
	b.pipelines[id] = p
	cp := *p

	return &cp, nil
}

// ReadPipeline returns a pipeline by ID.
func (b *InMemoryBackend) ReadPipeline(id string) (*Pipeline, error) {
	b.mu.RLock("ReadPipeline")
	defer b.mu.RUnlock()

	p, ok := b.pipelines[id]
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %s not found", ErrNotFound, id)
	}
	cp := *p

	return &cp, nil
}

// ListPipelines returns all pipelines.
func (b *InMemoryBackend) ListPipelines() []*Pipeline {
	b.mu.RLock("ListPipelines")
	defer b.mu.RUnlock()

	list := make([]*Pipeline, 0, len(b.pipelines))
	for _, p := range b.pipelines {
		cp := *p
		list = append(list, &cp)
	}

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

	if name != "" {
		p.Name = name
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

	cp := *p

	return &cp, nil
}

// DeletePipeline removes a pipeline by ID.
func (b *InMemoryBackend) DeletePipeline(id string) error {
	b.mu.Lock("DeletePipeline")
	defer b.mu.Unlock()

	if _, ok := b.pipelines[id]; !ok {
		return fmt.Errorf("%w: pipeline %s not found", ErrNotFound, id)
	}
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
	}
	b.presets[id] = p
	cp := *p

	return &cp, nil
}

// ReadPreset returns a preset by ID.
func (b *InMemoryBackend) ReadPreset(id string) (*Preset, error) {
	b.mu.RLock("ReadPreset")
	defer b.mu.RUnlock()

	p, ok := b.presets[id]
	if !ok {
		return nil, fmt.Errorf("%w: preset %s not found", ErrNotFound, id)
	}
	cp := *p

	return &cp, nil
}

// ListPresets returns all presets.
func (b *InMemoryBackend) ListPresets() []*Preset {
	b.mu.RLock("ListPresets")
	defer b.mu.RUnlock()

	list := make([]*Preset, 0, len(b.presets))
	for _, p := range b.presets {
		cp := *p
		list = append(list, &cp)
	}

	return list
}

// DeletePreset removes a preset by ID.
func (b *InMemoryBackend) DeletePreset(id string) error {
	b.mu.Lock("DeletePreset")
	defer b.mu.Unlock()

	if _, ok := b.presets[id]; !ok {
		return fmt.Errorf("%w: preset %s not found", ErrNotFound, id)
	}
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
	}
	b.jobs[id] = j
	cp := *j

	return &cp, nil
}

// ReadJob returns a job by ID.
func (b *InMemoryBackend) ReadJob(id string) (*Job, error) {
	b.mu.RLock("ReadJob")
	defer b.mu.RUnlock()

	j, ok := b.jobs[id]
	if !ok {
		return nil, fmt.Errorf("%w: job %s not found", ErrNotFound, id)
	}
	cp := *j

	return &cp, nil
}

// ListJobsByPipeline returns all jobs for a given pipeline.
func (b *InMemoryBackend) ListJobsByPipeline(pipelineID string) []*Job {
	b.mu.RLock("ListJobsByPipeline")
	defer b.mu.RUnlock()

	list := make([]*Job, 0)
	for _, j := range b.jobs {
		if j.PipelineID == pipelineID {
			cp := *j
			list = append(list, &cp)
		}
	}

	return list
}

// CancelJob removes a job by ID.
func (b *InMemoryBackend) CancelJob(id string) error {
	b.mu.Lock("CancelJob")
	defer b.mu.Unlock()

	if _, ok := b.jobs[id]; !ok {
		return fmt.Errorf("%w: job %s not found", ErrNotFound, id)
	}
	delete(b.jobs, id)

	return nil
}
