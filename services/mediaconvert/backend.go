package mediaconvert

import (
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
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrConflict)
)

// epochSeconds converts a [time.Time] to a float64 Unix epoch seconds value,
// which is the format expected by the MediaConvert SDK for timestamp fields.
func epochSeconds(t time.Time) float64 {
	return float64(t.Unix())
}

// Queue represents a MediaConvert queue.
type Queue struct {
	Arn                  string  `json:"arn"`
	Name                 string  `json:"name"`
	Description          string  `json:"description,omitempty"`
	PricingPlan          string  `json:"pricingPlan"`
	Status               string  `json:"status"`
	Type                 string  `json:"type"`
	CreatedAt            float64 `json:"createdAt"`
	LastUpdated          float64 `json:"lastUpdated"`
	ProgressingJobsCount int     `json:"progressingJobsCount"`
	SubmittedJobsCount   int     `json:"submittedJobsCount"`
}

// JobTemplate represents a MediaConvert job template.
type JobTemplate struct {
	Settings    map[string]any `json:"settings,omitempty"`
	Arn         string         `json:"arn"`
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Category    string         `json:"category,omitempty"`
	Queue       string         `json:"queue,omitempty"`
	Type        string         `json:"type"`
	CreatedAt   float64        `json:"createdAt"`
	LastUpdated float64        `json:"lastUpdated"`
	Priority    int            `json:"priority"`
}

// Job represents a MediaConvert transcoding job.
type Job struct {
	Settings    map[string]any `json:"settings,omitempty"`
	Arn         string         `json:"arn"`
	ID          string         `json:"id"`
	Queue       string         `json:"queue,omitempty"`
	Role        string         `json:"role"`
	Status      string         `json:"status"`
	JobTemplate string         `json:"jobTemplate,omitempty"`
	CreatedAt   float64        `json:"createdAt"`
}

// InMemoryBackend is the in-memory store for MediaConvert resources.
type InMemoryBackend struct {
	queues       map[string]*Queue
	jobTemplates map[string]*JobTemplate
	jobs         map[string]*Job
	tags         map[string]map[string]string
	mu           *lockmetrics.RWMutex
	accountID    string
	region       string
}

// NewInMemoryBackend creates a new in-memory MediaConvert backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		queues:       make(map[string]*Queue),
		jobTemplates: make(map[string]*JobTemplate),
		jobs:         make(map[string]*Job),
		tags:         make(map[string]map[string]string),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("mediaconvert"),
	}
}

// Region returns the region configured for this backend.
func (b *InMemoryBackend) Region() string { return b.region }

// CreateQueue creates a new MediaConvert queue.
func (b *InMemoryBackend) CreateQueue(name, description, pricingPlan, status string) (*Queue, error) {
	b.mu.Lock("CreateQueue")
	defer b.mu.Unlock()

	if _, ok := b.queues[name]; ok {
		return nil, fmt.Errorf("%w: queue %s already exists", ErrAlreadyExists, name)
	}

	if pricingPlan == "" {
		pricingPlan = "ON_DEMAND"
	}

	if status == "" {
		status = "ACTIVE"
	}

	now := epochSeconds(time.Now())
	q := &Queue{
		Arn:         arn.Build("mediaconvert", b.region, b.accountID, "queues/"+name),
		Name:        name,
		Description: description,
		PricingPlan: pricingPlan,
		Status:      status,
		Type:        "CUSTOM",
		CreatedAt:   now,
		LastUpdated: now,
	}
	b.queues[name] = q
	cp := *q

	return &cp, nil
}

// GetQueue returns a queue by name.
func (b *InMemoryBackend) GetQueue(name string) (*Queue, error) {
	b.mu.RLock("GetQueue")
	defer b.mu.RUnlock()

	q, ok := b.queues[name]
	if !ok {
		return nil, fmt.Errorf("%w: queue %s not found", ErrNotFound, name)
	}
	cp := *q

	return &cp, nil
}

// ListQueues returns all queues sorted by name.
func (b *InMemoryBackend) ListQueues() []*Queue {
	b.mu.RLock("ListQueues")
	defer b.mu.RUnlock()

	list := make([]*Queue, 0, len(b.queues))
	for _, q := range b.queues {
		cp := *q
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateQueue updates a queue's description and status.
func (b *InMemoryBackend) UpdateQueue(name, description, status string) (*Queue, error) {
	b.mu.Lock("UpdateQueue")
	defer b.mu.Unlock()

	q, ok := b.queues[name]
	if !ok {
		return nil, fmt.Errorf("%w: queue %s not found", ErrNotFound, name)
	}

	if description != "" {
		q.Description = description
	}

	if status != "" {
		q.Status = status
	}

	q.LastUpdated = epochSeconds(time.Now())
	cp := *q

	return &cp, nil
}

// DeleteQueue removes a queue by name.
func (b *InMemoryBackend) DeleteQueue(name string) error {
	b.mu.Lock("DeleteQueue")
	defer b.mu.Unlock()

	if _, ok := b.queues[name]; !ok {
		return fmt.Errorf("%w: queue %s not found", ErrNotFound, name)
	}
	delete(b.queues, name)

	return nil
}

// CreateJobTemplate creates a new MediaConvert job template.
func (b *InMemoryBackend) CreateJobTemplate(
	name, description, category, queue string,
	priority int,
	settings map[string]any,
) (*JobTemplate, error) {
	b.mu.Lock("CreateJobTemplate")
	defer b.mu.Unlock()

	if _, ok := b.jobTemplates[name]; ok {
		return nil, fmt.Errorf("%w: job template %s already exists", ErrAlreadyExists, name)
	}

	now := epochSeconds(time.Now())
	jt := &JobTemplate{
		Arn:         arn.Build("mediaconvert", b.region, b.accountID, "jobTemplates/"+name),
		Name:        name,
		Description: description,
		Category:    category,
		Queue:       queue,
		Priority:    priority,
		Settings:    settings,
		Type:        "CUSTOM",
		CreatedAt:   now,
		LastUpdated: now,
	}
	b.jobTemplates[name] = jt
	cp := *jt

	return &cp, nil
}

// GetJobTemplate returns a job template by name.
func (b *InMemoryBackend) GetJobTemplate(name string) (*JobTemplate, error) {
	b.mu.RLock("GetJobTemplate")
	defer b.mu.RUnlock()

	jt, ok := b.jobTemplates[name]
	if !ok {
		return nil, fmt.Errorf("%w: job template %s not found", ErrNotFound, name)
	}
	cp := *jt

	return &cp, nil
}

// ListJobTemplates returns all job templates sorted by name.
func (b *InMemoryBackend) ListJobTemplates() []*JobTemplate {
	b.mu.RLock("ListJobTemplates")
	defer b.mu.RUnlock()

	list := make([]*JobTemplate, 0, len(b.jobTemplates))
	for _, jt := range b.jobTemplates {
		cp := *jt
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateJobTemplate updates a job template's description, category, queue, priority, and settings.
func (b *InMemoryBackend) UpdateJobTemplate(
	name, description, category, queue string,
	priority *int,
	settings map[string]any,
) (*JobTemplate, error) {
	b.mu.Lock("UpdateJobTemplate")
	defer b.mu.Unlock()

	jt, ok := b.jobTemplates[name]
	if !ok {
		return nil, fmt.Errorf("%w: job template %s not found", ErrNotFound, name)
	}

	if description != "" {
		jt.Description = description
	}

	if category != "" {
		jt.Category = category
	}

	if queue != "" {
		jt.Queue = queue
	}

	if priority != nil {
		jt.Priority = *priority
	}

	if settings != nil {
		jt.Settings = settings
	}

	jt.LastUpdated = epochSeconds(time.Now())
	cp := *jt

	return &cp, nil
}

// DeleteJobTemplate removes a job template by name.
func (b *InMemoryBackend) DeleteJobTemplate(name string) error {
	b.mu.Lock("DeleteJobTemplate")
	defer b.mu.Unlock()

	if _, ok := b.jobTemplates[name]; !ok {
		return fmt.Errorf("%w: job template %s not found", ErrNotFound, name)
	}
	delete(b.jobTemplates, name)

	return nil
}

// CreateJob creates a new MediaConvert job.
func (b *InMemoryBackend) CreateJob(role, queue, jobTemplate string, settings map[string]any) (*Job, error) {
	b.mu.Lock("CreateJob")
	defer b.mu.Unlock()

	id := generateJobID()
	j := &Job{
		Arn:         arn.Build("mediaconvert", b.region, b.accountID, "jobs/"+id),
		ID:          id,
		Role:        role,
		Queue:       queue,
		JobTemplate: jobTemplate,
		Status:      "SUBMITTED",
		Settings:    settings,
		CreatedAt:   epochSeconds(time.Now()),
	}
	b.jobs[id] = j
	cp := *j

	return &cp, nil
}

// GetJob returns a job by ID.
func (b *InMemoryBackend) GetJob(id string) (*Job, error) {
	b.mu.RLock("GetJob")
	defer b.mu.RUnlock()

	j, ok := b.jobs[id]
	if !ok {
		return nil, fmt.Errorf("%w: job %s not found", ErrNotFound, id)
	}
	cp := *j

	return &cp, nil
}

// ListJobs returns all jobs sorted by creation time (newest first).
func (b *InMemoryBackend) ListJobs() []*Job {
	b.mu.RLock("ListJobs")
	defer b.mu.RUnlock()

	list := make([]*Job, 0, len(b.jobs))
	for _, j := range b.jobs {
		cp := *j
		list = append(list, &cp)
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].CreatedAt > list[j].CreatedAt
	})

	return list
}

// CancelJob cancels a job by ID.
func (b *InMemoryBackend) CancelJob(id string) error {
	b.mu.Lock("CancelJob")
	defer b.mu.Unlock()

	j, ok := b.jobs[id]
	if !ok {
		return fmt.Errorf("%w: job %s not found", ErrNotFound, id)
	}
	j.Status = "CANCELED"

	return nil
}

// generateJobID generates a MediaConvert-style job ID.
func generateJobID() string {
	ts := time.Now().UnixMilli()
	suffix := uuid.NewString()[:13]

	return fmt.Sprintf("%d-%s", ts, suffix)
}

// GetTags returns a copy of tags for the given resource ARN.
func (b *InMemoryBackend) GetTags(resourceARN string) map[string]string {
	b.mu.RLock("GetTags")
	defer b.mu.RUnlock()

	t := b.tags[resourceARN]
	cp := make(map[string]string, len(t))

	maps.Copy(cp, t)

	return cp
}

// TagResource adds or updates tags for the given resource ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)
}

// UntagResource removes the specified tag keys from the resource ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}
}
