package mediaconvert

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// AddJobTemplateInternal inserts a job template directly into the backend.
func (b *InMemoryBackend) AddJobTemplateInternal(jt *JobTemplate) {
	b.mu.Lock("AddJobTemplateInternal")
	defer b.mu.Unlock()

	b.jobTemplates.Put(jt)
}

// CreateJobTemplate creates a new MediaConvert job template.
func (b *InMemoryBackend) CreateJobTemplate(
	name, description, category, queue string,
	priority int,
	settings map[string]any,
	tags map[string]string,
) (*JobTemplate, error) {
	b.mu.Lock("CreateJobTemplate")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if b.jobTemplates.Has(name) {
		return nil, fmt.Errorf("%w: job template %s already exists", ErrAlreadyExists, name)
	}

	if priority < priorityMin || priority > priorityMax {
		return nil, fmt.Errorf("%w: priority must be between %d and %d", ErrValidation, priorityMin, priorityMax)
	}

	now := epochSeconds(time.Now())
	jt := &JobTemplate{
		Arn:         arn.Build("mediaconvert", b.region, b.accountID, "jobTemplates/"+name),
		Name:        name,
		Description: description,
		Category:    category,
		Queue:       queue,
		Priority:    priority,
		Settings:    deepCloneMap(settings),
		Tags:        nonNilTagsCopy(tags),
		Type:        presetCustom,
		CreatedAt:   now,
		LastUpdated: now,
	}
	b.jobTemplates.Put(jt)

	if len(tags) > 0 {
		b.storeTagsLocked(jt.Arn, tags)
	}

	return cloneJobTemplate(jt), nil
}

// GetJobTemplate returns a job template by name.
func (b *InMemoryBackend) GetJobTemplate(name string) (*JobTemplate, error) {
	b.mu.RLock("GetJobTemplate")
	defer b.mu.RUnlock()

	jt, ok := b.jobTemplates.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: job template %s not found", ErrNotFound, name)
	}

	return cloneJobTemplate(jt), nil
}

// ListJobTemplates returns all job templates sorted by name.
func (b *InMemoryBackend) ListJobTemplates() []*JobTemplate {
	b.mu.RLock("ListJobTemplates")
	defer b.mu.RUnlock()

	list := make([]*JobTemplate, 0, b.jobTemplates.Len())
	for _, jt := range b.jobTemplates.All() {
		list = append(list, cloneJobTemplate(jt))
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

	jt, ok := b.jobTemplates.Get(name)
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
		if *priority < priorityMin || *priority > priorityMax {
			return nil, fmt.Errorf("%w: priority must be between %d and %d", ErrValidation, priorityMin, priorityMax)
		}

		jt.Priority = *priority
	}

	if settings != nil {
		jt.Settings = deepCloneMap(settings)
	}

	jt.LastUpdated = epochSeconds(time.Now())

	return cloneJobTemplate(jt), nil
}

// DeleteJobTemplate removes a job template by name.
func (b *InMemoryBackend) DeleteJobTemplate(name string) error {
	b.mu.Lock("DeleteJobTemplate")
	defer b.mu.Unlock()

	jt, ok := b.jobTemplates.Get(name)
	if !ok {
		return fmt.Errorf("%w: job template %s not found", ErrNotFound, name)
	}
	delete(b.tags, jt.Arn)
	b.jobTemplates.Delete(name)

	return nil
}

func cloneJobTemplate(jt *JobTemplate) *JobTemplate {
	cp := *jt
	cp.Settings = deepCloneMap(jt.Settings)
	cp.Tags = nonNilTagsCopy(jt.Tags)

	return &cp
}
