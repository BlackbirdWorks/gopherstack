package iotanalytics

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/google/uuid"
)

// clonePipelineActivities deep-copies a slice of PipelineActivity.
func clonePipelineActivities(activities []PipelineActivity) []PipelineActivity {
	if activities == nil {
		return nil
	}

	cp := make([]PipelineActivity, len(activities))
	copy(cp, activities)

	return cp
}

// clonePipeline returns a deep copy of p.
func clonePipeline(p *Pipeline) *Pipeline {
	cp := *p
	cp.Tags = make(map[string]string, len(p.Tags))
	maps.Copy(cp.Tags, p.Tags)
	cp.Activities = clonePipelineActivities(p.Activities)

	if len(p.Reprocessings) > 0 {
		cp.Reprocessings = make(map[string]*PipelineReprocessing, len(p.Reprocessings))
		for k, v := range p.Reprocessings {
			rpCp := *v
			cp.Reprocessings[k] = &rpCp
		}
	} else {
		cp.Reprocessings = make(map[string]*PipelineReprocessing)
	}

	return &cp
}

// reprocessingSummariesSorted returns reprocessing summaries sorted by creation time ascending.
func reprocessingSummariesSorted(reprocessings map[string]*PipelineReprocessing) []pipelineReprocessingSummary {
	if len(reprocessings) == 0 {
		return nil
	}

	summaries := make([]pipelineReprocessingSummary, 0, len(reprocessings))

	for _, rp := range reprocessings {
		summaries = append(summaries, pipelineReprocessingSummary{
			ID:           rp.ID,
			Status:       rp.Status,
			CreationTime: rp.CreationTime,
			StartTime:    rp.StartTime,
			EndTime:      rp.EndTime,
		})
	}

	slices.SortFunc(summaries, func(a, b pipelineReprocessingSummary) int {
		return cmp.Compare(a.CreationTime, b.CreationTime)
	})

	return summaries
}

// CreatePipeline creates a new IoT Analytics pipeline.
func (b *InMemoryBackend) CreatePipeline(
	ctx context.Context,
	name string,
	tags map[string]string,
	activities []PipelineActivity,
) (*Pipeline, error) {
	if err := validateResourceName(name); err != nil {
		return nil, err
	}

	b.mu.Lock("CreatePipeline")
	defer b.mu.Unlock()

	if b.pipelines.Has(name) {
		return nil, ErrAlreadyExists
	}

	now := epochSeconds(time.Now())
	arn := resourceARN(ctx, "pipeline", name)
	p := &Pipeline{
		Name:          name,
		ARN:           arn,
		CreationTime:  now,
		LastUpdate:    now,
		Tags:          make(map[string]string),
		Reprocessings: make(map[string]*PipelineReprocessing),
		Activities:    clonePipelineActivities(activities),
	}
	maps.Copy(p.Tags, tags)
	b.pipelines.Put(p)
	b.tags[arn] = make(map[string]string)
	maps.Copy(b.tags[arn], tags)

	return clonePipeline(p), nil
}

// DescribePipeline returns pipeline metadata.
func (b *InMemoryBackend) DescribePipeline(name string) (*Pipeline, error) {
	b.mu.RLock("DescribePipeline")
	defer b.mu.RUnlock()

	p, ok := b.pipelines.Get(name)
	if !ok {
		return nil, ErrPipelineNotFound
	}

	return clonePipeline(p), nil
}

// UpdatePipeline updates a pipeline's activities and last update time.
func (b *InMemoryBackend) UpdatePipeline(name string, activities []PipelineActivity) error {
	b.mu.Lock("UpdatePipeline")
	defer b.mu.Unlock()

	p, ok := b.pipelines.Get(name)
	if !ok {
		return ErrPipelineNotFound
	}

	p.LastUpdate = epochSeconds(time.Now())

	if activities != nil {
		p.Activities = clonePipelineActivities(activities)
	}

	return nil
}

// DeletePipeline deletes a pipeline.
func (b *InMemoryBackend) DeletePipeline(name string) error {
	b.mu.Lock("DeletePipeline")
	defer b.mu.Unlock()

	p, ok := b.pipelines.Get(name)
	if !ok {
		return ErrPipelineNotFound
	}

	delete(b.tags, p.ARN)
	b.pipelines.Delete(name)

	return nil
}

// ListPipelines returns all pipelines sorted by name.
func (b *InMemoryBackend) ListPipelines() []*Pipeline {
	b.mu.RLock("ListPipelines")
	defer b.mu.RUnlock()

	items := b.pipelines.Snapshot()
	result := make([]*Pipeline, 0, len(items))

	for _, p := range items {
		result = append(result, clonePipeline(p))
	}

	return result
}

// AddPipelineInternal seeds a pipeline by name (test helper).
func (b *InMemoryBackend) AddPipelineInternal(name string) *Pipeline {
	p, _ := b.CreatePipeline(b.svcCtx, name, nil, nil)

	return p
}

// StartPipelineReprocessing creates a new reprocessing job for a pipeline.
// Optional startTime and endTime define the message window to reprocess.
func (b *InMemoryBackend) StartPipelineReprocessing(pipelineName string, startTime, endTime *float64) (string, error) {
	if startTime != nil && endTime != nil && *startTime >= *endTime {
		return "", fmt.Errorf("%w: startTime must be before endTime", ErrValidation)
	}

	b.mu.Lock("StartPipelineReprocessing")
	defer b.mu.Unlock()

	p, ok := b.pipelines.Get(pipelineName)
	if !ok {
		return "", ErrPipelineNotFound
	}

	if len(p.Reprocessings) >= maxPipelineReprocessings {
		return "", fmt.Errorf("%w: pipeline reprocessing limit (%d) exceeded", ErrValidation, maxPipelineReprocessings)
	}

	id := uuid.NewString()
	now := epochSeconds(time.Now())

	rp := &PipelineReprocessing{
		ID:           id,
		Status:       "RUNNING",
		CreationTime: now,
	}

	if startTime != nil {
		rp.StartTime = *startTime
	}

	if endTime != nil {
		rp.EndTime = *endTime
	}

	if p.Reprocessings == nil {
		p.Reprocessings = make(map[string]*PipelineReprocessing)
	}

	p.Reprocessings[id] = rp

	return id, nil
}

// CancelPipelineReprocessing cancels a running pipeline reprocessing job.
func (b *InMemoryBackend) CancelPipelineReprocessing(pipelineName, reprocessingID string) error {
	b.mu.Lock("CancelPipelineReprocessing")
	defer b.mu.Unlock()

	p, ok := b.pipelines.Get(pipelineName)
	if !ok {
		return ErrPipelineNotFound
	}

	rp, ok := p.Reprocessings[reprocessingID]
	if !ok {
		return ErrReprocessingNotFound
	}

	if rp.Status == "CANCELLED" {
		return fmt.Errorf("%w: reprocessing job is already cancelled", ErrValidation)
	}

	rp.Status = "CANCELLED"
	rp.EndTime = epochSeconds(time.Now())

	return nil
}

// RunPipelineActivity runs payloads through a pipeline activity and returns the results.
// The in-memory implementation returns payloads unchanged (pass-through).
func (b *InMemoryBackend) RunPipelineActivity(payloads [][]byte) ([][]byte, error) {
	result := make([][]byte, len(payloads))
	copy(result, payloads)

	return result, nil
}
