package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrPipelineNotFound is returned when a pipeline does not exist.
	ErrPipelineNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrPipelineAlreadyExists is returned when a pipeline already exists.
	ErrPipelineAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrPipelineExecutionNotFound is returned when a pipeline execution does not exist.
	ErrPipelineExecutionNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
)

// ParallelismConfiguration limits concurrent steps in a pipeline execution.
type ParallelismConfiguration struct {
	MaxParallelExecutionSteps int32 `json:"MaxParallelExecutionSteps,omitempty"`
}

// PipelineParameter is a name/value pair passed to StartPipelineExecution.
type PipelineParameter struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

// Pipeline represents a SageMaker Pipeline.
type Pipeline struct {
	CreationTime             time.Time                 `json:"CreationTime"`
	LastModifiedTime         time.Time                 `json:"LastModifiedTime"`
	Tags                     map[string]string         `json:"Tags,omitempty"`
	ParallelismConfiguration *ParallelismConfiguration `json:"ParallelismConfiguration,omitempty"`
	PipelineName             string                    `json:"PipelineName"`
	PipelineArn              string                    `json:"PipelineArn"`
	PipelineStatus           string                    `json:"PipelineStatus"`
	PipelineDefinition       string                    `json:"PipelineDefinition,omitempty"`
	PipelineDisplayName      string                    `json:"PipelineDisplayName,omitempty"`
	PipelineDescription      string                    `json:"PipelineDescription,omitempty"`
	RoleArn                  string                    `json:"RoleArn,omitempty"`
}

func clonePipeline(p *Pipeline) *Pipeline {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)

	return &cp
}

// SelectedStep names one step to run in a SelectiveExecutionConfig.
type SelectedStep struct {
	StepName string `json:"StepName"`
}

// SelectiveExecutionConfig restricts a pipeline execution to a subset of steps.
type SelectiveExecutionConfig struct {
	SourcePipelineExecutionArn string         `json:"SourcePipelineExecutionArn,omitempty"`
	SelectedSteps              []SelectedStep `json:"SelectedSteps"`
}

// PipelineExecution represents a single execution of a SageMaker Pipeline.
type PipelineExecution struct {
	StartTime                    time.Time                 `json:"StartTime"`
	ParallelismConfiguration     *ParallelismConfiguration `json:"ParallelismConfiguration,omitempty"`
	SelectiveExecutionConfig     *SelectiveExecutionConfig `json:"SelectiveExecutionConfig,omitempty"`
	PipelineArn                  string                    `json:"PipelineArn"`
	PipelineExecutionArn         string                    `json:"PipelineExecutionArn"`
	PipelineExecutionStatus      string                    `json:"PipelineExecutionStatus"`
	PipelineExecutionDisplayName string                    `json:"PipelineExecutionDisplayName,omitempty"`
	PipelineExecutionDescription string                    `json:"PipelineExecutionDescription,omitempty"`
	FailureReason                string                    `json:"FailureReason,omitempty"`
	// PipelineDefinition is the JSON pipeline definition snapshot captured from
	// the parent pipeline when this execution started, returned verbatim by
	// DescribePipelineDefinitionForExecution.
	PipelineDefinition string              `json:"PipelineDefinition,omitempty"`
	PipelineParameters []PipelineParameter `json:"PipelineParameters,omitempty"`
	PipelineVersionID  int64               `json:"PipelineVersionId,omitempty"`
}

func clonePipelineExecution(pe *PipelineExecution) *PipelineExecution {
	cp := *pe
	cp.PipelineParameters = make([]PipelineParameter, len(pe.PipelineParameters))
	copy(cp.PipelineParameters, pe.PipelineParameters)

	if pe.ParallelismConfiguration != nil {
		pc := *pe.ParallelismConfiguration
		cp.ParallelismConfiguration = &pc
	}

	if pe.SelectiveExecutionConfig != nil {
		sec := *pe.SelectiveExecutionConfig
		sec.SelectedSteps = append([]SelectedStep(nil), pe.SelectiveExecutionConfig.SelectedSteps...)
		cp.SelectiveExecutionConfig = &sec
	}

	return &cp
}

// CreatePipeline creates a new pipeline.
func (b *InMemoryBackend) CreatePipeline(
	ctx context.Context,
	name, definition, roleArn string,
	tags map[string]string,
) (*Pipeline, error) {
	b.mu.Lock("CreatePipeline")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.pipelinesStore(region).Get(name); ok {
		return nil, fmt.Errorf("%w: pipeline %s already exists", ErrPipelineAlreadyExists, name)
	}

	pArn := arn.Build("sagemaker", region, b.accountID, "pipeline/"+name)
	now := time.Now()

	p := &Pipeline{
		PipelineName:       name,
		PipelineArn:        pArn,
		PipelineStatus:     "Active",
		PipelineDefinition: definition,
		RoleArn:            roleArn,
		CreationTime:       now,
		LastModifiedTime:   now,
		Tags:               mergeTags(nil, tags),
	}
	b.pipelinesStore(region).Put(p)

	return clonePipeline(p), nil
}

// DescribePipeline returns a pipeline by name. If versionID is non-zero, the
// returned Pipeline's PipelineDefinition reflects that specific historical
// version instead of the current one (DescribePipelineInput.PipelineVersionId,
// api_op_DescribePipeline.go). lastRunTime is the StartTime of the most
// recent PipelineExecution for this pipeline (DescribePipelineOutput.LastRunTime),
// or the zero time if the pipeline has never been run.
func (b *InMemoryBackend) DescribePipeline(
	ctx context.Context, name string, versionID int64,
) (*Pipeline, time.Time, error) {
	b.mu.RLock("DescribePipeline")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelinesStoreRO(region).Get(name)
	if !ok {
		return nil, time.Time{}, fmt.Errorf("%w: pipeline %q not found", ErrPipelineNotFound, name)
	}

	result := clonePipeline(p)

	if versionID != 0 {
		v, found := findPipelineVersion(b.pipelineVersionsStoreRO(region)[name], versionID)
		if !found {
			return nil, time.Time{}, fmt.Errorf(
				"%w: pipeline %q version %d not found", ErrPipelineNotFound, name, versionID,
			)
		}

		result.PipelineDefinition = v.PipelineDefinition
	}

	var lastRunTime time.Time

	for _, pe := range b.pipelineExecutionsStoreRO(region).All() {
		if pe.PipelineArn == p.PipelineArn && pe.StartTime.After(lastRunTime) {
			lastRunTime = pe.StartTime
		}
	}

	return result, lastRunTime, nil
}

// ListPipelines returns all pipelines.
func (b *InMemoryBackend) ListPipelines(ctx context.Context, nextToken string) ([]*Pipeline, string) {
	b.mu.RLock("ListPipelines")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListPaged(b.pipelinesStoreRO(region), nextToken, clonePipeline,
		func(a, b *Pipeline) bool { return a.PipelineName < b.PipelineName })
}

// UpdatePipeline updates a pipeline definition.
func (b *InMemoryBackend) UpdatePipeline(ctx context.Context, name, definition string) (*Pipeline, error) {
	b.mu.Lock("UpdatePipeline")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelinesStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q not found", ErrPipelineNotFound, name)
	}

	if definition != "" {
		p.PipelineDefinition = definition
	}

	p.LastModifiedTime = time.Now()

	return clonePipeline(p), nil
}

// DeletePipeline deletes a pipeline.
func (b *InMemoryBackend) DeletePipeline(ctx context.Context, name string) (*Pipeline, error) {
	b.mu.Lock("DeletePipeline")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.pipelinesStore(region)

	p, ok := store.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q not found", ErrPipelineNotFound, name)
	}

	cp := clonePipeline(p)
	store.Delete(name)
	delete(b.pipelineVersionsStore(region), name)

	return cp, nil
}

// StartPipelineExecution creates a pipeline execution.
func (b *InMemoryBackend) StartPipelineExecution(ctx context.Context, pipelineName string) (*PipelineExecution, error) {
	b.mu.Lock("StartPipelineExecution")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelinesStore(region).Get(pipelineName)
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q not found", ErrPipelineNotFound, pipelineName)
	}

	execID := generateID()
	execArn := p.PipelineArn + "/execution/" + execID

	pe := &PipelineExecution{
		PipelineArn:             p.PipelineArn,
		PipelineExecutionArn:    execArn,
		PipelineExecutionStatus: pipelineStatusSucceeded,
		StartTime:               time.Now(),
	}
	b.pipelineExecutionsStore(region).Put(pe)

	return clonePipelineExecution(pe), nil
}

// DescribePipelineExecution returns a pipeline execution.
func (b *InMemoryBackend) DescribePipelineExecution(ctx context.Context, execArn string) (*PipelineExecution, error) {
	b.mu.RLock("DescribePipelineExecution")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	pe, ok := b.pipelineExecutionsStoreRO(region).Get(execArn)
	if !ok {
		return nil, fmt.Errorf(
			"%w: pipeline execution %q not found",
			ErrPipelineExecutionNotFound,
			execArn,
		)
	}

	return clonePipelineExecution(pe), nil
}

// ListPipelineExecutions returns executions for a pipeline.
func (b *InMemoryBackend) ListPipelineExecutions(
	ctx context.Context,
	pipelineName, nextToken string,
) ([]*PipelineExecution, string) {
	b.mu.RLock("ListPipelineExecutions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelinesStoreRO(region).Get(pipelineName)
	execStore := b.pipelineExecutionsStoreRO(region)
	list := make([]*PipelineExecution, 0, execStore.Len())

	if ok {
		for _, pe := range execStore.All() {
			if pe.PipelineArn == p.PipelineArn {
				list = append(list, clonePipelineExecution(pe))
			}
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].PipelineExecutionArn < list[j].PipelineExecutionArn
	})

	startIdx := parseNextToken(nextToken)
	if startIdx >= len(list) {
		return []*PipelineExecution{}, ""
	}

	end := startIdx + sagemakerDefaultPageSize
	var outToken string

	if end < len(list) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(list)
	}

	return list[startIdx:end], outToken
}

// CreatePipelineOptions holds full input for CreatePipeline.
type CreatePipelineOptions struct {
	Tags                     map[string]string
	ParallelismConfiguration *ParallelismConfiguration
	PipelineName             string
	PipelineDefinition       string
	PipelineDisplayName      string
	PipelineDescription      string
	RoleArn                  string
}

// CreatePipelineFull creates a pipeline with full AWS input fields.
func (b *InMemoryBackend) CreatePipelineFull(ctx context.Context, opts CreatePipelineOptions) (*Pipeline, error) {
	b.mu.Lock("CreatePipelineFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.pipelinesStore(region).Get(opts.PipelineName); ok {
		return nil, fmt.Errorf(
			"%w: pipeline %s already exists",
			ErrPipelineAlreadyExists,
			opts.PipelineName,
		)
	}

	pArn := arn.Build("sagemaker", region, b.accountID, "pipeline/"+opts.PipelineName)
	now := time.Now()

	p := &Pipeline{
		PipelineName:             opts.PipelineName,
		PipelineArn:              pArn,
		PipelineStatus:           "Active",
		PipelineDefinition:       opts.PipelineDefinition,
		PipelineDisplayName:      opts.PipelineDisplayName,
		PipelineDescription:      opts.PipelineDescription,
		RoleArn:                  opts.RoleArn,
		ParallelismConfiguration: opts.ParallelismConfiguration,
		CreationTime:             now,
		LastModifiedTime:         now,
		Tags:                     mergeTags(nil, opts.Tags),
	}
	b.pipelinesStore(region).Put(p)
	b.recordPipelineVersionLocked(region, p)

	return clonePipeline(p), nil
}

// UpdatePipelineFull updates a pipeline with full AWS input fields.
func (b *InMemoryBackend) UpdatePipelineFull(
	ctx context.Context,
	name, definition, displayName, description, roleArn string,
	parallelismConfig *ParallelismConfiguration,
) (*Pipeline, error) {
	b.mu.Lock("UpdatePipelineFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelinesStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q not found", ErrPipelineNotFound, name)
	}

	if definition != "" {
		p.PipelineDefinition = definition
	}
	if displayName != "" {
		p.PipelineDisplayName = displayName
	}
	if description != "" {
		p.PipelineDescription = description
	}
	if roleArn != "" {
		p.RoleArn = roleArn
	}
	if parallelismConfig != nil {
		p.ParallelismConfiguration = parallelismConfig
	}
	p.LastModifiedTime = time.Now()
	b.recordPipelineVersionLocked(region, p)

	return clonePipeline(p), nil
}

// StartPipelineExecutionOptions holds full input for StartPipelineExecution.
type StartPipelineExecutionOptions struct {
	ParallelismConfiguration     *ParallelismConfiguration
	SelectiveExecutionConfig     *SelectiveExecutionConfig
	PipelineName                 string
	PipelineExecutionDisplayName string
	PipelineExecutionDescription string
	PipelineParameters           []PipelineParameter
	PipelineVersionID            int64
}

// StartPipelineExecutionFull creates an execution with full AWS input fields.
func (b *InMemoryBackend) StartPipelineExecutionFull(
	ctx context.Context,
	opts StartPipelineExecutionOptions,
) (*PipelineExecution, error) {
	b.mu.Lock("StartPipelineExecutionFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.pipelinesStore(region).Get(opts.PipelineName)
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q not found", ErrPipelineNotFound, opts.PipelineName)
	}

	execID := generateID()
	execArn := p.PipelineArn + "/execution/" + execID

	params := make([]PipelineParameter, len(opts.PipelineParameters))
	copy(params, opts.PipelineParameters)

	pe := &PipelineExecution{
		PipelineArn:                  p.PipelineArn,
		PipelineExecutionArn:         execArn,
		PipelineExecutionStatus:      pipelineStatusSucceeded,
		PipelineExecutionDisplayName: opts.PipelineExecutionDisplayName,
		PipelineExecutionDescription: opts.PipelineExecutionDescription,
		PipelineParameters:           params,
		PipelineDefinition:           p.PipelineDefinition,
		ParallelismConfiguration:     opts.ParallelismConfiguration,
		SelectiveExecutionConfig:     opts.SelectiveExecutionConfig,
		PipelineVersionID:            opts.PipelineVersionID,
		StartTime:                    time.Now(),
	}
	b.pipelineExecutionsStore(region).Put(pe)
	b.recordPipelineExecutionOnLatestVersionLocked(region, opts.PipelineName, execArn)

	return clonePipelineExecution(pe), nil
}
