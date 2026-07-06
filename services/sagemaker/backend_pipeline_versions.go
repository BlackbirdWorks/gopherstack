package sagemaker

import (
	"context"
	"fmt"
	"time"
)

// ---------------------------------------------------------------------------
// Pipeline versions and execution-definition extras.
//
// AWS creates a new pipeline version every time CreatePipeline or
// UpdatePipeline succeeds (see "Pipeline versions" in the SageMaker docs).
// This file tracks that history and the handful of related read/update
// operations that reference it.
// ---------------------------------------------------------------------------

// PipelineVersion is a single versioned snapshot of a pipeline's definition.
type PipelineVersion struct {
	CreationTime                      time.Time `json:"CreationTime"`
	PipelineArn                       string    `json:"PipelineArn"`
	PipelineVersionDisplayName        string    `json:"PipelineVersionDisplayName,omitempty"`
	PipelineVersionDescription        string    `json:"PipelineVersionDescription,omitempty"`
	PipelineDefinition                string    `json:"PipelineDefinition,omitempty"`
	LastExecutionPipelineExecutionArn string    `json:"LastExecutionPipelineExecutionArn,omitempty"`
	PipelineVersionID                 int64     `json:"PipelineVersionId"`
}

func clonePipelineVersion(v *PipelineVersion) *PipelineVersion {
	cp := *v

	return &cp
}

// pipelineVersionsStore returns the region-scoped pipeline-name -> versions map.
func (b *InMemoryBackend) pipelineVersionsStore(r string) map[string][]*PipelineVersion {
	if b.pipelineVersions[r] == nil {
		b.pipelineVersions[r] = make(map[string][]*PipelineVersion)
	}

	return b.pipelineVersions[r]
}

// recordPipelineVersionLocked appends a new version snapshot for p. Callers
// must hold b.mu for writing.
func (b *InMemoryBackend) recordPipelineVersionLocked(region string, p *Pipeline) {
	store := b.pipelineVersionsStore(region)
	existing := store[p.PipelineName]

	v := &PipelineVersion{
		PipelineArn:                p.PipelineArn,
		PipelineVersionID:          int64(len(existing) + 1),
		PipelineVersionDisplayName: p.PipelineDisplayName,
		PipelineVersionDescription: p.PipelineDescription,
		PipelineDefinition:         p.PipelineDefinition,
		CreationTime:               p.LastModifiedTime,
	}
	store[p.PipelineName] = append(existing, v)
}

// recordPipelineExecutionOnLatestVersionLocked stamps the most recent pipeline
// version with the ARN of the execution that was just started from it.
// Callers must hold b.mu for writing.
func (b *InMemoryBackend) recordPipelineExecutionOnLatestVersionLocked(region, pipelineName, execArn string) {
	versions := b.pipelineVersionsStore(region)[pipelineName]
	if len(versions) == 0 {
		return
	}

	versions[len(versions)-1].LastExecutionPipelineExecutionArn = execArn
}

// ListPipelineVersions returns the version history for a pipeline, newest first.
func (b *InMemoryBackend) ListPipelineVersions(
	ctx context.Context,
	pipelineName, nextToken string,
	maxResults int32,
) ([]*PipelineVersion, string, error) {
	b.mu.RLock("ListPipelineVersions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.pipelinesStore(region).Get(pipelineName); !ok {
		return nil, "", fmt.Errorf("%w: pipeline %q not found", ErrPipelineNotFound, pipelineName)
	}

	versions := b.pipelineVersionsStore(region)[pipelineName]

	list := make([]*PipelineVersion, len(versions))
	for i, v := range versions {
		// Reverse so the newest (highest ID) version is first, matching the
		// default (Descending) sort order AWS uses for this operation.
		list[len(versions)-1-i] = clonePipelineVersion(v)
	}

	page, out := paginateSlice(list, nextToken, maxResults)

	return page, out, nil
}

// findPipelineByARNLocked returns the pipeline with the given ARN. Callers
// must hold b.mu (read or write).
func (b *InMemoryBackend) findPipelineByARNLocked(region, pipelineArn string) (*Pipeline, bool) {
	for _, p := range b.pipelinesStore(region).All() {
		if p.PipelineArn == pipelineArn {
			return p, true
		}
	}

	return nil, false
}

// UpdatePipelineVersion updates the description/display name of a specific
// pipeline version.
func (b *InMemoryBackend) UpdatePipelineVersion(
	ctx context.Context,
	pipelineArn string,
	versionID int64,
	description, displayName string,
) (*PipelineVersion, error) {
	b.mu.Lock("UpdatePipelineVersion")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.findPipelineByARNLocked(region, pipelineArn)
	if !ok {
		return nil, fmt.Errorf("%w: pipeline %q not found", ErrPipelineNotFound, pipelineArn)
	}

	versions := b.pipelineVersionsStore(region)[p.PipelineName]

	for _, v := range versions {
		if v.PipelineVersionID != versionID {
			continue
		}

		if description != "" {
			v.PipelineVersionDescription = description
		}

		if displayName != "" {
			v.PipelineVersionDisplayName = displayName
		}

		return clonePipelineVersion(v), nil
	}

	return nil, fmt.Errorf(
		"%w: pipeline version %d not found for pipeline %q", ErrPipelineNotFound, versionID, pipelineArn,
	)
}

// DescribePipelineDefinitionForExecution returns the pipeline definition JSON
// that was in effect when the given execution started.
func (b *InMemoryBackend) DescribePipelineDefinitionForExecution(
	ctx context.Context,
	execArn string,
) (string, time.Time, error) {
	b.mu.RLock("DescribePipelineDefinitionForExecution")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	pe, ok := b.pipelineExecutionsStore(region).Get(execArn)
	if !ok {
		return "", time.Time{}, fmt.Errorf(
			"%w: pipeline execution %q not found", ErrPipelineExecutionNotFound, execArn,
		)
	}

	if p, found := b.findPipelineByARNLocked(region, pe.PipelineArn); found {
		return pe.PipelineDefinition, p.CreationTime, nil
	}

	return pe.PipelineDefinition, pe.StartTime, nil
}

// UpdatePipelineExecution updates the description/display name/parallelism
// configuration of a pipeline execution.
func (b *InMemoryBackend) UpdatePipelineExecution(
	ctx context.Context,
	execArn, description, displayName string,
	parallelismConfig *ParallelismConfiguration,
) (*PipelineExecution, error) {
	b.mu.Lock("UpdatePipelineExecution")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	pe, ok := b.pipelineExecutionsStore(region).Get(execArn)
	if !ok {
		return nil, fmt.Errorf(
			"%w: pipeline execution %q not found", ErrPipelineExecutionNotFound, execArn,
		)
	}

	if description != "" {
		pe.PipelineExecutionDescription = description
	}

	if displayName != "" {
		pe.PipelineExecutionDisplayName = displayName
	}

	if parallelismConfig != nil {
		pe.ParallelismConfiguration = parallelismConfig
	}

	return clonePipelineExecution(pe), nil
}
