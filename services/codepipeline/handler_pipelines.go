package codepipeline

import (
	"context"
	"fmt"
)

// maxResultsCapListPipelines is the per-operation pagination cap for ListPipelines.
const maxResultsCapListPipelines int32 = 1000

// validArtifactStoreType returns true if t is a valid ArtifactStore type.
func validArtifactStoreType(t string) bool { return t == "S3" }

// validPipelineType returns true if t is a valid PipelineType value.
func validPipelineType(t string) bool {
	return t == "" || t == PipelineTypeV1 || t == PipelineTypeV2
}

// validExecutionMode returns true if m is a valid ExecutionMode value.
func validExecutionMode(m string) bool {
	return m == "" || m == ExecutionModeQueued || m == ExecutionModeSuperseded ||
		m == ExecutionModeParallel
}

type createPipelineInput struct {
	Pipeline *PipelineDeclaration `json:"pipeline"`
	Tags     []Tag                `json:"tags"`
}

type createPipelineOutput struct {
	Pipeline *PipelineDeclaration `json:"pipeline"`
	Tags     []Tag                `json:"tags"`
}

func (h *Handler) handleCreatePipeline(
	ctx context.Context,
	in *createPipelineInput,
) (*createPipelineOutput, error) {
	if in.Pipeline == nil {
		return nil, fmt.Errorf("%w: pipeline is required", errInvalidRequest)
	}

	if in.Pipeline.Name == "" {
		return nil, fmt.Errorf("%w: pipeline name is required", errInvalidRequest)
	}

	if in.Pipeline.RoleArn == "" {
		return nil, fmt.Errorf("%w: roleArn is required", ErrInvalidStructure)
	}

	if !validPipelineType(in.Pipeline.PipelineType) {
		return nil, fmt.Errorf(
			"%w: invalid pipelineType %q",
			ErrValidation,
			in.Pipeline.PipelineType,
		)
	}

	if !validExecutionMode(in.Pipeline.ExecutionMode) {
		return nil, fmt.Errorf(
			"%w: invalid executionMode %q",
			ErrValidation,
			in.Pipeline.ExecutionMode,
		)
	}

	if in.Pipeline.ArtifactStore.Type != "" &&
		!validArtifactStoreType(in.Pipeline.ArtifactStore.Type) {
		return nil, fmt.Errorf(
			"%w: invalid artifactStore type %q: must be S3",
			ErrValidation,
			in.Pipeline.ArtifactStore.Type,
		)
	}

	tagMap := tagsToMap(in.Tags)

	p, err := h.Backend.CreatePipeline(ctx, *in.Pipeline, tagMap)
	if err != nil {
		return nil, err
	}

	return &createPipelineOutput{
		Pipeline: &p.Declaration,
		Tags:     in.Tags,
	}, nil
}

type getPipelineInput struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
}

type getPipelineOutput struct {
	Pipeline *PipelineDeclaration `json:"pipeline"`
	Metadata *PipelineMetadata    `json:"metadata"`
}

func (h *Handler) handleGetPipeline(
	ctx context.Context,
	in *getPipelineInput,
) (*getPipelineOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	p, err := h.Backend.GetPipeline(ctx, in.Name)
	if err != nil {
		return nil, err
	}

	if in.Version != 0 && in.Version != p.Declaration.Version {
		return nil, fmt.Errorf("%w: pipeline %q version %d not found (current: %d)",
			ErrVersionNotFound, in.Name, in.Version, p.Declaration.Version)
	}

	return &getPipelineOutput{
		Pipeline: &p.Declaration,
		Metadata: &p.Metadata,
	}, nil
}

type updatePipelineInput struct {
	Pipeline *PipelineDeclaration `json:"pipeline"`
}

type updatePipelineOutput struct {
	Pipeline *PipelineDeclaration `json:"pipeline"`
}

func (h *Handler) handleUpdatePipeline(
	ctx context.Context,
	in *updatePipelineInput,
) (*updatePipelineOutput, error) {
	if in.Pipeline == nil {
		return nil, fmt.Errorf("%w: pipeline is required", errInvalidRequest)
	}

	if in.Pipeline.Name == "" {
		return nil, fmt.Errorf("%w: pipeline name is required", errInvalidRequest)
	}

	p, err := h.Backend.UpdatePipeline(ctx, *in.Pipeline)
	if err != nil {
		return nil, err
	}

	return &updatePipelineOutput{Pipeline: &p.Declaration}, nil
}

type deletePipelineInput struct {
	Name string `json:"name"`
}

type deletePipelineOutput struct{}

func (h *Handler) handleDeletePipeline(
	ctx context.Context,
	in *deletePipelineInput,
) (*deletePipelineOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	if err := h.Backend.DeletePipeline(ctx, in.Name); err != nil {
		return nil, err
	}

	return &deletePipelineOutput{}, nil
}

type listPipelinesInput struct {
	NextToken  string `json:"nextToken,omitempty"`
	MaxResults int32  `json:"maxResults,omitempty"`
}

type listPipelinesOutput struct {
	NextToken string            `json:"nextToken,omitempty"`
	Pipelines []PipelineSummary `json:"pipelines"`
}

func (h *Handler) handleListPipelines(
	ctx context.Context,
	in *listPipelinesInput,
) (*listPipelinesOutput, error) {
	summaries := h.Backend.ListPipelines(ctx)
	if summaries == nil {
		summaries = []PipelineSummary{}
	}

	page, nextToken, err := cpPaginate(summaries, in.NextToken, in.MaxResults, maxResultsCapListPipelines)
	if err != nil {
		return nil, err
	}

	return &listPipelinesOutput{NextToken: nextToken, Pipelines: page}, nil
}
