package codebuild

import (
	"context"
	"fmt"
)

type batchGetBuildBatchesInput struct {
	IDs []string `json:"ids"`
}

type batchGetBuildBatchesOutput struct {
	BuildBatches         []*BuildBatch `json:"buildBatches"`
	BuildBatchesNotFound []string      `json:"buildBatchesNotFound"`
}

func (h *Handler) handleBatchGetBuildBatches(
	_ context.Context,
	in *batchGetBuildBatchesInput,
) (*batchGetBuildBatchesOutput, error) {
	found, notFound := h.Backend.BatchGetBuildBatches(in.IDs)

	return &batchGetBuildBatchesOutput{
		BuildBatches:         found,
		BuildBatchesNotFound: notFound,
	}, nil
}

type deleteBuildBatchInput struct {
	ID string `json:"id"`
}

type deleteBuildBatchOutput struct {
	StatusCode string `json:"statusCode"`
}

func (h *Handler) handleDeleteBuildBatch(
	_ context.Context,
	in *deleteBuildBatchInput,
) (*deleteBuildBatchOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: id is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteBuildBatch(in.ID); err != nil {
		return nil, err
	}

	return &deleteBuildBatchOutput{StatusCode: buildStatusSucceeded}, nil
}

// buildBatchFilter mirrors the wire shape of the real SDK's BuildBatchFilter
// (a single optional status field).
type buildBatchFilter struct {
	Status string `json:"status"`
}

type listBuildBatchesInput struct {
	Filter     *buildBatchFilter `json:"filter"`
	NextToken  string            `json:"nextToken"`
	SortOrder  string            `json:"sortOrder"`
	MaxResults int32             `json:"maxResults"`
}

type listBuildBatchesOutput struct {
	NextToken string   `json:"nextToken,omitempty"`
	IDs       []string `json:"ids"`
}

func (h *Handler) handleListBuildBatches(
	_ context.Context,
	in *listBuildBatchesInput,
) (*listBuildBatchesOutput, error) {
	var status string
	if in.Filter != nil {
		status = in.Filter.Status
	}

	ids := h.Backend.ListBuildBatches(status)

	pg, err := paginateIDs(ids, in.NextToken, in.SortOrder, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listBuildBatchesOutput{IDs: pg.Data, NextToken: pg.Next}, nil
}

type listBuildBatchesForProjectInput struct {
	Filter      *buildBatchFilter `json:"filter"`
	ProjectName string            `json:"projectName"`
	NextToken   string            `json:"nextToken"`
	SortOrder   string            `json:"sortOrder"`
	MaxResults  int32             `json:"maxResults"`
}

type listBuildBatchesForProjectOutput struct {
	NextToken string   `json:"nextToken,omitempty"`
	IDs       []string `json:"ids"`
}

func (h *Handler) handleListBuildBatchesForProject(
	_ context.Context,
	in *listBuildBatchesForProjectInput,
) (*listBuildBatchesForProjectOutput, error) {
	if in.ProjectName == "" {
		return nil, fmt.Errorf("%w: projectName is required", errInvalidRequest)
	}

	var status string
	if in.Filter != nil {
		status = in.Filter.Status
	}

	ids, err := h.Backend.ListBuildBatchesForProject(in.ProjectName, status)
	if err != nil {
		return nil, err
	}

	pg, err := paginateIDs(ids, in.NextToken, in.SortOrder, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listBuildBatchesForProjectOutput{IDs: pg.Data, NextToken: pg.Next}, nil
}

// retryBuildBatchInput mirrors api_op_RetryBuildBatch.go's
// RetryBuildBatchInput. RetryType is accepted but not behaviorally
// distinguished -- see RetryBuildBatch's doc comment (build_batches.go).
type retryBuildBatchInput struct {
	ID        string `json:"id"`
	RetryType string `json:"retryType,omitempty"`
}

type retryBuildBatchOutput struct {
	BuildBatch *BuildBatch `json:"buildBatch"`
}

func (h *Handler) handleRetryBuildBatch(_ context.Context, in *retryBuildBatchInput) (*retryBuildBatchOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: id is required", errInvalidRequest)
	}

	bb, err := h.Backend.RetryBuildBatch(in.ID)
	if err != nil {
		return nil, err
	}

	return &retryBuildBatchOutput{BuildBatch: bb}, nil
}

// startBuildBatchInput mirrors aws-sdk-go-v2/service/codebuild@v1.72.4's
// api_op_StartBuildBatch.go StartBuildBatchInput. IdempotencyToken and
// LogsConfigOverride are intentionally not modeled -- see
// StartBuildBatchConfig's doc comment (build_batches.go).
type startBuildBatchInput struct {
	ArtifactsOverride                *ProjectArtifacts      `json:"artifactsOverride,omitempty"`
	BuildBatchConfigOverride         *BuildBatchConfig      `json:"buildBatchConfigOverride,omitempty"`
	CacheOverride                    *ProjectCache          `json:"cacheOverride,omitempty"`
	RegistryCredentialOverride       *RegistryCredential    `json:"registryCredentialOverride,omitempty"`
	SourceAuthOverride               *SourceAuth            `json:"sourceAuthOverride,omitempty"`
	GitSubmodulesConfigOverride      *GitSubmodulesConfig   `json:"gitSubmodulesConfigOverride,omitempty"`
	InsecureSslOverride              *bool                  `json:"insecureSslOverride,omitempty"`
	ReportBuildBatchStatusOverride   *bool                  `json:"reportBuildBatchStatusOverride,omitempty"`
	PrivilegedModeOverride           *bool                  `json:"privilegedModeOverride,omitempty"`
	GitCloneDepthOverride            *int32                 `json:"gitCloneDepthOverride,omitempty"`
	BuildTimeoutInMinutesOverride    *int32                 `json:"buildTimeoutInMinutesOverride,omitempty"`
	QueuedTimeoutInMinutesOverride   *int32                 `json:"queuedTimeoutInMinutesOverride,omitempty"`
	SourceVersion                    string                 `json:"sourceVersion,omitempty"`
	EncryptionKeyOverride            string                 `json:"encryptionKeyOverride,omitempty"`
	ImageOverride                    string                 `json:"imageOverride,omitempty"`
	ServiceRoleOverride              string                 `json:"serviceRoleOverride,omitempty"`
	BuildspecOverride                string                 `json:"buildspecOverride,omitempty"`
	SourceTypeOverride               string                 `json:"sourceTypeOverride,omitempty"`
	SourceLocationOverride           string                 `json:"sourceLocationOverride,omitempty"`
	EnvironmentTypeOverride          string                 `json:"environmentTypeOverride,omitempty"`
	CertificateOverride              string                 `json:"certificateOverride,omitempty"`
	ImagePullCredentialsTypeOverride string                 `json:"imagePullCredentialsTypeOverride,omitempty"`
	ComputeTypeOverride              string                 `json:"computeTypeOverride,omitempty"`
	ProjectName                      string                 `json:"projectName"`
	SecondaryArtifactsOverride       []ProjectArtifacts     `json:"secondaryArtifactsOverride,omitempty"`
	SecondarySourcesOverride         []ProjectSource        `json:"secondarySourcesOverride,omitempty"`
	SecondarySourcesVersionOverride  []ProjectSourceVersion `json:"secondarySourcesVersionOverride,omitempty"`
	EnvironmentVariablesOverride     []EnvironmentVariable  `json:"environmentVariablesOverride,omitempty"`
	DebugSessionEnabled              bool                   `json:"debugSessionEnabled,omitempty"`
}

type startBuildBatchOutput struct {
	BuildBatch *BuildBatch `json:"buildBatch"`
}

func (h *Handler) handleStartBuildBatch(_ context.Context, in *startBuildBatchInput) (*startBuildBatchOutput, error) {
	if in.ProjectName == "" {
		return nil, fmt.Errorf("%w: projectName is required", errInvalidRequest)
	}

	bb, err := h.Backend.StartBuildBatch(in.ProjectName, StartBuildBatchConfig{
		ArtifactsOverride:                in.ArtifactsOverride,
		BuildBatchConfigOverride:         in.BuildBatchConfigOverride,
		CacheOverride:                    in.CacheOverride,
		RegistryCredentialOverride:       in.RegistryCredentialOverride,
		SourceAuthOverride:               in.SourceAuthOverride,
		GitSubmodulesConfigOverride:      in.GitSubmodulesConfigOverride,
		InsecureSslOverride:              in.InsecureSslOverride,
		ReportBuildBatchStatusOverride:   in.ReportBuildBatchStatusOverride,
		PrivilegedModeOverride:           in.PrivilegedModeOverride,
		GitCloneDepthOverride:            in.GitCloneDepthOverride,
		BuildTimeoutInMinutesOverride:    in.BuildTimeoutInMinutesOverride,
		QueuedTimeoutInMinutesOverride:   in.QueuedTimeoutInMinutesOverride,
		SourceVersion:                    in.SourceVersion,
		EncryptionKeyOverride:            in.EncryptionKeyOverride,
		ImageOverride:                    in.ImageOverride,
		ServiceRoleOverride:              in.ServiceRoleOverride,
		BuildspecOverride:                in.BuildspecOverride,
		SourceTypeOverride:               in.SourceTypeOverride,
		SourceLocationOverride:           in.SourceLocationOverride,
		EnvironmentTypeOverride:          in.EnvironmentTypeOverride,
		CertificateOverride:              in.CertificateOverride,
		ImagePullCredentialsTypeOverride: in.ImagePullCredentialsTypeOverride,
		ComputeTypeOverride:              in.ComputeTypeOverride,
		SecondaryArtifactsOverride:       in.SecondaryArtifactsOverride,
		SecondarySourcesOverride:         in.SecondarySourcesOverride,
		SecondarySourcesVersionOverride:  in.SecondarySourcesVersionOverride,
		EnvVarsOverride:                  in.EnvironmentVariablesOverride,
		DebugSessionEnabled:              in.DebugSessionEnabled,
	})
	if err != nil {
		return nil, err
	}

	return &startBuildBatchOutput{BuildBatch: bb}, nil
}

type stopBuildBatchInput struct {
	ID string `json:"id"`
}

type stopBuildBatchOutput struct {
	BuildBatch *BuildBatch `json:"buildBatch"`
}

func (h *Handler) handleStopBuildBatch(_ context.Context, in *stopBuildBatchInput) (*stopBuildBatchOutput, error) {
	if in.ID == "" {
		return nil, fmt.Errorf("%w: id is required", errInvalidRequest)
	}

	bb, err := h.Backend.StopBuildBatch(in.ID)
	if err != nil {
		return nil, err
	}

	return &stopBuildBatchOutput{BuildBatch: bb}, nil
}
