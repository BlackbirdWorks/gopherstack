package codebuild

import (
	"context"
	"fmt"
)

type createProjectInput struct {
	Cache                   *ProjectCache          `json:"cache"`
	Source                  *ProjectSource         `json:"source"`
	Artifacts               *ProjectArtifacts      `json:"artifacts"`
	Tags                    map[string]string      `json:"tags"`
	BuildBatchConfig        *BuildBatchConfig      `json:"buildBatchConfig"`
	VpcConfig               *VpcConfig             `json:"vpcConfig"`
	LogsConfig              *LogsConfig            `json:"logsConfig"`
	Environment             *ProjectEnvironment    `json:"environment"`
	EncryptionKey           string                 `json:"encryptionKey"`
	Name                    string                 `json:"name"`
	Description             string                 `json:"description"`
	ServiceRole             string                 `json:"serviceRole"`
	ResourceAccessRole      string                 `json:"resourceAccessRole"`
	FileSystemLocations     []FileSystemLocation   `json:"fileSystemLocations"`
	SecondarySourceVersions []ProjectSourceVersion `json:"secondarySourceVersions"`
	SecondaryArtifacts      []ProjectArtifacts     `json:"secondaryArtifacts"`
	SecondarySources        []ProjectSource        `json:"secondarySources"`
	TimeoutInMinutes        int32                  `json:"timeoutInMinutes"`
	QueuedTimeoutInMinutes  int32                  `json:"queuedTimeoutInMinutes"`
	ConcurrentBuildLimit    int32                  `json:"concurrentBuildLimit"`
	AutoRetryLimit          int32                  `json:"autoRetryLimit"`
}

type createProjectOutput struct {
	Project *Project `json:"project"`
}

func (h *Handler) handleCreateProject(
	_ context.Context,
	in *createProjectInput,
) (*createProjectOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	p, err := h.Backend.CreateProject(ProjectConfig{
		Name:                    in.Name,
		Description:             in.Description,
		Source:                  in.Source,
		Artifacts:               in.Artifacts,
		Environment:             in.Environment,
		ServiceRole:             in.ServiceRole,
		EncryptionKey:           in.EncryptionKey,
		ResourceAccessRole:      in.ResourceAccessRole,
		TimeoutInMinutes:        in.TimeoutInMinutes,
		QueuedTimeoutInMinutes:  in.QueuedTimeoutInMinutes,
		ConcurrentBuildLimit:    in.ConcurrentBuildLimit,
		AutoRetryLimit:          in.AutoRetryLimit,
		Tags:                    in.Tags,
		SecondarySources:        in.SecondarySources,
		SecondaryArtifacts:      in.SecondaryArtifacts,
		SecondarySourceVersions: in.SecondarySourceVersions,
		FileSystemLocations:     in.FileSystemLocations,
		Cache:                   in.Cache,
		LogsConfig:              in.LogsConfig,
		VpcConfig:               in.VpcConfig,
		BuildBatchConfig:        in.BuildBatchConfig,
	})
	if err != nil {
		return nil, err
	}

	return &createProjectOutput{Project: p}, nil
}

type batchGetProjectsInput struct {
	Names []string `json:"names"`
}

type batchGetProjectsOutput struct {
	Projects         []*Project `json:"projects"`
	ProjectsNotFound []string   `json:"projectsNotFound"`
}

func (h *Handler) handleBatchGetProjects(
	_ context.Context,
	in *batchGetProjectsInput,
) (*batchGetProjectsOutput, error) {
	found, notFound := h.Backend.BatchGetProjects(in.Names)

	return &batchGetProjectsOutput{
		Projects:         found,
		ProjectsNotFound: notFound,
	}, nil
}

type updateProjectInput struct {
	Cache                   *ProjectCache          `json:"cache,omitempty"`
	Source                  *ProjectSource         `json:"source,omitempty"`
	Artifacts               *ProjectArtifacts      `json:"artifacts,omitempty"`
	Tags                    map[string]string      `json:"tags"`
	BuildBatchConfig        *BuildBatchConfig      `json:"buildBatchConfig,omitempty"`
	VpcConfig               *VpcConfig             `json:"vpcConfig,omitempty"`
	LogsConfig              *LogsConfig            `json:"logsConfig,omitempty"`
	Environment             *ProjectEnvironment    `json:"environment,omitempty"`
	EncryptionKey           string                 `json:"encryptionKey"`
	Name                    string                 `json:"name"`
	Description             string                 `json:"description"`
	ServiceRole             string                 `json:"serviceRole"`
	ResourceAccessRole      string                 `json:"resourceAccessRole"`
	FileSystemLocations     []FileSystemLocation   `json:"fileSystemLocations"`
	SecondarySourceVersions []ProjectSourceVersion `json:"secondarySourceVersions"`
	SecondaryArtifacts      []ProjectArtifacts     `json:"secondaryArtifacts"`
	SecondarySources        []ProjectSource        `json:"secondarySources"`
	TimeoutInMinutes        int32                  `json:"timeoutInMinutes"`
	QueuedTimeoutInMinutes  int32                  `json:"queuedTimeoutInMinutes"`
	ConcurrentBuildLimit    int32                  `json:"concurrentBuildLimit"`
	AutoRetryLimit          int32                  `json:"autoRetryLimit"`
}

type updateProjectOutput struct {
	Project *Project `json:"project"`
}

func (h *Handler) handleUpdateProject(
	_ context.Context,
	in *updateProjectInput,
) (*updateProjectOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	p, err := h.Backend.UpdateProject(in.Name, ProjectConfig{
		Description:             in.Description,
		Source:                  in.Source,
		Artifacts:               in.Artifacts,
		Environment:             in.Environment,
		ServiceRole:             in.ServiceRole,
		EncryptionKey:           in.EncryptionKey,
		ResourceAccessRole:      in.ResourceAccessRole,
		TimeoutInMinutes:        in.TimeoutInMinutes,
		QueuedTimeoutInMinutes:  in.QueuedTimeoutInMinutes,
		ConcurrentBuildLimit:    in.ConcurrentBuildLimit,
		AutoRetryLimit:          in.AutoRetryLimit,
		Tags:                    in.Tags,
		SecondarySources:        in.SecondarySources,
		SecondaryArtifacts:      in.SecondaryArtifacts,
		SecondarySourceVersions: in.SecondarySourceVersions,
		FileSystemLocations:     in.FileSystemLocations,
		Cache:                   in.Cache,
		LogsConfig:              in.LogsConfig,
		VpcConfig:               in.VpcConfig,
		BuildBatchConfig:        in.BuildBatchConfig,
	})
	if err != nil {
		return nil, err
	}

	return &updateProjectOutput{Project: p}, nil
}

type deleteProjectInput struct {
	Name string `json:"name"`
}

type deleteProjectOutput struct{}

func (h *Handler) handleDeleteProject(
	_ context.Context,
	in *deleteProjectInput,
) (*deleteProjectOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: name is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteProject(in.Name); err != nil {
		return nil, err
	}

	return &deleteProjectOutput{}, nil
}

type listProjectsInput struct{}

type listProjectsOutput struct {
	Projects []string `json:"projects"`
}

func (h *Handler) handleListProjects(
	_ context.Context,
	_ *listProjectsInput,
) (*listProjectsOutput, error) {
	names := h.Backend.ListProjects()

	return &listProjectsOutput{Projects: names}, nil
}

type updateProjectVisibilityInput struct {
	ProjectArn        string `json:"projectArn"`
	ProjectVisibility string `json:"projectVisibility"`
}

type updateProjectVisibilityOutput struct {
	ProjectArn         string `json:"projectArn"`
	ProjectVisibility  string `json:"projectVisibility"`
	PublicProjectAlias string `json:"publicProjectAlias,omitempty"`
}

func (h *Handler) handleUpdateProjectVisibility(
	_ context.Context,
	in *updateProjectVisibilityInput,
) (*updateProjectVisibilityOutput, error) {
	if in.ProjectArn == "" {
		return nil, fmt.Errorf("%w: projectArn is required", errInvalidRequest)
	}

	alias, err := h.Backend.UpdateProjectVisibility(in.ProjectArn, in.ProjectVisibility)
	if err != nil {
		return nil, err
	}

	return &updateProjectVisibilityOutput{
		ProjectArn:         in.ProjectArn,
		ProjectVisibility:  in.ProjectVisibility,
		PublicProjectAlias: alias,
	}, nil
}

type invalidateProjectCacheInput struct {
	ProjectName string `json:"projectName"`
}

type invalidateProjectCacheOutput struct{}

func (h *Handler) handleInvalidateProjectCache(
	_ context.Context,
	in *invalidateProjectCacheInput,
) (*invalidateProjectCacheOutput, error) {
	if in.ProjectName == "" {
		return nil, fmt.Errorf("%w: projectName is required", errInvalidRequest)
	}

	if err := h.Backend.InvalidateProjectCache(in.ProjectName); err != nil {
		return nil, err
	}

	return &invalidateProjectCacheOutput{}, nil
}

type listSharedProjectsInput struct{}

type listSharedProjectsOutput struct {
	Projects []string `json:"projects"`
}

func (h *Handler) handleListSharedProjects(
	_ context.Context,
	_ *listSharedProjectsInput,
) (*listSharedProjectsOutput, error) {
	return &listSharedProjectsOutput{Projects: h.Backend.ListSharedProjects()}, nil
}
