package codebuild

import (
	"context"
	"fmt"
)

// projectConfigFields holds the CodeBuild project configuration fields shared
// by the CreateProject and UpdateProject request bodies. Both operations
// accept the same set of project properties; only the surrounding input type
// and JSON `omitempty` presentation differ.
type projectConfigFields struct {
	Cache                   *ProjectCache          `json:"cache"`
	Source                  *ProjectSource         `json:"source"`
	Artifacts               *ProjectArtifacts      `json:"artifacts"`
	Tags                    wireTags               `json:"tags"`
	BuildBatchConfig        *BuildBatchConfig      `json:"buildBatchConfig"`
	VpcConfig               *VpcConfig             `json:"vpcConfig"`
	LogsConfig              *LogsConfig            `json:"logsConfig"`
	Environment             *ProjectEnvironment    `json:"environment"`
	Description             string                 `json:"description"`
	EncryptionKey           string                 `json:"encryptionKey"`
	ServiceRole             string                 `json:"serviceRole"`
	ResourceAccessRole      string                 `json:"resourceAccessRole"`
	SourceVersion           string                 `json:"sourceVersion"`
	SecondaryArtifacts      []ProjectArtifacts     `json:"secondaryArtifacts"`
	SecondarySourceVersions []ProjectSourceVersion `json:"secondarySourceVersions"`
	SecondarySources        []ProjectSource        `json:"secondarySources"`
	FileSystemLocations     []FileSystemLocation   `json:"fileSystemLocations"`
	TimeoutInMinutes        int32                  `json:"timeoutInMinutes"`
	QueuedTimeoutInMinutes  int32                  `json:"queuedTimeoutInMinutes"`
	ConcurrentBuildLimit    int32                  `json:"concurrentBuildLimit"`
	AutoRetryLimit          int32                  `json:"autoRetryLimit"`
}

// toProjectConfig converts the shared request fields plus a project name into
// a ProjectConfig for the backend. UpdateProject ignores ProjectConfig.Name
// (the name is passed separately), so setting it unconditionally is safe for
// both callers.
func (f projectConfigFields) toProjectConfig(name string) ProjectConfig {
	return ProjectConfig{
		Name:                    name,
		Description:             f.Description,
		Source:                  f.Source,
		Artifacts:               f.Artifacts,
		Environment:             f.Environment,
		ServiceRole:             f.ServiceRole,
		EncryptionKey:           f.EncryptionKey,
		ResourceAccessRole:      f.ResourceAccessRole,
		TimeoutInMinutes:        f.TimeoutInMinutes,
		QueuedTimeoutInMinutes:  f.QueuedTimeoutInMinutes,
		ConcurrentBuildLimit:    f.ConcurrentBuildLimit,
		AutoRetryLimit:          f.AutoRetryLimit,
		Tags:                    f.Tags,
		SecondarySources:        f.SecondarySources,
		SecondaryArtifacts:      f.SecondaryArtifacts,
		SecondarySourceVersions: f.SecondarySourceVersions,
		FileSystemLocations:     f.FileSystemLocations,
		Cache:                   f.Cache,
		LogsConfig:              f.LogsConfig,
		VpcConfig:               f.VpcConfig,
		BuildBatchConfig:        f.BuildBatchConfig,
		SourceVersion:           f.SourceVersion,
	}
}

type createProjectInput struct {
	Name string `json:"name"`
	projectConfigFields
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

	p, err := h.Backend.CreateProject(in.toProjectConfig(in.Name))
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
	Name string `json:"name"`
	projectConfigFields
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

	p, err := h.Backend.UpdateProject(in.Name, in.toProjectConfig(in.Name))
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

type listProjectsInput struct {
	NextToken string `json:"nextToken"`
	SortBy    string `json:"sortBy"`
	SortOrder string `json:"sortOrder"`
}

type listProjectsOutput struct {
	NextToken string   `json:"nextToken,omitempty"`
	Projects  []string `json:"projects"`
}

func (h *Handler) handleListProjects(
	_ context.Context,
	in *listProjectsInput,
) (*listProjectsOutput, error) {
	names := h.Backend.ListProjectsSortedBy(in.SortBy)

	pg, err := paginateIDs(names, in.NextToken, in.SortOrder, 0)
	if err != nil {
		return nil, err
	}

	return &listProjectsOutput{Projects: pg.Data, NextToken: pg.Next}, nil
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
