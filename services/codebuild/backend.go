// Package codebuild provides an in-memory implementation of the AWS CodeBuild service.
package codebuild

import (
	"maps"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the same name already exists.
	ErrAlreadyExists = awserr.New("InvalidInputException", awserr.ErrAlreadyExists)
)

// ProjectSource represents the source configuration for a CodeBuild project.
type ProjectSource struct {
	Type     string `json:"type"`
	Location string `json:"location,omitempty"`
}

// ProjectArtifacts represents the artifacts configuration for a CodeBuild project.
type ProjectArtifacts struct {
	Type     string `json:"type"`
	Location string `json:"location,omitempty"`
}

// ProjectEnvironment represents the build environment for a CodeBuild project.
type ProjectEnvironment struct {
	Type           string `json:"type"`
	Image          string `json:"image"`
	ComputeType    string `json:"computeType"`
	PrivilegedMode bool   `json:"privilegedMode,omitempty"`
}

// Project represents an in-memory AWS CodeBuild project.
type Project struct {
	Tags         map[string]string  `json:"tags,omitempty"`
	Source       ProjectSource      `json:"source"`
	Artifacts    ProjectArtifacts   `json:"artifacts"`
	Name         string             `json:"name"`
	Arn          string             `json:"arn"`
	Description  string             `json:"description,omitempty"`
	ServiceRole  string             `json:"serviceRole,omitempty"`
	Environment  ProjectEnvironment `json:"environment"`
	Created      float64            `json:"created,omitempty"`
	LastModified float64            `json:"lastModified,omitempty"`
}

// Build represents an in-memory AWS CodeBuild build execution.
type Build struct {
	Tags         map[string]string `json:"tags,omitempty"`
	ID           string            `json:"id"`
	Arn          string            `json:"arn"`
	ProjectName  string            `json:"projectName"`
	BuildStatus  string            `json:"buildStatus"`
	CurrentPhase string            `json:"currentPhase,omitempty"`
	StartTime    float64           `json:"startTime,omitempty"`
	EndTime      float64           `json:"endTime,omitempty"`
}

// InMemoryBackend is a thread-safe in-memory store for CodeBuild resources.
type InMemoryBackend struct {
	projects        map[string]*Project
	builds          map[string]*Build
	buildsByProject map[string]map[string]struct{} // project name → set of build full IDs
	projectARNIndex map[string]string              // ARN → project name
	buildARNIndex   map[string]string              // ARN → build ID
	mu              *lockmetrics.RWMutex
	accountID       string
	region          string
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		projects:        make(map[string]*Project),
		builds:          make(map[string]*Build),
		buildsByProject: make(map[string]map[string]struct{}),
		projectARNIndex: make(map[string]string),
		buildARNIndex:   make(map[string]string),
		accountID:       accountID,
		region:          region,
		mu:              lockmetrics.New("codebuild"),
	}
}

// Region returns the region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.region }

func (b *InMemoryBackend) buildProjectARN(name string) string {
	return arn.Build("codebuild", b.region, b.accountID, "project/"+name)
}

func (b *InMemoryBackend) buildBuildARN(projectName, buildID string) string {
	return arn.Build("codebuild", b.region, b.accountID, "build/"+projectName+":"+buildID)
}

func randomID() string {
	return uuid.NewString()[:8]
}

// lookupByNameOrARN finds a project by name or by its ARN.
func (b *InMemoryBackend) lookupByNameOrARN(nameOrARN string) (*Project, bool) {
	if p, ok := b.projects[nameOrARN]; ok {
		return p, true
	}

	if name, ok := b.projectARNIndex[nameOrARN]; ok {
		return b.projects[name], true
	}

	return nil, false
}

// CreateProject creates a new CodeBuild project.
func (b *InMemoryBackend) CreateProject(
	name, description string,
	source ProjectSource,
	artifacts ProjectArtifacts,
	environment ProjectEnvironment,
	serviceRole string,
	tags map[string]string,
) (*Project, error) {
	b.mu.Lock("CreateProject")
	defer b.mu.Unlock()

	if _, exists := b.projects[name]; exists {
		return nil, ErrAlreadyExists
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	now := float64(time.Now().Unix())
	p := &Project{
		Name:         name,
		Arn:          b.buildProjectARN(name),
		Description:  description,
		Source:       source,
		Artifacts:    artifacts,
		Environment:  environment,
		ServiceRole:  serviceRole,
		Tags:         tagsCopy,
		Created:      now,
		LastModified: now,
	}
	b.projects[name] = p
	b.projectARNIndex[p.Arn] = name

	out := *p

	return &out, nil
}

// BatchGetProjects returns projects by name or ARN. Missing names are returned separately.
func (b *InMemoryBackend) BatchGetProjects(names []string) ([]*Project, []string) {
	b.mu.RLock("BatchGetProjects")
	defer b.mu.RUnlock()

	found := make([]*Project, 0, len(names))
	notFound := make([]string, 0)

	for _, name := range names {
		if p, ok := b.lookupByNameOrARN(name); ok {
			out := *p
			found = append(found, &out)
		} else {
			notFound = append(notFound, name)
		}
	}

	return found, notFound
}

// UpdateProject updates fields on an existing project.
func (b *InMemoryBackend) UpdateProject(
	name, description string,
	source *ProjectSource,
	artifacts *ProjectArtifacts,
	environment *ProjectEnvironment,
	serviceRole string,
) (*Project, error) {
	b.mu.Lock("UpdateProject")
	defer b.mu.Unlock()

	p, ok := b.lookupByNameOrARN(name)
	if !ok {
		return nil, ErrNotFound
	}

	if description != "" {
		p.Description = description
	}

	if source != nil {
		p.Source = *source
	}

	if artifacts != nil {
		p.Artifacts = *artifacts
	}

	if environment != nil {
		p.Environment = *environment
	}

	if serviceRole != "" {
		p.ServiceRole = serviceRole
	}

	p.LastModified = float64(time.Now().Unix())

	out := *p

	return &out, nil
}

// DeleteProject removes a project by name and all builds associated with it.
func (b *InMemoryBackend) DeleteProject(name string) error {
	b.mu.Lock("DeleteProject")
	defer b.mu.Unlock()

	p, ok := b.projects[name]
	if !ok {
		return ErrNotFound
	}

	delete(b.projectARNIndex, p.Arn)
	delete(b.projects, name)

	// Use the per-project build index for O(k) cleanup instead of O(n) scan.
	for id := range b.buildsByProject[name] {
		if build, ok2 := b.builds[id]; ok2 {
			delete(b.buildARNIndex, build.Arn)
			delete(b.builds, id)
		}
	}
	delete(b.buildsByProject, name)

	return nil
}

// ListProjects returns all project names.
func (b *InMemoryBackend) ListProjects() []string {
	b.mu.RLock("ListProjects")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.projects))
	for name := range b.projects {
		names = append(names, name)
	}

	return names
}

// StartBuild creates a new build for the given project.
func (b *InMemoryBackend) StartBuild(projectName string) (*Build, error) {
	b.mu.Lock("StartBuild")
	defer b.mu.Unlock()

	if _, ok := b.projects[projectName]; !ok {
		return nil, ErrNotFound
	}

	buildID := randomID()
	fullID := projectName + ":" + buildID
	build := &Build{
		ID:           fullID,
		Arn:          b.buildBuildARN(projectName, buildID),
		ProjectName:  projectName,
		BuildStatus:  "IN_PROGRESS",
		StartTime:    float64(time.Now().Unix()),
		CurrentPhase: "SUBMITTED",
	}
	b.builds[fullID] = build
	b.buildARNIndex[build.Arn] = fullID
	if b.buildsByProject[projectName] == nil {
		b.buildsByProject[projectName] = make(map[string]struct{})
	}
	b.buildsByProject[projectName][fullID] = struct{}{}

	out := *build

	return &out, nil
}

// BatchGetBuilds returns builds by ID. Missing IDs are returned separately.
func (b *InMemoryBackend) BatchGetBuilds(ids []string) ([]*Build, []string) {
	b.mu.RLock("BatchGetBuilds")
	defer b.mu.RUnlock()

	found := make([]*Build, 0, len(ids))
	notFound := make([]string, 0)

	for _, id := range ids {
		if build, ok := b.builds[id]; ok {
			out := *build
			found = append(found, &out)
		} else {
			notFound = append(notFound, id)
		}
	}

	return found, notFound
}

// StopBuild marks a build as SUCCEEDED (stopped).
func (b *InMemoryBackend) StopBuild(id string) (*Build, error) {
	b.mu.Lock("StopBuild")
	defer b.mu.Unlock()

	build, ok := b.builds[id]
	if !ok {
		return nil, ErrNotFound
	}

	build.BuildStatus = "SUCCEEDED"
	build.EndTime = float64(time.Now().Unix())
	build.CurrentPhase = "COMPLETED"

	out := *build

	return &out, nil
}

// ListBuildsForProject returns all build IDs for a given project.
func (b *InMemoryBackend) ListBuildsForProject(projectName string) ([]string, error) {
	b.mu.RLock("ListBuildsForProject")
	defer b.mu.RUnlock()

	if _, ok := b.projects[projectName]; !ok {
		return nil, ErrNotFound
	}

	buildSet := b.buildsByProject[projectName]
	ids := make([]string, 0, len(buildSet))

	for id := range buildSet {
		ids = append(ids, id)
	}

	return ids, nil
}

// ListTagsForResource returns the tags for a CodeBuild resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if name, ok := b.projectARNIndex[resourceARN]; ok {
		p := b.projects[name]
		out := make(map[string]string, len(p.Tags))
		maps.Copy(out, p.Tags)

		return out, nil
	}

	if id, ok := b.buildARNIndex[resourceARN]; ok {
		build := b.builds[id]
		out := make(map[string]string, len(build.Tags))
		maps.Copy(out, build.Tags)

		return out, nil
	}

	return nil, ErrNotFound
}

// TagResource adds or updates tags on a CodeBuild resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if name, ok := b.projectARNIndex[resourceARN]; ok {
		p := b.projects[name]
		if p.Tags == nil {
			p.Tags = make(map[string]string)
		}

		maps.Copy(p.Tags, tags)

		return nil
	}

	if id, ok := b.buildARNIndex[resourceARN]; ok {
		build := b.builds[id]
		if build.Tags == nil {
			build.Tags = make(map[string]string)
		}

		maps.Copy(build.Tags, tags)

		return nil
	}

	return ErrNotFound
}

// UntagResource removes tags from a CodeBuild resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if name, ok := b.projectARNIndex[resourceARN]; ok {
		p := b.projects[name]
		for _, k := range tagKeys {
			delete(p.Tags, k)
		}

		return nil
	}

	if id, ok := b.buildARNIndex[resourceARN]; ok {
		build := b.builds[id]
		for _, k := range tagKeys {
			delete(build.Tags, k)
		}

		return nil
	}

	return ErrNotFound
}
