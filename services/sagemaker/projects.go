package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrProjectNotFound is returned when a project does not exist.
var ErrProjectNotFound = awserr.New("ValidationException", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// Project
// ---------------------------------------------------------------------------

// Project represents a SageMaker project.
type Project struct {
	CreationTime       time.Time         `json:"CreationTime"`
	Tags               map[string]string `json:"Tags,omitempty"`
	ProjectName        string            `json:"ProjectName"`
	ProjectArn         string            `json:"ProjectArn"`
	ProjectID          string            `json:"ProjectId"`
	ProjectStatus      string            `json:"ProjectStatus"`
	ProjectDescription string            `json:"ProjectDescription,omitempty"`
}

func cloneProject(p *Project) *Project {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)

	return &cp
}

// MarshalJSON emits CreationTime as an AWS awsjson1.1 epoch-seconds number
// rather than Go's default RFC3339 string — this struct is marshaled
// directly by handleDescribeProject.
func (p *Project) MarshalJSON() ([]byte, error) {
	type alias Project

	return json.Marshal(struct {
		*alias
		CreationTime float64 `json:"CreationTime"`
	}{
		alias:        (*alias)(p),
		CreationTime: epochSeconds(p.CreationTime),
	})
}

// UnmarshalJSON is the inverse of [Project.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (p *Project) UnmarshalJSON(data []byte) error {
	type alias Project

	aux := struct {
		*alias
		CreationTime float64 `json:"CreationTime"`
	}{alias: (*alias)(p)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	p.CreationTime = timeFromEpochSeconds(aux.CreationTime)

	return nil
}

// CreateProject creates a SageMaker project.
func (b *InMemoryBackend) CreateProject(
	ctx context.Context,
	name, description string,
	tags map[string]string,
) (*Project, error) {
	b.mu.Lock("CreateProject")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name == "" {
		return nil, fmt.Errorf("%w: ProjectName is required", ErrValidation)
	}

	if _, ok := b.projectsStore(region).Get(name); ok {
		return nil, fmt.Errorf("%w: project %q already exists", ErrValidation, name)
	}

	projectARN := arn.Build("sagemaker", region, b.accountID, "project/"+name)

	p := &Project{
		ProjectName:        name,
		ProjectArn:         projectARN,
		ProjectID:          generateID(),
		ProjectStatus:      "CreateCompleted",
		ProjectDescription: description,
		Tags:               mergeTags(nil, tags),
		CreationTime:       time.Now(),
	}
	b.projectsStore(region).Put(p)

	return cloneProject(p), nil
}

// DescribeProject returns a project by name.
func (b *InMemoryBackend) DescribeProject(ctx context.Context, name string) (*Project, error) {
	b.mu.RLock("DescribeProject")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	p, ok := b.projectsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: project %q not found", ErrProjectNotFound, name)
	}

	return cloneProject(p), nil
}

// DeleteProject removes a project by name.
func (b *InMemoryBackend) DeleteProject(ctx context.Context, name string) error {
	b.mu.Lock("DeleteProject")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.projectsStore(region).Get(name); !ok {
		return fmt.Errorf("%w: project %q not found", ErrProjectNotFound, name)
	}

	store := b.projectsStore(region)
	store.Delete(name)

	return nil
}

// ListProjects returns all projects sorted by name.
func (b *InMemoryBackend) ListProjects(ctx context.Context, nextToken string) ([]*Project, string) {
	b.mu.RLock("ListProjects")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.projectsStoreRO(region),
		nextToken,
		cloneProject,
		func(v *Project) string { return v.ProjectName },
	)
}

// ---------------------------------------------------------------------------
// UpdateProject
// ---------------------------------------------------------------------------

// UpdateProject updates a project's description and merges in new tags.
func (b *InMemoryBackend) UpdateProject(
	ctx context.Context,
	name, description string,
	tags map[string]string,
) (*Project, error) {
	b.mu.Lock("UpdateProject")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	p, ok := b.projectsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: project %q not found", ErrProjectNotFound, name)
	}

	if description != "" {
		p.ProjectDescription = description
	}

	if len(tags) > 0 {
		p.Tags = mergeTags(p.Tags, tags)
	}

	return cloneProject(p), nil
}
