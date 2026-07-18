package codebuild

import (
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) buildProjectARN(name string) string {
	return arn.Build("codebuild", b.region, b.accountID, "project/"+name)
}

// lookupByNameOrARN finds a project by name or by its ARN.
func (b *InMemoryBackend) lookupByNameOrARN(nameOrARN string) (*Project, bool) {
	if p, ok := b.projects.Get(nameOrARN); ok {
		return p, true
	}

	if matches := b.projectsByARN.Get(nameOrARN); len(matches) > 0 {
		return matches[0], true
	}

	return nil, false
}

// ProjectConfig holds all configurable fields for creating or updating a project.
type ProjectConfig struct {
	Cache                   *ProjectCache
	Source                  *ProjectSource
	Artifacts               *ProjectArtifacts
	Tags                    map[string]string
	BuildBatchConfig        *BuildBatchConfig
	VpcConfig               *VpcConfig
	LogsConfig              *LogsConfig
	Environment             *ProjectEnvironment
	EncryptionKey           string
	Name                    string
	Description             string
	ServiceRole             string
	ResourceAccessRole      string
	FileSystemLocations     []FileSystemLocation
	SecondarySourceVersions []ProjectSourceVersion
	SecondaryArtifacts      []ProjectArtifacts
	SecondarySources        []ProjectSource
	TimeoutInMinutes        int32
	QueuedTimeoutInMinutes  int32
	ConcurrentBuildLimit    int32
	AutoRetryLimit          int32
}

// CreateProject creates a new CodeBuild project.
func (b *InMemoryBackend) CreateProject(cfg ProjectConfig) (*Project, error) {
	b.mu.Lock("CreateProject")
	defer b.mu.Unlock()

	if cfg.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if b.projects.Has(cfg.Name) {
		return nil, ErrAlreadyExists
	}

	tagsCopy := make(map[string]string, len(cfg.Tags))
	maps.Copy(tagsCopy, cfg.Tags)

	now := float64(time.Now().Unix())
	p := &Project{
		Name:                    cfg.Name,
		Arn:                     b.buildProjectARN(cfg.Name),
		Description:             cfg.Description,
		ServiceRole:             cfg.ServiceRole,
		EncryptionKey:           cfg.EncryptionKey,
		ResourceAccessRole:      cfg.ResourceAccessRole,
		TimeoutInMinutes:        cfg.TimeoutInMinutes,
		QueuedTimeoutInMinutes:  cfg.QueuedTimeoutInMinutes,
		ConcurrentBuildLimit:    cfg.ConcurrentBuildLimit,
		AutoRetryLimit:          cfg.AutoRetryLimit,
		Tags:                    tagsCopy,
		SecondarySources:        cfg.SecondarySources,
		SecondaryArtifacts:      cfg.SecondaryArtifacts,
		SecondarySourceVersions: cfg.SecondarySourceVersions,
		FileSystemLocations:     cfg.FileSystemLocations,
		Cache:                   cfg.Cache,
		LogsConfig:              cfg.LogsConfig,
		VpcConfig:               cfg.VpcConfig,
		BuildBatchConfig:        cfg.BuildBatchConfig,
		Created:                 now,
		LastModified:            now,
	}

	if cfg.Source != nil {
		p.Source = *cfg.Source
	}

	if cfg.Artifacts != nil {
		p.Artifacts = *cfg.Artifacts
	}

	if cfg.Environment != nil {
		p.Environment = *cfg.Environment
	}

	b.projects.Put(p)

	out := *p

	return &out, nil
}

// BatchGetProjects returns projects by name or ARN. Missing names are returned separately.
func (b *InMemoryBackend) BatchGetProjects(names []string) ([]*Project, []string) {
	b.mu.RLock("BatchGetProjects")
	defer b.mu.RUnlock()

	found := make([]*Project, 0, len(names))
	notFound := make([]string, 0, len(names))

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

// applyProjectOptionalFields copies non-zero optional fields from cfg into p.
func applyProjectOptionalFields(p *Project, cfg ProjectConfig) {
	if cfg.Source != nil {
		p.Source = *cfg.Source
	}

	if cfg.Artifacts != nil {
		p.Artifacts = *cfg.Artifacts
	}

	if cfg.Environment != nil {
		p.Environment = *cfg.Environment
	}

	if cfg.SecondarySources != nil {
		p.SecondarySources = cfg.SecondarySources
	}

	if cfg.SecondaryArtifacts != nil {
		p.SecondaryArtifacts = cfg.SecondaryArtifacts
	}

	if cfg.SecondarySourceVersions != nil {
		p.SecondarySourceVersions = cfg.SecondarySourceVersions
	}

	if cfg.FileSystemLocations != nil {
		p.FileSystemLocations = cfg.FileSystemLocations
	}

	if cfg.Cache != nil {
		p.Cache = cfg.Cache
	}

	if cfg.LogsConfig != nil {
		p.LogsConfig = cfg.LogsConfig
	}

	if cfg.VpcConfig != nil {
		p.VpcConfig = cfg.VpcConfig
	}

	if cfg.BuildBatchConfig != nil {
		p.BuildBatchConfig = cfg.BuildBatchConfig
	}
}

// UpdateProject updates fields on an existing project.
func (b *InMemoryBackend) UpdateProject(name string, cfg ProjectConfig) (*Project, error) {
	b.mu.Lock("UpdateProject")
	defer b.mu.Unlock()

	p, ok := b.lookupByNameOrARN(name)
	if !ok {
		return nil, ErrNotFound
	}

	if cfg.Description != "" {
		p.Description = cfg.Description
	}

	if cfg.ServiceRole != "" {
		p.ServiceRole = cfg.ServiceRole
	}

	if cfg.EncryptionKey != "" {
		p.EncryptionKey = cfg.EncryptionKey
	}

	if cfg.ResourceAccessRole != "" {
		p.ResourceAccessRole = cfg.ResourceAccessRole
	}

	if cfg.TimeoutInMinutes != 0 {
		p.TimeoutInMinutes = cfg.TimeoutInMinutes
	}

	if cfg.QueuedTimeoutInMinutes != 0 {
		p.QueuedTimeoutInMinutes = cfg.QueuedTimeoutInMinutes
	}

	if cfg.ConcurrentBuildLimit != 0 {
		p.ConcurrentBuildLimit = cfg.ConcurrentBuildLimit
	}

	if cfg.AutoRetryLimit != 0 {
		p.AutoRetryLimit = cfg.AutoRetryLimit
	}

	applyProjectOptionalFields(p, cfg)

	if len(cfg.Tags) > 0 {
		p.Tags = mergeTags(p.Tags, cfg.Tags)
	}

	p.LastModified = float64(time.Now().Unix())

	out := *p

	return &out, nil
}

// mergeTags returns a new map containing dst's entries merged with src.
func mergeTags(dst, src map[string]string) map[string]string {
	if dst == nil {
		dst = make(map[string]string, len(src))
	}

	maps.Copy(dst, src)

	return dst
}

// DeleteProject removes a project by name and all builds associated with it.
func (b *InMemoryBackend) DeleteProject(name string) error {
	b.mu.Lock("DeleteProject")
	defer b.mu.Unlock()

	if !b.projects.Has(name) {
		return ErrNotFound
	}

	b.projects.Delete(name)

	// Use the per-project build index for O(k) cleanup instead of O(n) scan.
	// IDs are gathered first since deleting from the table mutates the very
	// index slice Get returned.
	group := b.buildsByProject.Get(name)
	ids := make([]string, len(group))

	for i, bld := range group {
		ids[i] = bld.ID
	}

	for _, id := range ids {
		b.builds.Delete(id)
	}

	return nil
}

// ListProjects returns all project names in sorted order.
func (b *InMemoryBackend) ListProjects() []string {
	b.mu.RLock("ListProjects")
	defer b.mu.RUnlock()

	items := b.projects.Snapshot()
	names := make([]string, len(items))

	for i, p := range items {
		names[i] = p.Name
	}

	return names
}

// UpdateProjectVisibility sets the visibility of a project by ARN.
// Returns the publicProjectAlias (non-empty only when visibility is PUBLIC_READ).
func (b *InMemoryBackend) UpdateProjectVisibility(projectArn, visibility string) (string, error) {
	b.mu.Lock("UpdateProjectVisibility")
	defer b.mu.Unlock()

	matches := b.projectsByARN.Get(projectArn)
	if len(matches) == 0 {
		return "", ErrNotFound
	}

	p := matches[0]
	p.Visibility = visibility

	if visibility == "PUBLIC_READ" {
		if p.PublicProjectAlias == "" {
			p.PublicProjectAlias = uuid.NewString()
		}
	} else {
		p.PublicProjectAlias = ""
	}

	return p.PublicProjectAlias, nil
}

// InvalidateProjectCache is a no-op cache invalidation (returns ErrNotFound if project missing).
func (b *InMemoryBackend) InvalidateProjectCache(projectName string) error {
	b.mu.RLock("InvalidateProjectCache")
	defer b.mu.RUnlock()

	if !b.projects.Has(projectName) {
		return ErrNotFound
	}

	return nil
}

// ListSharedProjects returns an empty list (no shared projects in emulator).
func (b *InMemoryBackend) ListSharedProjects() []string {
	return []string{}
}
