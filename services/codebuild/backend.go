// Package codebuild provides an in-memory implementation of the AWS CodeBuild service.
package codebuild

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	// buildStatusSucceeded is the terminal succeeded status for builds/batches.
	buildStatusSucceeded = "SUCCEEDED"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the same name already exists.
	ErrAlreadyExists = awserr.New("InvalidInputException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when request input fails validation.
	ErrValidation = awserr.New("InvalidInputException", awserr.ErrInvalidParameter)
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

// ReportExportConfig represents the export configuration for a CodeBuild report group.
type ReportExportConfig struct {
	ExportConfigType string `json:"exportConfigType,omitempty"`
}

// ReportGroup represents an in-memory AWS CodeBuild report group.
type ReportGroup struct {
	Tags         map[string]string  `json:"tags,omitempty"`
	ExportConfig ReportExportConfig `json:"exportConfig"`
	Arn          string             `json:"arn"`
	Name         string             `json:"name"`
	Type         string             `json:"type"`
	Status       string             `json:"status"`
	Created      float64            `json:"created,omitempty"`
	LastModified float64            `json:"lastModified,omitempty"`
}

// Report represents an in-memory AWS CodeBuild report.
type Report struct {
	Arn            string  `json:"arn"`
	ReportGroupArn string  `json:"reportGroupArn,omitempty"`
	ExecutionID    string  `json:"executionId,omitempty"`
	Type           string  `json:"type,omitempty"`
	Status         string  `json:"status"`
	Created        float64 `json:"created,omitempty"`
	Expired        float64 `json:"expired,omitempty"`
}

// Fleet represents an in-memory AWS CodeBuild compute fleet.
type Fleet struct {
	Tags            map[string]string `json:"tags,omitempty"`
	Arn             string            `json:"arn"`
	Name            string            `json:"name"`
	ComputeType     string            `json:"computeType,omitempty"`
	EnvironmentType string            `json:"environmentType,omitempty"`
	Status          string            `json:"status"`
	BaseCapacity    int32             `json:"baseCapacity"`
	Created         float64           `json:"created,omitempty"`
	LastModified    float64           `json:"lastModified,omitempty"`
}

// BuildBatch represents an in-memory AWS CodeBuild build batch.
type BuildBatch struct {
	Tags             map[string]string `json:"tags,omitempty"`
	ID               string            `json:"id"`
	Arn              string            `json:"arn"`
	ProjectName      string            `json:"projectName"`
	BuildBatchStatus string            `json:"buildBatchStatus"`
	StartTime        float64           `json:"startTime,omitempty"`
}

// CommandExecution represents an in-memory AWS CodeBuild command execution.
type CommandExecution struct {
	ID         string  `json:"id"`
	SandboxID  string  `json:"sandboxId"`
	SandboxArn string  `json:"sandboxArn,omitempty"`
	Command    string  `json:"command,omitempty"`
	Status     string  `json:"status"`
	StartTime  float64 `json:"startTime,omitempty"`
}

// Sandbox represents an in-memory AWS CodeBuild sandbox.
type Sandbox struct {
	ID          string  `json:"id"`
	Arn         string  `json:"arn"`
	ProjectName string  `json:"projectName,omitempty"`
	Status      string  `json:"status"`
	StartTime   float64 `json:"startTime,omitempty"`
}

// Webhook represents an in-memory AWS CodeBuild webhook.
type Webhook struct {
	ProjectName  string `json:"projectName"`
	URL          string `json:"url,omitempty"`
	BranchFilter string `json:"branchFilter,omitempty"`
	BuildType    string `json:"buildType,omitempty"`
}

// InMemoryBackend is a thread-safe in-memory store for CodeBuild resources.
type InMemoryBackend struct {
	projects            map[string]*Project
	builds              map[string]*Build
	buildsByProject     map[string]map[string]struct{} // project name → set of build full IDs
	projectARNIndex     map[string]string              // ARN → project name
	buildARNIndex       map[string]string              // ARN → build ID
	fleets              map[string]*Fleet              // name → Fleet
	fleetARNIndex       map[string]string              // ARN → name
	reportGroups        map[string]*ReportGroup        // name → ReportGroup
	reportGroupARNIndex map[string]string              // ARN → name
	reports             map[string]*Report             // ARN → Report
	buildBatches        map[string]*BuildBatch         // ID → BuildBatch
	commandExecutions   map[string]*CommandExecution   // ID → CommandExecution
	sandboxes           map[string]*Sandbox            // ID → Sandbox
	webhooks            map[string]*Webhook            // projectName → Webhook
	mu                  *lockmetrics.RWMutex
	accountID           string
	region              string
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		projects:            make(map[string]*Project),
		builds:              make(map[string]*Build),
		buildsByProject:     make(map[string]map[string]struct{}),
		projectARNIndex:     make(map[string]string),
		buildARNIndex:       make(map[string]string),
		fleets:              make(map[string]*Fleet),
		fleetARNIndex:       make(map[string]string),
		reportGroups:        make(map[string]*ReportGroup),
		reportGroupARNIndex: make(map[string]string),
		reports:             make(map[string]*Report),
		buildBatches:        make(map[string]*BuildBatch),
		commandExecutions:   make(map[string]*CommandExecution),
		sandboxes:           make(map[string]*Sandbox),
		webhooks:            make(map[string]*Webhook),
		accountID:           accountID,
		region:              region,
		mu:                  lockmetrics.New("codebuild"),
	}
}

// Region returns the region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state in the backend, resetting it to a pristine empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.projects = make(map[string]*Project)
	b.builds = make(map[string]*Build)
	b.buildsByProject = make(map[string]map[string]struct{})
	b.projectARNIndex = make(map[string]string)
	b.buildARNIndex = make(map[string]string)
	b.fleets = make(map[string]*Fleet)
	b.fleetARNIndex = make(map[string]string)
	b.reportGroups = make(map[string]*ReportGroup)
	b.reportGroupARNIndex = make(map[string]string)
	b.reports = make(map[string]*Report)
	b.buildBatches = make(map[string]*BuildBatch)
	b.commandExecutions = make(map[string]*CommandExecution)
	b.sandboxes = make(map[string]*Sandbox)
	b.webhooks = make(map[string]*Webhook)
}

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

// ListProjects returns all project names in sorted order.
func (b *InMemoryBackend) ListProjects() []string {
	b.mu.RLock("ListProjects")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.projects))
	for name := range b.projects {
		names = append(names, name)
	}

	sort.Strings(names)

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
	notFound := make([]string, 0, len(ids))

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

	build.BuildStatus = buildStatusSucceeded
	build.EndTime = float64(time.Now().Unix())
	build.CurrentPhase = "COMPLETED"

	out := *build

	return &out, nil
}

// ListBuilds returns all build IDs in the backend in sorted order.
func (b *InMemoryBackend) ListBuilds() []string {
	b.mu.RLock("ListBuilds")
	defer b.mu.RUnlock()

	ids := make([]string, 0, len(b.builds))
	for id := range b.builds {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	return ids
}

// BatchDeleteBuilds deletes builds by ID and returns the IDs that were deleted.
func (b *InMemoryBackend) BatchDeleteBuilds(ids []string) []string {
	b.mu.Lock("BatchDeleteBuilds")
	defer b.mu.Unlock()

	deleted := make([]string, 0, len(ids))

	for _, id := range ids {
		build, ok := b.builds[id]
		if !ok {
			continue
		}

		projectName := build.ProjectName
		delete(b.buildARNIndex, build.Arn)
		delete(b.builds, id)

		if projectBuilds, ok2 := b.buildsByProject[projectName]; ok2 {
			delete(projectBuilds, id)
		}

		deleted = append(deleted, id)
	}

	return deleted
}

// RetryBuild creates a new build for the same project as an existing build.
func (b *InMemoryBackend) RetryBuild(id string) (*Build, error) {
	b.mu.Lock("RetryBuild")
	defer b.mu.Unlock()

	existing, ok := b.builds[id]
	if !ok {
		return nil, ErrNotFound
	}

	projectName := existing.ProjectName
	if _, ok2 := b.projects[projectName]; !ok2 {
		return nil, fmt.Errorf("%w: project %s not found", ErrNotFound, projectName)
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

// ListBuildsForProject returns all build IDs for a given project in sorted order.
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

	sort.Strings(ids)

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

	if name, ok := b.fleetARNIndex[resourceARN]; ok {
		f := b.fleets[name]
		out := make(map[string]string, len(f.Tags))
		maps.Copy(out, f.Tags)

		return out, nil
	}

	if name, ok := b.reportGroupARNIndex[resourceARN]; ok {
		rg := b.reportGroups[name]
		out := make(map[string]string, len(rg.Tags))
		maps.Copy(out, rg.Tags)

		return out, nil
	}

	return nil, ErrNotFound
}

// TagResource adds or updates tags on a CodeBuild resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	if name, ok := b.projectARNIndex[resourceARN]; ok {
		p := b.projects[name]
		if p.Tags == nil {
			p.Tags = make(map[string]string)
		}

		maps.Copy(p.Tags, tagsCopy)

		return nil
	}

	if id, ok := b.buildARNIndex[resourceARN]; ok {
		build := b.builds[id]
		if build.Tags == nil {
			build.Tags = make(map[string]string)
		}

		maps.Copy(build.Tags, tagsCopy)

		return nil
	}

	if name, ok := b.fleetARNIndex[resourceARN]; ok {
		f := b.fleets[name]
		if f.Tags == nil {
			f.Tags = make(map[string]string)
		}

		maps.Copy(f.Tags, tagsCopy)

		return nil
	}

	if name, ok := b.reportGroupARNIndex[resourceARN]; ok {
		rg := b.reportGroups[name]
		if rg.Tags == nil {
			rg.Tags = make(map[string]string)
		}

		maps.Copy(rg.Tags, tagsCopy)

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

	if name, ok := b.fleetARNIndex[resourceARN]; ok {
		f := b.fleets[name]
		for _, k := range tagKeys {
			delete(f.Tags, k)
		}

		return nil
	}

	if name, ok := b.reportGroupARNIndex[resourceARN]; ok {
		rg := b.reportGroups[name]
		for _, k := range tagKeys {
			delete(rg.Tags, k)
		}

		return nil
	}

	return ErrNotFound
}

// --- Fleet operations ---

func (b *InMemoryBackend) buildFleetARN(name string) string {
	return arn.Build("codebuild", b.region, b.accountID, "fleet/"+name)
}

func (b *InMemoryBackend) buildReportGroupARN(name string) string {
	return arn.Build("codebuild", b.region, b.accountID, "report-group/"+name)
}

func (b *InMemoryBackend) buildWebhookURL(projectName string) string {
	return "https://codebuild." + b.region + ".amazonaws.com/webhooks/" + projectName
}

// CreateFleet creates a new compute fleet.
func (b *InMemoryBackend) CreateFleet(
	name string, baseCapacity int32, computeType, environmentType string, tags map[string]string,
) (*Fleet, error) {
	b.mu.Lock("CreateFleet")
	defer b.mu.Unlock()

	if _, exists := b.fleets[name]; exists {
		return nil, ErrAlreadyExists
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	now := float64(time.Now().Unix())
	f := &Fleet{
		Arn:             b.buildFleetARN(name),
		Name:            name,
		BaseCapacity:    baseCapacity,
		ComputeType:     computeType,
		EnvironmentType: environmentType,
		Status:          "ACTIVE",
		Tags:            tagsCopy,
		Created:         now,
		LastModified:    now,
	}
	b.fleets[name] = f
	b.fleetARNIndex[f.Arn] = name

	out := *f

	return &out, nil
}

// BatchGetFleets returns fleets by name or ARN. Missing names are returned separately.
func (b *InMemoryBackend) BatchGetFleets(names []string) ([]*Fleet, []string) {
	b.mu.RLock("BatchGetFleets")
	defer b.mu.RUnlock()

	found := make([]*Fleet, 0, len(names))
	notFound := make([]string, 0, len(names))

	for _, nameOrARN := range names {
		name := nameOrARN
		if n, ok := b.fleetARNIndex[nameOrARN]; ok {
			name = n
		}

		if f, ok := b.fleets[name]; ok {
			out := *f
			found = append(found, &out)
		} else {
			notFound = append(notFound, nameOrARN)
		}
	}

	return found, notFound
}

// --- ReportGroup operations ---

// CreateReportGroup creates a new report group.
func (b *InMemoryBackend) CreateReportGroup(
	name, rtype string, exportConfig ReportExportConfig, tags map[string]string,
) (*ReportGroup, error) {
	b.mu.Lock("CreateReportGroup")
	defer b.mu.Unlock()

	if _, exists := b.reportGroups[name]; exists {
		return nil, ErrAlreadyExists
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	now := float64(time.Now().Unix())
	rg := &ReportGroup{
		Arn:          b.buildReportGroupARN(name),
		Name:         name,
		Type:         rtype,
		Status:       "ACTIVE",
		ExportConfig: exportConfig,
		Tags:         tagsCopy,
		Created:      now,
		LastModified: now,
	}
	b.reportGroups[name] = rg
	b.reportGroupARNIndex[rg.Arn] = name

	out := *rg

	return &out, nil
}

// BatchGetReportGroups returns report groups by ARN. Missing ARNs are returned separately.
func (b *InMemoryBackend) BatchGetReportGroups(arns []string) ([]*ReportGroup, []string) {
	b.mu.RLock("BatchGetReportGroups")
	defer b.mu.RUnlock()

	found := make([]*ReportGroup, 0, len(arns))
	notFound := make([]string, 0, len(arns))

	for _, a := range arns {
		name, ok := b.reportGroupARNIndex[a]
		if !ok {
			// also try by name for convenience
			if _, foundByName := b.reportGroups[a]; foundByName {
				name = a
				ok = true
			}
		}

		if ok {
			rg := b.reportGroups[name]
			out := *rg
			found = append(found, &out)
		} else {
			notFound = append(notFound, a)
		}
	}

	return found, notFound
}

// --- Report operations ---

// AddReportInternal seeds a Report directly into the backend (test helper).
func (b *InMemoryBackend) AddReportInternal(r *Report) {
	b.mu.Lock("AddReportInternal")
	defer b.mu.Unlock()

	b.reports[r.Arn] = r
}

// BatchGetReports returns reports by ARN. Missing ARNs are returned separately.
func (b *InMemoryBackend) BatchGetReports(arns []string) ([]*Report, []string) {
	b.mu.RLock("BatchGetReports")
	defer b.mu.RUnlock()

	found := make([]*Report, 0, len(arns))
	notFound := make([]string, 0, len(arns))

	for _, a := range arns {
		if r, ok := b.reports[a]; ok {
			out := *r
			found = append(found, &out)
		} else {
			notFound = append(notFound, a)
		}
	}

	return found, notFound
}

// --- BuildBatch operations ---

// AddBuildBatchInternal seeds a BuildBatch directly into the backend (test helper).
func (b *InMemoryBackend) AddBuildBatchInternal(bb *BuildBatch) {
	b.mu.Lock("AddBuildBatchInternal")
	defer b.mu.Unlock()

	b.buildBatches[bb.ID] = bb
}

// BatchGetBuildBatches returns build batches by ID. Missing IDs are returned separately.
func (b *InMemoryBackend) BatchGetBuildBatches(ids []string) ([]*BuildBatch, []string) {
	b.mu.RLock("BatchGetBuildBatches")
	defer b.mu.RUnlock()

	found := make([]*BuildBatch, 0, len(ids))
	notFound := make([]string, 0, len(ids))

	for _, id := range ids {
		if bb, ok := b.buildBatches[id]; ok {
			out := *bb
			found = append(found, &out)
		} else {
			notFound = append(notFound, id)
		}
	}

	return found, notFound
}

// --- CommandExecution operations ---

// AddCommandExecutionInternal seeds a CommandExecution directly into the backend (test helper).
func (b *InMemoryBackend) AddCommandExecutionInternal(ce *CommandExecution) {
	b.mu.Lock("AddCommandExecutionInternal")
	defer b.mu.Unlock()

	b.commandExecutions[ce.ID] = ce
}

// BatchGetCommandExecutions returns command executions by ID within a sandbox.
// Missing IDs are returned separately.
func (b *InMemoryBackend) BatchGetCommandExecutions(sandboxID string, ids []string) ([]*CommandExecution, []string) {
	b.mu.RLock("BatchGetCommandExecutions")
	defer b.mu.RUnlock()

	found := make([]*CommandExecution, 0, len(ids))
	notFound := make([]string, 0, len(ids))

	for _, id := range ids {
		ce, ok := b.commandExecutions[id]
		if ok && ce.SandboxID == sandboxID {
			out := *ce
			found = append(found, &out)
		} else {
			notFound = append(notFound, id)
		}
	}

	return found, notFound
}

// --- Sandbox operations ---

// AddSandboxInternal seeds a Sandbox directly into the backend (test helper).
func (b *InMemoryBackend) AddSandboxInternal(s *Sandbox) {
	b.mu.Lock("AddSandboxInternal")
	defer b.mu.Unlock()

	b.sandboxes[s.ID] = s
}

// BatchGetSandboxes returns sandboxes by ID or ARN. Missing IDs are returned separately.
func (b *InMemoryBackend) BatchGetSandboxes(ids []string) ([]*Sandbox, []string) {
	b.mu.RLock("BatchGetSandboxes")
	defer b.mu.RUnlock()

	found := make([]*Sandbox, 0, len(ids))
	notFound := make([]string, 0, len(ids))

	for _, id := range ids {
		if s, ok := b.sandboxes[id]; ok {
			out := *s
			found = append(found, &out)
		} else {
			notFound = append(notFound, id)
		}
	}

	return found, notFound
}

// ListBuildBatches returns all build batch IDs in sorted order.
func (b *InMemoryBackend) ListBuildBatches() []string {
	b.mu.RLock("ListBuildBatches")
	defer b.mu.RUnlock()

	ids := make([]string, 0, len(b.buildBatches))
	for id := range b.buildBatches {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	return ids
}

// ListFleets returns all fleet ARNs in sorted order.
func (b *InMemoryBackend) ListFleets() []string {
	b.mu.RLock("ListFleets")
	defer b.mu.RUnlock()

	arns := make([]string, 0, len(b.fleets))
	for _, f := range b.fleets {
		arns = append(arns, f.Arn)
	}

	sort.Strings(arns)

	return arns
}

// ListReportGroups returns all report group ARNs in sorted order.
func (b *InMemoryBackend) ListReportGroups() []string {
	b.mu.RLock("ListReportGroups")
	defer b.mu.RUnlock()

	arns := make([]string, 0, len(b.reportGroups))
	for _, rg := range b.reportGroups {
		arns = append(arns, rg.Arn)
	}

	sort.Strings(arns)

	return arns
}

// ListSandboxes returns all sandbox IDs in sorted order.
func (b *InMemoryBackend) ListSandboxes() []string {
	b.mu.RLock("ListSandboxes")
	defer b.mu.RUnlock()

	ids := make([]string, 0, len(b.sandboxes))
	for id := range b.sandboxes {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	return ids
}

// StartBuildBatch creates a new build batch for a project.
func (b *InMemoryBackend) StartBuildBatch(projectName string) (*BuildBatch, error) {
	b.mu.Lock("StartBuildBatch")
	defer b.mu.Unlock()

	if _, ok := b.projects[projectName]; !ok {
		return nil, ErrNotFound
	}

	id := projectName + ":" + uuid.NewString()
	bb := &BuildBatch{
		ID:               id,
		ProjectName:      projectName,
		BuildBatchStatus: buildStatusSucceeded,
	}
	b.buildBatches[id] = bb

	out := *bb

	return &out, nil
}

// StartCommandExecution creates a new command execution in a sandbox.
func (b *InMemoryBackend) StartCommandExecution(sandboxID, command, execType string) (*CommandExecution, error) {
	b.mu.Lock("StartCommandExecution")
	defer b.mu.Unlock()

	if _, ok := b.sandboxes[sandboxID]; !ok {
		return nil, ErrNotFound
	}

	id := uuid.NewString()
	ce := &CommandExecution{
		ID:        id,
		SandboxID: sandboxID,
		Command:   command,
		Status:    buildStatusSucceeded,
	}

	_ = execType
	b.commandExecutions[id] = ce

	out := *ce

	return &out, nil
}

// StartSandbox creates a new sandbox for a project.
func (b *InMemoryBackend) StartSandbox(projectName string) (*Sandbox, error) {
	b.mu.Lock("StartSandbox")
	defer b.mu.Unlock()

	if _, ok := b.projects[projectName]; !ok {
		return nil, ErrNotFound
	}

	id := uuid.NewString()
	sb := &Sandbox{
		ID:          id,
		ProjectName: projectName,
		Status:      "READY",
	}
	b.sandboxes[id] = sb

	out := *sb

	return &out, nil
}

// --- Webhook operations ---

// CreateWebhook creates a webhook for a CodeBuild project.
func (b *InMemoryBackend) CreateWebhook(projectName, branchFilter, buildType string) (*Webhook, error) {
	b.mu.Lock("CreateWebhook")
	defer b.mu.Unlock()

	if _, ok := b.projects[projectName]; !ok {
		return nil, ErrNotFound
	}

	if _, exists := b.webhooks[projectName]; exists {
		return nil, ErrAlreadyExists
	}

	w := &Webhook{
		ProjectName:  projectName,
		URL:          b.buildWebhookURL(projectName),
		BranchFilter: branchFilter,
		BuildType:    buildType,
	}
	b.webhooks[projectName] = w

	out := *w

	return &out, nil
}
