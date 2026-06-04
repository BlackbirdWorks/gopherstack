package sagemaker

import (
	"fmt"
	"maps"
	"sort"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

var (
	// ErrModelPackageGroupNotFound is returned when a model package group does not exist.
	ErrModelPackageGroupNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrModelPackageGroupHasPackages is returned when deleting a group that still has packages.
	ErrModelPackageGroupHasPackages = awserr.New("ConflictException", awserr.ErrConflict)
	// ErrAutoMLJobNotFound is returned when an AutoML job does not exist.
	ErrAutoMLJobNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrAutoMLJobNotStoppable is returned when stopping an already-terminal AutoML job.
	ErrAutoMLJobNotStoppable = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrCodeRepositoryNotFound is returned when a code repository does not exist.
	ErrCodeRepositoryNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrProjectNotFound is returned when a project does not exist.
	ErrProjectNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrSpaceNotFound is returned when a space does not exist.
	ErrSpaceNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrSMImageNotFound is returned when a SageMaker image does not exist.
	ErrSMImageNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrImageHasVersions is returned when deleting an image that still has versions.
	ErrImageHasVersions = awserr.New("ResourceInUse", awserr.ErrConflict)
	// ErrImageVersionNotFound is returned when an image version does not exist.
	ErrImageVersionNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrCompilationJobNotFound is returned when a compilation job does not exist.
	ErrCompilationJobNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrCompilationJobNotStoppable is returned when stopping an already-terminal compilation job.
	ErrCompilationJobNotStoppable = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrMonitoringScheduleNotFound is returned when a monitoring schedule does not exist.
	ErrMonitoringScheduleNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrMonitoringScheduleAlreadyStopped is returned when stopping an already-stopped schedule.
	ErrMonitoringScheduleAlreadyStopped = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrMonitoringScheduleNotStopped is returned when starting a non-stopped schedule.
	ErrMonitoringScheduleNotStopped = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrWorkteamNotFound is returned when a workteam does not exist.
	ErrWorkteamNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
)

const (
	jobStatusStopped = "STOPPED"
)

// ---------------------------------------------------------------------------
// ModelPackageGroup
// ---------------------------------------------------------------------------

// ModelPackageGroup represents a SageMaker model package group.
type ModelPackageGroup struct {
	CreationTime                 time.Time         `json:"CreationTime"`
	Tags                         map[string]string `json:"Tags,omitempty"`
	ModelPackageGroupName        string            `json:"ModelPackageGroupName"`
	ModelPackageGroupArn         string            `json:"ModelPackageGroupArn"`
	ModelPackageGroupDescription string            `json:"ModelPackageGroupDescription,omitempty"`
	ModelPackageGroupStatus      string            `json:"ModelPackageGroupStatus"`
}

func cloneModelPackageGroup(g *ModelPackageGroup) *ModelPackageGroup {
	cp := *g
	cp.Tags = maps.Clone(g.Tags)

	return &cp
}

// CreateModelPackageGroup creates a new model package group.
func (b *InMemoryBackend) CreateModelPackageGroup(
	name, description string,
	tags map[string]string,
) (*ModelPackageGroup, error) {
	b.mu.Lock("CreateModelPackageGroup")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: ModelPackageGroupName is required", ErrValidation)
	}

	if _, ok := b.modelPackageGroups[name]; ok {
		return nil, fmt.Errorf("%w: model package group %q already exists", ErrValidation, name)
	}

	groupARN := arn.Build("sagemaker", b.region, b.accountID, "model-package-group/"+name)

	g := &ModelPackageGroup{
		ModelPackageGroupName:        name,
		ModelPackageGroupArn:         groupARN,
		ModelPackageGroupDescription: description,
		ModelPackageGroupStatus:      algorithmStatusCompleted,
		Tags:                         mergeTags(nil, tags),
		CreationTime:                 time.Now(),
	}
	b.modelPackageGroups[name] = g

	return cloneModelPackageGroup(g), nil
}

// DescribeModelPackageGroup returns a model package group by name.
func (b *InMemoryBackend) DescribeModelPackageGroup(name string) (*ModelPackageGroup, error) {
	b.mu.RLock("DescribeModelPackageGroup")
	defer b.mu.RUnlock()

	g, ok := b.modelPackageGroups[name]
	if !ok {
		return nil, fmt.Errorf("%w: model package group %q not found", ErrModelPackageGroupNotFound, name)
	}

	return cloneModelPackageGroup(g), nil
}

// DeleteModelPackageGroup removes a model package group by name.
func (b *InMemoryBackend) DeleteModelPackageGroup(name string) error {
	b.mu.Lock("DeleteModelPackageGroup")
	defer b.mu.Unlock()

	if _, ok := b.modelPackageGroups[name]; !ok {
		return fmt.Errorf("%w: model package group %q not found", ErrModelPackageGroupNotFound, name)
	}

	// AWS rejects deletion when model packages still exist in the group.
	for _, mp := range b.modelPackages {
		if mp.ModelPackageGroupName == name {
			return fmt.Errorf("%w: model package group %q has model packages and cannot be deleted",
				ErrModelPackageGroupHasPackages, name)
		}
	}

	delete(b.modelPackageGroups, name)

	return nil
}

// ListModelPackageGroups returns all model package groups, sorted by name.
func (b *InMemoryBackend) ListModelPackageGroups(nextToken string) ([]*ModelPackageGroup, string) {
	b.mu.RLock("ListModelPackageGroups")
	defer b.mu.RUnlock()

	keys := make([]string, 0, len(b.modelPackageGroups))
	for k := range b.modelPackageGroups {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(keys))

	out := make([]*ModelPackageGroup, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneModelPackageGroup(b.modelPackageGroups[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// ---------------------------------------------------------------------------
// ModelPackage CRUD (name/ARN dual lookup)
// ---------------------------------------------------------------------------

// CreateModelPackage creates a model package.
func (b *InMemoryBackend) CreateModelPackage(
	name, groupName, description string,
	tags map[string]string,
) (*ModelPackage, error) {
	b.mu.Lock("CreateModelPackage")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: ModelPackageName is required", ErrValidation)
	}

	mpARN := arn.Build("sagemaker", b.region, b.accountID, "model-package/"+name)

	if _, ok := b.modelPackages[mpARN]; ok {
		return nil, fmt.Errorf("%w: model package %q already exists", ErrValidation, name)
	}

	mp := &ModelPackage{
		ModelPackageName:        name,
		ModelPackageArn:         mpARN,
		ModelPackageGroupName:   groupName,
		ModelPackageStatus:      "Completed",
		ModelPackageDescription: description,
		Tags:                    mergeTags(nil, tags),
		CreationTime:            time.Now(),
	}
	b.modelPackages[mpARN] = mp
	b.modelPackageARNIndex[name] = mpARN

	return cloneModelPackage(mp), nil
}

// DescribeModelPackage returns a model package by name or ARN.
func (b *InMemoryBackend) DescribeModelPackage(nameOrArn string) (*ModelPackage, error) {
	b.mu.RLock("DescribeModelPackage")
	defer b.mu.RUnlock()

	// Try direct ARN lookup first.
	if mp, ok := b.modelPackages[nameOrArn]; ok {
		return cloneModelPackage(mp), nil
	}

	// Try name → ARN index.
	if arnStr, ok := b.modelPackageARNIndex[nameOrArn]; ok {
		if mp, found := b.modelPackages[arnStr]; found {
			return cloneModelPackage(mp), nil
		}
	}

	return nil, fmt.Errorf("%w: model package %q not found", ErrModelPackageNotFound, nameOrArn)
}

// DeleteModelPackage removes a model package by name or ARN.
func (b *InMemoryBackend) DeleteModelPackage(nameOrArn string) error {
	b.mu.Lock("DeleteModelPackage")
	defer b.mu.Unlock()

	arnStr := nameOrArn
	if v, ok := b.modelPackageARNIndex[nameOrArn]; ok {
		arnStr = v
	}

	if _, ok := b.modelPackages[arnStr]; !ok {
		return fmt.Errorf("%w: model package %q not found", ErrModelPackageNotFound, nameOrArn)
	}

	mp := b.modelPackages[arnStr]
	delete(b.modelPackageARNIndex, mp.ModelPackageName)
	delete(b.modelPackages, arnStr)

	return nil
}

// ListModelPackages returns model packages, optionally filtered by group name.
func (b *InMemoryBackend) ListModelPackages(groupName, nextToken string) ([]*ModelPackage, string) {
	b.mu.RLock("ListModelPackages")
	defer b.mu.RUnlock()

	var arns []string
	for k := range b.modelPackages {
		mp := b.modelPackages[k]
		if groupName == "" || mp.ModelPackageGroupName == groupName {
			arns = append(arns, k)
		}
	}

	sort.Strings(arns)

	start := 0
	if nextToken != "" {
		for i, k := range arns {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(arns))

	out := make([]*ModelPackage, 0, end-start)
	for _, k := range arns[start:end] {
		out = append(out, cloneModelPackage(b.modelPackages[k]))
	}

	next := ""
	if end < len(arns) {
		next = arns[end]
	}

	return out, next
}

// ---------------------------------------------------------------------------
// AutoMLJob
// ---------------------------------------------------------------------------

// AutoMLJob represents a SageMaker AutoML job.
type AutoMLJob struct {
	CreationTime    time.Time         `json:"CreationTime"`
	Tags            map[string]string `json:"Tags,omitempty"`
	AutoMLJobName   string            `json:"AutoMLJobName"`
	AutoMLJobArn    string            `json:"AutoMLJobArn"`
	AutoMLJobStatus string            `json:"AutoMLJobStatus"`
	RoleArn         string            `json:"RoleArn,omitempty"`
}

func cloneAutoMLJob(j *AutoMLJob) *AutoMLJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)

	return &cp
}

// CreateAutoMLJob creates an AutoML job.
func (b *InMemoryBackend) CreateAutoMLJob(
	name, roleArn string,
	tags map[string]string,
) (*AutoMLJob, error) {
	b.mu.Lock("CreateAutoMLJob")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: AutoMLJobName is required", ErrValidation)
	}

	if _, ok := b.autoMLJobs[name]; ok {
		return nil, fmt.Errorf("%w: AutoML job %q already exists", ErrValidation, name)
	}

	jobARN := arn.Build("sagemaker", b.region, b.accountID, "automl-job/"+name)

	j := &AutoMLJob{
		AutoMLJobName:   name,
		AutoMLJobArn:    jobARN,
		AutoMLJobStatus: trainingJobStatusInProgress,
		RoleArn:         roleArn,
		Tags:            mergeTags(nil, tags),
		CreationTime:    time.Now(),
	}
	b.autoMLJobs[name] = j

	return cloneAutoMLJob(j), nil
}

// DescribeAutoMLJob returns an AutoML job by name.
func (b *InMemoryBackend) DescribeAutoMLJob(name string) (*AutoMLJob, error) {
	b.mu.RLock("DescribeAutoMLJob")
	defer b.mu.RUnlock()

	j, ok := b.autoMLJobs[name]
	if !ok {
		return nil, fmt.Errorf("%w: AutoML job %q not found", ErrAutoMLJobNotFound, name)
	}

	return cloneAutoMLJob(j), nil
}

// StopAutoMLJob sets an AutoML job status to "Stopped".
func (b *InMemoryBackend) StopAutoMLJob(name string) error {
	b.mu.Lock("StopAutoMLJob")
	defer b.mu.Unlock()

	j, ok := b.autoMLJobs[name]
	if !ok {
		return fmt.Errorf("%w: AutoML job %q not found", ErrAutoMLJobNotFound, name)
	}

	// AWS rejects stopping a job that is already in a terminal state.
	if j.AutoMLJobStatus == algorithmStatusCompleted || j.AutoMLJobStatus == pipelineStatusStopped {
		return fmt.Errorf("%w: AutoML job %q cannot be stopped (status: %s)",
			ErrAutoMLJobNotStoppable, name, j.AutoMLJobStatus)
	}

	j.AutoMLJobStatus = pipelineStatusStopped

	return nil
}

// ListAutoMLJobs returns all AutoML jobs sorted by name.
func (b *InMemoryBackend) ListAutoMLJobs(nextToken string) ([]*AutoMLJob, string) {
	b.mu.RLock("ListAutoMLJobs")
	defer b.mu.RUnlock()

	keys := make([]string, 0, len(b.autoMLJobs))
	for k := range b.autoMLJobs {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(keys))

	out := make([]*AutoMLJob, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneAutoMLJob(b.autoMLJobs[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// ---------------------------------------------------------------------------
// CodeRepository
// ---------------------------------------------------------------------------

// CodeRepository represents a SageMaker code repository.
type CodeRepository struct {
	CreationTime       time.Time         `json:"CreationTime"`
	LastModifiedTime   time.Time         `json:"LastModifiedTime"`
	Tags               map[string]string `json:"Tags,omitempty"`
	GitConfig          map[string]string `json:"GitConfig,omitempty"`
	CodeRepositoryName string            `json:"CodeRepositoryName"`
	CodeRepositoryArn  string            `json:"CodeRepositoryArn"`
}

func cloneCodeRepository(r *CodeRepository) *CodeRepository {
	cp := *r
	cp.Tags = maps.Clone(r.Tags)
	cp.GitConfig = maps.Clone(r.GitConfig)

	return &cp
}

// CreateCodeRepository creates a code repository.
func (b *InMemoryBackend) CreateCodeRepository(
	name string,
	gitConfig map[string]string,
	tags map[string]string,
) (*CodeRepository, error) {
	b.mu.Lock("CreateCodeRepository")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: CodeRepositoryName is required", ErrValidation)
	}

	if _, ok := b.codeRepositories[name]; ok {
		return nil, fmt.Errorf("%w: code repository %q already exists", ErrValidation, name)
	}

	repoARN := arn.Build("sagemaker", b.region, b.accountID, "code-repository/"+name)
	now := time.Now()

	r := &CodeRepository{
		CodeRepositoryName: name,
		CodeRepositoryArn:  repoARN,
		GitConfig:          maps.Clone(gitConfig),
		Tags:               mergeTags(nil, tags),
		CreationTime:       now,
		LastModifiedTime:   now,
	}
	b.codeRepositories[name] = r

	return cloneCodeRepository(r), nil
}

// DescribeCodeRepository returns a code repository by name.
func (b *InMemoryBackend) DescribeCodeRepository(name string) (*CodeRepository, error) {
	b.mu.RLock("DescribeCodeRepository")
	defer b.mu.RUnlock()

	r, ok := b.codeRepositories[name]
	if !ok {
		return nil, fmt.Errorf("%w: code repository %q not found", ErrCodeRepositoryNotFound, name)
	}

	return cloneCodeRepository(r), nil
}

// UpdateCodeRepository updates the git config of a code repository.
func (b *InMemoryBackend) UpdateCodeRepository(
	name string,
	gitConfig map[string]string,
) (*CodeRepository, error) {
	b.mu.Lock("UpdateCodeRepository")
	defer b.mu.Unlock()

	r, ok := b.codeRepositories[name]
	if !ok {
		return nil, fmt.Errorf("%w: code repository %q not found", ErrCodeRepositoryNotFound, name)
	}

	if gitConfig != nil {
		r.GitConfig = maps.Clone(gitConfig)
	}

	r.LastModifiedTime = time.Now()

	return cloneCodeRepository(r), nil
}

// DeleteCodeRepository removes a code repository by name.
func (b *InMemoryBackend) DeleteCodeRepository(name string) error {
	b.mu.Lock("DeleteCodeRepository")
	defer b.mu.Unlock()

	if _, ok := b.codeRepositories[name]; !ok {
		return fmt.Errorf("%w: code repository %q not found", ErrCodeRepositoryNotFound, name)
	}

	delete(b.codeRepositories, name)

	return nil
}

// ListCodeRepositories returns all code repositories sorted by name.
func (b *InMemoryBackend) ListCodeRepositories(nextToken string) ([]*CodeRepository, string) {
	b.mu.RLock("ListCodeRepositories")
	defer b.mu.RUnlock()

	keys := make([]string, 0, len(b.codeRepositories))
	for k := range b.codeRepositories {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(keys))

	out := make([]*CodeRepository, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneCodeRepository(b.codeRepositories[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

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

// CreateProject creates a SageMaker project.
func (b *InMemoryBackend) CreateProject(
	name, description string,
	tags map[string]string,
) (*Project, error) {
	b.mu.Lock("CreateProject")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: ProjectName is required", ErrValidation)
	}

	if _, ok := b.projects[name]; ok {
		return nil, fmt.Errorf("%w: project %q already exists", ErrValidation, name)
	}

	projectARN := arn.Build("sagemaker", b.region, b.accountID, "project/"+name)

	p := &Project{
		ProjectName:        name,
		ProjectArn:         projectARN,
		ProjectID:          generateID(),
		ProjectStatus:      "CreateCompleted",
		ProjectDescription: description,
		Tags:               mergeTags(nil, tags),
		CreationTime:       time.Now(),
	}
	b.projects[name] = p

	return cloneProject(p), nil
}

// DescribeProject returns a project by name.
func (b *InMemoryBackend) DescribeProject(name string) (*Project, error) {
	b.mu.RLock("DescribeProject")
	defer b.mu.RUnlock()

	p, ok := b.projects[name]
	if !ok {
		return nil, fmt.Errorf("%w: project %q not found", ErrProjectNotFound, name)
	}

	return cloneProject(p), nil
}

// DeleteProject removes a project by name.
func (b *InMemoryBackend) DeleteProject(name string) error {
	b.mu.Lock("DeleteProject")
	defer b.mu.Unlock()

	if _, ok := b.projects[name]; !ok {
		return fmt.Errorf("%w: project %q not found", ErrProjectNotFound, name)
	}

	delete(b.projects, name)

	return nil
}

// ListProjects returns all projects sorted by name.
func (b *InMemoryBackend) ListProjects(nextToken string) ([]*Project, string) {
	b.mu.RLock("ListProjects")
	defer b.mu.RUnlock()

	keys := make([]string, 0, len(b.projects))
	for k := range b.projects {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(keys))

	out := make([]*Project, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneProject(b.projects[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// ---------------------------------------------------------------------------
// Space
// ---------------------------------------------------------------------------

// Space represents a SageMaker Studio space.
type Space struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	SpaceName        string            `json:"SpaceName"`
	SpaceArn         string            `json:"SpaceArn"`
	DomainID         string            `json:"DomainId"`
	SpaceStatus      string            `json:"SpaceStatus"`
}

func cloneSpace(s *Space) *Space {
	cp := *s
	cp.Tags = maps.Clone(s.Tags)

	return &cp
}

func spaceKey(domainID, spaceName string) string {
	return domainID + "/" + spaceName
}

// CreateSpace creates a SageMaker Studio space.
func (b *InMemoryBackend) CreateSpace(domainID, spaceName string, tags map[string]string) (*Space, error) {
	b.mu.Lock("CreateSpace")
	defer b.mu.Unlock()

	if domainID == "" {
		return nil, fmt.Errorf("%w: DomainID is required", ErrValidation)
	}

	if spaceName == "" {
		return nil, fmt.Errorf("%w: SpaceName is required", ErrValidation)
	}

	key := spaceKey(domainID, spaceName)

	if _, ok := b.spaces[key]; ok {
		return nil, fmt.Errorf("%w: space %q already exists in domain %q", ErrValidation, spaceName, domainID)
	}

	spaceARN := arn.Build("sagemaker", b.region, b.accountID, "space/"+domainID+"/"+spaceName)
	now := time.Now()

	s := &Space{
		SpaceName:        spaceName,
		SpaceArn:         spaceARN,
		DomainID:         domainID,
		SpaceStatus:      "InService",
		Tags:             mergeTags(nil, tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	b.spaces[key] = s

	return cloneSpace(s), nil
}

// DescribeSpace returns a space by domain ID and space name.
func (b *InMemoryBackend) DescribeSpace(domainID, spaceName string) (*Space, error) {
	b.mu.RLock("DescribeSpace")
	defer b.mu.RUnlock()

	s, ok := b.spaces[spaceKey(domainID, spaceName)]
	if !ok {
		return nil, fmt.Errorf("%w: space %q not found in domain %q", ErrSpaceNotFound, spaceName, domainID)
	}

	return cloneSpace(s), nil
}

// DeleteSpace removes a space.
func (b *InMemoryBackend) DeleteSpace(domainID, spaceName string) error {
	b.mu.Lock("DeleteSpace")
	defer b.mu.Unlock()

	key := spaceKey(domainID, spaceName)

	if _, ok := b.spaces[key]; !ok {
		return fmt.Errorf("%w: space %q not found in domain %q", ErrSpaceNotFound, spaceName, domainID)
	}

	delete(b.spaces, key)

	return nil
}

// ListSpaces returns all spaces optionally filtered by domain ID.
func (b *InMemoryBackend) ListSpaces(domainID, nextToken string) ([]*Space, string) {
	b.mu.RLock("ListSpaces")
	defer b.mu.RUnlock()

	var keys []string
	for k, s := range b.spaces {
		if domainID == "" || s.DomainID == domainID {
			keys = append(keys, k)
		}
	}

	sort.Strings(keys)

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(keys))

	out := make([]*Space, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneSpace(b.spaces[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// ---------------------------------------------------------------------------
// SMImage
// ---------------------------------------------------------------------------

// SMImage represents a SageMaker image.
type SMImage struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	ImageName        string            `json:"ImageName"`
	ImageArn         string            `json:"ImageArn"`
	ImageStatus      string            `json:"ImageStatus"`
	Description      string            `json:"Description,omitempty"`
	RoleArn          string            `json:"RoleArn,omitempty"`
}

func cloneSMImage(img *SMImage) *SMImage {
	cp := *img
	cp.Tags = maps.Clone(img.Tags)

	return &cp
}

// CreateImage creates a SageMaker image.
func (b *InMemoryBackend) CreateImage(name, description, roleArn string, tags map[string]string) (*SMImage, error) {
	b.mu.Lock("CreateImage")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: ImageName is required", ErrValidation)
	}

	if _, ok := b.smImages[name]; ok {
		return nil, fmt.Errorf("%w: image %q already exists", ErrValidation, name)
	}

	imageARN := arn.Build("sagemaker", b.region, b.accountID, "image/"+name)
	now := time.Now()

	img := &SMImage{
		ImageName:        name,
		ImageArn:         imageARN,
		ImageStatus:      "CREATED",
		Description:      description,
		RoleArn:          roleArn,
		Tags:             mergeTags(nil, tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	b.smImages[name] = img

	return cloneSMImage(img), nil
}

// DescribeImage returns a SageMaker image by name.
func (b *InMemoryBackend) DescribeImage(name string) (*SMImage, error) {
	b.mu.RLock("DescribeImage")
	defer b.mu.RUnlock()

	img, ok := b.smImages[name]
	if !ok {
		return nil, fmt.Errorf("%w: image %q not found", ErrSMImageNotFound, name)
	}

	return cloneSMImage(img), nil
}

// DeleteImage removes a SageMaker image by name.
func (b *InMemoryBackend) DeleteImage(name string) error {
	b.mu.Lock("DeleteImage")
	defer b.mu.Unlock()

	if _, ok := b.smImages[name]; !ok {
		return fmt.Errorf("%w: image %q not found", ErrSMImageNotFound, name)
	}

	// AWS rejects deletion when image versions still exist.
	if versions, ok := b.imageVersions[name]; ok && len(versions) > 0 {
		return fmt.Errorf("%w: image %q has versions and cannot be deleted", ErrImageHasVersions, name)
	}

	delete(b.smImages, name)

	return nil
}

// ListImages returns all images sorted by name.
func (b *InMemoryBackend) ListImages(nextToken string) ([]*SMImage, string) {
	b.mu.RLock("ListImages")
	defer b.mu.RUnlock()

	keys := make([]string, 0, len(b.smImages))
	for k := range b.smImages {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(keys))

	out := make([]*SMImage, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneSMImage(b.smImages[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// ---------------------------------------------------------------------------
// ImageVersion
// ---------------------------------------------------------------------------

// ImageVersion represents a version of a SageMaker image.
type ImageVersion struct {
	CreationTime       time.Time `json:"CreationTime"`
	LastModifiedTime   time.Time `json:"LastModifiedTime"`
	ImageArn           string    `json:"ImageArn"`
	ImageVersionArn    string    `json:"ImageVersionArn"`
	ImageVersionStatus string    `json:"ImageVersionStatus"`
	Version            int       `json:"Version"`
}

func cloneImageVersion(v *ImageVersion) *ImageVersion {
	cp := *v

	return &cp
}

// CreateImageVersion creates a new version for an image.
func (b *InMemoryBackend) CreateImageVersion(imageName string) (*ImageVersion, error) {
	b.mu.Lock("CreateImageVersion")
	defer b.mu.Unlock()

	img, ok := b.smImages[imageName]
	if !ok {
		return nil, fmt.Errorf("%w: image %q not found", ErrSMImageNotFound, imageName)
	}

	b.imageVersionCounts[imageName]++
	version := b.imageVersionCounts[imageName]

	versionARN := arn.Build(
		"sagemaker", b.region, b.accountID,
		"image-version/"+imageName+"/"+strconv.Itoa(version),
	)
	now := time.Now()

	iv := &ImageVersion{
		ImageArn:           img.ImageArn,
		ImageVersionArn:    versionARN,
		ImageVersionStatus: "CREATED",
		Version:            version,
		CreationTime:       now,
		LastModifiedTime:   now,
	}

	if b.imageVersions[imageName] == nil {
		b.imageVersions[imageName] = make(map[int]*ImageVersion)
	}

	b.imageVersions[imageName][version] = iv

	return cloneImageVersion(iv), nil
}

// DescribeImageVersion returns an image version by image name and version number.
func (b *InMemoryBackend) DescribeImageVersion(imageName string, version int) (*ImageVersion, error) {
	b.mu.RLock("DescribeImageVersion")
	defer b.mu.RUnlock()

	versions, ok := b.imageVersions[imageName]
	if !ok {
		return nil, fmt.Errorf("%w: no versions found for image %q", ErrImageVersionNotFound, imageName)
	}

	iv, ok := versions[version]
	if !ok {
		return nil, fmt.Errorf(
			"%w: version %d not found for image %q", ErrImageVersionNotFound, version, imageName,
		)
	}

	return cloneImageVersion(iv), nil
}

// DeleteImageVersion removes an image version.
func (b *InMemoryBackend) DeleteImageVersion(imageName string, version int) error {
	b.mu.Lock("DeleteImageVersion")
	defer b.mu.Unlock()

	versions, ok := b.imageVersions[imageName]
	if !ok {
		return fmt.Errorf("%w: no versions found for image %q", ErrImageVersionNotFound, imageName)
	}

	if _, exists := versions[version]; !exists {
		return fmt.Errorf("%w: version %d not found for image %q", ErrImageVersionNotFound, version, imageName)
	}

	delete(versions, version)

	return nil
}

// ListImageVersions returns all versions for an image sorted by version number.
func (b *InMemoryBackend) ListImageVersions(imageName, nextToken string) ([]*ImageVersion, string) {
	b.mu.RLock("ListImageVersions")
	defer b.mu.RUnlock()

	versions := b.imageVersions[imageName]

	nums := make([]int, 0, len(versions))
	for v := range versions {
		nums = append(nums, v)
	}

	sort.Ints(nums)

	start := 0
	if nextToken != "" {
		if n, err := strconv.Atoi(nextToken); err == nil {
			for i, v := range nums {
				if v == n {
					start = i

					break
				}
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(nums))

	out := make([]*ImageVersion, 0, end-start)
	for _, n := range nums[start:end] {
		out = append(out, cloneImageVersion(versions[n]))
	}

	next := ""
	if end < len(nums) {
		next = strconv.Itoa(nums[end])
	}

	return out, next
}

// ---------------------------------------------------------------------------
// CompilationJob
// ---------------------------------------------------------------------------

// CompilationJob represents a SageMaker Neo compilation job.
type CompilationJob struct {
	CreationTime         time.Time         `json:"CreationTime"`
	LastModifiedTime     time.Time         `json:"LastModifiedTime"`
	Tags                 map[string]string `json:"Tags,omitempty"`
	CompilationJobName   string            `json:"CompilationJobName"`
	CompilationJobArn    string            `json:"CompilationJobArn"`
	CompilationJobStatus string            `json:"CompilationJobStatus"`
	RoleArn              string            `json:"RoleArn,omitempty"`
}

func cloneCompilationJob(j *CompilationJob) *CompilationJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)

	return &cp
}

// CreateCompilationJob creates a compilation job.
func (b *InMemoryBackend) CreateCompilationJob(
	name, roleArn string,
	tags map[string]string,
) (*CompilationJob, error) {
	b.mu.Lock("CreateCompilationJob")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: CompilationJobName is required", ErrValidation)
	}

	if _, ok := b.compilationJobs[name]; ok {
		return nil, fmt.Errorf("%w: compilation job %q already exists", ErrValidation, name)
	}

	jobARN := arn.Build("sagemaker", b.region, b.accountID, "compilation-job/"+name)
	now := time.Now()

	j := &CompilationJob{
		CompilationJobName:   name,
		CompilationJobArn:    jobARN,
		CompilationJobStatus: "INPROGRESS",
		RoleArn:              roleArn,
		Tags:                 mergeTags(nil, tags),
		CreationTime:         now,
		LastModifiedTime:     now,
	}
	b.compilationJobs[name] = j

	return cloneCompilationJob(j), nil
}

// DescribeCompilationJob returns a compilation job by name.
func (b *InMemoryBackend) DescribeCompilationJob(name string) (*CompilationJob, error) {
	b.mu.RLock("DescribeCompilationJob")
	defer b.mu.RUnlock()

	j, ok := b.compilationJobs[name]
	if !ok {
		return nil, fmt.Errorf("%w: compilation job %q not found", ErrCompilationJobNotFound, name)
	}

	return cloneCompilationJob(j), nil
}

// DeleteCompilationJob removes a compilation job by name.
func (b *InMemoryBackend) DeleteCompilationJob(name string) error {
	b.mu.Lock("DeleteCompilationJob")
	defer b.mu.Unlock()

	if _, ok := b.compilationJobs[name]; !ok {
		return fmt.Errorf("%w: compilation job %q not found", ErrCompilationJobNotFound, name)
	}

	delete(b.compilationJobs, name)

	return nil
}

// StopCompilationJob sets a compilation job status to "STOPPED".
func (b *InMemoryBackend) StopCompilationJob(name string) error {
	b.mu.Lock("StopCompilationJob")
	defer b.mu.Unlock()

	j, ok := b.compilationJobs[name]
	if !ok {
		return fmt.Errorf("%w: compilation job %q not found", ErrCompilationJobNotFound, name)
	}

	// AWS rejects stopping a job that is already in a terminal state.
	if j.CompilationJobStatus == "COMPLETED" || j.CompilationJobStatus == jobStatusStopped {
		return fmt.Errorf("%w: compilation job %q is not running (status: %s)",
			ErrCompilationJobNotStoppable, name, j.CompilationJobStatus)
	}

	j.CompilationJobStatus = jobStatusStopped
	j.LastModifiedTime = time.Now()

	return nil
}

// ListCompilationJobs returns all compilation jobs sorted by name.
func (b *InMemoryBackend) ListCompilationJobs(nextToken string) ([]*CompilationJob, string) {
	b.mu.RLock("ListCompilationJobs")
	defer b.mu.RUnlock()

	keys := make([]string, 0, len(b.compilationJobs))
	for k := range b.compilationJobs {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(keys))

	out := make([]*CompilationJob, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneCompilationJob(b.compilationJobs[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// ---------------------------------------------------------------------------
// MonitoringSchedule
// ---------------------------------------------------------------------------

// MonitoringSchedule represents a SageMaker monitoring schedule.
type MonitoringSchedule struct {
	CreationTime             time.Time         `json:"CreationTime"`
	LastModifiedTime         time.Time         `json:"LastModifiedTime"`
	Tags                     map[string]string `json:"Tags,omitempty"`
	MonitoringScheduleName   string            `json:"MonitoringScheduleName"`
	MonitoringScheduleArn    string            `json:"MonitoringScheduleArn"`
	MonitoringScheduleStatus string            `json:"MonitoringScheduleStatus"`
}

func cloneMonitoringSchedule(ms *MonitoringSchedule) *MonitoringSchedule {
	cp := *ms
	cp.Tags = maps.Clone(ms.Tags)

	return &cp
}

// CreateMonitoringSchedule creates a monitoring schedule.
func (b *InMemoryBackend) CreateMonitoringSchedule(
	name string,
	tags map[string]string,
) (*MonitoringSchedule, error) {
	b.mu.Lock("CreateMonitoringSchedule")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", ErrValidation)
	}

	if _, ok := b.monitoringSchedules[name]; ok {
		return nil, fmt.Errorf("%w: monitoring schedule %q already exists", ErrValidation, name)
	}

	schedARN := arn.Build("sagemaker", b.region, b.accountID, "monitoring-schedule/"+name)
	now := time.Now()

	ms := &MonitoringSchedule{
		MonitoringScheduleName:   name,
		MonitoringScheduleArn:    schedARN,
		MonitoringScheduleStatus: "Scheduled",
		Tags:                     mergeTags(nil, tags),
		CreationTime:             now,
		LastModifiedTime:         now,
	}
	b.monitoringSchedules[name] = ms

	return cloneMonitoringSchedule(ms), nil
}

// DescribeMonitoringSchedule returns a monitoring schedule by name.
func (b *InMemoryBackend) DescribeMonitoringSchedule(name string) (*MonitoringSchedule, error) {
	b.mu.RLock("DescribeMonitoringSchedule")
	defer b.mu.RUnlock()

	ms, ok := b.monitoringSchedules[name]
	if !ok {
		return nil, fmt.Errorf("%w: monitoring schedule %q not found", ErrMonitoringScheduleNotFound, name)
	}

	return cloneMonitoringSchedule(ms), nil
}

// DeleteMonitoringSchedule removes a monitoring schedule.
func (b *InMemoryBackend) DeleteMonitoringSchedule(name string) error {
	b.mu.Lock("DeleteMonitoringSchedule")
	defer b.mu.Unlock()

	if _, ok := b.monitoringSchedules[name]; !ok {
		return fmt.Errorf("%w: monitoring schedule %q not found", ErrMonitoringScheduleNotFound, name)
	}

	delete(b.monitoringSchedules, name)

	return nil
}

// StopMonitoringSchedule sets a monitoring schedule status to "Stopped".
func (b *InMemoryBackend) StopMonitoringSchedule(name string) error {
	b.mu.Lock("StopMonitoringSchedule")
	defer b.mu.Unlock()

	ms, ok := b.monitoringSchedules[name]
	if !ok {
		return fmt.Errorf("%w: monitoring schedule %q not found", ErrMonitoringScheduleNotFound, name)
	}

	// AWS rejects stopping an already-stopped schedule.
	if ms.MonitoringScheduleStatus == pipelineStatusStopped {
		return fmt.Errorf("%w: monitoring schedule %q is already stopped", ErrMonitoringScheduleAlreadyStopped, name)
	}

	ms.MonitoringScheduleStatus = pipelineStatusStopped
	ms.LastModifiedTime = time.Now()

	return nil
}

// StartMonitoringSchedule sets a monitoring schedule status to "Scheduled".
func (b *InMemoryBackend) StartMonitoringSchedule(name string) error {
	b.mu.Lock("StartMonitoringSchedule")
	defer b.mu.Unlock()

	ms, ok := b.monitoringSchedules[name]
	if !ok {
		return fmt.Errorf("%w: monitoring schedule %q not found", ErrMonitoringScheduleNotFound, name)
	}

	// AWS rejects starting a schedule that is not in Stopped state.
	if ms.MonitoringScheduleStatus != pipelineStatusStopped {
		return fmt.Errorf("%w: monitoring schedule %q is not stopped (status: %s)",
			ErrMonitoringScheduleNotStopped, name, ms.MonitoringScheduleStatus)
	}

	ms.MonitoringScheduleStatus = "Scheduled"
	ms.LastModifiedTime = time.Now()

	return nil
}

// UpdateMonitoringSchedule updates a monitoring schedule (marks it modified).
func (b *InMemoryBackend) UpdateMonitoringSchedule(name string) (*MonitoringSchedule, error) {
	b.mu.Lock("UpdateMonitoringSchedule")
	defer b.mu.Unlock()

	ms, ok := b.monitoringSchedules[name]
	if !ok {
		return nil, fmt.Errorf("%w: monitoring schedule %q not found", ErrMonitoringScheduleNotFound, name)
	}

	ms.LastModifiedTime = time.Now()

	return cloneMonitoringSchedule(ms), nil
}

// ListMonitoringSchedules returns all monitoring schedules sorted by name.
func (b *InMemoryBackend) ListMonitoringSchedules(nextToken string) ([]*MonitoringSchedule, string) {
	b.mu.RLock("ListMonitoringSchedules")
	defer b.mu.RUnlock()

	keys := make([]string, 0, len(b.monitoringSchedules))
	for k := range b.monitoringSchedules {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(keys))

	out := make([]*MonitoringSchedule, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneMonitoringSchedule(b.monitoringSchedules[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// ---------------------------------------------------------------------------
// Workteam
// ---------------------------------------------------------------------------

// Workteam represents a SageMaker Ground Truth workteam.
type Workteam struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	WorkteamName     string            `json:"WorkteamName"`
	WorkteamArn      string            `json:"WorkteamArn"`
	Description      string            `json:"Description,omitempty"`
}

func cloneWorkteam(w *Workteam) *Workteam {
	cp := *w
	cp.Tags = maps.Clone(w.Tags)

	return &cp
}

// CreateWorkteam creates a workteam.
func (b *InMemoryBackend) CreateWorkteam(
	name, description string,
	tags map[string]string,
) (*Workteam, error) {
	b.mu.Lock("CreateWorkteam")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: WorkteamName is required", ErrValidation)
	}

	if _, ok := b.workteams[name]; ok {
		return nil, fmt.Errorf("%w: workteam %q already exists", ErrValidation, name)
	}

	workteamARN := arn.Build("sagemaker", b.region, b.accountID, "workteam/"+name)
	now := time.Now()

	w := &Workteam{
		WorkteamName:     name,
		WorkteamArn:      workteamARN,
		Description:      description,
		Tags:             mergeTags(nil, tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	b.workteams[name] = w

	return cloneWorkteam(w), nil
}

// DescribeWorkteam returns a workteam by name.
func (b *InMemoryBackend) DescribeWorkteam(name string) (*Workteam, error) {
	b.mu.RLock("DescribeWorkteam")
	defer b.mu.RUnlock()

	w, ok := b.workteams[name]
	if !ok {
		return nil, fmt.Errorf("%w: workteam %q not found", ErrWorkteamNotFound, name)
	}

	return cloneWorkteam(w), nil
}

// DeleteWorkteam removes a workteam.
func (b *InMemoryBackend) DeleteWorkteam(name string) error {
	b.mu.Lock("DeleteWorkteam")
	defer b.mu.Unlock()

	if _, ok := b.workteams[name]; !ok {
		return fmt.Errorf("%w: workteam %q not found", ErrWorkteamNotFound, name)
	}

	delete(b.workteams, name)

	return nil
}

// ListWorkteams returns all workteams sorted by name.
func (b *InMemoryBackend) ListWorkteams(nextToken string) ([]*Workteam, string) {
	b.mu.RLock("ListWorkteams")
	defer b.mu.RUnlock()

	keys := make([]string, 0, len(b.workteams))
	for k := range b.workteams {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(keys))

	out := make([]*Workteam, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneWorkteam(b.workteams[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}
