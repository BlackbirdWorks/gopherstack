package sagemaker

import (
	"context"
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
	// ResourcePolicy is the resource policy JSON document attached via
	// PutModelPackageGroupPolicy, if any.
	ResourcePolicy string `json:"ResourcePolicy,omitempty"`
}

func cloneModelPackageGroup(g *ModelPackageGroup) *ModelPackageGroup {
	cp := *g
	cp.Tags = maps.Clone(g.Tags)

	return &cp
}

// CreateModelPackageGroup creates a new model package group.
func (b *InMemoryBackend) CreateModelPackageGroup(
	ctx context.Context,
	name, description string,
	tags map[string]string,
) (*ModelPackageGroup, error) {
	b.mu.Lock("CreateModelPackageGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name == "" {
		return nil, fmt.Errorf("%w: ModelPackageGroupName is required", ErrValidation)
	}

	if _, ok := b.modelPackageGroupsStore(region).Get(name); ok {
		return nil, fmt.Errorf("%w: model package group %q already exists", ErrValidation, name)
	}

	groupARN := arn.Build("sagemaker", region, b.accountID, "model-package-group/"+name)

	g := &ModelPackageGroup{
		ModelPackageGroupName:        name,
		ModelPackageGroupArn:         groupARN,
		ModelPackageGroupDescription: description,
		ModelPackageGroupStatus:      algorithmStatusCompleted,
		Tags:                         mergeTags(nil, tags),
		CreationTime:                 time.Now(),
	}
	b.modelPackageGroupsStore(region).Put(g)

	return cloneModelPackageGroup(g), nil
}

// DescribeModelPackageGroup returns a model package group by name.
func (b *InMemoryBackend) DescribeModelPackageGroup(ctx context.Context, name string) (*ModelPackageGroup, error) {
	b.mu.RLock("DescribeModelPackageGroup")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	g, ok := b.modelPackageGroupsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: model package group %q not found", ErrModelPackageGroupNotFound, name)
	}

	return cloneModelPackageGroup(g), nil
}

// DeleteModelPackageGroup removes a model package group by name.
func (b *InMemoryBackend) DeleteModelPackageGroup(ctx context.Context, name string) error {
	b.mu.Lock("DeleteModelPackageGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.modelPackageGroupsStore(region).Get(name); !ok {
		return fmt.Errorf("%w: model package group %q not found", ErrModelPackageGroupNotFound, name)
	}

	// AWS rejects deletion when model packages still exist in the group.
	for _, mp := range b.modelPackagesStore(region).All() {
		if mp.ModelPackageGroupName == name {
			return fmt.Errorf("%w: model package group %q has model packages and cannot be deleted",
				ErrModelPackageGroupHasPackages, name)
		}
	}

	store := b.modelPackageGroupsStore(region)
	store.Delete(name)

	return nil
}

// ListModelPackageGroups returns all model package groups, sorted by name.
func (b *InMemoryBackend) ListModelPackageGroups(ctx context.Context, nextToken string) ([]*ModelPackageGroup, string) {
	b.mu.RLock("ListModelPackageGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.modelPackageGroupsStoreRO(region),
		nextToken,
		cloneModelPackageGroup,
		func(v *ModelPackageGroup) string { return v.ModelPackageGroupName },
	)
}

// ErrModelPackageGroupPolicyNotFound is returned when a model package group
// has no resource policy attached.
var ErrModelPackageGroupPolicyNotFound = awserr.New("ValidationException", awserr.ErrNotFound)

// GetModelPackageGroupPolicy returns the resource policy attached to a model
// package group.
func (b *InMemoryBackend) GetModelPackageGroupPolicy(ctx context.Context, name string) (string, error) {
	b.mu.RLock("GetModelPackageGroupPolicy")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	g, ok := b.modelPackageGroupsStoreRO(region).Get(name)
	if !ok {
		return "", fmt.Errorf("%w: model package group %q not found", ErrModelPackageGroupNotFound, name)
	}

	if g.ResourcePolicy == "" {
		return "", fmt.Errorf(
			"%w: model package group %q has no resource policy attached",
			ErrModelPackageGroupPolicyNotFound, name,
		)
	}

	return g.ResourcePolicy, nil
}

// PutModelPackageGroupPolicy attaches (or replaces) the resource policy for a
// model package group.
func (b *InMemoryBackend) PutModelPackageGroupPolicy(
	ctx context.Context,
	name, policy string,
) (*ModelPackageGroup, error) {
	b.mu.Lock("PutModelPackageGroupPolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	g, ok := b.modelPackageGroupsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: model package group %q not found", ErrModelPackageGroupNotFound, name)
	}

	if policy == "" {
		return nil, fmt.Errorf("%w: ResourcePolicy is required", ErrValidation)
	}

	g.ResourcePolicy = policy

	return cloneModelPackageGroup(g), nil
}

// DeleteModelPackageGroupPolicy removes the resource policy from a model
// package group.
func (b *InMemoryBackend) DeleteModelPackageGroupPolicy(ctx context.Context, name string) error {
	b.mu.Lock("DeleteModelPackageGroupPolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	g, ok := b.modelPackageGroupsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: model package group %q not found", ErrModelPackageGroupNotFound, name)
	}

	g.ResourcePolicy = ""

	return nil
}

// ---------------------------------------------------------------------------
// ModelPackage CRUD (name/ARN dual lookup)
// ---------------------------------------------------------------------------

// CreateModelPackage creates a model package.
func (b *InMemoryBackend) CreateModelPackage(
	ctx context.Context,
	name, groupName, description string,
	tags map[string]string,
) (*ModelPackage, error) {
	b.mu.Lock("CreateModelPackage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name == "" {
		return nil, fmt.Errorf("%w: ModelPackageName is required", ErrValidation)
	}

	mpARN := arn.Build("sagemaker", region, b.accountID, "model-package/"+name)

	if _, ok := b.modelPackagesStore(region).Get(mpARN); ok {
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
	b.modelPackagesStore(region).Put(mp)
	b.modelPackageARNIndexStore(region)[name] = mpARN

	return cloneModelPackage(mp), nil
}

// DescribeModelPackage returns a model package by name or ARN.
func (b *InMemoryBackend) DescribeModelPackage(ctx context.Context, nameOrArn string) (*ModelPackage, error) {
	b.mu.RLock("DescribeModelPackage")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	// Try direct ARN lookup first.
	if mp, ok := b.modelPackagesStoreRO(region).Get(nameOrArn); ok {
		return cloneModelPackage(mp), nil
	}

	// Try name → ARN index.
	if arnStr, ok := b.modelPackageARNIndexStoreRO(region)[nameOrArn]; ok {
		if mp, found := b.modelPackagesStoreRO(region).Get(arnStr); found {
			return cloneModelPackage(mp), nil
		}
	}

	return nil, fmt.Errorf("%w: model package %q not found", ErrModelPackageNotFound, nameOrArn)
}

// DeleteModelPackage removes a model package by name or ARN.
func (b *InMemoryBackend) DeleteModelPackage(ctx context.Context, nameOrArn string) error {
	b.mu.Lock("DeleteModelPackage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	arnStr := nameOrArn
	if v, ok := b.modelPackageARNIndexStore(region)[nameOrArn]; ok {
		arnStr = v
	}

	if _, ok := b.modelPackagesStore(region).Get(arnStr); !ok {
		return fmt.Errorf("%w: model package %q not found", ErrModelPackageNotFound, nameOrArn)
	}

	mp := tableGet(b.modelPackagesStore(region), arnStr)
	arnIdxStore := b.modelPackageARNIndexStore(region)
	delete(arnIdxStore, mp.ModelPackageName)
	mpStore := b.modelPackagesStore(region)
	mpStore.Delete(arnStr)

	return nil
}

// ListModelPackages returns model packages, optionally filtered by group name.
func (b *InMemoryBackend) ListModelPackages(
	ctx context.Context,
	groupName, nextToken string,
) ([]*ModelPackage, string) {
	b.mu.RLock("ListModelPackages")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	var arns []string
	for _, mp := range b.modelPackagesStoreRO(region).All() {
		if groupName == "" || mp.ModelPackageGroupName == groupName {
			arns = append(arns, mp.ModelPackageArn)
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
		out = append(out, cloneModelPackage(tableGet(b.modelPackagesStoreRO(region), k)))
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

// AutoMLOutputDataConfig specifies the S3 output location for an AutoML job.
type AutoMLOutputDataConfig struct {
	S3OutputPath string `json:"S3OutputPath,omitempty"`
	KmsKeyID     string `json:"KmsKeyId,omitempty"`
}

// AutoMLJobObjective specifies the optimization metric for an AutoML job.
type AutoMLJobObjective struct {
	MetricName string `json:"MetricName"`
}

// AutoMLJob represents a SageMaker AutoML job.
type AutoMLJob struct {
	CreationTime       time.Time               `json:"CreationTime"`
	Tags               map[string]string       `json:"Tags,omitempty"`
	OutputDataConfig   *AutoMLOutputDataConfig `json:"OutputDataConfig,omitempty"`
	AutoMLJobObjective *AutoMLJobObjective     `json:"AutoMLJobObjective,omitempty"`
	AutoMLJobName      string                  `json:"AutoMLJobName"`
	AutoMLJobArn       string                  `json:"AutoMLJobArn"`
	AutoMLJobStatus    string                  `json:"AutoMLJobStatus"`
	RoleArn            string                  `json:"RoleArn,omitempty"`
}

func cloneAutoMLJob(j *AutoMLJob) *AutoMLJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)

	if j.OutputDataConfig != nil {
		odc := *j.OutputDataConfig
		cp.OutputDataConfig = &odc
	}

	if j.AutoMLJobObjective != nil {
		obj := *j.AutoMLJobObjective
		cp.AutoMLJobObjective = &obj
	}

	return &cp
}

// CreateAutoMLJob creates an AutoML job.
func (b *InMemoryBackend) CreateAutoMLJob(
	ctx context.Context,
	name, roleArn string,
	tags map[string]string,
) (*AutoMLJob, error) {
	b.mu.Lock("CreateAutoMLJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name == "" {
		return nil, fmt.Errorf("%w: AutoMLJobName is required", ErrValidation)
	}

	if _, ok := b.autoMLJobsStore(region).Get(name); ok {
		return nil, fmt.Errorf("%w: AutoML job %q already exists", ErrValidation, name)
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "automl-job/"+name)

	j := &AutoMLJob{
		AutoMLJobName:   name,
		AutoMLJobArn:    jobARN,
		AutoMLJobStatus: trainingJobStatusInProgress,
		RoleArn:         roleArn,
		Tags:            mergeTags(nil, tags),
		CreationTime:    time.Now(),
	}
	b.autoMLJobsStore(region).Put(j)

	return cloneAutoMLJob(j), nil
}

// DescribeAutoMLJob returns an AutoML job by name.
func (b *InMemoryBackend) DescribeAutoMLJob(ctx context.Context, name string) (*AutoMLJob, error) {
	b.mu.RLock("DescribeAutoMLJob")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	j, ok := b.autoMLJobsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: AutoML job %q not found", ErrAutoMLJobNotFound, name)
	}

	return cloneAutoMLJob(j), nil
}

// StopAutoMLJob sets an AutoML job status to "Stopped".
func (b *InMemoryBackend) StopAutoMLJob(ctx context.Context, name string) error {
	b.mu.Lock("StopAutoMLJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	j, ok := b.autoMLJobsStore(region).Get(name)
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
func (b *InMemoryBackend) ListAutoMLJobs(ctx context.Context, nextToken string) ([]*AutoMLJob, string) {
	b.mu.RLock("ListAutoMLJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.autoMLJobsStoreRO(region),
		nextToken,
		cloneAutoMLJob,
		func(v *AutoMLJob) string { return v.AutoMLJobName },
	)
}

// SetAutoMLJobExtras sets optional configuration fields on an existing AutoML job
// that were not included in the original CreateAutoMLJob signature.
func (b *InMemoryBackend) SetAutoMLJobExtras(
	ctx context.Context,
	name string,
	outputDataConfig *AutoMLOutputDataConfig,
	objective *AutoMLJobObjective,
) error {
	b.mu.Lock("SetAutoMLJobExtras")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	j, ok := b.autoMLJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: AutoML job %q not found", ErrAutoMLJobNotFound, name)
	}

	if outputDataConfig != nil {
		odc := *outputDataConfig
		j.OutputDataConfig = &odc
	}

	if objective != nil {
		obj := *objective
		j.AutoMLJobObjective = &obj
	}

	return nil
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
	ctx context.Context,
	name string,
	gitConfig map[string]string,
	tags map[string]string,
) (*CodeRepository, error) {
	b.mu.Lock("CreateCodeRepository")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name == "" {
		return nil, fmt.Errorf("%w: CodeRepositoryName is required", ErrValidation)
	}

	if _, ok := b.codeRepositoriesStore(region).Get(name); ok {
		return nil, fmt.Errorf("%w: code repository %q already exists", ErrValidation, name)
	}

	repoARN := arn.Build("sagemaker", region, b.accountID, "code-repository/"+name)
	now := time.Now()

	r := &CodeRepository{
		CodeRepositoryName: name,
		CodeRepositoryArn:  repoARN,
		GitConfig:          maps.Clone(gitConfig),
		Tags:               mergeTags(nil, tags),
		CreationTime:       now,
		LastModifiedTime:   now,
	}
	b.codeRepositoriesStore(region).Put(r)

	return cloneCodeRepository(r), nil
}

// DescribeCodeRepository returns a code repository by name.
func (b *InMemoryBackend) DescribeCodeRepository(ctx context.Context, name string) (*CodeRepository, error) {
	b.mu.RLock("DescribeCodeRepository")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	r, ok := b.codeRepositoriesStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: code repository %q not found", ErrCodeRepositoryNotFound, name)
	}

	return cloneCodeRepository(r), nil
}

// UpdateCodeRepository updates the git config of a code repository.
func (b *InMemoryBackend) UpdateCodeRepository(
	ctx context.Context,
	name string,
	gitConfig map[string]string,
) (*CodeRepository, error) {
	b.mu.Lock("UpdateCodeRepository")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	r, ok := b.codeRepositoriesStore(region).Get(name)
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
func (b *InMemoryBackend) DeleteCodeRepository(ctx context.Context, name string) error {
	b.mu.Lock("DeleteCodeRepository")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.codeRepositoriesStore(region).Get(name); !ok {
		return fmt.Errorf("%w: code repository %q not found", ErrCodeRepositoryNotFound, name)
	}

	store := b.codeRepositoriesStore(region)
	store.Delete(name)

	return nil
}

// ListCodeRepositories returns all code repositories sorted by name.
func (b *InMemoryBackend) ListCodeRepositories(ctx context.Context, nextToken string) ([]*CodeRepository, string) {
	b.mu.RLock("ListCodeRepositories")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.codeRepositoriesStoreRO(region),
		nextToken,
		cloneCodeRepository,
		func(v *CodeRepository) string { return v.CodeRepositoryName },
	)
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
func (b *InMemoryBackend) CreateSpace(
	ctx context.Context,
	domainID, spaceName string,
	tags map[string]string,
) (*Space, error) {
	b.mu.Lock("CreateSpace")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if domainID == "" {
		return nil, fmt.Errorf("%w: DomainID is required", ErrValidation)
	}

	if spaceName == "" {
		return nil, fmt.Errorf("%w: SpaceName is required", ErrValidation)
	}

	key := spaceKey(domainID, spaceName)

	if _, ok := b.spacesStore(region).Get(key); ok {
		return nil, fmt.Errorf("%w: space %q already exists in domain %q", ErrValidation, spaceName, domainID)
	}

	spaceARN := arn.Build("sagemaker", region, b.accountID, "space/"+domainID+"/"+spaceName)
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
	b.spacesStore(region).Put(s)

	return cloneSpace(s), nil
}

// DescribeSpace returns a space by domain ID and space name.
func (b *InMemoryBackend) DescribeSpace(ctx context.Context, domainID, spaceName string) (*Space, error) {
	b.mu.RLock("DescribeSpace")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	s, ok := b.spacesStoreRO(region).Get(spaceKey(domainID, spaceName))
	if !ok {
		return nil, fmt.Errorf("%w: space %q not found in domain %q", ErrSpaceNotFound, spaceName, domainID)
	}

	return cloneSpace(s), nil
}

// DeleteSpace removes a space.
func (b *InMemoryBackend) DeleteSpace(ctx context.Context, domainID, spaceName string) error {
	b.mu.Lock("DeleteSpace")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	key := spaceKey(domainID, spaceName)

	if _, ok := b.spacesStore(region).Get(key); !ok {
		return fmt.Errorf("%w: space %q not found in domain %q", ErrSpaceNotFound, spaceName, domainID)
	}

	store := b.spacesStore(region)
	store.Delete(key)

	return nil
}

// ListSpaces returns all spaces optionally filtered by domain ID.
func (b *InMemoryBackend) ListSpaces(ctx context.Context, domainID, nextToken string) ([]*Space, string) {
	b.mu.RLock("ListSpaces")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	var keys []string
	for _, s := range b.spacesStoreRO(region).All() {
		if domainID == "" || s.DomainID == domainID {
			keys = append(keys, spaceKey(s.DomainID, s.SpaceName))
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
		out = append(out, cloneSpace(tableGet(b.spacesStoreRO(region), k)))
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
	DisplayName      string            `json:"DisplayName,omitempty"`
	RoleArn          string            `json:"RoleArn,omitempty"`
}

func cloneSMImage(img *SMImage) *SMImage {
	cp := *img
	cp.Tags = maps.Clone(img.Tags)

	return &cp
}

// CreateImage creates a SageMaker image.
func (b *InMemoryBackend) CreateImage(
	ctx context.Context,
	name, description, roleArn string,
	tags map[string]string,
) (*SMImage, error) {
	b.mu.Lock("CreateImage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name == "" {
		return nil, fmt.Errorf("%w: ImageName is required", ErrValidation)
	}

	if _, ok := b.smImagesStore(region).Get(name); ok {
		return nil, fmt.Errorf("%w: image %q already exists", ErrValidation, name)
	}

	imageARN := arn.Build("sagemaker", region, b.accountID, "image/"+name)
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
	b.smImagesStore(region).Put(img)

	return cloneSMImage(img), nil
}

// DescribeImage returns a SageMaker image by name.
func (b *InMemoryBackend) DescribeImage(ctx context.Context, name string) (*SMImage, error) {
	b.mu.RLock("DescribeImage")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	img, ok := b.smImagesStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: image %q not found", ErrSMImageNotFound, name)
	}

	return cloneSMImage(img), nil
}

// UpdateImageOptions bundles the mutable fields accepted by UpdateImage.
// Nil pointer fields are left unchanged.
type UpdateImageOptions struct {
	Description      *string
	DisplayName      *string
	RoleArn          *string
	DeleteProperties []string
}

// UpdateImage updates a SageMaker image's mutable metadata (Description,
// DisplayName, RoleArn), optionally clearing Description/DisplayName first
// via DeleteProperties.
func (b *InMemoryBackend) UpdateImage(
	ctx context.Context,
	name string,
	opts UpdateImageOptions,
) (*SMImage, error) {
	b.mu.Lock("UpdateImage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	img, ok := b.smImagesStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: image %q not found", ErrSMImageNotFound, name)
	}

	for _, prop := range opts.DeleteProperties {
		switch prop {
		case "Description":
			img.Description = ""
		case "DisplayName":
			img.DisplayName = ""
		default:
			return nil, fmt.Errorf("%w: DeleteProperties value %q is not supported", ErrValidation, prop)
		}
	}

	if opts.Description != nil {
		img.Description = *opts.Description
	}

	if opts.DisplayName != nil {
		img.DisplayName = *opts.DisplayName
	}

	if opts.RoleArn != nil {
		img.RoleArn = *opts.RoleArn
	}

	img.LastModifiedTime = time.Now()

	return cloneSMImage(img), nil
}

// DeleteImage removes a SageMaker image by name.
func (b *InMemoryBackend) DeleteImage(ctx context.Context, name string) error {
	b.mu.Lock("DeleteImage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.smImagesStore(region).Get(name); !ok {
		return fmt.Errorf("%w: image %q not found", ErrSMImageNotFound, name)
	}

	// AWS rejects deletion when image versions still exist.
	if versions, ok := b.imageVersionsStore(region)[name]; ok && len(versions) > 0 {
		return fmt.Errorf("%w: image %q has versions and cannot be deleted", ErrImageHasVersions, name)
	}

	store := b.smImagesStore(region)
	store.Delete(name)

	return nil
}

// ListImages returns all images sorted by name.
func (b *InMemoryBackend) ListImages(ctx context.Context, nextToken string) ([]*SMImage, string) {
	b.mu.RLock("ListImages")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.smImagesStoreRO(region),
		nextToken,
		cloneSMImage,
		func(v *SMImage) string { return v.ImageName },
	)
}

// ---------------------------------------------------------------------------
// ImageVersion
// ---------------------------------------------------------------------------

// ImageVersion represents a version of a SageMaker image.
type ImageVersion struct {
	CreationTime       time.Time `json:"CreationTime"`
	LastModifiedTime   time.Time `json:"LastModifiedTime"`
	JobType            string    `json:"JobType,omitempty"`
	ImageArn           string    `json:"ImageArn"`
	ImageVersionArn    string    `json:"ImageVersionArn"`
	ImageVersionStatus string    `json:"ImageVersionStatus"`
	MLFramework        string    `json:"MLFramework,omitempty"`
	Processor          string    `json:"Processor,omitempty"`
	ProgrammingLang    string    `json:"ProgrammingLang,omitempty"`
	ReleaseNotes       string    `json:"ReleaseNotes,omitempty"`
	VendorGuidance     string    `json:"VendorGuidance,omitempty"`
	Aliases            []string  `json:"Aliases,omitempty"`
	Version            int       `json:"Version"`
	Horovod            bool      `json:"Horovod,omitempty"`
}

func cloneImageVersion(v *ImageVersion) *ImageVersion {
	cp := *v
	cp.Aliases = append([]string(nil), v.Aliases...)

	return &cp
}

// CreateImageVersion creates a new version for an image.
func (b *InMemoryBackend) CreateImageVersion(ctx context.Context, imageName string) (*ImageVersion, error) {
	b.mu.Lock("CreateImageVersion")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	img, ok := b.smImagesStore(region).Get(imageName)
	if !ok {
		return nil, fmt.Errorf("%w: image %q not found", ErrSMImageNotFound, imageName)
	}

	b.imageVersionCountsStore(region)[imageName]++
	version := b.imageVersionCountsStore(region)[imageName]

	versionARN := arn.Build(
		"sagemaker", region, b.accountID,
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

	if b.imageVersionsStore(region)[imageName] == nil {
		b.imageVersionsStore(region)[imageName] = make(map[int]*ImageVersion)
	}

	b.imageVersionsStore(region)[imageName][version] = iv

	return cloneImageVersion(iv), nil
}

// DescribeImageVersion returns an image version by image name and version number.
func (b *InMemoryBackend) DescribeImageVersion(
	ctx context.Context,
	imageName string,
	version int,
) (*ImageVersion, error) {
	b.mu.RLock("DescribeImageVersion")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	versions, ok := b.imageVersionsStoreRO(region)[imageName]
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

// UpdateImageVersionOptions bundles the mutable fields accepted by
// UpdateImageVersion. Nil/empty fields are left unchanged.
type UpdateImageVersionOptions struct {
	Horovod         *bool
	JobType         string
	MLFramework     string
	Processor       string
	ProgrammingLang string
	ReleaseNotes    string
	VendorGuidance  string
	AliasesToAdd    []string
	AliasesToDelete []string
}

// UpdateImageVersion updates a SageMaker image version's mutable metadata.
func (b *InMemoryBackend) UpdateImageVersion(
	ctx context.Context,
	imageName string,
	version int,
	opts UpdateImageVersionOptions,
) (*ImageVersion, error) {
	b.mu.Lock("UpdateImageVersion")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	versions, ok := b.imageVersionsStore(region)[imageName]
	if !ok {
		return nil, fmt.Errorf("%w: no versions found for image %q", ErrImageVersionNotFound, imageName)
	}

	if version <= 0 {
		version = b.imageVersionCountsStore(region)[imageName]
	}

	iv, ok := versions[version]
	if !ok {
		return nil, fmt.Errorf(
			"%w: version %d not found for image %q", ErrImageVersionNotFound, version, imageName,
		)
	}

	applyImageVersionUpdate(iv, opts)
	iv.LastModifiedTime = time.Now()

	return cloneImageVersion(iv), nil
}

// applyImageVersionUpdate mutates iv in place per opts. Split out from
// UpdateImageVersion to keep that method's cyclomatic complexity low.
func applyImageVersionUpdate(iv *ImageVersion, opts UpdateImageVersionOptions) {
	if opts.Horovod != nil {
		iv.Horovod = *opts.Horovod
	}

	if opts.JobType != "" {
		iv.JobType = opts.JobType
	}

	if opts.MLFramework != "" {
		iv.MLFramework = opts.MLFramework
	}

	if opts.Processor != "" {
		iv.Processor = opts.Processor
	}

	if opts.ProgrammingLang != "" {
		iv.ProgrammingLang = opts.ProgrammingLang
	}

	if opts.ReleaseNotes != "" {
		iv.ReleaseNotes = opts.ReleaseNotes
	}

	if opts.VendorGuidance != "" {
		iv.VendorGuidance = opts.VendorGuidance
	}

	iv.Aliases = applyAliasChanges(iv.Aliases, opts.AliasesToAdd, opts.AliasesToDelete)
}

// applyAliasChanges returns aliases with additions appended (de-duplicated)
// and deletions removed.
func applyAliasChanges(aliases, toAdd, toDelete []string) []string {
	del := make(map[string]bool, len(toDelete))
	for _, a := range toDelete {
		del[a] = true
	}

	seen := make(map[string]bool, len(aliases)+len(toAdd))
	out := make([]string, 0, len(aliases)+len(toAdd))

	for _, a := range append(append([]string(nil), aliases...), toAdd...) {
		if del[a] || seen[a] {
			continue
		}

		seen[a] = true
		out = append(out, a)
	}

	return out
}

// DeleteImageVersion removes an image version.
func (b *InMemoryBackend) DeleteImageVersion(ctx context.Context, imageName string, version int) error {
	b.mu.Lock("DeleteImageVersion")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	versions, ok := b.imageVersionsStore(region)[imageName]
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
func (b *InMemoryBackend) ListImageVersions(
	ctx context.Context,
	imageName, nextToken string,
) ([]*ImageVersion, string) {
	b.mu.RLock("ListImageVersions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	versions := b.imageVersionsStoreRO(region)[imageName]

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

// CompilationInputConfig specifies the model source for a Neo compilation job.
type CompilationInputConfig struct {
	S3Uri            string `json:"S3Uri,omitempty"`
	DataInputConfig  string `json:"DataInputConfig,omitempty"`
	Framework        string `json:"Framework,omitempty"`
	FrameworkVersion string `json:"FrameworkVersion,omitempty"`
}

// CompilationOutputConfig specifies the output destination for a Neo compilation job.
type CompilationOutputConfig struct {
	S3OutputLocation string `json:"S3OutputLocation,omitempty"`
	TargetDevice     string `json:"TargetDevice,omitempty"`
	KmsKeyID         string `json:"KmsKeyId,omitempty"`
}

// CompilationJob represents a SageMaker Neo compilation job.
type CompilationJob struct {
	CreationTime         time.Time                `json:"CreationTime"`
	LastModifiedTime     time.Time                `json:"LastModifiedTime"`
	Tags                 map[string]string        `json:"Tags,omitempty"`
	InputConfig          *CompilationInputConfig  `json:"InputConfig,omitempty"`
	OutputConfig         *CompilationOutputConfig `json:"OutputConfig,omitempty"`
	StoppingCondition    *StoppingCondition       `json:"StoppingCondition,omitempty"`
	CompilationJobName   string                   `json:"CompilationJobName"`
	CompilationJobArn    string                   `json:"CompilationJobArn"`
	CompilationJobStatus string                   `json:"CompilationJobStatus"`
	RoleArn              string                   `json:"RoleArn,omitempty"`
}

func cloneCompilationJob(j *CompilationJob) *CompilationJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)

	if j.InputConfig != nil {
		ic := *j.InputConfig
		cp.InputConfig = &ic
	}

	if j.OutputConfig != nil {
		oc := *j.OutputConfig
		cp.OutputConfig = &oc
	}

	if j.StoppingCondition != nil {
		sc := *j.StoppingCondition
		cp.StoppingCondition = &sc
	}

	return &cp
}

// CreateCompilationJob creates a compilation job.
func (b *InMemoryBackend) CreateCompilationJob(
	ctx context.Context,
	name, roleArn string,
	tags map[string]string,
) (*CompilationJob, error) {
	b.mu.Lock("CreateCompilationJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name == "" {
		return nil, fmt.Errorf("%w: CompilationJobName is required", ErrValidation)
	}

	if _, ok := b.compilationJobsStore(region).Get(name); ok {
		return nil, fmt.Errorf("%w: compilation job %q already exists", ErrValidation, name)
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "compilation-job/"+name)
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
	b.compilationJobsStore(region).Put(j)

	return cloneCompilationJob(j), nil
}

// DescribeCompilationJob returns a compilation job by name.
func (b *InMemoryBackend) DescribeCompilationJob(ctx context.Context, name string) (*CompilationJob, error) {
	b.mu.RLock("DescribeCompilationJob")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	j, ok := b.compilationJobsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: compilation job %q not found", ErrCompilationJobNotFound, name)
	}

	return cloneCompilationJob(j), nil
}

// DeleteCompilationJob removes a compilation job by name.
func (b *InMemoryBackend) DeleteCompilationJob(ctx context.Context, name string) error {
	b.mu.Lock("DeleteCompilationJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.compilationJobsStore(region).Get(name); !ok {
		return fmt.Errorf("%w: compilation job %q not found", ErrCompilationJobNotFound, name)
	}

	store := b.compilationJobsStore(region)
	store.Delete(name)

	return nil
}

// StopCompilationJob sets a compilation job status to "STOPPED".
func (b *InMemoryBackend) StopCompilationJob(ctx context.Context, name string) error {
	b.mu.Lock("StopCompilationJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	j, ok := b.compilationJobsStore(region).Get(name)
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
func (b *InMemoryBackend) ListCompilationJobs(ctx context.Context, nextToken string) ([]*CompilationJob, string) {
	b.mu.RLock("ListCompilationJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.compilationJobsStoreRO(region),
		nextToken,
		cloneCompilationJob,
		func(v *CompilationJob) string { return v.CompilationJobName },
	)
}

// SetCompilationJobExtras sets optional configuration fields on an existing compilation job
// that were not included in the original CreateCompilationJob signature.
func (b *InMemoryBackend) SetCompilationJobExtras(
	ctx context.Context,
	name string,
	inputConfig *CompilationInputConfig,
	outputConfig *CompilationOutputConfig,
	stoppingCondition *StoppingCondition,
) error {
	b.mu.Lock("SetCompilationJobExtras")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	j, ok := b.compilationJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: compilation job %q not found", ErrCompilationJobNotFound, name)
	}

	if inputConfig != nil {
		ic := *inputConfig
		j.InputConfig = &ic
	}

	if outputConfig != nil {
		oc := *outputConfig
		j.OutputConfig = &oc
	}

	if stoppingCondition != nil {
		sc := *stoppingCondition
		j.StoppingCondition = &sc
	}

	j.LastModifiedTime = time.Now()

	return nil
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
	ctx context.Context,
	name string,
	tags map[string]string,
) (*MonitoringSchedule, error) {
	b.mu.Lock("CreateMonitoringSchedule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if name == "" {
		return nil, fmt.Errorf("%w: MonitoringScheduleName is required", ErrValidation)
	}

	if _, ok := b.monitoringSchedulesStore(region).Get(name); ok {
		return nil, fmt.Errorf("%w: monitoring schedule %q already exists", ErrValidation, name)
	}

	schedARN := arn.Build("sagemaker", region, b.accountID, "monitoring-schedule/"+name)
	now := time.Now()

	ms := &MonitoringSchedule{
		MonitoringScheduleName:   name,
		MonitoringScheduleArn:    schedARN,
		MonitoringScheduleStatus: "Scheduled",
		Tags:                     mergeTags(nil, tags),
		CreationTime:             now,
		LastModifiedTime:         now,
	}
	b.monitoringSchedulesStore(region).Put(ms)

	return cloneMonitoringSchedule(ms), nil
}

// DescribeMonitoringSchedule returns a monitoring schedule by name.
func (b *InMemoryBackend) DescribeMonitoringSchedule(ctx context.Context, name string) (*MonitoringSchedule, error) {
	b.mu.RLock("DescribeMonitoringSchedule")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	ms, ok := b.monitoringSchedulesStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: monitoring schedule %q not found", ErrMonitoringScheduleNotFound, name)
	}

	return cloneMonitoringSchedule(ms), nil
}

// DeleteMonitoringSchedule removes a monitoring schedule.
func (b *InMemoryBackend) DeleteMonitoringSchedule(ctx context.Context, name string) error {
	b.mu.Lock("DeleteMonitoringSchedule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.monitoringSchedulesStore(region).Get(name); !ok {
		return fmt.Errorf("%w: monitoring schedule %q not found", ErrMonitoringScheduleNotFound, name)
	}

	store := b.monitoringSchedulesStore(region)
	store.Delete(name)

	return nil
}

// StopMonitoringSchedule sets a monitoring schedule status to "Stopped".
func (b *InMemoryBackend) StopMonitoringSchedule(ctx context.Context, name string) error {
	b.mu.Lock("StopMonitoringSchedule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	ms, ok := b.monitoringSchedulesStore(region).Get(name)
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
func (b *InMemoryBackend) StartMonitoringSchedule(ctx context.Context, name string) error {
	b.mu.Lock("StartMonitoringSchedule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	ms, ok := b.monitoringSchedulesStore(region).Get(name)
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
func (b *InMemoryBackend) UpdateMonitoringSchedule(ctx context.Context, name string) (*MonitoringSchedule, error) {
	b.mu.Lock("UpdateMonitoringSchedule")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	ms, ok := b.monitoringSchedulesStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: monitoring schedule %q not found", ErrMonitoringScheduleNotFound, name)
	}

	ms.LastModifiedTime = time.Now()

	return cloneMonitoringSchedule(ms), nil
}

// ListMonitoringSchedules returns all monitoring schedules sorted by name.
func (b *InMemoryBackend) ListMonitoringSchedules(
	ctx context.Context,
	nextToken string,
) ([]*MonitoringSchedule, string) {
	b.mu.RLock("ListMonitoringSchedules")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.monitoringSchedulesStoreRO(region),
		nextToken,
		cloneMonitoringSchedule,
		func(v *MonitoringSchedule) string { return v.MonitoringScheduleName },
	)
}

// ---------------------------------------------------------------------------
// Workteam
// ---------------------------------------------------------------------------

// CognitoMemberDefinition identifies an Amazon Cognito user group.
type CognitoMemberDefinition struct {
	UserPool  string `json:"UserPool"`
	UserGroup string `json:"UserGroup"`
	ClientID  string `json:"ClientId"`
}

// OidcMemberDefinition identifies a list of OIDC IdP user groups.
type OidcMemberDefinition struct {
	Groups []string `json:"Groups,omitempty"`
}

// MemberDefinition identifies workers that make up a work team, using either
// an Amazon Cognito user group or an OIDC IdP user group.
type MemberDefinition struct {
	CognitoMemberDefinition *CognitoMemberDefinition `json:"CognitoMemberDefinition,omitempty"`
	OidcMemberDefinition    *OidcMemberDefinition    `json:"OidcMemberDefinition,omitempty"`
}

// Workteam represents a SageMaker Ground Truth workteam.
type Workteam struct {
	CreateDate        time.Time          `json:"CreateDate"`
	LastUpdatedDate   time.Time          `json:"LastUpdatedDate"`
	Tags              map[string]string  `json:"-"`
	WorkteamName      string             `json:"WorkteamName"`
	WorkteamArn       string             `json:"WorkteamArn"`
	WorkforceArn      string             `json:"WorkforceArn,omitempty"`
	Description       string             `json:"Description,omitempty"`
	SubDomain         string             `json:"SubDomain,omitempty"`
	MemberDefinitions []MemberDefinition `json:"MemberDefinitions,omitempty"`
}

func cloneWorkteam(w *Workteam) *Workteam {
	cp := *w
	cp.Tags = maps.Clone(w.Tags)
	cp.MemberDefinitions = append([]MemberDefinition(nil), w.MemberDefinitions...)

	return &cp
}

// CreateWorkteamOptions holds the parameters for creating a workteam.
type CreateWorkteamOptions struct {
	Tags              map[string]string
	Name              string
	Description       string
	WorkforceName     string
	MemberDefinitions []MemberDefinition
}

// CreateWorkteam creates a workteam.
func (b *InMemoryBackend) CreateWorkteam(ctx context.Context, opts CreateWorkteamOptions) (*Workteam, error) {
	b.mu.Lock("CreateWorkteam")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if opts.Name == "" {
		return nil, fmt.Errorf("%w: WorkteamName is required", ErrValidation)
	}

	if _, ok := b.workteamsStore(region).Get(opts.Name); ok {
		return nil, fmt.Errorf("%w: workteam %q already exists", ErrValidation, opts.Name)
	}

	var workforceARN string

	if opts.WorkforceName != "" {
		wf, ok := b.workforcesStore(region).Get(opts.WorkforceName)
		if !ok {
			return nil, fmt.Errorf(
				"%w: workforce %q not found", ErrWorkforceNotFound, opts.WorkforceName,
			)
		}

		workforceARN = wf.WorkforceArn
	}

	workteamARN := arn.Build("sagemaker", region, b.accountID, "workteam/"+opts.Name)
	now := time.Now()

	w := &Workteam{
		WorkteamName:      opts.Name,
		WorkteamArn:       workteamARN,
		WorkforceArn:      workforceARN,
		Description:       opts.Description,
		MemberDefinitions: append([]MemberDefinition(nil), opts.MemberDefinitions...),
		SubDomain:         "https://" + generateID() + ".labeling.sagemaker.aws",
		Tags:              mergeTags(nil, opts.Tags),
		CreateDate:        now,
		LastUpdatedDate:   now,
	}
	b.workteamsStore(region).Put(w)

	return cloneWorkteam(w), nil
}

// UpdateWorkteamOptions holds the parameters for updating a workteam.
type UpdateWorkteamOptions struct {
	Name              string
	Description       string
	MemberDefinitions []MemberDefinition
}

// UpdateWorkteam updates a workteam's description and/or member definitions.
func (b *InMemoryBackend) UpdateWorkteam(ctx context.Context, opts UpdateWorkteamOptions) (*Workteam, error) {
	b.mu.Lock("UpdateWorkteam")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	w, ok := b.workteamsStore(region).Get(opts.Name)
	if !ok {
		return nil, fmt.Errorf("%w: workteam %q not found", ErrWorkteamNotFound, opts.Name)
	}

	if opts.Description != "" {
		w.Description = opts.Description
	}

	if opts.MemberDefinitions != nil {
		w.MemberDefinitions = append([]MemberDefinition(nil), opts.MemberDefinitions...)
	}

	w.LastUpdatedDate = time.Now()

	return cloneWorkteam(w), nil
}

// DescribeWorkteam returns a workteam by name.
func (b *InMemoryBackend) DescribeWorkteam(ctx context.Context, name string) (*Workteam, error) {
	b.mu.RLock("DescribeWorkteam")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	w, ok := b.workteamsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: workteam %q not found", ErrWorkteamNotFound, name)
	}

	return cloneWorkteam(w), nil
}

// DeleteWorkteam removes a workteam.
func (b *InMemoryBackend) DeleteWorkteam(ctx context.Context, name string) error {
	b.mu.Lock("DeleteWorkteam")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.workteamsStore(region).Get(name); !ok {
		return fmt.Errorf("%w: workteam %q not found", ErrWorkteamNotFound, name)
	}

	store := b.workteamsStore(region)
	store.Delete(name)

	return nil
}

// ListWorkteams returns all workteams sorted by name.
func (b *InMemoryBackend) ListWorkteams(ctx context.Context, nextToken string) ([]*Workteam, string) {
	b.mu.RLock("ListWorkteams")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.workteamsStoreRO(region),
		nextToken,
		cloneWorkteam,
		func(v *Workteam) string { return v.WorkteamName },
	)
}

// ---------------------------------------------------------------------------
// Image alias listing
// ---------------------------------------------------------------------------

// ListImageAliases returns the aliases attached to an image. If version is
// positive, only that version's aliases are considered; otherwise, aliases
// from every version of the image are aggregated.
func (b *InMemoryBackend) ListImageAliases(
	ctx context.Context,
	imageName string,
	version int32,
	nextToken string,
) ([]string, string, error) {
	b.mu.RLock("ListImageAliases")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.smImagesStoreRO(region).Get(imageName); !ok {
		return nil, "", fmt.Errorf("%w: image %q not found", ErrSMImageNotFound, imageName)
	}

	versions := b.imageVersionsStoreRO(region)[imageName]

	var candidates []*ImageVersion

	if version > 0 {
		if iv, ok := versions[int(version)]; ok {
			candidates = append(candidates, iv)
		}
	} else {
		for _, iv := range versions {
			candidates = append(candidates, iv)
		}
	}

	aliases := dedupeAliases(candidates)
	sort.Strings(aliases)

	page, out := paginateSlice(aliases, nextToken, 0)

	return page, out, nil
}

// dedupeAliases flattens the Aliases of every given image version into a
// single de-duplicated slice.
func dedupeAliases(versions []*ImageVersion) []string {
	seen := map[string]bool{}
	aliases := make([]string, 0)

	for _, iv := range versions {
		for _, a := range iv.Aliases {
			if seen[a] {
				continue
			}

			seen[a] = true

			aliases = append(aliases, a)
		}
	}

	return aliases
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
