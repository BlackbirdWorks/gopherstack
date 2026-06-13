package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ---------------------------------------------------------------------------
// EdgePackagingJob
// ---------------------------------------------------------------------------

var (
	// ErrEdgePackagingJobNotFound is returned when an edge packaging job does not exist.
	ErrEdgePackagingJobNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrEdgePackagingJobAlreadyExists is returned when an edge packaging job already exists.
	ErrEdgePackagingJobAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// EdgePackagingJob represents a SageMaker edge packaging job.
type EdgePackagingJob struct {
	CreationTime           time.Time         `json:"CreationTime"`
	LastModifiedTime       time.Time         `json:"LastModifiedTime"`
	Tags                   map[string]string `json:"Tags,omitempty"`
	EdgePackagingJobName   string            `json:"EdgePackagingJobName"`
	EdgePackagingJobArn    string            `json:"EdgePackagingJobArn"`
	EdgePackagingJobStatus string            `json:"EdgePackagingJobStatus"`
	ModelName              string            `json:"ModelName,omitempty"`
	ModelVersion           string            `json:"ModelVersion,omitempty"`
	RoleArn                string            `json:"RoleArn,omitempty"`
	CompilationJobName     string            `json:"CompilationJobName,omitempty"`
	FailureReason          string            `json:"FailureReason,omitempty"`
}

func cloneEdgePackagingJob(j *EdgePackagingJob) *EdgePackagingJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)

	return &cp
}

// CreateEdgePackagingJobOptions holds input fields for CreateEdgePackagingJob.
type CreateEdgePackagingJobOptions struct {
	Tags                 map[string]string
	EdgePackagingJobName string
	ModelName            string
	ModelVersion         string
	RoleArn              string
	CompilationJobName   string
}

// CreateEdgePackagingJob creates a SageMaker edge packaging job.
func (b *InMemoryBackend) CreateEdgePackagingJob(
	ctx context.Context,
	opts CreateEdgePackagingJobOptions,
) (*EdgePackagingJob, error) {
	b.mu.Lock("CreateEdgePackagingJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if opts.EdgePackagingJobName == "" {
		return nil, fmt.Errorf("%w: EdgePackagingJobName is required", ErrValidation)
	}

	if _, ok := b.edgePackagingJobsStore(region)[opts.EdgePackagingJobName]; ok {
		return nil, fmt.Errorf(
			"%w: edge packaging job %q already exists",
			ErrEdgePackagingJobAlreadyExists,
			opts.EdgePackagingJobName,
		)
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "edge-packaging-job/"+opts.EdgePackagingJobName)
	now := time.Now()

	j := &EdgePackagingJob{
		EdgePackagingJobName:   opts.EdgePackagingJobName,
		EdgePackagingJobArn:    jobARN,
		EdgePackagingJobStatus: "STARTING",
		ModelName:              opts.ModelName,
		ModelVersion:           opts.ModelVersion,
		RoleArn:                opts.RoleArn,
		CompilationJobName:     opts.CompilationJobName,
		Tags:                   mergeTags(nil, opts.Tags),
		CreationTime:           now,
		LastModifiedTime:       now,
	}
	b.edgePackagingJobsStore(region)[opts.EdgePackagingJobName] = j

	return cloneEdgePackagingJob(j), nil
}

// DescribeEdgePackagingJob returns an edge packaging job by name.
func (b *InMemoryBackend) DescribeEdgePackagingJob(ctx context.Context, name string) (*EdgePackagingJob, error) {
	b.mu.RLock("DescribeEdgePackagingJob")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	j, ok := b.edgePackagingJobsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: edge packaging job %q not found", ErrEdgePackagingJobNotFound, name)
	}

	return cloneEdgePackagingJob(j), nil
}

// StopEdgePackagingJob stops an edge packaging job.
func (b *InMemoryBackend) StopEdgePackagingJob(ctx context.Context, name string) error {
	b.mu.Lock("StopEdgePackagingJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	j, ok := b.edgePackagingJobsStore(region)[name]
	if !ok {
		return fmt.Errorf("%w: edge packaging job %q not found", ErrEdgePackagingJobNotFound, name)
	}

	j.EdgePackagingJobStatus = "STOPPING"
	j.LastModifiedTime = time.Now()

	return nil
}

// ListEdgePackagingJobsFilter holds optional filters for ListEdgePackagingJobs.
type ListEdgePackagingJobsFilter struct {
	StatusEquals string
	NameContains string
}

// ListEdgePackagingJobs returns edge packaging jobs with optional filters.
func (b *InMemoryBackend) ListEdgePackagingJobs(
	ctx context.Context,
	nextToken string,
	filter ListEdgePackagingJobsFilter,
) ([]*EdgePackagingJob, string) {
	b.mu.RLock("ListEdgePackagingJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	var keys []string

	for name, j := range b.edgePackagingJobsStore(region) {
		if filter.StatusEquals != "" && j.EdgePackagingJobStatus != filter.StatusEquals {
			continue
		}

		if filter.NameContains != "" && !strings.Contains(name, filter.NameContains) {
			continue
		}

		keys = append(keys, name)
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

	store := b.edgePackagingJobsStore(region)

	out := make([]*EdgePackagingJob, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneEdgePackagingJob(store[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// ---------------------------------------------------------------------------
// InferenceRecommendationsJob
// ---------------------------------------------------------------------------

var (
	// ErrInferenceRecommendationsJobNotFound is returned when the job does not exist.
	ErrInferenceRecommendationsJobNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrInferenceRecommendationsJobAlreadyExists is returned when the job already exists.
	ErrInferenceRecommendationsJobAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// InferenceRecommendationsJob represents a SageMaker inference recommendations job.
type InferenceRecommendationsJob struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	JobName          string            `json:"JobName"`
	JobArn           string            `json:"JobArn"`
	JobType          string            `json:"JobType,omitempty"`
	JobDescription   string            `json:"JobDescription,omitempty"`
	Status           string            `json:"Status"`
	RoleArn          string            `json:"RoleArn,omitempty"`
}

func cloneInferenceRecommendationsJob(j *InferenceRecommendationsJob) *InferenceRecommendationsJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)

	return &cp
}

// CreateInferenceRecommendationsJobOptions holds input fields.
type CreateInferenceRecommendationsJobOptions struct {
	Tags           map[string]string
	JobName        string
	JobType        string
	JobDescription string
	RoleArn        string
}

// CreateInferenceRecommendationsJob creates an inference recommendations job.
func (b *InMemoryBackend) CreateInferenceRecommendationsJob(
	ctx context.Context,
	opts CreateInferenceRecommendationsJobOptions,
) (*InferenceRecommendationsJob, error) {
	b.mu.Lock("CreateInferenceRecommendationsJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if opts.JobName == "" {
		return nil, fmt.Errorf("%w: JobName is required", ErrValidation)
	}

	if _, ok := b.inferenceRecommendationsJobsStore(region)[opts.JobName]; ok {
		return nil, fmt.Errorf(
			"%w: inference recommendations job %q already exists",
			ErrInferenceRecommendationsJobAlreadyExists,
			opts.JobName,
		)
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "inference-recommendations-job/"+opts.JobName)
	now := time.Now()

	j := &InferenceRecommendationsJob{
		JobName:          opts.JobName,
		JobArn:           jobARN,
		JobType:          opts.JobType,
		JobDescription:   opts.JobDescription,
		Status:           "IN_PROGRESS",
		RoleArn:          opts.RoleArn,
		Tags:             mergeTags(nil, opts.Tags),
		CreationTime:     now,
		LastModifiedTime: now,
	}
	b.inferenceRecommendationsJobsStore(region)[opts.JobName] = j

	return cloneInferenceRecommendationsJob(j), nil
}

// DescribeInferenceRecommendationsJob returns an inference recommendations job by name.
func (b *InMemoryBackend) DescribeInferenceRecommendationsJob(
	ctx context.Context,
	name string,
) (*InferenceRecommendationsJob, error) {
	b.mu.RLock("DescribeInferenceRecommendationsJob")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	j, ok := b.inferenceRecommendationsJobsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf(
			"%w: inference recommendations job %q not found",
			ErrInferenceRecommendationsJobNotFound,
			name,
		)
	}

	return cloneInferenceRecommendationsJob(j), nil
}

// StopInferenceRecommendationsJob stops an inference recommendations job.
func (b *InMemoryBackend) StopInferenceRecommendationsJob(ctx context.Context, name string) error {
	b.mu.Lock("StopInferenceRecommendationsJob")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	j, ok := b.inferenceRecommendationsJobsStore(region)[name]
	if !ok {
		return fmt.Errorf(
			"%w: inference recommendations job %q not found",
			ErrInferenceRecommendationsJobNotFound,
			name,
		)
	}

	j.Status = "STOPPING"
	j.LastModifiedTime = time.Now()

	return nil
}

// ListInferenceRecommendationsJobs returns inference recommendations jobs.
func (b *InMemoryBackend) ListInferenceRecommendationsJobs(
	ctx context.Context,
	nextToken string,
) ([]*InferenceRecommendationsJob, string) {
	b.mu.RLock("ListInferenceRecommendationsJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.inferenceRecommendationsJobsStore(region)

	keys := make([]string, 0, len(store))
	for k := range store {
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

	out := make([]*InferenceRecommendationsJob, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneInferenceRecommendationsJob(store[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// ---------------------------------------------------------------------------
// Update operations for existing resources
// ---------------------------------------------------------------------------

// UpdateUserProfile updates a user profile in a domain. Returns the updated profile.
func (b *InMemoryBackend) UpdateUserProfile(
	ctx context.Context,
	domainID, userProfileName string,
) (*UserProfile, error) {
	b.mu.Lock("UpdateUserProfile")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	key := userProfileKey{DomainID: domainID, UserProfileName: userProfileName}

	up, ok := b.userProfilesStore(region)[key]
	if !ok {
		return nil, fmt.Errorf(
			"%w: user profile %q not found in domain %q",
			ErrUserProfileNotFound,
			userProfileName,
			domainID,
		)
	}

	up.LastModifiedTime = time.Now()

	return cloneUserProfile(up), nil
}

// UpdateSpace updates a space in a domain. Returns the updated space.
func (b *InMemoryBackend) UpdateSpace(ctx context.Context, domainID, spaceName string) (*Space, error) {
	b.mu.Lock("UpdateSpace")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	key := spaceKey(domainID, spaceName)

	s, ok := b.spacesStore(region)[key]
	if !ok {
		return nil, fmt.Errorf("%w: space %q not found in domain %q", ErrSpaceNotFound, spaceName, domainID)
	}

	s.LastModifiedTime = time.Now()

	return cloneSpace(s), nil
}

// UpdateModelPackage updates the approval status of a model package (by name or ARN).
func (b *InMemoryBackend) UpdateModelPackage(
	ctx context.Context,
	nameOrArn, approvalStatus string,
) (*ModelPackage, error) {
	b.mu.Lock("UpdateModelPackage")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	arnStr := nameOrArn
	if v, ok := b.modelPackageARNIndexStore(region)[nameOrArn]; ok {
		arnStr = v
	}

	mp, ok := b.modelPackagesStore(region)[arnStr]
	if !ok {
		return nil, fmt.Errorf("%w: model package %q not found", ErrModelPackageNotFound, nameOrArn)
	}

	if approvalStatus != "" {
		mp.ModelApprovalStatus = approvalStatus
	}

	return cloneModelPackage(mp), nil
}

// UpdateMlflowTrackingServer updates an MLflow tracking server.
func (b *InMemoryBackend) UpdateMlflowTrackingServer(
	ctx context.Context,
	name, mlflowVersion string,
) (*MlflowTrackingServer, error) {
	b.mu.Lock("UpdateMlflowTrackingServer")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	s, ok := b.mlflowTrackingServersStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: MLflow tracking server %q not found", ErrMlflowTrackingServerNotFound, name)
	}

	if mlflowVersion != "" {
		s.MlflowVersion = mlflowVersion
	}

	s.LastModifiedTime = time.Now()

	return cloneMlflowTrackingServer(s), nil
}

// ---------------------------------------------------------------------------
// List operations for batch3 resources
// ---------------------------------------------------------------------------

// ListMlflowTrackingServers returns all MLflow tracking servers.
func (b *InMemoryBackend) ListMlflowTrackingServers(
	ctx context.Context,
	nextToken string,
) ([]*MlflowTrackingServer, string) {
	b.mu.RLock("ListMlflowTrackingServers")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.mlflowTrackingServersStore(region)

	keys := make([]string, 0, len(store))
	for k := range store {
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

	out := make([]*MlflowTrackingServer, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneMlflowTrackingServer(store[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// ListModelCards returns all model cards.
func (b *InMemoryBackend) ListModelCards(ctx context.Context, nextToken string) ([]*ModelCard, string) {
	b.mu.RLock("ListModelCards")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.modelCardsStore(region)

	keys := make([]string, 0, len(store))
	for k := range store {
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

	out := make([]*ModelCard, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneModelCard(store[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// ListOptimizationJobs returns all optimization jobs.
func (b *InMemoryBackend) ListOptimizationJobs(ctx context.Context, nextToken string) ([]*OptimizationJob, string) {
	b.mu.RLock("ListOptimizationJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.optimizationJobsStore(region)

	keys := make([]string, 0, len(store))
	for k := range store {
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

	out := make([]*OptimizationJob, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneOptimizationJob(store[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// ListStudioLifecycleConfigs returns all Studio lifecycle configs.
func (b *InMemoryBackend) ListStudioLifecycleConfigs(
	ctx context.Context,
	nextToken string,
) ([]*StudioLifecycleConfig, string) {
	b.mu.RLock("ListStudioLifecycleConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.studioLifecycleConfigsStore(region)

	keys := make([]string, 0, len(store))
	for k := range store {
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

	out := make([]*StudioLifecycleConfig, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneStudioLifecycleConfig(store[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// ListInferenceExperiments returns all inference experiments.
func (b *InMemoryBackend) ListInferenceExperiments(
	ctx context.Context,
	nextToken string,
) ([]*InferenceExperiment, string) {
	b.mu.RLock("ListInferenceExperiments")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.inferenceExperimentsStore(region)

	keys := make([]string, 0, len(store))
	for k := range store {
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

	out := make([]*InferenceExperiment, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneInferenceExperiment(store[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// ListFlowDefinitions returns all flow definitions.
func (b *InMemoryBackend) ListFlowDefinitions(ctx context.Context, nextToken string) ([]*FlowDefinition, string) {
	b.mu.RLock("ListFlowDefinitions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.flowDefinitionsStore(region)

	keys := make([]string, 0, len(store))
	for k := range store {
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

	out := make([]*FlowDefinition, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneFlowDefinition(store[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// ListHumanTaskUIs returns all human task UIs.
func (b *InMemoryBackend) ListHumanTaskUIs(ctx context.Context, nextToken string) ([]*HumanTaskUI, string) {
	b.mu.RLock("ListHumanTaskUIs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.humanTaskUisStore(region)

	keys := make([]string, 0, len(store))
	for k := range store {
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

	out := make([]*HumanTaskUI, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneHumanTaskUI(store[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// ListAppImageConfigs returns all App image configs.
func (b *InMemoryBackend) ListAppImageConfigs(ctx context.Context, nextToken string) ([]*AppImageConfig, string) {
	b.mu.RLock("ListAppImageConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.appImageConfigsStore(region)

	keys := make([]string, 0, len(store))
	for k := range store {
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

	out := make([]*AppImageConfig, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneAppImageConfig(store[k]))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// ListTrainingJobsForHyperParameterTuningJob returns training jobs for an HP tuning job.
// Since this emulator does not launch training jobs automatically, it always returns empty.
func (b *InMemoryBackend) ListTrainingJobsForHyperParameterTuningJob(
	ctx context.Context,
	jobName, _ string,
) ([]*TrainingJob, string, error) {
	b.mu.RLock("ListTrainingJobsForHyperParameterTuningJob")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.hpTuningJobsStore(region)[jobName]; !ok {
		return nil, "", fmt.Errorf("%w: HP tuning job %q not found", ErrHPTuningJobNotFound, jobName)
	}

	return []*TrainingJob{}, "", nil
}
