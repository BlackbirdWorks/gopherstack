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
	ErrEdgePackagingJobNotFound = awserr.New("ResourceNotFound", ErrResourceNotFound)
	// ErrEdgePackagingJobAlreadyExists is returned when an edge packaging job already exists.
	ErrEdgePackagingJobAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

const (
	edgePackagingJobStatusStarting  = "STARTING"
	edgePackagingJobStatusCompleted = "COMPLETED"
	edgePackagingJobStatusStopping  = "STOPPING"
	edgePackagingJobStatusStopped   = "STOPPED"
)

// EdgeOutputConfig mirrors types.EdgeOutputConfig (api_op_CreateEdgePackagingJob.go:
// OutputConfig, "This member is required").
type EdgeOutputConfig struct {
	S3OutputLocation       string `json:"S3OutputLocation"`
	KmsKeyID               string `json:"KmsKeyId,omitempty"`
	PresetDeploymentConfig string `json:"PresetDeploymentConfig,omitempty"`
	PresetDeploymentType   string `json:"PresetDeploymentType,omitempty"`
}

// EdgePackagingJob represents a SageMaker edge packaging job.
type EdgePackagingJob struct {
	CreationTime           time.Time         `json:"CreationTime"`
	LastModifiedTime       time.Time         `json:"LastModifiedTime"`
	Tags                   map[string]string `json:"Tags,omitempty"`
	OutputConfig           EdgeOutputConfig  `json:"OutputConfig"`
	EdgePackagingJobName   string            `json:"EdgePackagingJobName"`
	EdgePackagingJobArn    string            `json:"EdgePackagingJobArn"`
	EdgePackagingJobStatus string            `json:"EdgePackagingJobStatus"`
	ModelName              string            `json:"ModelName,omitempty"`
	ModelVersion           string            `json:"ModelVersion,omitempty"`
	RoleArn                string            `json:"RoleArn,omitempty"`
	CompilationJobName     string            `json:"CompilationJobName,omitempty"`
	ResourceKey            string            `json:"ResourceKey,omitempty"`
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
	ResourceKey          string
	OutputConfig         EdgeOutputConfig
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

	if _, ok := b.edgePackagingJobsStore(region).Get(opts.EdgePackagingJobName); ok {
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
		EdgePackagingJobStatus: edgePackagingJobStatusStarting,
		ModelName:              opts.ModelName,
		ModelVersion:           opts.ModelVersion,
		RoleArn:                opts.RoleArn,
		CompilationJobName:     opts.CompilationJobName,
		ResourceKey:            opts.ResourceKey,
		OutputConfig:           opts.OutputConfig,
		Tags:                   mergeTags(nil, opts.Tags),
		CreationTime:           now,
		LastModifiedTime:       now,
	}
	b.edgePackagingJobsStore(region).Put(j)

	b.scheduleEdgePackagingJobCompletion(b.lifecycleCtx, region, opts.EdgePackagingJobName)

	return cloneEdgePackagingJob(j), nil
}

// scheduleEdgePackagingJobCompletion drives STARTING -> COMPLETED after
// [edgePackagingJobStartingToCompleted]. Nothing previously advanced a
// STARTING job -- no ticker, no later call -- so DescribeEdgePackagingJob
// showed STARTING for the entire lifetime of every edge packaging job ever
// created.
func (b *InMemoryBackend) scheduleEdgePackagingJobCompletion(ctx context.Context, region, name string) {
	b.runDelayed(ctx, edgePackagingJobStartingToCompleted, func() {
		b.mu.Lock("scheduleEdgePackagingJobCompletion.goroutine")
		defer b.mu.Unlock()

		j, ok := b.edgePackagingJobsStore(region).Get(name)
		if !ok || j.EdgePackagingJobStatus != edgePackagingJobStatusStarting {
			return
		}

		j.EdgePackagingJobStatus = edgePackagingJobStatusCompleted
		j.LastModifiedTime = time.Now()
	})
}

// DescribeEdgePackagingJob returns an edge packaging job by name.
func (b *InMemoryBackend) DescribeEdgePackagingJob(ctx context.Context, name string) (*EdgePackagingJob, error) {
	b.mu.RLock("DescribeEdgePackagingJob")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	j, ok := b.edgePackagingJobsStoreRO(region).Get(name)
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

	j, ok := b.edgePackagingJobsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: edge packaging job %q not found", ErrEdgePackagingJobNotFound, name)
	}

	j.EdgePackagingJobStatus = edgePackagingJobStatusStopping
	j.LastModifiedTime = time.Now()

	b.runDelayed(b.lifecycleCtx, edgePackagingJobStoppingToStopped, func() {
		b.mu.Lock("StopEdgePackagingJob.goroutine")
		defer b.mu.Unlock()

		j2, found := b.edgePackagingJobsStore(region).Get(name)
		if !found || j2.EdgePackagingJobStatus != edgePackagingJobStatusStopping {
			return
		}

		j2.EdgePackagingJobStatus = edgePackagingJobStatusStopped
		j2.LastModifiedTime = time.Now()
	})

	return nil
}

// ListEdgePackagingJobsFilter holds optional filters for ListEdgePackagingJobs.
type ListEdgePackagingJobsFilter struct {
	CreationTimeAfter      *time.Time
	CreationTimeBefore     *time.Time
	LastModifiedTimeAfter  *time.Time
	LastModifiedTimeBefore *time.Time
	StatusEquals           string
	NameContains           string
	ModelNameContains      string
	SortBy                 string
	SortOrder              string
	MaxResults             int32
}

// sortByModelName, sortByStatusUpper and sortByCreationTimeUpper are three of
// ListEdgePackagingJobsSortBy's five values (types/enums.go:5332-5341);
// sortByName and sortByLastModifiedTime (list_helpers.go) already cover the
// other two, spelled identically across sort-by enum families.
const (
	sortByModelName         = "MODEL_NAME"
	sortByStatusUpper       = "STATUS"
	sortByCreationTimeUpper = "CREATION_TIME"
)

// ListEdgePackagingJobs returns edge packaging jobs with optional filters.
// SortBy is one of NAME|MODEL_NAME|CREATION_TIME|LAST_MODIFIED_TIME|STATUS
// (types/enums.go:5332-5341, ListEdgePackagingJobsSortBy). Neither SortBy nor
// SortOrder documents a default on this op (api_op_ListEdgePackagingJobs.go),
// so an unset value keeps this backend's own pre-existing order (ascending by
// name) rather than inventing one, the same conservative stance parity-23
// applied to ListFlowDefinitions/ListHumanTaskUis.
func (b *InMemoryBackend) ListEdgePackagingJobs(
	ctx context.Context,
	nextToken string,
	filter ListEdgePackagingJobsFilter,
) ([]*EdgePackagingJob, string) {
	b.mu.RLock("ListEdgePackagingJobs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*EdgePackagingJob, 0, b.edgePackagingJobsStoreRO(region).Len())

	for _, j := range b.edgePackagingJobsStoreRO(region).All() {
		if edgePackagingJobMatchesFilter(j, filter) {
			list = append(list, cloneEdgePackagingJob(j))
		}
	}

	desc := filter.SortOrder == sortOrderDescending
	sort.Slice(list, func(i, k int) bool {
		var less bool

		switch filter.SortBy {
		case sortByModelName:
			less = list[i].ModelName < list[k].ModelName
		case sortByLastModifiedTime:
			less = list[i].LastModifiedTime.Before(list[k].LastModifiedTime)
		case sortByStatusUpper:
			less = list[i].EdgePackagingJobStatus < list[k].EdgePackagingJobStatus
		case sortByCreationTimeUpper:
			less = list[i].CreationTime.Before(list[k].CreationTime)
		default:
			less = list[i].EdgePackagingJobName < list[k].EdgePackagingJobName
		}

		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, nextToken, filter.MaxResults)
}

// edgePackagingJobMatchesFilter reports whether j satisfies every filter
// dimension in filter.
func edgePackagingJobMatchesFilter(j *EdgePackagingJob, filter ListEdgePackagingJobsFilter) bool {
	if filter.StatusEquals != "" && j.EdgePackagingJobStatus != filter.StatusEquals {
		return false
	}

	if filter.NameContains != "" && !strings.Contains(j.EdgePackagingJobName, filter.NameContains) {
		return false
	}

	if filter.ModelNameContains != "" && !strings.Contains(j.ModelName, filter.ModelNameContains) {
		return false
	}

	if !timeWindowOK(j.CreationTime, filter.CreationTimeAfter, filter.CreationTimeBefore) {
		return false
	}

	return timeWindowOK(j.LastModifiedTime, filter.LastModifiedTimeAfter, filter.LastModifiedTimeBefore)
}
