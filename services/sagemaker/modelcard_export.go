package sagemaker

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrModelCardExportJobNotFound is returned when a model card export job ARN does not exist.
var ErrModelCardExportJobNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

const (
	modelCardExportJobStatusCompleted = "Completed"
	keyModelCardNameField             = "ModelCardName"
)

// ModelCardExportJob represents an export of a model card to S3.
type ModelCardExportJob struct {
	CreatedAt              time.Time `json:"-"`
	LastModifiedAt         time.Time `json:"-"`
	ModelCardExportJobArn  string    `json:"ModelCardExportJobArn"`
	ModelCardExportJobName string    `json:"ModelCardExportJobName"`
	ModelCardName          string    `json:"ModelCardName"`
	Status                 string    `json:"Status"`
	S3OutputPath           string    `json:"-"`
	S3ExportArtifacts      string    `json:"-"`
	ModelCardVersion       int       `json:"ModelCardVersion"`
}

func cloneModelCardExportJob(j *ModelCardExportJob) *ModelCardExportJob {
	cp := *j

	return &cp
}

// CreateModelCardExportJob creates a model card export job for an existing
// model card. The job completes synchronously (status Completed) since there
// is no real S3 export to perform in-memory.
func (b *InMemoryBackend) CreateModelCardExportJob(
	ctx context.Context,
	jobName, modelCardName string,
	modelCardVersion int,
	s3OutputPath string,
) (*ModelCardExportJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateModelCardExportJob")
	defer b.mu.Unlock()

	if jobName == "" {
		return nil, fmt.Errorf("%w: ModelCardExportJobName is required", ErrValidation)
	}

	if modelCardName == "" {
		return nil, fmt.Errorf("%w: ModelCardName is required", ErrValidation)
	}

	if s3OutputPath == "" {
		return nil, fmt.Errorf("%w: OutputConfig.S3OutputPath is required", ErrValidation)
	}

	card, ok := b.modelCardsStore(region).Get(modelCardName)
	if !ok {
		return nil, fmt.Errorf("%w: model card %q not found", ErrModelCardNotFound, modelCardName)
	}

	version := modelCardVersion
	if version == 0 {
		version = card.ModelCardVersion
	}

	jobARN := arn.Build("sagemaker", region, b.accountID, "model-card/"+modelCardName+"/export-job/"+jobName)
	now := time.Now()

	j := &ModelCardExportJob{
		ModelCardExportJobArn:  jobARN,
		ModelCardExportJobName: jobName,
		ModelCardName:          modelCardName,
		ModelCardVersion:       version,
		Status:                 modelCardExportJobStatusCompleted,
		S3OutputPath:           s3OutputPath,
		S3ExportArtifacts:      s3OutputPath + "/" + modelCardName + "-" + jobName + ".pdf",
		CreatedAt:              now,
		LastModifiedAt:         now,
	}

	b.modelCardExportJobsStore(region).Put(j)

	return cloneModelCardExportJob(j), nil
}

// DescribeModelCardExportJob returns a model card export job by ARN.
func (b *InMemoryBackend) DescribeModelCardExportJob(
	ctx context.Context,
	jobArn string,
) (*ModelCardExportJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeModelCardExportJob")
	defer b.mu.RUnlock()

	j, ok := b.modelCardExportJobsStoreRO(region).Get(jobArn)
	if !ok {
		return nil, fmt.Errorf("%w: model card export job %q not found", ErrModelCardExportJobNotFound, jobArn)
	}

	return cloneModelCardExportJob(j), nil
}

// ListModelCardExportJobsParams bundles the filter/sort criteria for
// ListModelCardExportJobs, mirroring ListModelCardExportJobsInput
// (api_op_ListModelCardExportJobs.go:29-61).
type ListModelCardExportJobsParams struct {
	CreationTimeAfter  *time.Time
	CreationTimeBefore *time.Time
	ModelCardName      string
	NameContains       string
	StatusEquals       string
	SortBy             string
	SortOrder          string
	NextToken          string
	ModelCardVersion   int32
	MaxResults         int32
}

// ListModelCardExportJobs lists export jobs for a model card, optionally
// filtered and sorted per params. SortBy defaults to CreationTime (the real
// op's documented default); SortOrder has no documented default and is kept
// as Ascending, this campaign's recurring undocumented-default fallback.
func (b *InMemoryBackend) ListModelCardExportJobs(
	ctx context.Context,
	params ListModelCardExportJobsParams,
) ([]*ModelCardExportJob, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListModelCardExportJobs")
	defer b.mu.RUnlock()

	list := make([]*ModelCardExportJob, 0)

	for _, j := range b.modelCardExportJobsStoreRO(region).All() {
		if !matchesModelCardExportJobListParams(j, params) {
			continue
		}

		list = append(list, cloneModelCardExportJob(j))
	}

	sort.Slice(list, func(i, k int) bool { return modelCardExportJobSortLess(list[i], list[k], params) })

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

func matchesModelCardExportJobListParams(j *ModelCardExportJob, params ListModelCardExportJobsParams) bool {
	if params.ModelCardName != "" && j.ModelCardName != params.ModelCardName {
		return false
	}

	if params.NameContains != "" && !strings.Contains(j.ModelCardExportJobName, params.NameContains) {
		return false
	}

	if params.StatusEquals != "" && j.Status != params.StatusEquals {
		return false
	}

	if params.ModelCardVersion != 0 && j.ModelCardVersion != int(params.ModelCardVersion) {
		return false
	}

	if params.CreationTimeAfter != nil && !j.CreatedAt.After(*params.CreationTimeAfter) {
		return false
	}

	if params.CreationTimeBefore != nil && !j.CreatedAt.Before(*params.CreationTimeBefore) {
		return false
	}

	return true
}

func modelCardExportJobSortLess(a, b *ModelCardExportJob, params ListModelCardExportJobsParams) bool {
	var less bool

	switch params.SortBy {
	case keyGenericName:
		less = a.ModelCardExportJobName < b.ModelCardExportJobName
	case keyStatus:
		less = a.Status < b.Status
	default:
		less = a.CreatedAt.Before(b.CreatedAt)
	}

	if params.SortOrder == sortOrderDescending {
		return !less
	}

	return less
}
