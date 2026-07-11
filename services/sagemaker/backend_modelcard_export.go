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

	j, ok := b.modelCardExportJobsStore(region).Get(jobArn)
	if !ok {
		return nil, fmt.Errorf("%w: model card export job %q not found", ErrModelCardExportJobNotFound, jobArn)
	}

	return cloneModelCardExportJob(j), nil
}

// ListModelCardExportJobs lists export jobs for a model card, optionally
// filtered by name-contains and status, sorted by creation time.
func (b *InMemoryBackend) ListModelCardExportJobs(
	ctx context.Context,
	modelCardName, nameContains, statusEquals, nextToken string,
	maxResults int32,
) ([]*ModelCardExportJob, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListModelCardExportJobs")
	defer b.mu.RUnlock()

	list := make([]*ModelCardExportJob, 0)

	for _, j := range b.modelCardExportJobsStore(region).All() {
		if modelCardName != "" && j.ModelCardName != modelCardName {
			continue
		}

		if nameContains != "" && !strings.Contains(j.ModelCardExportJobName, nameContains) {
			continue
		}

		if statusEquals != "" && j.Status != statusEquals {
			continue
		}

		list = append(list, cloneModelCardExportJob(j))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].CreatedAt.Before(list[j].CreatedAt) })

	return paginateSlice(list, nextToken, maxResults)
}
