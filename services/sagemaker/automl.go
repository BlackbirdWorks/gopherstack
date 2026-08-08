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

var (
	// ErrAutoMLJobNotFound is returned when an AutoML job does not exist.
	ErrAutoMLJobNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrAutoMLJobNotStoppable is returned when stopping an already-terminal AutoML job.
	ErrAutoMLJobNotStoppable = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

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

// AutoMLS3DataSource locates AutoML channel input data in Amazon S3.
// SDK ref: aws-sdk-go-v2/service/sagemaker/types.AutoMLS3DataSource.
type AutoMLS3DataSource struct {
	S3DataType string `json:"S3DataType"`
	S3Uri      string `json:"S3Uri"`
}

// AutoMLDataSource is the data source for an AutoML channel.
type AutoMLDataSource struct {
	S3DataSource *AutoMLS3DataSource `json:"S3DataSource,omitempty"`
}

// AutoMLChannel is a named input source for CreateAutoMLJob/DescribeAutoMLJob.
// SDK ref: aws-sdk-go-v2/service/sagemaker/types.AutoMLChannel — the field on
// CreateAutoMLJobInput/DescribeAutoMLJobOutput is InputDataConfig
// ([]types.AutoMLChannel), not "AutoMLJobInputDataConfig" (that name does not
// exist in the SDK).
type AutoMLChannel struct {
	DataSource                *AutoMLDataSource `json:"DataSource,omitempty"`
	ChannelType               string            `json:"ChannelType,omitempty"`
	CompressionType           string            `json:"CompressionType,omitempty"`
	ContentType               string            `json:"ContentType,omitempty"`
	TargetAttributeName       string            `json:"TargetAttributeName,omitempty"`
	SampleWeightAttributeName string            `json:"SampleWeightAttributeName,omitempty"`
}

// AutoMLJob represents a SageMaker AutoML job.
type AutoMLJob struct {
	CreationTime             time.Time               `json:"CreationTime"`
	LastModifiedTime         time.Time               `json:"LastModifiedTime"`
	Tags                     map[string]string       `json:"Tags,omitempty"`
	OutputDataConfig         *AutoMLOutputDataConfig `json:"OutputDataConfig,omitempty"`
	AutoMLJobObjective       *AutoMLJobObjective     `json:"AutoMLJobObjective,omitempty"`
	AutoMLJobName            string                  `json:"AutoMLJobName"`
	AutoMLJobArn             string                  `json:"AutoMLJobArn"`
	AutoMLJobStatus          string                  `json:"AutoMLJobStatus"`
	AutoMLJobSecondaryStatus string                  `json:"AutoMLJobSecondaryStatus"`
	RoleArn                  string                  `json:"RoleArn,omitempty"`
	InputDataConfig          []AutoMLChannel         `json:"InputDataConfig"`
}

func cloneAutoMLJob(j *AutoMLJob) *AutoMLJob {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)

	if j.InputDataConfig != nil {
		cp.InputDataConfig = append([]AutoMLChannel{}, j.InputDataConfig...)
	}

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

// MarshalJSON emits CreationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings — this
// struct is marshaled directly by handleDescribeAutoMLJob.
func (j *AutoMLJob) MarshalJSON() ([]byte, error) {
	type alias AutoMLJob

	return json.Marshal(struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(j),
		CreationTime:     epochSeconds(j.CreationTime),
		LastModifiedTime: epochSeconds(j.LastModifiedTime),
	})
}

// UnmarshalJSON is the inverse of [AutoMLJob.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (j *AutoMLJob) UnmarshalJSON(data []byte) error {
	type alias AutoMLJob

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(j)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	j.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	j.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
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
	now := time.Now()

	j := &AutoMLJob{
		AutoMLJobName:            name,
		AutoMLJobArn:             jobARN,
		AutoMLJobStatus:          trainingJobStatusInProgress,
		AutoMLJobSecondaryStatus: secondaryStatusStarting,
		RoleArn:                  roleArn,
		InputDataConfig:          []AutoMLChannel{},
		Tags:                     mergeTags(nil, tags),
		CreationTime:             now,
		LastModifiedTime:         now,
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
	j.AutoMLJobSecondaryStatus = pipelineStatusStopped
	j.LastModifiedTime = time.Now()

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
	inputDataConfig []AutoMLChannel,
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

	if inputDataConfig != nil {
		j.InputDataConfig = append([]AutoMLChannel(nil), inputDataConfig...)
	}

	return nil
}
