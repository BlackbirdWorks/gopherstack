package batch

import (
	"context"
	"fmt"
	"strings"
)

// --- Job operation handlers ---

type listJobsInput struct {
	MaxResults *int32  `json:"maxResults,omitempty"`
	NextToken  *string `json:"nextToken,omitempty"`
	JobQueue   string  `json:"jobQueue"`
	JobStatus  string  `json:"jobStatus"`
}

type jobSummary struct {
	StartedAt    *int64 `json:"startedAt,omitempty"`
	StoppedAt    *int64 `json:"stoppedAt,omitempty"`
	JobID        string `json:"jobId"`
	JobARN       string `json:"jobArn,omitempty"`
	JobName      string `json:"jobName"`
	Status       string `json:"status"`
	StatusReason string `json:"statusReason,omitempty"`
	CreatedAt    int64  `json:"createdAt"`
}

type listJobsOutput struct {
	NextToken      *string      `json:"nextToken,omitempty"`
	JobSummaryList []jobSummary `json:"jobSummaryList"`
}

func isValidJobStatus(s string) bool {
	switch s {
	case "SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING", "SUCCEEDED", "FAILED":
		return true
	default:
		return false
	}
}

func (h *Handler) handleListJobs(ctx context.Context, in *listJobsInput) (*listJobsOutput, error) {
	// AWS Batch ListJobs requires a grouping key; this simulator scopes jobs by
	// job queue, so jobQueue is mandatory (AWS returns ClientException
	// otherwise). jobStatus remains an optional filter.
	if strings.TrimSpace(in.JobQueue) == "" {
		return nil, fmt.Errorf("%w: jobQueue is required", ErrValidation)
	}

	if in.JobStatus != "" && !isValidJobStatus(in.JobStatus) {
		return nil, fmt.Errorf("%w: invalid jobStatus %q", ErrValidation, in.JobStatus)
	}

	var maxResults int32
	if in.MaxResults != nil {
		maxResults = *in.MaxResults
	}

	var nextToken string
	if in.NextToken != nil {
		nextToken = *in.NextToken
	}

	jobs, outToken, err := h.Backend.ListJobs(ctx, in.JobQueue, in.JobStatus, nextToken, maxResults)
	if err != nil {
		return nil, err
	}

	summaries := make([]jobSummary, 0, len(jobs))
	for _, j := range jobs {
		summaries = append(summaries, jobSummary{
			JobID:        j.JobID,
			JobARN:       j.JobARN,
			JobName:      j.JobName,
			Status:       j.Status,
			CreatedAt:    j.CreatedAt,
			StartedAt:    j.StartedAt,
			StoppedAt:    j.StoppedAt,
			StatusReason: j.StatusReason,
		})
	}

	out := &listJobsOutput{JobSummaryList: summaries}
	if outToken != "" {
		out.NextToken = &outToken
	}

	return out, nil
}

type describeJobsInput struct {
	Jobs []string `json:"jobs"`
}

// jobDetail mirrors aws-sdk-go-v2/service/batch/types.JobDetail's field names
// and nesting (see deserializers.go's awsRestjson1_deserializeDocumentJobDetail
// case list). NodeDetails/EcsProperties/EksProperties (the describe-side
// variants) are not modeled -- they require simulating ECS/EKS/multi-node
// execution details, which is out of scope for this emulator (see
// PARITY.md); Container, IsCancelled, IsTerminated, and PlatformCapabilities
// are modeled.
type jobDetail struct {
	StoppedAt                    *int64                        `json:"stoppedAt,omitempty"`
	StartedAt                    *int64                        `json:"startedAt,omitempty"`
	RetryStrategy                *RetryStrategy                `json:"retryStrategy,omitempty"`
	Timeout                      *JobTimeout                   `json:"timeout,omitempty"`
	ArrayProperties              *ArrayProperties              `json:"arrayProperties,omitempty"`
	ConsumableResourceProperties *ConsumableResourceProperties `json:"consumableResourceProperties,omitempty"`
	Container                    *ContainerDetail              `json:"container,omitempty"`
	Tags                         map[string]string             `json:"tags"`
	Parameters                   map[string]string             `json:"parameters,omitempty"`
	JobARN                       string                        `json:"jobArn,omitempty"`
	JobID                        string                        `json:"jobId"`
	JobName                      string                        `json:"jobName"`
	JobQueue                     string                        `json:"jobQueue"`
	JobDefinition                string                        `json:"jobDefinition"`
	Status                       string                        `json:"status"`
	StatusReason                 string                        `json:"statusReason,omitempty"`
	ShareIdentifier              string                        `json:"shareIdentifier,omitempty"`
	DependsOn                    []JobDependency               `json:"dependsOn,omitempty"`
	PlatformCapabilities         []string                      `json:"platformCapabilities,omitempty"`
	CreatedAt                    int64                         `json:"createdAt"`
	SchedulingPriorityOverride   int32                         `json:"schedulingPriority,omitempty"`
	PropagateTags                bool                          `json:"propagateTags,omitempty"`
	IsCancelled                  bool                          `json:"isCancelled"`
	IsTerminated                 bool                          `json:"isTerminated"`
}

type describeJobsOutput struct {
	Jobs []jobDetail `json:"jobs"`
}

func (h *Handler) handleDescribeJobs(ctx context.Context, in *describeJobsInput) (*describeJobsOutput, error) {
	jobs := h.Backend.DescribeJobs(ctx, in.Jobs)

	details := make([]jobDetail, 0, len(jobs))
	for _, j := range jobs {
		details = append(details, jobDetail{
			JobID:                        j.JobID,
			JobARN:                       j.JobARN,
			JobName:                      j.JobName,
			JobQueue:                     j.JobQueue,
			JobDefinition:                j.JobDefinition,
			Status:                       j.Status,
			StatusReason:                 j.StatusReason,
			CreatedAt:                    j.CreatedAt,
			StartedAt:                    j.StartedAt,
			StoppedAt:                    j.StoppedAt,
			Tags:                         tagsOrEmpty(j.Tags),
			RetryStrategy:                j.RetryStrategy,
			Timeout:                      j.Timeout,
			ArrayProperties:              j.ArrayProperties,
			ConsumableResourceProperties: j.ConsumableResourceProperties,
			Container:                    j.Container,
			Parameters:                   j.Parameters,
			DependsOn:                    j.DependsOn,
			ShareIdentifier:              j.ShareIdentifier,
			PlatformCapabilities:         j.PlatformCapabilities,
			SchedulingPriorityOverride:   j.SchedulingPriorityOverride,
			PropagateTags:                j.PropagateTags,
			IsCancelled:                  j.IsCancelled,
			IsTerminated:                 j.IsTerminated,
		})
	}

	return &describeJobsOutput{Jobs: details}, nil
}

type containerOverridesInput struct {
	Environment []keyValuePair `json:"environment,omitempty"`
	Command     []string       `json:"command,omitempty"`
}

type keyValuePair struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// arrayPropertiesInput mirrors aws-sdk-go-v2/service/batch/types.
// ArrayProperties as accepted on SubmitJobInput: only "size" is settable by
// the caller (statusSummary/index are describe-side, output-only fields).
type arrayPropertiesInput struct {
	Size int32 `json:"size,omitempty"`
}

type submitJobInput struct {
	Tags                       map[string]string                  `json:"tags"`
	Parameters                 map[string]string                  `json:"parameters,omitempty"`
	RetryStrategy              *RetryStrategy                     `json:"retryStrategy,omitempty"`
	Timeout                    *JobTimeout                        `json:"timeout,omitempty"`
	ArrayProperties            *arrayPropertiesInput              `json:"arrayProperties,omitempty"`
	ContainerOverrides         *containerOverridesInput           `json:"containerOverrides,omitempty"`
	ConsumableOverride         *consumableResourcePropertiesInput `json:"consumableResourcePropertiesOverride,omitempty"`
	JobName                    string                             `json:"jobName"`
	JobQueue                   string                             `json:"jobQueue"`
	JobDefinition              string                             `json:"jobDefinition"`
	ShareIdentifier            string                             `json:"shareIdentifier,omitempty"`
	DependsOn                  []JobDependency                    `json:"dependsOn,omitempty"`
	SchedulingPriorityOverride int32                              `json:"schedulingPriorityOverride,omitempty"`
	PropagateTags              bool                               `json:"propagateTags,omitempty"`
}

// submitJobOutput mirrors aws-sdk-go-v2/service/batch's SubmitJobOutput:
// jobArn is part of the real response and must be echoed back, not just
// available via a follow-up DescribeJobs call.
type submitJobOutput struct {
	JobID   string `json:"jobId"`
	JobARN  string `json:"jobArn,omitempty"`
	JobName string `json:"jobName"`
}

func (h *Handler) handleSubmitJob(ctx context.Context, in *submitJobInput) (*submitJobOutput, error) {
	var overrides *ContainerOverrides
	if in.ContainerOverrides != nil {
		env := make([]KeyValuePair, len(in.ContainerOverrides.Environment))
		for i, kv := range in.ContainerOverrides.Environment {
			env[i] = KeyValuePair(kv)
		}
		overrides = &ContainerOverrides{
			Command:     in.ContainerOverrides.Command,
			Environment: env,
		}
	}

	var arrayProps *ArrayProperties
	if in.ArrayProperties != nil {
		arrayProps = &ArrayProperties{Size: in.ArrayProperties.Size}
	}

	j, err := h.Backend.SubmitJob(
		ctx,
		in.JobName,
		in.JobQueue,
		in.JobDefinition,
		in.Tags,
		in.Parameters,
		in.DependsOn,
		in.RetryStrategy,
		in.Timeout,
		arrayProps,
		overrides,
		consumableResourcePropertiesFromInput(in.ConsumableOverride),
		in.ShareIdentifier,
		in.SchedulingPriorityOverride,
		in.PropagateTags,
	)
	if err != nil {
		return nil, err
	}

	return &submitJobOutput{
		JobID:   j.JobID,
		JobARN:  j.JobARN,
		JobName: j.JobName,
	}, nil
}

type terminateJobInput struct {
	JobID  string `json:"jobId"`
	Reason string `json:"reason"`
}

func (h *Handler) handleTerminateJob(ctx context.Context, in *terminateJobInput) (*emptyOutput, error) {
	if err := h.Backend.TerminateJob(ctx, in.JobID, in.Reason); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type cancelJobInput struct {
	JobID  string `json:"jobId"`
	Reason string `json:"reason"`
}

func (h *Handler) handleCancelJob(ctx context.Context, in *cancelJobInput) (*emptyOutput, error) {
	if err := h.Backend.CancelJob(ctx, in.JobID, in.Reason); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
