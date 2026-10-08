package batch

import (
	"context"
	"fmt"
	"strings"
)

// --- Job operation handlers ---

type listJobsInput struct {
	MaxResults *int32               `json:"maxResults,omitempty"`
	NextToken  *string              `json:"nextToken,omitempty"`
	JobQueue   string               `json:"jobQueue"`
	JobStatus  string               `json:"jobStatus"`
	ArrayJobID string               `json:"arrayJobId,omitempty"`
	MultiNode  string               `json:"multiNodeJobId,omitempty"`
	Filters    []keyValuesPairInput `json:"filters,omitempty"`
}

// keyValuesPairInput mirrors aws-sdk-go-v2/service/batch/types.KeyValuesPair.
type keyValuesPairInput struct {
	Name   string   `json:"name"`
	Values []string `json:"values"`
}

// arrayPropertiesSummary mirrors aws-sdk-go-v2/service/batch/types.
// ArrayPropertiesSummary. StatusSummaryLastUpdatedAt is unsourced -- the
// Job model's ArrayProperties tracks no such timestamp (see PARITY.md).
type arrayPropertiesSummary struct {
	StatusSummary              map[string]int32 `json:"statusSummary,omitempty"`
	StatusSummaryLastUpdatedAt *int64           `json:"statusSummaryLastUpdatedAt,omitempty"`
	Size                       int32            `json:"size,omitempty"`
	Index                      int32            `json:"index,omitempty"`
}

// nodePropertiesSummary mirrors types.NodePropertiesSummary.
type nodePropertiesSummary struct {
	IsMainNode bool  `json:"isMainNode"`
	NodeIndex  int32 `json:"nodeIndex"`
	NumNodes   int32 `json:"numNodes"`
}

// nodeDetails mirrors types.NodeDetails.
type nodeDetails struct {
	IsMainNode bool  `json:"isMainNode"`
	NodeIndex  int32 `json:"nodeIndex"`
}

type jobSummary struct {
	StartedAt       *int64                  `json:"startedAt,omitempty"`
	StoppedAt       *int64                  `json:"stoppedAt,omitempty"`
	ArrayProperties *arrayPropertiesSummary `json:"arrayProperties,omitempty"`
	NodeProperties  *nodePropertiesSummary  `json:"nodeProperties,omitempty"`
	JobID           string                  `json:"jobId"`
	JobARN          string                  `json:"jobArn,omitempty"`
	JobName         string                  `json:"jobName"`
	JobDefinition   string                  `json:"jobDefinition,omitempty"`
	ShareIdentifier string                  `json:"shareIdentifier,omitempty"`
	Status          string                  `json:"status"`
	StatusReason    string                  `json:"statusReason,omitempty"`
	CreatedAt       int64                   `json:"createdAt"`
}

// jobSummaryArrayProperties projects a Job's ArrayProperties onto the
// narrower ArrayPropertiesSummary wire shape.
func jobSummaryArrayProperties(j *Job) *arrayPropertiesSummary {
	if j.ArrayProperties == nil {
		return nil
	}

	return &arrayPropertiesSummary{
		Index:                      j.ArrayProperties.Index,
		Size:                       j.ArrayProperties.Size,
		StatusSummary:              j.ArrayProperties.StatusSummary,
		StatusSummaryLastUpdatedAt: j.ArrayProperties.StatusSummaryLastUpdatedAt,
	}
}

func jobSummaryNodeProperties(j *Job) *nodePropertiesSummary {
	if j.nodeIndex == nil {
		return nil
	}

	return &nodePropertiesSummary{IsMainNode: j.isMainNode, NodeIndex: *j.nodeIndex, NumNodes: j.nodeCount}
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
	set := 0

	for _, v := range []string{in.JobQueue, in.ArrayJobID, in.MultiNode} {
		if strings.TrimSpace(v) != "" {
			set++
		}
	}

	if set != 1 {
		return nil, fmt.Errorf("%w: specify exactly one of jobQueue, arrayJobId or multiNodeJobId", ErrValidation)
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

	jobs, outToken, err := h.listJobsFor(ctx, in, nextToken, maxResults)
	if err != nil {
		return nil, err
	}

	summaries := make([]jobSummary, 0, len(jobs))
	for _, j := range jobs {
		summaries = append(summaries, jobSummary{
			JobID:           j.JobID,
			JobARN:          j.JobARN,
			JobName:         j.JobName,
			JobDefinition:   j.JobDefinition,
			ShareIdentifier: j.ShareIdentifier,
			Status:          j.Status,
			CreatedAt:       j.CreatedAt,
			StartedAt:       j.StartedAt,
			StoppedAt:       j.StoppedAt,
			StatusReason:    j.StatusReason,
			ArrayProperties: jobSummaryArrayProperties(j),
			NodeProperties:  jobSummaryNodeProperties(j),
		})
	}

	out := &listJobsOutput{JobSummaryList: summaries}
	if outToken != "" {
		out.NextToken = &outToken
	}

	return out, nil
}

func (h *Handler) listJobsFor(
	ctx context.Context, in *listJobsInput, nextToken string, maxResults int32,
) ([]*Job, string, error) {
	switch {
	case strings.TrimSpace(in.ArrayJobID) != "":
		return h.Backend.ListJobChildren(ctx, in.ArrayJobID, in.JobStatus, nextToken, maxResults, false)
	case strings.TrimSpace(in.MultiNode) != "":
		return h.Backend.ListJobChildren(ctx, in.MultiNode, in.JobStatus, nextToken, maxResults, true)
	}

	filters := make([]KeyValueFilter, 0, len(in.Filters))
	for _, f := range in.Filters {
		filters = append(filters, KeyValueFilter(f))
	}

	return h.Backend.ListJobs(ctx, in.JobQueue, in.JobStatus, nextToken, maxResults, filters)
}

type describeJobsInput struct {
	Jobs []string `json:"jobs"`
}

// jobDetail mirrors aws-sdk-go-v2/service/batch/types.JobDetail's field names
// and nesting (see deserializers.go's awsRestjson1_deserializeDocumentJobDetail
// case list). Pod/task/node runtime placement (podName, nodeName, taskArn,
// containerInstanceArn) needs real execution and is never populated.
type jobDetail struct {
	StoppedAt                    *int64                        `json:"stoppedAt,omitempty"`
	RetryStrategy                *RetryStrategy                `json:"retryStrategy,omitempty"`
	Timeout                      *JobTimeout                   `json:"timeout,omitempty"`
	ArrayProperties              *ArrayProperties              `json:"arrayProperties,omitempty"`
	ConsumableResourceProperties *ConsumableResourceProperties `json:"consumableResourceProperties,omitempty"`
	Container                    *ContainerDetail              `json:"container,omitempty"`
	NodeProperties               *NodeProperties               `json:"nodeProperties,omitempty"`
	NodeDetails                  *nodeDetails                  `json:"nodeDetails,omitempty"`
	EksProperties                *EksProperties                `json:"eksProperties,omitempty"`
	EcsProperties                map[string]any                `json:"ecsProperties,omitempty"`
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
	Attempts                     []JobAttempt                  `json:"attempts,omitempty"`
	PlatformCapabilities         []string                      `json:"platformCapabilities,omitempty"`
	CreatedAt                    int64                         `json:"createdAt"`
	// StartedAt is required on JobDetail even for a job that hasn't reached
	// RUNNING yet -- 0 until then, never omitted (see int64OrZero).
	StartedAt                  int64 `json:"startedAt"`
	SchedulingPriorityOverride int32 `json:"schedulingPriority,omitempty"`
	PropagateTags              bool  `json:"propagateTags,omitempty"`
	IsCancelled                bool  `json:"isCancelled"`
	IsTerminated               bool  `json:"isTerminated"`
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
			StartedAt:                    int64OrZero(j.StartedAt),
			StoppedAt:                    j.StoppedAt,
			Tags:                         tagsOrEmpty(j.Tags),
			RetryStrategy:                j.RetryStrategy,
			Timeout:                      j.Timeout,
			ArrayProperties:              j.ArrayProperties,
			ConsumableResourceProperties: j.ConsumableResourceProperties,
			Container:                    j.Container,
			NodeProperties:               j.NodeProperties,
			NodeDetails:                  jobNodeDetails(j),
			EksProperties:                j.EksProperties,
			EcsProperties:                j.EcsProperties,
			Parameters:                   j.Parameters,
			DependsOn:                    j.DependsOn,
			Attempts:                     j.Attempts,
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

func jobNodeDetails(j *Job) *nodeDetails {
	if j.nodeIndex == nil {
		return nil
	}

	return &nodeDetails{IsMainNode: j.isMainNode, NodeIndex: *j.nodeIndex}
}

type containerOverridesInput struct {
	InstanceType         string                `json:"instanceType,omitempty"`
	Environment          []keyValuePair        `json:"environment,omitempty"`
	Command              []string              `json:"command,omitempty"`
	ResourceRequirements []ResourceRequirement `json:"resourceRequirements,omitempty"`
	Memory               int32                 `json:"memory,omitempty"`
	Vcpus                int32                 `json:"vcpus,omitempty"`
}

func (c *containerOverridesInput) toModel() *ContainerOverrides {
	if c == nil {
		return nil
	}

	env := make([]KeyValuePair, len(c.Environment))
	for i, kv := range c.Environment {
		env[i] = KeyValuePair(kv)
	}

	return &ContainerOverrides{
		InstanceType:         c.InstanceType,
		Command:              c.Command,
		Environment:          env,
		ResourceRequirements: c.ResourceRequirements,
		Memory:               c.Memory,
		Vcpus:                c.Vcpus,
	}
}

type nodePropertyOverrideInput struct {
	ConsumableOverride *consumableResourcePropertiesInput `json:"consumableResourcePropertiesOverride,omitempty"`
	ContainerOverrides *containerOverridesInput           `json:"containerOverrides,omitempty"`
	EcsOverride        map[string]any                     `json:"ecsPropertiesOverride,omitempty"`
	EksOverride        *EksPropertiesOverride             `json:"eksPropertiesOverride,omitempty"`
	TargetNodes        string                             `json:"targetNodes"`
	InstanceTypes      []string                           `json:"instanceTypes,omitempty"`
}

type nodeOverridesInput struct {
	NodePropertyOverrides []nodePropertyOverrideInput `json:"nodePropertyOverrides,omitempty"`
	NumNodes              int32                       `json:"numNodes,omitempty"`
}

func (n *nodeOverridesInput) toModel() *NodeOverrides {
	if n == nil {
		return nil
	}

	out := &NodeOverrides{NumNodes: n.NumNodes}

	for _, o := range n.NodePropertyOverrides {
		m := NodePropertyOverride{
			TargetNodes:        o.TargetNodes,
			ContainerOverrides: o.ContainerOverrides.toModel(),
			EcsOverride:        o.EcsOverride,
			EksOverride:        o.EksOverride,
			InstanceTypes:      o.InstanceTypes,
		}

		if list := consumableResourcePropertiesFromInput(o.ConsumableOverride); list != nil {
			m.ConsumableOverride = &ConsumableResourceProperties{ConsumableResourceList: list}
		}

		out.NodePropertyOverrides = append(out.NodePropertyOverrides, m)
	}

	return out
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
	NodeOverrides              *nodeOverridesInput                `json:"nodeOverrides,omitempty"`
	EksOverride                *EksPropertiesOverride             `json:"eksPropertiesOverride,omitempty"`
	EcsOverride                map[string]any                     `json:"ecsPropertiesOverride,omitempty"`
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
	overrides := in.ContainerOverrides.toModel()

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
		WithNodeOverrides(in.NodeOverrides.toModel()),
		WithEksOverride(in.EksOverride),
		WithEcsOverride(in.EcsOverride),
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
