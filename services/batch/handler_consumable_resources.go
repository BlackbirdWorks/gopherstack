package batch

import (
	"context"
	"fmt"
)

// --- ConsumableResource handlers ---

type createConsumableResourceInput struct {
	Tags                   map[string]string `json:"tags"`
	ConsumableResourceName string            `json:"consumableResourceName"`
	ResourceType           string            `json:"resourceType"`
	TotalQuantity          int64             `json:"totalQuantity"`
}

type createConsumableResourceOutput struct {
	ConsumableResourceArn  string `json:"consumableResourceArn"`
	ConsumableResourceName string `json:"consumableResourceName"`
}

func (h *Handler) handleCreateConsumableResource(
	ctx context.Context,
	in *createConsumableResourceInput,
) (*createConsumableResourceOutput, error) {
	if in.ConsumableResourceName == "" {
		return nil, fmt.Errorf("%w: consumableResourceName is required", ErrValidation)
	}

	cr, err := h.Backend.CreateConsumableResource(
		ctx,
		in.ConsumableResourceName,
		in.ResourceType,
		in.TotalQuantity,
		in.Tags,
	)
	if err != nil {
		return nil, err
	}

	return &createConsumableResourceOutput{
		ConsumableResourceArn:  cr.ConsumableResourceArn,
		ConsumableResourceName: cr.ConsumableResourceName,
	}, nil
}

type deleteConsumableResourceInput struct {
	ConsumableResource string `json:"consumableResource"`
}

func (h *Handler) handleDeleteConsumableResource(
	ctx context.Context,
	in *deleteConsumableResourceInput,
) (*emptyOutput, error) {
	if in.ConsumableResource == "" {
		return nil, fmt.Errorf("%w: consumableResource is required", ErrValidation)
	}

	if err := h.Backend.DeleteConsumableResource(ctx, in.ConsumableResource); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type describeConsumableResourceInput struct {
	ConsumableResource string `json:"consumableResource"`
}

type describeConsumableResourceOutput struct {
	Tags                   map[string]string `json:"tags"`
	ConsumableResourceArn  string            `json:"consumableResourceArn"`
	ConsumableResourceName string            `json:"consumableResourceName"`
	ResourceType           string            `json:"resourceType,omitempty"`
	CreatedAt              int64             `json:"createdAt"`
	TotalQuantity          int64             `json:"totalQuantity"`
	AvailableQuantity      int64             `json:"availableQuantity"`
	InUseQuantity          int64             `json:"inUseQuantity"`
}

func (h *Handler) handleDescribeConsumableResource(
	ctx context.Context,
	in *describeConsumableResourceInput,
) (*describeConsumableResourceOutput, error) {
	if in.ConsumableResource == "" {
		return nil, fmt.Errorf("%w: consumableResource is required", ErrValidation)
	}

	cr, err := h.Backend.DescribeConsumableResource(ctx, in.ConsumableResource)
	if err != nil {
		return nil, err
	}

	return &describeConsumableResourceOutput{
		ConsumableResourceArn:  cr.ConsumableResourceArn,
		ConsumableResourceName: cr.ConsumableResourceName,
		ResourceType:           cr.ResourceType,
		Tags:                   tagsOrEmpty(cr.Tags),
		CreatedAt:              cr.CreatedAt,
		TotalQuantity:          cr.TotalQuantity,
		AvailableQuantity:      cr.AvailableQuantity,
		InUseQuantity:          cr.InUseQuantity,
	}, nil
}

// --- UpdateConsumableResource handler ---

type updateConsumableResourceInput struct {
	ConsumableResource string `json:"consumableResource"`
	Operation          string `json:"operation"`
	Quantity           int64  `json:"quantity"`
}

type updateConsumableResourceOutput struct {
	Tags                   map[string]string `json:"tags"`
	ConsumableResourceArn  string            `json:"consumableResourceArn"`
	ConsumableResourceName string            `json:"consumableResourceName"`
	ResourceType           string            `json:"resourceType,omitempty"`
	CreatedAt              int64             `json:"createdAt"`
	TotalQuantity          int64             `json:"totalQuantity"`
	AvailableQuantity      int64             `json:"availableQuantity"`
	InUseQuantity          int64             `json:"inUseQuantity"`
}

func (h *Handler) handleUpdateConsumableResource(
	ctx context.Context,
	in *updateConsumableResourceInput,
) (*updateConsumableResourceOutput, error) {
	if in.ConsumableResource == "" {
		return nil, fmt.Errorf("%w: consumableResource is required", ErrValidation)
	}

	cr, err := h.Backend.UpdateConsumableResource(ctx, in.ConsumableResource, in.Operation, in.Quantity)
	if err != nil {
		return nil, err
	}

	return &updateConsumableResourceOutput{
		ConsumableResourceArn:  cr.ConsumableResourceArn,
		ConsumableResourceName: cr.ConsumableResourceName,
		ResourceType:           cr.ResourceType,
		Tags:                   tagsOrEmpty(cr.Tags),
		CreatedAt:              cr.CreatedAt,
		TotalQuantity:          cr.TotalQuantity,
		AvailableQuantity:      cr.AvailableQuantity,
		InUseQuantity:          cr.InUseQuantity,
	}, nil
}

// --- ListConsumableResources handler ---

// consumableResourceSummary mirrors aws-sdk-go-v2/service/batch/types.
// ConsumableResourceSummary exactly (a narrower shape than ConsumableResource
// itself -- no tags or createdAt).
type consumableResourceSummary struct {
	ConsumableResourceArn  string `json:"consumableResourceArn"`
	ConsumableResourceName string `json:"consumableResourceName"`
	ResourceType           string `json:"resourceType,omitempty"`
	TotalQuantity          int64  `json:"totalQuantity,omitempty"`
	InUseQuantity          int64  `json:"inUseQuantity,omitempty"`
}

type listConsumableResourcesInput struct {
	MaxResults *int32  `json:"maxResults,omitempty"`
	NextToken  *string `json:"nextToken,omitempty"`
}

// listConsumableResourcesOutput mirrors aws-sdk-go-v2/service/batch's
// ListConsumableResourcesOutput: the wire key is "consumableResources", not
// "consumableResourceSummaryList" -- a real SDK client parsing the previous
// key would have always seen an empty list.
type listConsumableResourcesOutput struct {
	NextToken           *string                     `json:"nextToken,omitempty"`
	ConsumableResources []consumableResourceSummary `json:"consumableResources"`
}

func (h *Handler) handleListConsumableResources(
	ctx context.Context,
	in *listConsumableResourcesInput,
) (*listConsumableResourcesOutput, error) {
	all := h.Backend.ListConsumableResources(ctx)

	names := make([]string, len(all))
	byName := make(map[string]*ConsumableResource, len(all))

	for i, cr := range all {
		names[i] = cr.ConsumableResourceName
		byName[cr.ConsumableResourceName] = cr
	}

	var maxResults int32
	if in.MaxResults != nil {
		maxResults = *in.MaxResults
	}

	var nextToken string
	if in.NextToken != nil {
		nextToken = *in.NextToken
	}

	pageNames, outToken := paginateMapKeys(names, nextToken, maxResults)

	summaries := make([]consumableResourceSummary, 0, len(pageNames))
	for _, n := range pageNames {
		cr := byName[n]
		summaries = append(summaries, consumableResourceSummary{
			ConsumableResourceArn:  cr.ConsumableResourceArn,
			ConsumableResourceName: cr.ConsumableResourceName,
			ResourceType:           cr.ResourceType,
			TotalQuantity:          cr.TotalQuantity,
			InUseQuantity:          cr.InUseQuantity,
		})
	}

	out := &listConsumableResourcesOutput{ConsumableResources: summaries}
	if outToken != "" {
		out.NextToken = &outToken
	}

	return out, nil
}

type listJobsByConsumableResourceInput struct {
	MaxResults         *int32  `json:"maxResults,omitempty"`
	NextToken          *string `json:"nextToken,omitempty"`
	ConsumableResource string  `json:"consumableResource"`
}

// listJobsByConsumableResourceSummary mirrors aws-sdk-go-v2/service/batch/
// types.ListJobsByConsumableResourceSummary -- a distinct, narrower shape
// from the full Job (jobQueueArn/jobStatus/quantity, not the full job
// detail).
type listJobsByConsumableResourceSummary struct {
	ConsumableResourceProperties *ConsumableResourceProperties `json:"consumableResourceProperties,omitempty"`
	JobDefinitionArn             string                        `json:"jobDefinitionArn,omitempty"`
	JobArn                       string                        `json:"jobArn"`
	JobName                      string                        `json:"jobName"`
	JobQueueArn                  string                        `json:"jobQueueArn"`
	JobStatus                    string                        `json:"jobStatus"`
	ShareIdentifier              string                        `json:"shareIdentifier,omitempty"`
	StatusReason                 string                        `json:"statusReason,omitempty"`
	CreatedAt                    int64                         `json:"createdAt"`
	StartedAt                    int64                         `json:"startedAt,omitempty"`
	Quantity                     int64                         `json:"quantity"`
}

type listJobsByConsumableResourceOutput struct {
	NextToken *string                               `json:"nextToken,omitempty"`
	Jobs      []listJobsByConsumableResourceSummary `json:"jobs"`
}

func (h *Handler) handleListJobsByConsumableResource(
	ctx context.Context,
	in *listJobsByConsumableResourceInput,
) (*listJobsByConsumableResourceOutput, error) {
	jobs, err := h.Backend.ListJobsByConsumableResource(ctx, in.ConsumableResource)
	if err != nil {
		return nil, err
	}

	ids := make([]string, len(jobs))
	byID := make(map[string]*Job, len(jobs))

	for i, j := range jobs {
		ids[i] = j.JobID
		byID[j.JobID] = j
	}

	var maxResults int32
	if in.MaxResults != nil {
		maxResults = *in.MaxResults
	}

	var nextToken string
	if in.NextToken != nil {
		nextToken = *in.NextToken
	}

	pageIDs, outToken := paginateMapKeys(ids, nextToken, maxResults)

	summaries := make([]listJobsByConsumableResourceSummary, 0, len(pageIDs))
	for _, id := range pageIDs {
		j := byID[id]
		summaries = append(summaries, listJobsByConsumableResourceSummaryFromJob(j, in.ConsumableResource))
	}

	out := &listJobsByConsumableResourceOutput{Jobs: summaries}
	if outToken != "" {
		out.NextToken = &outToken
	}

	return out, nil
}

// listJobsByConsumableResourceSummaryFromJob projects a full Job onto the
// narrower ListJobsByConsumableResourceSummary wire shape, extracting the
// requested quantity for consumableResource from the job's
// ConsumableResourceProperties.
func listJobsByConsumableResourceSummaryFromJob(j *Job, consumableResource string) listJobsByConsumableResourceSummary {
	var quantity int64

	if j.ConsumableResourceProperties != nil {
		for _, crp := range j.ConsumableResourceProperties.ConsumableResourceList {
			if crp.ConsumableResource == consumableResource {
				quantity = crp.Quantity

				break
			}
		}
	}

	var startedAt int64
	if j.StartedAt != nil {
		startedAt = *j.StartedAt
	}

	return listJobsByConsumableResourceSummary{
		JobArn:                       j.JobARN,
		JobName:                      j.JobName,
		JobQueueArn:                  j.JobQueue,
		JobStatus:                    j.Status,
		StatusReason:                 j.StatusReason,
		ShareIdentifier:              j.ShareIdentifier,
		JobDefinitionArn:             j.JobDefinition,
		CreatedAt:                    j.CreatedAt,
		StartedAt:                    startedAt,
		Quantity:                     quantity,
		ConsumableResourceProperties: j.ConsumableResourceProperties,
	}
}
