package batch

import (
	"context"
	"fmt"
)

// --- QuotaShare handlers ---

// createQuotaShareInput mirrors aws-sdk-go-v2/service/batch's
// CreateQuotaShareInput exactly (see serializers.go's
// awsRestjson1_serializeOpDocumentCreateQuotaShareInput): quotaShareName,
// jobQueue, capacityLimits, resourceSharingConfiguration, and
// preemptionConfiguration are all required; state and tags are optional.
type createQuotaShareInput struct {
	Tags                         map[string]string                       `json:"tags"`
	PreemptionConfiguration      *QuotaSharePreemptionConfiguration      `json:"preemptionConfiguration"`
	ResourceSharingConfiguration *QuotaShareResourceSharingConfiguration `json:"resourceSharingConfiguration"`
	QuotaShareName               string                                  `json:"quotaShareName"`
	JobQueue                     string                                  `json:"jobQueue"`
	State                        string                                  `json:"state,omitempty"`
	CapacityLimits               []QuotaShareCapacityLimit               `json:"capacityLimits"`
}

// createQuotaShareOutput mirrors CreateQuotaShareOutput exactly (see
// deserializers.go's awsRestjson1_deserializeOpDocumentCreateQuotaShareOutput):
// only quotaShareArn/quotaShareName come back, not the full detail.
type createQuotaShareOutput struct {
	QuotaShareArn  string `json:"quotaShareArn,omitempty"`
	QuotaShareName string `json:"quotaShareName,omitempty"`
}

func (h *Handler) handleCreateQuotaShare(
	ctx context.Context,
	in *createQuotaShareInput,
) (*createQuotaShareOutput, error) {
	qs, err := h.Backend.CreateQuotaShare(
		ctx,
		in.QuotaShareName,
		in.JobQueue,
		in.CapacityLimits,
		in.PreemptionConfiguration,
		in.ResourceSharingConfiguration,
		in.State,
		in.Tags,
	)
	if err != nil {
		return nil, err
	}

	return &createQuotaShareOutput{QuotaShareArn: qs.QuotaShareArn, QuotaShareName: qs.QuotaShareName}, nil
}

type describeQuotaShareInput struct {
	QuotaShareArn string `json:"quotaShareArn"`
}

// describeQuotaShareOutput mirrors DescribeQuotaShareOutput exactly (see
// deserializers.go's awsRestjson1_deserializeOpDocumentDescribeQuotaShareOutput):
// unlike quotaShareDetail (ListQuotaShares' item shape), this output DOES
// carry a tags field.
type describeQuotaShareOutput struct {
	Tags                         map[string]string                       `json:"tags"`
	PreemptionConfiguration      *QuotaSharePreemptionConfiguration      `json:"preemptionConfiguration,omitempty"`
	ResourceSharingConfiguration *QuotaShareResourceSharingConfiguration `json:"resourceSharingConfiguration,omitempty"`
	QuotaShareArn                string                                  `json:"quotaShareArn,omitempty"`
	QuotaShareName               string                                  `json:"quotaShareName,omitempty"`
	JobQueueArn                  string                                  `json:"jobQueueArn,omitempty"`
	State                        string                                  `json:"state,omitempty"`
	Status                       string                                  `json:"status,omitempty"`
	CapacityLimits               []QuotaShareCapacityLimit               `json:"capacityLimits,omitempty"`
}

func (h *Handler) handleDescribeQuotaShare(
	ctx context.Context,
	in *describeQuotaShareInput,
) (*describeQuotaShareOutput, error) {
	if in.QuotaShareArn == "" {
		return nil, fmt.Errorf("%w: quotaShareArn is required", ErrValidation)
	}

	qs, err := h.Backend.DescribeQuotaShare(ctx, in.QuotaShareArn)
	if err != nil {
		return nil, err
	}

	return &describeQuotaShareOutput{
		QuotaShareArn:                qs.QuotaShareArn,
		QuotaShareName:               qs.QuotaShareName,
		JobQueueArn:                  qs.JobQueueArn,
		CapacityLimits:               qs.CapacityLimits,
		PreemptionConfiguration:      qs.PreemptionConfiguration,
		ResourceSharingConfiguration: qs.ResourceSharingConfiguration,
		State:                        qs.State,
		Status:                       qs.Status,
		Tags:                         tagsOrEmpty(qs.Tags),
	}, nil
}

type deleteQuotaShareInput struct {
	QuotaShareArn string `json:"quotaShareArn"`
}

func (h *Handler) handleDeleteQuotaShare(ctx context.Context, in *deleteQuotaShareInput) (*emptyOutput, error) {
	if in.QuotaShareArn == "" {
		return nil, fmt.Errorf("%w: quotaShareArn is required", ErrValidation)
	}

	if err := h.Backend.DeleteQuotaShare(ctx, in.QuotaShareArn); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// updateQuotaShareInput mirrors UpdateQuotaShareInput exactly: only
// quotaShareArn is required, everything else is an optional partial update.
type updateQuotaShareInput struct {
	PreemptionConfiguration      *QuotaSharePreemptionConfiguration      `json:"preemptionConfiguration,omitempty"`
	ResourceSharingConfiguration *QuotaShareResourceSharingConfiguration `json:"resourceSharingConfiguration,omitempty"`
	QuotaShareArn                string                                  `json:"quotaShareArn"`
	State                        string                                  `json:"state,omitempty"`
	CapacityLimits               []QuotaShareCapacityLimit               `json:"capacityLimits,omitempty"`
}

// updateQuotaShareOutput mirrors UpdateQuotaShareOutput exactly: same
// narrow {quotaShareArn, quotaShareName} shape as CreateQuotaShareOutput.
type updateQuotaShareOutput struct {
	QuotaShareArn  string `json:"quotaShareArn,omitempty"`
	QuotaShareName string `json:"quotaShareName,omitempty"`
}

func (h *Handler) handleUpdateQuotaShare(
	ctx context.Context,
	in *updateQuotaShareInput,
) (*updateQuotaShareOutput, error) {
	if in.QuotaShareArn == "" {
		return nil, fmt.Errorf("%w: quotaShareArn is required", ErrValidation)
	}

	qs, err := h.Backend.UpdateQuotaShare(
		ctx,
		in.QuotaShareArn,
		in.CapacityLimits,
		in.PreemptionConfiguration,
		in.ResourceSharingConfiguration,
		in.State,
	)
	if err != nil {
		return nil, err
	}

	return &updateQuotaShareOutput{QuotaShareArn: qs.QuotaShareArn, QuotaShareName: qs.QuotaShareName}, nil
}

// quotaShareDetail mirrors aws-sdk-go-v2/service/batch/types.QuotaShareDetail
// exactly -- unlike describeQuotaShareOutput, it carries NO tags field (see
// deserializers.go's awsRestjson1_deserializeDocumentQuotaShareDetail case
// list, which has no "tags" case at all).
type quotaShareDetail struct {
	PreemptionConfiguration      *QuotaSharePreemptionConfiguration      `json:"preemptionConfiguration,omitempty"`
	ResourceSharingConfiguration *QuotaShareResourceSharingConfiguration `json:"resourceSharingConfiguration,omitempty"`
	QuotaShareArn                string                                  `json:"quotaShareArn,omitempty"`
	QuotaShareName               string                                  `json:"quotaShareName,omitempty"`
	JobQueueArn                  string                                  `json:"jobQueueArn,omitempty"`
	State                        string                                  `json:"state,omitempty"`
	Status                       string                                  `json:"status,omitempty"`
	CapacityLimits               []QuotaShareCapacityLimit               `json:"capacityLimits,omitempty"`
}

type listQuotaSharesInput struct {
	NextToken  *string `json:"nextToken,omitempty"`
	MaxResults *int32  `json:"maxResults,omitempty"`
	JobQueue   string  `json:"jobQueue"`
}

type listQuotaSharesOutput struct {
	NextToken   *string            `json:"nextToken,omitempty"`
	QuotaShares []quotaShareDetail `json:"quotaShares"`
}

func (h *Handler) handleListQuotaShares(
	ctx context.Context,
	in *listQuotaSharesInput,
) (*listQuotaSharesOutput, error) {
	if in.JobQueue == "" {
		return nil, fmt.Errorf("%w: jobQueue is required", ErrValidation)
	}

	all, err := h.Backend.ListQuotaShares(ctx, in.JobQueue)
	if err != nil {
		return nil, err
	}

	names := make([]string, len(all))
	byName := make(map[string]*QuotaShare, len(all))

	for i, qs := range all {
		names[i] = qs.QuotaShareName
		byName[qs.QuotaShareName] = qs
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

	details := make([]quotaShareDetail, 0, len(pageNames))
	for _, n := range pageNames {
		qs := byName[n]
		details = append(details, quotaShareDetail{
			QuotaShareArn:                qs.QuotaShareArn,
			QuotaShareName:               qs.QuotaShareName,
			JobQueueArn:                  qs.JobQueueArn,
			CapacityLimits:               qs.CapacityLimits,
			PreemptionConfiguration:      qs.PreemptionConfiguration,
			ResourceSharingConfiguration: qs.ResourceSharingConfiguration,
			State:                        qs.State,
			Status:                       qs.Status,
		})
	}

	out := &listQuotaSharesOutput{QuotaShares: details}
	if outToken != "" {
		out.NextToken = &outToken
	}

	return out, nil
}
