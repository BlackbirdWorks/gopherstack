package awsconfig

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Operation name constants for aggregator ops.
const (
	opDeleteAggregationAuthorization               = "DeleteAggregationAuthorization"
	opDeleteConfigurationAggregator                = "DeleteConfigurationAggregator"
	opDeletePendingAggregationRequest              = "DeletePendingAggregationRequest"
	opDescribeAggregationAuthorizations            = "DescribeAggregationAuthorizations"
	opDescribeConfigurationAggregatorSourcesStatus = "DescribeConfigurationAggregatorSourcesStatus"
	opDescribeConfigurationAggregators             = "DescribeConfigurationAggregators"
	opDescribePendingAggregationRequests           = "DescribePendingAggregationRequests"
	opPutAggregationAuthorization                  = "PutAggregationAuthorization"
	opPutConfigurationAggregator                   = "PutConfigurationAggregator"
)

// aggregatorSupportedOps returns the operation names this family handles.
func aggregatorSupportedOps() []string {
	return []string{
		opDescribeAggregationAuthorizations,
		opDeleteAggregationAuthorization,
		opPutAggregationAuthorization,
		opDescribeConfigurationAggregators,
		opDescribeConfigurationAggregatorSourcesStatus,
		opDeleteConfigurationAggregator,
		opPutConfigurationAggregator,
		opDeletePendingAggregationRequest,
		opDescribePendingAggregationRequests,
	}
}

// DescribeAggregationAuthorizations request/response types and handler.
type describeAggregationAuthorizationsInput struct {
	NextToken string `json:"NextToken,omitempty"`
	Limit     int32  `json:"Limit,omitempty"`
}
type describeAggregationAuthorizationsOutput struct {
	NextToken                 string                     `json:"NextToken,omitempty"`
	AggregationAuthorizations []AggregationAuthorization `json:"AggregationAuthorizations"`
}

func (h *Handler) handleDescribeAggregationAuthorizations(
	_ context.Context, in *describeAggregationAuthorizationsInput,
) (*describeAggregationAuthorizationsOutput, error) {
	p, err := paginate(h.Backend.DescribeAggregationAuthorizations(), in.NextToken, in.Limit, unboundedPageDefault)
	if err != nil {
		return nil, err
	}

	return &describeAggregationAuthorizationsOutput{
		AggregationAuthorizations: p.Data,
		NextToken:                 p.Next,
	}, nil
}

// DeleteAggregationAuthorization request/response types and handler.
type deleteAggregationAuthorizationInput struct {
	AuthorizedAccountID string `json:"AuthorizedAccountId"`
	AuthorizedAwsRegion string `json:"AuthorizedAwsRegion"`
}

type deleteAggregationAuthorizationOutput struct{}

func (h *Handler) handleDeleteAggregationAuthorization(
	_ context.Context,
	in *deleteAggregationAuthorizationInput,
) (*deleteAggregationAuthorizationOutput, error) {
	if err := h.Backend.DeleteAggregationAuthorization(in.AuthorizedAccountID, in.AuthorizedAwsRegion); err != nil {
		return nil, err
	}

	return &deleteAggregationAuthorizationOutput{}, nil
}

// PutAggregationAuthorization request/response types and handler.
type putAggregationAuthorizationInput struct {
	AuthorizedAccountID string `json:"AuthorizedAccountId"`
	AuthorizedAwsRegion string `json:"AuthorizedAwsRegion"`
	Tags                []Tag  `json:"Tags,omitempty"`
}

func (h *Handler) handlePutAggregationAuthorization(
	_ context.Context, in *putAggregationAuthorizationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutAggregationAuthorization(
		in.AuthorizedAccountID,
		in.AuthorizedAwsRegion,
		in.Tags,
	)
}

// DescribeConfigurationAggregators request/response types and handler.
type describeConfigurationAggregatorsInput struct {
	NextToken string `json:"NextToken,omitempty"`
	Limit     int32  `json:"Limit,omitempty"`
}
type describeConfigurationAggregatorsOutput struct {
	NextToken                string                    `json:"NextToken,omitempty"`
	ConfigurationAggregators []ConfigurationAggregator `json:"ConfigurationAggregators"`
}

func (h *Handler) handleDescribeConfigurationAggregators(
	_ context.Context, in *describeConfigurationAggregatorsInput,
) (*describeConfigurationAggregatorsOutput, error) {
	p, err := paginate(h.Backend.DescribeConfigurationAggregators(), in.NextToken, in.Limit, unboundedPageDefault)
	if err != nil {
		return nil, err
	}

	return &describeConfigurationAggregatorsOutput{
		ConfigurationAggregators: p.Data,
		NextToken:                p.Next,
	}, nil
}

// DescribeConfigurationAggregatorSourcesStatus request/response types and handler.
type describeConfigurationAggregatorSourcesStatusInput struct {
	ConfigurationAggregatorName string `json:"ConfigurationAggregatorName"`
	NextToken                   string `json:"NextToken,omitempty"`
	Limit                       int32  `json:"Limit,omitempty"`
}
type describeConfigurationAggregatorSourcesStatusOutput struct {
	NextToken                  string                   `json:"NextToken,omitempty"`
	AggregatedSourceStatusList []AggregatedSourceStatus `json:"AggregatedSourceStatusList"`
}

func (h *Handler) handleDescribeConfigurationAggregatorSourcesStatus(
	_ context.Context, in *describeConfigurationAggregatorSourcesStatusInput,
) (*describeConfigurationAggregatorSourcesStatusOutput, error) {
	statuses, err := h.Backend.DescribeConfigurationAggregatorSourcesStatus(in.ConfigurationAggregatorName)
	if err != nil {
		return nil, err
	}

	p, err := paginate(statuses, in.NextToken, in.Limit, unboundedPageDefault)
	if err != nil {
		return nil, err
	}

	return &describeConfigurationAggregatorSourcesStatusOutput{
		AggregatedSourceStatusList: p.Data,
		NextToken:                  p.Next,
	}, nil
}

// DeleteConfigurationAggregator request/response types and handler.
type deleteConfigurationAggregatorInput struct {
	ConfigurationAggregatorName string `json:"ConfigurationAggregatorName"`
}

type deleteConfigurationAggregatorOutput struct{}

func (h *Handler) handleDeleteConfigurationAggregator(
	_ context.Context,
	in *deleteConfigurationAggregatorInput,
) (*deleteConfigurationAggregatorOutput, error) {
	if err := h.Backend.DeleteConfigurationAggregator(in.ConfigurationAggregatorName); err != nil {
		return nil, err
	}

	return &deleteConfigurationAggregatorOutput{}, nil
}

// PutConfigurationAggregator request/response types and handler.
type putConfigurationAggregatorInput struct {
	OrganizationAggregationSource *OrganizationAggregationSource `json:"OrganizationAggregationSource,omitempty"`
	ConfigurationAggregatorName   string                         `json:"ConfigurationAggregatorName"`
	AccountAggregationSources     []AccountAggregationSource     `json:"AccountAggregationSources,omitempty"`
	Tags                          []Tag                          `json:"Tags,omitempty"`
}

func (h *Handler) handlePutConfigurationAggregator(
	_ context.Context, in *putConfigurationAggregatorInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutConfigurationAggregator(
		in.ConfigurationAggregatorName,
		in.AccountAggregationSources,
		in.OrganizationAggregationSource,
		in.Tags,
	)
}

// DeletePendingAggregationRequest request/response types and handler.
type deletePendingAggregationRequestInput struct {
	RequesterAccountID string `json:"RequesterAccountId"`
	RequesterAwsRegion string `json:"RequesterAwsRegion"`
}

func (h *Handler) handleDeletePendingAggregationRequest(
	_ context.Context, in *deletePendingAggregationRequestInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeletePendingAggregationRequest(
		in.RequesterAccountID,
		in.RequesterAwsRegion,
	)
}

// DescribePendingAggregationRequests request/response types and handler.
type describePendingAggregationRequestsInput struct {
	NextToken string `json:"NextToken,omitempty"`
	Limit     int32  `json:"Limit,omitempty"`
}
type describePendingAggregationRequestsOutput struct {
	NextToken                  string                      `json:"NextToken,omitempty"`
	PendingAggregationRequests []PendingAggregationRequest `json:"PendingAggregationRequests"`
}

func (h *Handler) handleDescribePendingAggregationRequests(
	_ context.Context, in *describePendingAggregationRequestsInput,
) (*describePendingAggregationRequestsOutput, error) {
	p, err := paginate(h.Backend.DescribePendingAggregationRequests(), in.NextToken, in.Limit, unboundedPageDefault)
	if err != nil {
		return nil, err
	}

	return &describePendingAggregationRequestsOutput{
		PendingAggregationRequests: p.Data,
		NextToken:                  p.Next,
	}, nil
}

// buildAggregatorDispatch returns dispatch entries for aggregator ops.
func (h *Handler) buildAggregatorDispatch() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opDescribeAggregationAuthorizations: service.WrapOp(h.handleDescribeAggregationAuthorizations),
		opDeleteAggregationAuthorization:    service.WrapOp(h.handleDeleteAggregationAuthorization),
		opPutAggregationAuthorization:       service.WrapOp(h.handlePutAggregationAuthorization),
		opDescribeConfigurationAggregators:  service.WrapOp(h.handleDescribeConfigurationAggregators),
		opDescribeConfigurationAggregatorSourcesStatus: service.WrapOp(
			h.handleDescribeConfigurationAggregatorSourcesStatus,
		),
		opDeleteConfigurationAggregator:      service.WrapOp(h.handleDeleteConfigurationAggregator),
		opPutConfigurationAggregator:         service.WrapOp(h.handlePutConfigurationAggregator),
		opDeletePendingAggregationRequest:    service.WrapOp(h.handleDeletePendingAggregationRequest),
		opDescribePendingAggregationRequests: service.WrapOp(h.handleDescribePendingAggregationRequests),
	}
}
