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
type describeAggregationAuthorizationsOutput struct {
	AggregationAuthorizations []AggregationAuthorization `json:"AggregationAuthorizations"`
}

func (h *Handler) handleDescribeAggregationAuthorizations(
	_ context.Context, _ *emptyInput,
) (*describeAggregationAuthorizationsOutput, error) {
	return &describeAggregationAuthorizationsOutput{
		AggregationAuthorizations: h.Backend.DescribeAggregationAuthorizations(),
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
type describeConfigurationAggregatorsOutput struct {
	ConfigurationAggregators []ConfigurationAggregator `json:"ConfigurationAggregators"`
}

func (h *Handler) handleDescribeConfigurationAggregators(
	_ context.Context, _ *emptyInput,
) (*describeConfigurationAggregatorsOutput, error) {
	return &describeConfigurationAggregatorsOutput{
		ConfigurationAggregators: h.Backend.DescribeConfigurationAggregators(),
	}, nil
}

// DescribeConfigurationAggregatorSourcesStatus request/response types and handler.
type describeConfigurationAggregatorSourcesStatusInput struct {
	ConfigurationAggregatorName string `json:"ConfigurationAggregatorName"`
}
type describeConfigurationAggregatorSourcesStatusOutput struct {
	AggregatedSourceStatusList []AggregatedSourceStatus `json:"AggregatedSourceStatusList"`
}

func (h *Handler) handleDescribeConfigurationAggregatorSourcesStatus(
	_ context.Context, in *describeConfigurationAggregatorSourcesStatusInput,
) (*describeConfigurationAggregatorSourcesStatusOutput, error) {
	statuses, err := h.Backend.DescribeConfigurationAggregatorSourcesStatus(in.ConfigurationAggregatorName)
	if err != nil {
		return nil, err
	}

	return &describeConfigurationAggregatorSourcesStatusOutput{AggregatedSourceStatusList: statuses}, nil
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
type describePendingAggregationRequestsOutput struct {
	PendingAggregationRequests []PendingAggregationRequest `json:"PendingAggregationRequests"`
}

func (h *Handler) handleDescribePendingAggregationRequests(
	_ context.Context, _ *emptyInput,
) (*describePendingAggregationRequestsOutput, error) {
	return &describePendingAggregationRequestsOutput{
		PendingAggregationRequests: h.Backend.DescribePendingAggregationRequests(),
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
