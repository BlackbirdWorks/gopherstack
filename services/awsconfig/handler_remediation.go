package awsconfig

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Operation name constants for remediation ops.
const (
	opDeleteRemediationConfiguration     = "DeleteRemediationConfiguration"
	opDeleteRemediationExceptions        = "DeleteRemediationExceptions"
	opDescribeRemediationConfigurations  = "DescribeRemediationConfigurations"
	opDescribeRemediationExceptions      = "DescribeRemediationExceptions"
	opDescribeRemediationExecutionStatus = "DescribeRemediationExecutionStatus"
	opPutRemediationConfigurations       = "PutRemediationConfigurations"
	opPutRemediationExceptions           = "PutRemediationExceptions"
	opStartRemediationExecution          = "StartRemediationExecution"
)

// remediationSupportedOps returns the operation names this family handles.
func remediationSupportedOps() []string {
	return []string{
		opDeleteRemediationConfiguration,
		opDeleteRemediationExceptions,
		opDescribeRemediationConfigurations,
		opDescribeRemediationExceptions,
		opDescribeRemediationExecutionStatus,
		opPutRemediationConfigurations,
		opPutRemediationExceptions,
		opStartRemediationExecution,
	}
}

// DeleteRemediationConfiguration request/response types and handler.
type deleteRemediationConfigurationInput struct {
	ConfigRuleName string `json:"ConfigRuleName"`
}

func (h *Handler) handleDeleteRemediationConfiguration(
	_ context.Context, in *deleteRemediationConfigurationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteRemediationConfiguration(in.ConfigRuleName)
}

// DeleteRemediationExceptions request/response types and handler.
type deleteRemediationExceptionsInput struct {
	ConfigRuleName string                            `json:"ConfigRuleName"`
	ResourceKeys   []RemediationExceptionResourceKey `json:"ResourceKeys"`
}

func (h *Handler) handleDeleteRemediationExceptions(
	_ context.Context, in *deleteRemediationExceptionsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteRemediationExceptions(in.ConfigRuleName, in.ResourceKeys)
}

// DescribeRemediationConfigurations request/response types and handler.
type describeRemediationConfigurationsInput struct {
	ConfigRuleNames []string `json:"ConfigRuleNames"`
}
type describeRemediationConfigurationsOutput struct {
	RemediationConfigurations []RemediationConfiguration `json:"RemediationConfigurations"`
}

func (h *Handler) handleDescribeRemediationConfigurations(
	_ context.Context, in *describeRemediationConfigurationsInput,
) (*describeRemediationConfigurationsOutput, error) {
	return &describeRemediationConfigurationsOutput{
		RemediationConfigurations: h.Backend.DescribeRemediationConfigurations(in.ConfigRuleNames),
	}, nil
}

// DescribeRemediationExceptions request/response types and handler.
type describeRemediationExceptionsInput struct {
	ConfigRuleName string `json:"ConfigRuleName"`
}
type describeRemediationExceptionsOutput struct {
	RemediationExceptions []RemediationException `json:"RemediationExceptions"`
}

func (h *Handler) handleDescribeRemediationExceptions(
	_ context.Context, in *describeRemediationExceptionsInput,
) (*describeRemediationExceptionsOutput, error) {
	return &describeRemediationExceptionsOutput{
		RemediationExceptions: h.Backend.DescribeRemediationExceptions(in.ConfigRuleName),
	}, nil
}

// DescribeRemediationExecutionStatus request/response types and handler.
type describeRemediationExecutionStatusInput struct {
	ConfigRuleName string        `json:"ConfigRuleName"`
	ResourceKeys   []ResourceKey `json:"ResourceKeys,omitempty"`
}
type describeRemediationExecutionStatusOutput struct {
	RemediationExecutionStatuses []RemediationExecutionStatusEntry `json:"RemediationExecutionStatuses"`
}

func (h *Handler) handleDescribeRemediationExecutionStatus(
	_ context.Context, in *describeRemediationExecutionStatusInput,
) (*describeRemediationExecutionStatusOutput, error) {
	statuses, err := h.Backend.DescribeRemediationExecutionStatus(in.ConfigRuleName, in.ResourceKeys)
	if err != nil {
		return nil, err
	}

	return &describeRemediationExecutionStatusOutput{RemediationExecutionStatuses: statuses}, nil
}

// PutRemediationConfigurations request/response types and handler.
type putRemediationConfigurationsInput struct {
	RemediationConfigurations []RemediationConfiguration `json:"RemediationConfigurations"`
}

func (h *Handler) handlePutRemediationConfigurations(
	_ context.Context, in *putRemediationConfigurationsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutRemediationConfigurations(in.RemediationConfigurations)
}

// PutRemediationExceptions request/response types and handler. ExpirationTime
// and Message are real optional members of PutRemediationExceptionsInput but
// aren't modeled: gopherstack's RemediationException has no fields to reflect
// them into (DescribeRemediationExceptions doesn't report them either), so
// they're left for the JSON decoder to silently discard rather than accepted
// into a field nothing reads.
type putRemediationExceptionsInput struct {
	ConfigRuleName string                            `json:"ConfigRuleName"`
	ResourceKeys   []RemediationExceptionResourceKey `json:"ResourceKeys"`
}

func (h *Handler) handlePutRemediationExceptions(
	_ context.Context, in *putRemediationExceptionsInput,
) (*emptyOutput, error) {
	if in.ConfigRuleName == "" {
		return nil, fmt.Errorf("%w: ConfigRuleName is required", ErrInvalidParameterValue)
	}

	if len(in.ResourceKeys) == 0 {
		return nil, fmt.Errorf("%w: ResourceKeys is required", ErrInvalidParameterValue)
	}

	return &emptyOutput{}, h.Backend.PutRemediationExceptions(in.ConfigRuleName, in.ResourceKeys)
}

// StartRemediationExecution request/response types and handler.
type startRemediationExecutionInput struct {
	ConfigRuleName string        `json:"ConfigRuleName"`
	ResourceKeys   []ResourceKey `json:"ResourceKeys"`
}
type startRemediationExecutionOutput struct {
	FailedItems []ResourceKey `json:"FailedItems,omitempty"`
}

func (h *Handler) handleStartRemediationExecution(
	_ context.Context, in *startRemediationExecutionInput,
) (*startRemediationExecutionOutput, error) {
	if err := h.Backend.StartRemediationExecution(in.ConfigRuleName, in.ResourceKeys); err != nil {
		return nil, err
	}

	return &startRemediationExecutionOutput{}, nil
}

// buildRemediationDispatch returns dispatch entries for remediation ops.
func (h *Handler) buildRemediationDispatch() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opDeleteRemediationConfiguration:    service.WrapOp(h.handleDeleteRemediationConfiguration),
		opDeleteRemediationExceptions:       service.WrapOp(h.handleDeleteRemediationExceptions),
		opDescribeRemediationConfigurations: service.WrapOp(h.handleDescribeRemediationConfigurations),
		opDescribeRemediationExceptions:     service.WrapOp(h.handleDescribeRemediationExceptions),
		opDescribeRemediationExecutionStatus: service.WrapOp(
			h.handleDescribeRemediationExecutionStatus,
		),
		opPutRemediationConfigurations: service.WrapOp(h.handlePutRemediationConfigurations),
		opPutRemediationExceptions:     service.WrapOp(h.handlePutRemediationExceptions),
		opStartRemediationExecution:    service.WrapOp(h.handleStartRemediationExecution),
	}
}
