package awsconfig

import (
	"context"

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
	ConfigRuleName    string `json:"ConfigRuleName"`
	ResourceGroupName string `json:"ResourceGroupName"`
}

func (h *Handler) handleDeleteRemediationExceptions(
	_ context.Context, in *deleteRemediationExceptionsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteRemediationExceptions(
		in.ConfigRuleName,
		in.ResourceGroupName,
	)
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
type describeRemediationExecutionStatusOutput struct {
	RemediationExecutionStatuses []any `json:"RemediationExecutionStatuses"`
}

func (h *Handler) handleDescribeRemediationExecutionStatus(
	_ context.Context, _ *emptyInput,
) (*describeRemediationExecutionStatusOutput, error) {
	return &describeRemediationExecutionStatusOutput{
		RemediationExecutionStatuses: h.Backend.DescribeRemediationExecutionStatus(),
	}, nil
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

// PutRemediationExceptions request/response types and handler.
type putRemediationExceptionsInput struct {
	ConfigRuleName string `json:"ConfigRuleName"`
	ResourceType   string `json:"ResourceType"`
	ResourceID     string `json:"ResourceId"`
}

func (h *Handler) handlePutRemediationExceptions(
	_ context.Context, in *putRemediationExceptionsInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutRemediationExceptions(in.ConfigRuleName, in.ResourceType, in.ResourceID)
}

// StartRemediationExecution request/response types and handler.
func (h *Handler) handleStartRemediationExecution(
	_ context.Context, _ *emptyInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.StartRemediationExecution()
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
