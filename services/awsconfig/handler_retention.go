package awsconfig

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Operation name constants for retention configuration ops.
const (
	opDeleteRetentionConfiguration    = "DeleteRetentionConfiguration"
	opDescribeRetentionConfigurations = "DescribeRetentionConfigurations"
	opPutRetentionConfiguration       = "PutRetentionConfiguration"
)

// retentionSupportedOps returns the operation names this family handles.
func retentionSupportedOps() []string {
	return []string{
		opPutRetentionConfiguration,
		opDescribeRetentionConfigurations,
		opDeleteRetentionConfiguration,
	}
}

// PutRetentionConfiguration request/response types and handler.
type putRetentionConfigurationInput struct {
	RetentionConfigurationName string `json:"RetentionConfigurationName"`
	RetentionPeriodInDays      int32  `json:"RetentionPeriodInDays"`
}

func (h *Handler) handlePutRetentionConfiguration(
	_ context.Context, in *putRetentionConfigurationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.PutRetentionConfiguration(in.RetentionConfigurationName, in.RetentionPeriodInDays)
}

// DescribeRetentionConfigurations request/response types and handler.
type describeRetentionConfigurationsOutput struct {
	RetentionConfigurations []RetentionConfiguration `json:"RetentionConfigurations"`
}

func (h *Handler) handleDescribeRetentionConfigurations(
	_ context.Context, _ *emptyInput,
) (*describeRetentionConfigurationsOutput, error) {
	return &describeRetentionConfigurationsOutput{
		RetentionConfigurations: h.Backend.DescribeRetentionConfigurations(),
	}, nil
}

// DeleteRetentionConfiguration request/response types and handler.
type deleteRetentionConfigurationInput struct {
	RetentionConfigurationName string `json:"RetentionConfigurationName"`
}

func (h *Handler) handleDeleteRetentionConfiguration(
	_ context.Context, in *deleteRetentionConfigurationInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteRetentionConfiguration(in.RetentionConfigurationName)
}

// buildRetentionDispatch returns dispatch entries for retention configuration ops.
func (h *Handler) buildRetentionDispatch() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opPutRetentionConfiguration:       service.WrapOp(h.handlePutRetentionConfiguration),
		opDescribeRetentionConfigurations: service.WrapOp(h.handleDescribeRetentionConfigurations),
		opDeleteRetentionConfiguration:    service.WrapOp(h.handleDeleteRetentionConfiguration),
	}
}
