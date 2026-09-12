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

// defaultRetentionConfigurationName is the only name real AWS Config ever
// assigns a retention configuration: PutRetentionConfigurationInput has no
// name field at all (api_op_PutRetentionConfiguration.go) -- the API always
// names the (singleton, per-region) object "default".
const defaultRetentionConfigurationName = "default"

// PutRetentionConfiguration request/response types and handler. Real
// PutRetentionConfigurationInput carries no name field (verified above);
// this previously required a fabricated "RetentionConfigurationName" wire
// key that no real client ever sends, so every real PutRetentionConfiguration
// call failed with InvalidParameterValueException regardless of state.
type putRetentionConfigurationInput struct {
	RetentionPeriodInDays int32 `json:"RetentionPeriodInDays"`
}

type putRetentionConfigurationOutput struct {
	RetentionConfiguration *RetentionConfiguration `json:"RetentionConfiguration,omitempty"`
}

func (h *Handler) handlePutRetentionConfiguration(
	_ context.Context, in *putRetentionConfigurationInput,
) (*putRetentionConfigurationOutput, error) {
	err := h.Backend.PutRetentionConfiguration(
		defaultRetentionConfigurationName,
		in.RetentionPeriodInDays,
	)
	if err != nil {
		return nil, err
	}

	return &putRetentionConfigurationOutput{
		RetentionConfiguration: &RetentionConfiguration{
			Name:                  defaultRetentionConfigurationName,
			RetentionPeriodInDays: in.RetentionPeriodInDays,
		},
	}, nil
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
