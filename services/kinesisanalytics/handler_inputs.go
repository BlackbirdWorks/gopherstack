package kinesisanalytics

import (
	"context"
	"fmt"
)

func (h *Handler) handleAddApplicationInput(
	ctx context.Context,
	in *addApplicationInputInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	if in.Input == nil {
		return nil, fmt.Errorf("%w: Input is required", ErrValidation)
	}

	desc, err := convertInputConfig(in.Input)
	if err != nil {
		return nil, err
	}

	if addErr := h.Backend.AddApplicationInput(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, desc,
	); addErr != nil {
		return nil, addErr
	}

	return &struct{}{}, nil
}

func (h *Handler) handleAddApplicationInputProcessingConfiguration(
	ctx context.Context,
	in *addApplicationInputProcessingConfigurationInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	var cfg *InputProcessingConfigurationDesc
	if in.InputProcessingConfiguration != nil && in.InputProcessingConfiguration.InputLambdaProcessor != nil {
		cfg = &InputProcessingConfigurationDesc{
			InputLambdaProcessor: &LambdaProcessorDesc{
				ResourceARN: in.InputProcessingConfiguration.InputLambdaProcessor.ResourceARN,
				RoleARN:     in.InputProcessingConfiguration.InputLambdaProcessor.RoleARN,
			},
		}
	}

	if err := h.Backend.AddApplicationInputProcessingConfiguration(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, in.InputID, cfg,
	); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleDeleteApplicationInputProcessingConfiguration(
	ctx context.Context,
	in *deleteApplicationInputProcessingConfigurationInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	if in.InputID == "" {
		return nil, errInputID
	}

	if err := h.Backend.DeleteApplicationInputProcessingConfiguration(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, in.InputID,
	); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// handleDiscoverInputSchema returns a minimal stub schema.
// NOTE: Full schema inference from live Kinesis/Firehose/S3 streams is out of scope;
// no SQL engine is available and cross-service ARN sampling is not implemented.
// The stub response matches the AWS wire shape so SDK consumers can parse without errors.
func (h *Handler) handleDiscoverInputSchema(
	_ context.Context,
	_ *discoverInputSchemaInput,
) (*discoverInputSchemaOutput, error) {
	return &discoverInputSchemaOutput{
		InputSchema: &SourceSchema{
			RecordFormat: RecordFormat{
				RecordFormatType: recordFormatJSON,
				MappingParameters: &MappingParameters{
					JSONMappingParameters: &JSONMappingParameters{RecordRowPath: "$"},
				},
			},
			RecordColumns: []RecordColumn{
				{Name: "COL_1", SQLType: "VARCHAR(4)"},
			},
		},
		ParsedInputRecords:    [][]string{{"value1"}},
		ProcessedInputRecords: []string{`{"COL_1":"value1"}`},
		RawInputRecords:       []string{`{"COL_1":"value1"}`},
	}, nil
}
