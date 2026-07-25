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

	if in.InputID == "" {
		return nil, errInputID
	}

	if in.InputProcessingConfiguration == nil {
		return nil, fmt.Errorf("%w: InputProcessingConfiguration is required", ErrValidation)
	}

	cfg, err := convertInputProcessingConfig(in.InputProcessingConfiguration)
	if err != nil {
		return nil, err
	}

	if addErr := h.Backend.AddApplicationInputProcessingConfiguration(
		ctx, in.ApplicationName, in.CurrentApplicationVersionID, in.InputID, cfg,
	); addErr != nil {
		return nil, addErr
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

// handleDiscoverInputSchema validates the request the way real AWS does (exactly one of a
// streaming source or an S3Configuration, with the corresponding role/location fields all
// present) and, once the request is well-formed, returns a fixed-shape schema.
//
// NOTE: real schema inference by sampling a live Kinesis/Firehose stream or S3 object is out
// of scope -- this emulator has no data plane to read from and no SQL/type-inference engine.
// The response shape matches the real AWS wire format (InputSchema/ParsedInputRecords/
// ProcessedInputRecords/RawInputRecords) so SDK consumers parse it without error, but its
// content is a fixed synthetic sample rather than a real inference result. What IS real here
// is the request validation: a malformed request (no source, both sources, or a source missing
// its required role/location sub-fields) is rejected with InvalidArgumentException exactly as
// real AWS would reject it, instead of always synthesizing a "successful" response regardless
// of input.
func (h *Handler) handleDiscoverInputSchema(
	_ context.Context,
	in *discoverInputSchemaInput,
) (*discoverInputSchemaOutput, error) {
	if err := validateDiscoverInputSchemaInput(in); err != nil {
		return nil, err
	}

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

// validateDiscoverInputSchemaInput enforces the same "exactly one source" contract real AWS
// enforces for DiscoverInputSchema: a streaming source (ResourceARN + RoleARN) or an S3
// source (S3Configuration, whose BucketARN/FileKey/RoleARN are all required members per
// aws-sdk-go-v2/service/kinesisanalytics/types.S3Configuration) -- never neither, never both.
func validateDiscoverInputSchemaInput(in *discoverInputSchemaInput) error {
	hasStreamingSource := in.ResourceARN != ""
	hasS3Source := in.S3Configuration != nil

	switch {
	case hasStreamingSource && hasS3Source:
		return fmt.Errorf(
			"%w: exactly one of ResourceARN or S3Configuration must be specified", ErrValidation,
		)
	case hasStreamingSource:
		if in.RoleARN == "" {
			return fmt.Errorf("%w: RoleARN is required when ResourceARN is specified", ErrValidation)
		}
	case hasS3Source:
		if in.S3Configuration.BucketARN == "" || in.S3Configuration.FileKey == "" || in.S3Configuration.RoleARN == "" {
			return fmt.Errorf(
				"%w: S3Configuration.BucketARN, FileKey, and RoleARN are all required", ErrValidation,
			)
		}
	default:
		return fmt.Errorf("%w: one of ResourceARN or S3Configuration must be specified", ErrValidation)
	}

	// InputProcessingConfiguration is optional here, but when supplied it carries the same
	// required-field contract as everywhere else it appears (validateInputProcessingConfiguration).
	if _, err := convertInputProcessingConfig(in.InputProcessingConfiguration); err != nil {
		return err
	}

	return nil
}
