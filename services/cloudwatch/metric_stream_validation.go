package cloudwatch

import (
	"fmt"
	"strings"
)

// knownMetricStreamOutputFormats are the three documented values for
// PutMetricStreamInput.OutputFormat (types.MetricStreamOutputFormat in
// aws-sdk-go-v2/service/cloudwatch): "json", "opentelemetry0.7", "opentelemetry1.0".
//
//nolint:gochecknoglobals // read-only lookup table, mirrors a fixed AWS enum
var knownMetricStreamOutputFormats = map[string]bool{
	"json":             true,
	"opentelemetry0.7": true,
	"opentelemetry1.0": true,
}

// validateMetricStream validates a PutMetricStream request against the
// PutMetricStreamInput model (aws-sdk-go-v2/service/cloudwatch): Name,
// FirehoseArn, RoleArn, and OutputFormat are all documented as required (the
// model marks them "This member is required" even for updates -- PutMetricStream
// is a full-replace PUT, not a partial-patch, so every field is required on
// every call, create or update), OutputFormat must be one of the three
// documented values, and IncludeFilters/ExcludeFilters are mutually exclusive
// ("You cannot include ExcludeFilters and IncludeFilters in the same operation").
func validateMetricStream(stream *MetricStream) error {
	if strings.TrimSpace(stream.Name) == "" {
		return fmt.Errorf("%w: Name parameter is required for metric stream", ErrValidation)
	}

	if strings.TrimSpace(stream.FirehoseArn) == "" {
		return fmt.Errorf("%w: FirehoseArn parameter is required", ErrValidation)
	}

	if strings.TrimSpace(stream.RoleArn) == "" {
		return fmt.Errorf("%w: RoleArn parameter is required", ErrValidation)
	}

	if strings.TrimSpace(stream.OutputFormat) == "" {
		return fmt.Errorf("%w: OutputFormat parameter is required", ErrValidation)
	}

	if !knownMetricStreamOutputFormats[stream.OutputFormat] {
		return fmt.Errorf(
			"%w: OutputFormat must be one of: json, opentelemetry0.7, opentelemetry1.0",
			ErrValidation,
		)
	}

	if len(stream.IncludeFilters) > 0 && len(stream.ExcludeFilters) > 0 {
		return fmt.Errorf(
			"%w: IncludeFilters and ExcludeFilters cannot both be specified",
			ErrValidation,
		)
	}

	return nil
}
