package apprunner

import (
	"context"
	"fmt"
)

type traceConfigurationInput struct {
	Vendor string `json:"Vendor"`
}

type createObservabilityConfigurationInput struct {
	TraceConfiguration             *traceConfigurationInput `json:"TraceConfiguration"`
	ObservabilityConfigurationName string                   `json:"ObservabilityConfigurationName"`
	Tags                           []tagInput               `json:"Tags"`
}

type traceConfigurationOutput struct {
	Vendor string `json:"Vendor"`
}

type observabilityConfigurationOutput struct {
	TraceConfiguration                 *traceConfigurationOutput `json:"TraceConfiguration,omitempty"`
	ObservabilityConfigurationArn      string                    `json:"ObservabilityConfigurationArn"`
	ObservabilityConfigurationName     string                    `json:"ObservabilityConfigurationName"`
	Status                             string                    `json:"Status"`
	ObservabilityConfigurationRevision int32                     `json:"ObservabilityConfigurationRevision"`
	Latest                             bool                      `json:"Latest"`
	CreatedAt                          int64                     `json:"CreatedAt"`
}

type createObservabilityConfigurationOutput struct {
	ObservabilityConfiguration observabilityConfigurationOutput `json:"ObservabilityConfiguration"`
}

// toObservabilityConfigurationOutput builds the wire shape for an
// ObservabilityConfiguration. TraceConfiguration is only included when a
// vendor was actually configured -- real AWS: "If not specified, tracing
// isn't enabled" (types.go, ObservabilityConfiguration.TraceConfiguration
// doc comment), so an unconfigured configuration correctly omits it rather
// than fabricating a vendor.
func toObservabilityConfigurationOutput(cfg *ObservabilityConfiguration) observabilityConfigurationOutput {
	out := observabilityConfigurationOutput{
		ObservabilityConfigurationArn:      cfg.ObservabilityConfigurationArn,
		ObservabilityConfigurationName:     cfg.ObservabilityConfigurationName,
		ObservabilityConfigurationRevision: cfg.ObservabilityConfigurationRevision,
		Status:                             cfg.Status,
		Latest:                             cfg.Latest,
		CreatedAt:                          cfg.CreatedAt.Unix(),
	}

	if cfg.TracingVendor != "" {
		out.TraceConfiguration = &traceConfigurationOutput{Vendor: cfg.TracingVendor}
	}

	return out
}

func (h *Handler) handleCreateObservabilityConfiguration(
	_ context.Context,
	in *createObservabilityConfigurationInput,
) (*createObservabilityConfigurationOutput, error) {
	if in.ObservabilityConfigurationName == "" {
		return nil, fmt.Errorf("%w: ObservabilityConfigurationName is required", errInvalidRequest)
	}

	vendor := ""
	if in.TraceConfiguration != nil {
		vendor = in.TraceConfiguration.Vendor
	}

	tags := tagsFromInput(in.Tags)
	cfg, err := h.Backend.CreateObservabilityConfiguration(in.ObservabilityConfigurationName, vendor, tags)
	if err != nil {
		return nil, err
	}

	return &createObservabilityConfigurationOutput{
		ObservabilityConfiguration: toObservabilityConfigurationOutput(cfg),
	}, nil
}

type describeObservabilityConfigurationInput struct {
	ObservabilityConfigurationArn string `json:"ObservabilityConfigurationArn"`
}

type describeObservabilityConfigurationOutput struct {
	ObservabilityConfiguration observabilityConfigurationOutput `json:"ObservabilityConfiguration"`
}

func (h *Handler) handleDescribeObservabilityConfiguration(
	_ context.Context,
	in *describeObservabilityConfigurationInput,
) (*describeObservabilityConfigurationOutput, error) {
	if in.ObservabilityConfigurationArn == "" {
		return nil, fmt.Errorf("%w: ObservabilityConfigurationArn is required", errInvalidRequest)
	}

	cfg, err := h.Backend.DescribeObservabilityConfiguration(in.ObservabilityConfigurationArn)
	if err != nil {
		return nil, err
	}

	return &describeObservabilityConfigurationOutput{
		ObservabilityConfiguration: toObservabilityConfigurationOutput(cfg),
	}, nil
}

type deleteObservabilityConfigurationInput struct {
	ObservabilityConfigurationArn string `json:"ObservabilityConfigurationArn"`
}

type deleteObservabilityConfigurationOutput struct {
	ObservabilityConfiguration observabilityConfigurationOutput `json:"ObservabilityConfiguration"`
}

func (h *Handler) handleDeleteObservabilityConfiguration(
	_ context.Context,
	in *deleteObservabilityConfigurationInput,
) (*deleteObservabilityConfigurationOutput, error) {
	if in.ObservabilityConfigurationArn == "" {
		return nil, fmt.Errorf("%w: ObservabilityConfigurationArn is required", errInvalidRequest)
	}

	cfg, err := h.Backend.DeleteObservabilityConfiguration(in.ObservabilityConfigurationArn)
	if err != nil {
		return nil, err
	}

	return &deleteObservabilityConfigurationOutput{
		ObservabilityConfiguration: toObservabilityConfigurationOutput(cfg),
	}, nil
}

type listObservabilityConfigurationsInput struct {
	// *bool, not bool: LatestOnly's doc (aws-sdk-go-v2/service/apprunner@
	// v1.42.4 api_op_ListObservabilityConfigurations.go) says "Default: true",
	// and the SDK's own serializer omits the key from the wire whenever the
	// Go value is false (serializers.go: `if v.LatestOnly { ... }`), so an
	// omitted key must resolve to true, not to Go's bool zero value.
	LatestOnly                     *bool  `json:"LatestOnly,omitempty"`
	ObservabilityConfigurationName string `json:"ObservabilityConfigurationName"`
	NextToken                      string `json:"NextToken"`
	MaxResults                     int32  `json:"MaxResults"`
}

// observabilityConfigurationSummaryOutput mirrors types.ObservabilityConfigurationSummary,
// which is narrower than types.ObservabilityConfiguration: it has no Status, Latest, or
// CreatedAt (see deserializers.go:6215 in the pinned SDK — those three keys have no case
// in the summary's own document deserializer and would be silently dropped by a real
// client).
type observabilityConfigurationSummaryOutput struct {
	ObservabilityConfigurationArn      string `json:"ObservabilityConfigurationArn"`
	ObservabilityConfigurationName     string `json:"ObservabilityConfigurationName"`
	ObservabilityConfigurationRevision int32  `json:"ObservabilityConfigurationRevision"`
}

type listObservabilityConfigurationsOutput struct {
	NextToken                             string                                    `json:"NextToken,omitempty"`
	ObservabilityConfigurationSummaryList []observabilityConfigurationSummaryOutput `json:"ObservabilityConfigurationSummaryList"` //nolint:lll // existing issue.
}

func (h *Handler) handleListObservabilityConfigurations(
	_ context.Context,
	in *listObservabilityConfigurationsInput,
) (*listObservabilityConfigurationsOutput, error) {
	cfgs, nextToken, err := h.Backend.ListObservabilityConfigurations(
		in.ObservabilityConfigurationName,
		in.LatestOnly == nil || *in.LatestOnly,
		in.MaxResults,
		in.NextToken,
	)
	if err != nil {
		return nil, err
	}

	out := make([]observabilityConfigurationSummaryOutput, 0, len(cfgs))
	for _, c := range cfgs {
		out = append(out, observabilityConfigurationSummaryOutput{
			ObservabilityConfigurationArn:      c.ObservabilityConfigurationArn,
			ObservabilityConfigurationName:     c.ObservabilityConfigurationName,
			ObservabilityConfigurationRevision: c.ObservabilityConfigurationRevision,
		})
	}

	return &listObservabilityConfigurationsOutput{
		ObservabilityConfigurationSummaryList: out,
		NextToken:                             nextToken,
	}, nil
}
