package apprunner

import (
	"context"
	"fmt"
)

type createAutoScalingConfigurationInput struct {
	AutoScalingConfigurationName string     `json:"AutoScalingConfigurationName"`
	Tags                         []tagInput `json:"Tags"`
	MaxConcurrency               int32      `json:"MaxConcurrency"`
	MaxSize                      int32      `json:"MaxSize"`
	MinSize                      int32      `json:"MinSize"`
}

type autoScalingConfigurationOutput struct {
	DeletedAt                        *int64 `json:"DeletedAt,omitempty"`
	AutoScalingConfigurationArn      string `json:"AutoScalingConfigurationArn"`
	AutoScalingConfigurationName     string `json:"AutoScalingConfigurationName"`
	Status                           string `json:"Status"`
	CreatedAt                        int64  `json:"CreatedAt"`
	AutoScalingConfigurationRevision int32  `json:"AutoScalingConfigurationRevision"`
	MaxConcurrency                   int32  `json:"MaxConcurrency"`
	MaxSize                          int32  `json:"MaxSize"`
	MinSize                          int32  `json:"MinSize"`
	IsDefault                        bool   `json:"IsDefault"`
	HasAssociatedService             bool   `json:"HasAssociatedService"`
	Latest                           bool   `json:"Latest"`
}

type createAutoScalingConfigurationOutput struct {
	AutoScalingConfiguration autoScalingConfigurationOutput `json:"AutoScalingConfiguration"`
}

func toAutoScalingConfigurationOutput(cfg *AutoScalingConfiguration) autoScalingConfigurationOutput {
	out := autoScalingConfigurationOutput{
		AutoScalingConfigurationArn:      cfg.AutoScalingConfigurationArn,
		AutoScalingConfigurationName:     cfg.AutoScalingConfigurationName,
		AutoScalingConfigurationRevision: cfg.AutoScalingConfigurationRevision,
		Status:                           cfg.Status,
		MaxConcurrency:                   cfg.MaxConcurrency,
		MaxSize:                          cfg.MaxSize,
		MinSize:                          cfg.MinSize,
		IsDefault:                        cfg.IsDefault,
		HasAssociatedService:             cfg.HasAssociatedService,
		Latest:                           cfg.Latest,
		CreatedAt:                        cfg.CreatedAt.Unix(),
	}

	if !cfg.DeletedAt.IsZero() {
		deletedAt := cfg.DeletedAt.Unix()
		out.DeletedAt = &deletedAt
	}

	return out
}

func (h *Handler) handleCreateAutoScalingConfiguration(
	_ context.Context,
	in *createAutoScalingConfigurationInput,
) (*createAutoScalingConfigurationOutput, error) {
	if in.AutoScalingConfigurationName == "" {
		return nil, fmt.Errorf("%w: AutoScalingConfigurationName is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)
	cfg, err := h.Backend.CreateAutoScalingConfiguration(
		in.AutoScalingConfigurationName,
		in.MaxConcurrency, in.MaxSize, in.MinSize,
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createAutoScalingConfigurationOutput{
		AutoScalingConfiguration: toAutoScalingConfigurationOutput(cfg),
	}, nil
}

type describeAutoScalingConfigurationInput struct {
	AutoScalingConfigurationArn string `json:"AutoScalingConfigurationArn"`
}

type describeAutoScalingConfigurationOutput struct {
	AutoScalingConfiguration autoScalingConfigurationOutput `json:"AutoScalingConfiguration"`
}

func (h *Handler) handleDescribeAutoScalingConfiguration(
	_ context.Context,
	in *describeAutoScalingConfigurationInput,
) (*describeAutoScalingConfigurationOutput, error) {
	if in.AutoScalingConfigurationArn == "" {
		return nil, fmt.Errorf("%w: AutoScalingConfigurationArn is required", errInvalidRequest)
	}

	cfg, err := h.Backend.DescribeAutoScalingConfiguration(in.AutoScalingConfigurationArn)
	if err != nil {
		return nil, err
	}

	return &describeAutoScalingConfigurationOutput{
		AutoScalingConfiguration: toAutoScalingConfigurationOutput(cfg),
	}, nil
}

type deleteAutoScalingConfigurationInput struct {
	AutoScalingConfigurationArn string `json:"AutoScalingConfigurationArn"`
}

type deleteAutoScalingConfigurationOutput struct {
	AutoScalingConfiguration autoScalingConfigurationOutput `json:"AutoScalingConfiguration"`
}

func (h *Handler) handleDeleteAutoScalingConfiguration(
	_ context.Context,
	in *deleteAutoScalingConfigurationInput,
) (*deleteAutoScalingConfigurationOutput, error) {
	if in.AutoScalingConfigurationArn == "" {
		return nil, fmt.Errorf("%w: AutoScalingConfigurationArn is required", errInvalidRequest)
	}

	cfg, err := h.Backend.DeleteAutoScalingConfiguration(in.AutoScalingConfigurationArn)
	if err != nil {
		return nil, err
	}

	return &deleteAutoScalingConfigurationOutput{
		AutoScalingConfiguration: toAutoScalingConfigurationOutput(cfg),
	}, nil
}

type autoScalingConfigurationSummaryOutput struct {
	AutoScalingConfigurationArn      string `json:"AutoScalingConfigurationArn"`
	AutoScalingConfigurationName     string `json:"AutoScalingConfigurationName"`
	Status                           string `json:"Status"`
	AutoScalingConfigurationRevision int32  `json:"AutoScalingConfigurationRevision"`
	IsDefault                        bool   `json:"IsDefault"`
	HasAssociatedService             bool   `json:"HasAssociatedService"`
	CreatedAt                        int64  `json:"CreatedAt"`
}

type listAutoScalingConfigurationsInput struct {
	// *bool, not bool: LatestOnly's doc (aws-sdk-go-v2/service/apprunner@
	// v1.42.4 api_op_ListAutoScalingConfigurations.go) says "Default: true",
	// and the SDK's own serializer omits the key from the wire whenever the
	// Go value is false (serializers.go: `if v.LatestOnly { ... }`), so an
	// omitted key must resolve to true, not to Go's bool zero value.
	LatestOnly                   *bool  `json:"LatestOnly,omitempty"`
	AutoScalingConfigurationName string `json:"AutoScalingConfigurationName"`
	NextToken                    string `json:"NextToken"`
	MaxResults                   int32  `json:"MaxResults"`
}

type listAutoScalingConfigurationsOutput struct {
	NextToken                           string                                  `json:"NextToken,omitempty"`
	AutoScalingConfigurationSummaryList []autoScalingConfigurationSummaryOutput `json:"AutoScalingConfigurationSummaryList"` //nolint:lll // existing issue.
}

func (h *Handler) handleListAutoScalingConfigurations(
	_ context.Context,
	in *listAutoScalingConfigurationsInput,
) (*listAutoScalingConfigurationsOutput, error) {
	cfgs, nextToken, err := h.Backend.ListAutoScalingConfigurations(
		in.AutoScalingConfigurationName,
		in.LatestOnly == nil || *in.LatestOnly,
		in.MaxResults,
		in.NextToken,
	)
	if err != nil {
		return nil, err
	}

	out := make([]autoScalingConfigurationSummaryOutput, 0, len(cfgs))
	for _, c := range cfgs {
		out = append(out, autoScalingConfigurationSummaryOutput{
			AutoScalingConfigurationArn:      c.AutoScalingConfigurationArn,
			AutoScalingConfigurationName:     c.AutoScalingConfigurationName,
			AutoScalingConfigurationRevision: c.AutoScalingConfigurationRevision,
			Status:                           c.Status,
			IsDefault:                        c.IsDefault,
			HasAssociatedService:             c.HasAssociatedService,
			CreatedAt:                        c.CreatedAt.Unix(),
		})
	}

	return &listAutoScalingConfigurationsOutput{
		AutoScalingConfigurationSummaryList: out,
		NextToken:                           nextToken,
	}, nil
}

type updateDefaultAutoScalingConfigurationInput struct {
	AutoScalingConfigurationArn string `json:"AutoScalingConfigurationArn"`
}

type updateDefaultAutoScalingConfigurationOutput struct {
	AutoScalingConfiguration autoScalingConfigurationOutput `json:"AutoScalingConfiguration"`
}

func (h *Handler) handleUpdateDefaultAutoScalingConfiguration(
	_ context.Context,
	in *updateDefaultAutoScalingConfigurationInput,
) (*updateDefaultAutoScalingConfigurationOutput, error) {
	if in.AutoScalingConfigurationArn == "" {
		return nil, fmt.Errorf("%w: AutoScalingConfigurationArn is required", errInvalidRequest)
	}

	cfg, err := h.Backend.UpdateDefaultAutoScalingConfiguration(in.AutoScalingConfigurationArn)
	if err != nil {
		return nil, err
	}

	return &updateDefaultAutoScalingConfigurationOutput{
		AutoScalingConfiguration: toAutoScalingConfigurationOutput(cfg),
	}, nil
}

type listServicesForAutoScalingConfigurationInput struct {
	AutoScalingConfigurationArn string `json:"AutoScalingConfigurationArn"`
	NextToken                   string `json:"NextToken"`
	MaxResults                  int32  `json:"MaxResults"`
}

type listServicesForAutoScalingConfigurationOutput struct {
	NextToken      string   `json:"NextToken,omitempty"`
	ServiceArnList []string `json:"ServiceArnList"`
}

func (h *Handler) handleListServicesForAutoScalingConfiguration(
	_ context.Context,
	in *listServicesForAutoScalingConfigurationInput,
) (*listServicesForAutoScalingConfigurationOutput, error) {
	if in.AutoScalingConfigurationArn == "" {
		return nil, fmt.Errorf("%w: AutoScalingConfigurationArn is required", errInvalidRequest)
	}

	arns, nextToken, err := h.Backend.ListServicesForAutoScalingConfiguration(
		in.AutoScalingConfigurationArn,
		in.MaxResults,
		in.NextToken,
	)
	if err != nil {
		return nil, err
	}

	if arns == nil {
		arns = []string{}
	}

	return &listServicesForAutoScalingConfigurationOutput{
		ServiceArnList: arns,
		NextToken:      nextToken,
	}, nil
}
