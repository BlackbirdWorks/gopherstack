package kinesis

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// describeLimitsOutput mirrors kinesis@v1.46.4 DescribeLimitsOutput
// (api_op_DescribeLimits.go:34-51): all four members are required.
type describeLimitsOutput struct {
	ShardLimit               int `json:"ShardLimit"`
	OpenShardCount           int `json:"OpenShardCount"`
	OnDemandStreamCount      int `json:"OnDemandStreamCount"`
	OnDemandStreamCountLimit int `json:"OnDemandStreamCountLimit"`
}

// jsonMTBCommitmentInput mirrors
// types.MinimumThroughputBillingCommitmentInput (Status is its only,
// required, member).
type jsonMTBCommitmentInput struct {
	Status string `json:"Status"`
}

// jsonMTBCommitmentOutput mirrors
// types.MinimumThroughputBillingCommitmentOutput. Timestamps are epoch-seconds
// (JSON-protocol unixTimestamp format, confirmed against
// deserializers.go:6758-6790) and omitted when unset.
type jsonMTBCommitmentOutput struct {
	Status               string  `json:"Status"`
	EarliestAllowedEndAt float64 `json:"EarliestAllowedEndAt,omitempty"`
	EndedAt              float64 `json:"EndedAt,omitempty"`
	StartedAt            float64 `json:"StartedAt,omitempty"`
}

func minimumThroughputBillingCommitmentToWire(
	c MinimumThroughputBillingCommitmentOutput,
) jsonMTBCommitmentOutput {
	return jsonMTBCommitmentOutput{
		Status:               c.Status,
		EarliestAllowedEndAt: awstime.Epoch(c.EarliestAllowedEndAt),
		EndedAt:              awstime.Epoch(c.EndedAt),
		StartedAt:            awstime.Epoch(c.StartedAt),
	}
}

// jsonAccountSettingsResp is the wire shape shared by DescribeAccountSettings
// and UpdateAccountSettings (kinesis@v1.46.4 api_op_DescribeAccountSettings.go:34-45,
// api_op_UpdateAccountSettings.go:42-51 -- both Outputs have the single member
// MinimumThroughputBillingCommitment).
type jsonAccountSettingsResp struct {
	MinimumThroughputBillingCommitment jsonMTBCommitmentOutput `json:"MinimumThroughputBillingCommitment"`
}

// jsonUpdateAccountSettingsReq mirrors UpdateAccountSettingsInput
// (api_op_UpdateAccountSettings.go:42-51): MinimumThroughputBillingCommitment
// is its only, required, member.
type jsonUpdateAccountSettingsReq struct {
	MinimumThroughputBillingCommitment *jsonMTBCommitmentInput `json:"MinimumThroughputBillingCommitment"`
}

// jsonUpdateMaxRecordSizeReq mirrors UpdateMaxRecordSizeInput
// (api_op_UpdateMaxRecordSize.go:30-47). The wire field is MaxRecordSizeInKiB,
// not MaxRecordSizeBytes, its unit is KiB, and there is no StreamName member
// (only StreamARN, plus StreamId, reserved for future use and not modeled here).
type jsonUpdateMaxRecordSizeReq struct {
	StreamARN          string `json:"StreamARN"`
	MaxRecordSizeInKiB int    `json:"MaxRecordSizeInKiB"`
}

func (h *Handler) handleDescribeLimits(
	ctx context.Context,
	_ *http.Request,
	_ []byte,
) (any, error) {
	return &describeLimitsOutput{
		OpenShardCount:           h.Backend.CountOpenShards(ctx),
		ShardLimit:               kinesisDefaultShardLimit,
		OnDemandStreamCount:      h.Backend.CountOnDemandStreams(ctx),
		OnDemandStreamCountLimit: h.Backend.OnDemandStreamCountLimit(ctx),
	}, nil
}

func (h *Handler) handleDescribeAccountSettings(
	ctx context.Context,
	_ *http.Request,
	_ []byte,
) (any, error) {
	out, err := h.Backend.DescribeAccountSettings(ctx)
	if err != nil {
		return nil, err
	}

	return jsonAccountSettingsResp{
		MinimumThroughputBillingCommitment: minimumThroughputBillingCommitmentToWire(
			out.MinimumThroughputBillingCommitment,
		),
	}, nil
}

func (h *Handler) handleUpdateAccountSettings(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonUpdateAccountSettingsReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	var commitment *MinimumThroughputBillingCommitmentInput
	if req.MinimumThroughputBillingCommitment != nil {
		commitment = &MinimumThroughputBillingCommitmentInput{
			Status: req.MinimumThroughputBillingCommitment.Status,
		}
	}

	out, err := h.Backend.UpdateAccountSettings(ctx, &UpdateAccountSettingsInput{
		MinimumThroughputBillingCommitment: commitment,
	})
	if err != nil {
		return nil, err
	}

	return jsonAccountSettingsResp{
		MinimumThroughputBillingCommitment: minimumThroughputBillingCommitmentToWire(
			out.MinimumThroughputBillingCommitment,
		),
	}, nil
}

func (h *Handler) handleUpdateMaxRecordSize(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonUpdateMaxRecordSizeReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.UpdateMaxRecordSize(ctx, &UpdateMaxRecordSizeInput{
		StreamARN:          req.StreamARN,
		MaxRecordSizeInKiB: req.MaxRecordSizeInKiB,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}
