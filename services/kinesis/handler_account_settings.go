package kinesis

import (
	"context"
	"encoding/json"
	"net/http"
)

type describeLimitsOutput struct {
	ShardLimit     int `json:"ShardLimit"`
	OpenShardCount int `json:"OpenShardCount"`
}

type jsonDescribeAccountSettingsResp struct {
	ShardLimit               int `json:"ShardLimit"`
	OnDemandStreamCount      int `json:"OnDemandStreamCount"`
	OnDemandStreamCountLimit int `json:"OnDemandStreamCountLimit"`
}

type jsonUpdateAccountSettingsReq struct {
	OnDemandStreamCountLimit int `json:"OnDemandStreamCountLimit"`
}

type jsonUpdateMaxRecordSizeReq struct {
	StreamName         string `json:"StreamName"`
	StreamARN          string `json:"StreamARN"`
	MaxRecordSizeBytes int    `json:"MaxRecordSizeBytes"`
}

func (h *Handler) handleDescribeLimits(
	ctx context.Context,
	_ *http.Request,
	_ []byte,
) (any, error) {
	return &describeLimitsOutput{
		OpenShardCount: h.Backend.CountOpenShards(ctx),
		ShardLimit:     kinesisDefaultShardLimit,
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

	return jsonDescribeAccountSettingsResp{
		ShardLimit:               out.ShardLimit,
		OnDemandStreamCount:      out.OnDemandStreamCount,
		OnDemandStreamCountLimit: out.OnDemandStreamCountLimit,
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

	if err := h.Backend.UpdateAccountSettings(ctx, &UpdateAccountSettingsInput{
		OnDemandStreamCountLimit: req.OnDemandStreamCountLimit,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
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
		StreamName:         req.StreamName,
		StreamARN:          req.StreamARN,
		MaxRecordSizeBytes: req.MaxRecordSizeBytes,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}
