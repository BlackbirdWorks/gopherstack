package kinesis

import (
	"context"
	"encoding/json"
	"net/http"
)

type jsonMergeShardsReq struct {
	StreamName           string `json:"StreamName"`
	StreamARN            string `json:"StreamARN"`
	ShardToMerge         string `json:"ShardToMerge"`
	AdjacentShardToMerge string `json:"AdjacentShardToMerge"`
}

type jsonSplitShardReq struct {
	StreamName         string `json:"StreamName"`
	StreamARN          string `json:"StreamARN"`
	ShardToSplit       string `json:"ShardToSplit"`
	NewStartingHashKey string `json:"NewStartingHashKey"`
}

type jsonUpdateShardCountReq struct {
	StreamName       string `json:"StreamName"`
	ScalingType      string `json:"ScalingType"`
	TargetShardCount int    `json:"TargetShardCount"`
}

type jsonUpdateShardCountResp struct {
	StreamName        string `json:"StreamName"`
	CurrentShardCount int    `json:"CurrentShardCount"`
	TargetShardCount  int    `json:"TargetShardCount"`
}

func (h *Handler) handleMergeShards(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonMergeShardsReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.MergeShards(ctx, &MergeShardsInput{
		StreamName:           req.StreamName,
		StreamARN:            req.StreamARN,
		ShardToMerge:         req.ShardToMerge,
		AdjacentShardToMerge: req.AdjacentShardToMerge,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleSplitShard(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonSplitShardReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.SplitShard(ctx, &SplitShardInput{
		StreamName:         req.StreamName,
		StreamARN:          req.StreamARN,
		ShardToSplit:       req.ShardToSplit,
		NewStartingHashKey: req.NewStartingHashKey,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleUpdateShardCount(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonUpdateShardCountReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.UpdateShardCount(ctx, &UpdateShardCountInput{
		StreamName:       req.StreamName,
		TargetShardCount: req.TargetShardCount,
		ScalingType:      req.ScalingType,
	})
	if err != nil {
		return nil, err
	}

	return jsonUpdateShardCountResp{
		StreamName:        out.StreamName,
		CurrentShardCount: out.CurrentShardCount,
		TargetShardCount:  out.TargetShardCount,
	}, nil
}
