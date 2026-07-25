package kinesis

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type jsonGetShardIteratorReq struct {
	Timestamp              *float64 `json:"Timestamp"`
	StreamName             string   `json:"StreamName"`
	StreamARN              string   `json:"StreamARN"`
	ShardID                string   `json:"ShardId"`
	ShardIteratorType      string   `json:"ShardIteratorType"`
	StartingSequenceNumber string   `json:"StartingSequenceNumber"`
}

type jsonGetShardIteratorResp struct {
	ShardIterator string `json:"ShardIterator"`
}

func (h *Handler) handleGetShardIterator(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonGetShardIteratorReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	streamName := req.StreamName
	if streamName == "" && req.StreamARN != "" {
		streamName = streamNameFromARN(req.StreamARN)
	}

	var ts *time.Time
	if req.Timestamp != nil {
		t := time.UnixMilli(int64(*req.Timestamp * millisPerSecond))
		ts = &t
	}

	out, err := h.Backend.GetShardIterator(ctx, &GetShardIteratorInput{
		StreamName:             streamName,
		ShardID:                req.ShardID,
		ShardIteratorType:      req.ShardIteratorType,
		StartingSequenceNumber: req.StartingSequenceNumber,
		Timestamp:              ts,
	})
	if err != nil {
		return nil, err
	}

	return jsonGetShardIteratorResp{
		ShardIterator: out.ShardIterator,
	}, nil
}
