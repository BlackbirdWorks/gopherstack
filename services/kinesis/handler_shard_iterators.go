package kinesis

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type jsonGetShardIteratorReq struct {
	StreamName             string  `json:"StreamName"`
	StreamARN              string  `json:"StreamARN"`
	ShardID                string  `json:"ShardId"`
	ShardIteratorType      string  `json:"ShardIteratorType"`
	StartingSequenceNumber string  `json:"StartingSequenceNumber"`
	Timestamp              float64 `json:"Timestamp"`
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

	out, err := h.Backend.GetShardIterator(ctx, &GetShardIteratorInput{
		StreamName:             streamName,
		ShardID:                req.ShardID,
		ShardIteratorType:      req.ShardIteratorType,
		StartingSequenceNumber: req.StartingSequenceNumber,
		Timestamp:              time.UnixMilli(int64(req.Timestamp * millisPerSecond)),
	})
	if err != nil {
		return nil, err
	}

	return jsonGetShardIteratorResp{
		ShardIterator: out.ShardIterator,
	}, nil
}
