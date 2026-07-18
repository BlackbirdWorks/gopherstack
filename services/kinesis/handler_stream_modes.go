package kinesis

import (
	"context"
	"encoding/json"
	"net/http"
)

type jsonUpdateStreamWarmThroughputReq struct {
	StreamName         string `json:"StreamName"`
	StreamARN          string `json:"StreamARN"`
	WriteCapacityUnits int64  `json:"WriteCapacityUnits"`
	ReadCapacityUnits  int64  `json:"ReadCapacityUnits"`
}

func (h *Handler) handleUpdateStreamMode(ctx context.Context, _ *http.Request, body []byte) (any, error) {
	var req struct {
		StreamModeDetails *jsonStreamModeDetails `json:"StreamModeDetails"`
		StreamARN         string                 `json:"StreamARN"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}
	if req.StreamModeDetails == nil {
		return nil, ErrInvalidArgument
	}

	return struct{}{}, h.Backend.UpdateStreamMode(ctx, &UpdateStreamModeInput{
		StreamARN:         req.StreamARN,
		StreamModeDetails: StreamModeDetails{StreamMode: req.StreamModeDetails.StreamMode},
	})
}

func (h *Handler) handleUpdateStreamWarmThroughput(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonUpdateStreamWarmThroughputReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.UpdateStreamWarmThroughput(ctx, &UpdateStreamWarmThroughputInput{
		StreamName:         req.StreamName,
		StreamARN:          req.StreamARN,
		WriteCapacityUnits: req.WriteCapacityUnits,
		ReadCapacityUnits:  req.ReadCapacityUnits,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}
