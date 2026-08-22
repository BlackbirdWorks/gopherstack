package kinesis

import (
	"context"
	"encoding/json"
	"net/http"
)

type jsonRetentionPeriodReq struct {
	StreamName           string `json:"StreamName"`
	StreamARN            string `json:"StreamARN"`
	RetentionPeriodHours int    `json:"RetentionPeriodHours"`
}

func (h *Handler) handleIncreaseStreamRetentionPeriod(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonRetentionPeriodReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	streamName, ctx := resolveStreamNameAndRegion(ctx, req.StreamName, req.StreamARN, h.defaultRegion())

	if err := h.Backend.IncreaseStreamRetentionPeriod(ctx, &IncreaseStreamRetentionPeriodInput{
		StreamName:           streamName,
		RetentionPeriodHours: req.RetentionPeriodHours,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleDecreaseStreamRetentionPeriod(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonRetentionPeriodReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	streamName, ctx := resolveStreamNameAndRegion(ctx, req.StreamName, req.StreamARN, h.defaultRegion())

	if err := h.Backend.DecreaseStreamRetentionPeriod(ctx, &DecreaseStreamRetentionPeriodInput{
		StreamName:           streamName,
		RetentionPeriodHours: req.RetentionPeriodHours,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}
