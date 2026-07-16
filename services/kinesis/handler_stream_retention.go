package kinesis

import (
	"context"
	"encoding/json"
	"net/http"
)

type jsonRetentionPeriodReq struct {
	StreamName           string `json:"StreamName"`
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

	if err := h.Backend.IncreaseStreamRetentionPeriod(ctx, &IncreaseStreamRetentionPeriodInput{
		StreamName:           req.StreamName,
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

	if err := h.Backend.DecreaseStreamRetentionPeriod(ctx, &DecreaseStreamRetentionPeriodInput{
		StreamName:           req.StreamName,
		RetentionPeriodHours: req.RetentionPeriodHours,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}
