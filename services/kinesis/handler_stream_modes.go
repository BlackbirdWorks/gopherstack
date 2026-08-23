package kinesis

import (
	"context"
	"encoding/json"
	"net/http"
)

// jsonUpdateStreamWarmThroughputReq mirrors UpdateStreamWarmThroughputInput
// (api_op_UpdateStreamWarmThroughput.go:63-70). WarmThroughputMiBps is its
// only required member.
type jsonUpdateStreamWarmThroughputReq struct {
	StreamName          string `json:"StreamName"`
	StreamARN           string `json:"StreamARN"`
	WarmThroughputMiBps int    `json:"WarmThroughputMiBps"`
}

// jsonWarmThroughputObject mirrors types.WarmThroughputObject.
type jsonWarmThroughputObject struct {
	CurrentMiBps int `json:"CurrentMiBps"`
	TargetMiBps  int `json:"TargetMiBps"`
}

// jsonUpdateStreamWarmThroughputResp mirrors UpdateStreamWarmThroughputOutput
// (api_op_UpdateStreamWarmThroughput.go:76-88).
type jsonUpdateStreamWarmThroughputResp struct {
	StreamARN      string                   `json:"StreamARN"`
	StreamName     string                   `json:"StreamName"`
	WarmThroughput jsonWarmThroughputObject `json:"WarmThroughput"`
}

func (h *Handler) handleUpdateStreamMode(ctx context.Context, _ *http.Request, body []byte) (any, error) {
	var req struct {
		StreamModeDetails   *jsonStreamModeDetails `json:"StreamModeDetails"`
		StreamARN           string                 `json:"StreamARN"`
		WarmThroughputMiBps int                    `json:"WarmThroughputMiBps,omitempty"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}
	if req.StreamModeDetails == nil {
		return nil, ErrInvalidArgument
	}

	return struct{}{}, h.Backend.UpdateStreamMode(ctx, &UpdateStreamModeInput{
		StreamARN:           req.StreamARN,
		StreamModeDetails:   StreamModeDetails{StreamMode: req.StreamModeDetails.StreamMode},
		WarmThroughputMiBps: req.WarmThroughputMiBps,
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

	out, err := h.Backend.UpdateStreamWarmThroughput(ctx, &UpdateStreamWarmThroughputInput{
		StreamName:          req.StreamName,
		StreamARN:           req.StreamARN,
		WarmThroughputMiBps: req.WarmThroughputMiBps,
	})
	if err != nil {
		return nil, err
	}

	return jsonUpdateStreamWarmThroughputResp{
		StreamARN:  out.StreamARN,
		StreamName: out.StreamName,
		WarmThroughput: jsonWarmThroughputObject{
			CurrentMiBps: out.WarmThroughput.CurrentMiBps,
			TargetMiBps:  out.WarmThroughput.TargetMiBps,
		},
	}, nil
}
