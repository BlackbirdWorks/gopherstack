package kinesis

import (
	"context"
	"encoding/json"
	"net/http"
)

type jsonEncryptionReq struct {
	StreamName     string `json:"StreamName"`
	StreamARN      string `json:"StreamARN"`
	EncryptionType string `json:"EncryptionType"`
	KeyID          string `json:"KeyId"`
}

func (h *Handler) handleStartStreamEncryption(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonEncryptionReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.StartStreamEncryption(ctx, &StartStreamEncryptionInput{
		StreamName:     req.StreamName,
		StreamARN:      req.StreamARN,
		EncryptionType: req.EncryptionType,
		KeyID:          req.KeyID,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

func (h *Handler) handleStopStreamEncryption(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonEncryptionReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.StopStreamEncryption(ctx, &StopStreamEncryptionInput{
		StreamName:     req.StreamName,
		StreamARN:      req.StreamARN,
		EncryptionType: req.EncryptionType,
		KeyID:          req.KeyID,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}
