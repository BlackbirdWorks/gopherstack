package rekognition

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) streamProcessorOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateStreamProcessor":   service.WrapOp(h.handleCreateStreamProcessor),
		"DeleteStreamProcessor":   service.WrapOp(h.handleDeleteStreamProcessor),
		"DescribeStreamProcessor": service.WrapOp(h.handleDescribeStreamProcessor),
		"ListStreamProcessors":    service.WrapOp(h.handleListStreamProcessors),
		"StartStreamProcessor":    service.WrapOp(h.handleStartStreamProcessor),
		"StopStreamProcessor":     service.WrapOp(h.handleStopStreamProcessor),
		"UpdateStreamProcessor":   service.WrapOp(h.handleUpdateStreamProcessor),
	}
}

// --- Stream processor requests ---

type createStreamProcessorReq struct {
	Tags    map[string]string `json:"Tags"`
	Name    string            `json:"Name"`
	RoleArn string            `json:"RoleArn"`
}

type createStreamProcessorResp struct {
	StreamProcessorArn string `json:"StreamProcessorArn"`
}

func (h *Handler) handleCreateStreamProcessor(
	_ context.Context,
	req *createStreamProcessorReq,
) (*createStreamProcessorResp, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	proc, err := h.Backend.CreateStreamProcessor(req.Name, req.RoleArn, req.Tags)
	if err != nil {
		return nil, err
	}

	return &createStreamProcessorResp{StreamProcessorArn: proc.StreamProcessorARN}, nil
}

type deleteStreamProcessorReq struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteStreamProcessor(_ context.Context, req *deleteStreamProcessorReq) (*struct{}, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if err := h.Backend.DeleteStreamProcessor(req.Name); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeStreamProcessorReq struct {
	Name string `json:"Name"`
}

type describeStreamProcessorResp struct {
	Name               string  `json:"Name"`
	RoleArn            string  `json:"RoleArn"`
	Status             string  `json:"Status"`
	StreamProcessorArn string  `json:"StreamProcessorArn"`
	CreationTimestamp  float64 `json:"CreationTimestamp"`
}

func (h *Handler) handleDescribeStreamProcessor(
	_ context.Context,
	req *describeStreamProcessorReq,
) (*describeStreamProcessorResp, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	proc, err := h.Backend.DescribeStreamProcessor(req.Name)
	if err != nil {
		return nil, err
	}

	return &describeStreamProcessorResp{
		CreationTimestamp:  float64(proc.CreationTimestamp.Unix()),
		Name:               proc.Name,
		RoleArn:            proc.RoleARN,
		Status:             proc.Status,
		StreamProcessorArn: proc.StreamProcessorARN,
	}, nil
}

type listStreamProcessorsReq struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type streamProcessorEntry struct {
	Name   string `json:"Name"`
	Status string `json:"Status"`
}

type listStreamProcessorsResp struct {
	NextToken        string                 `json:"NextToken,omitempty"`
	StreamProcessors []streamProcessorEntry `json:"StreamProcessors"`
}

func (h *Handler) handleListStreamProcessors(
	_ context.Context,
	req *listStreamProcessorsReq,
) (*listStreamProcessorsResp, error) {
	procs, nextToken, err := h.Backend.ListStreamProcessors(req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	entries := make([]streamProcessorEntry, 0, len(procs))

	for _, p := range procs {
		entries = append(entries, streamProcessorEntry{
			Name:   p.Name,
			Status: p.Status,
		})
	}

	return &listStreamProcessorsResp{
		NextToken:        nextToken,
		StreamProcessors: entries,
	}, nil
}

type streamProcessorNameReq struct {
	Name string `json:"Name"`
}

func (h *Handler) handleStartStreamProcessor(_ context.Context, req *streamProcessorNameReq) (*struct{}, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if err := h.Backend.StartStreamProcessor(req.Name); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleStopStreamProcessor(_ context.Context, req *streamProcessorNameReq) (*struct{}, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if err := h.Backend.StopStreamProcessor(req.Name); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type updateStreamProcessorReq struct {
	Name string `json:"Name"`
}

func (h *Handler) handleUpdateStreamProcessor(_ context.Context, req *updateStreamProcessorReq) (*struct{}, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	if err := h.Backend.UpdateStreamProcessor(req.Name); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}
