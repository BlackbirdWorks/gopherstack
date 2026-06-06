package appstream

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// --- AppBlock handlers ---

type createAppBlockInput struct {
	Tags        map[string]string `json:"Tags"`
	Name        string            `json:"Name"`
	Description string            `json:"Description"`
}

func (h *Handler) opCreateAppBlock(_ context.Context, body []byte) (any, error) {
	var req createAppBlockInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	ab, err := h.Backend.CreateAppBlock(req.Name, req.Description, req.Tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{"AppBlock": appBlockToResponse(ab)}, nil
}

type deleteAppBlockInput struct {
	Name string `json:"Name"`
}

func (h *Handler) opDeleteAppBlock(_ context.Context, body []byte) (any, error) {
	var req deleteAppBlockInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DeleteAppBlock(req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type describeAppBlocksInput struct {
	Arns []string `json:"Arns"`
}

func (h *Handler) opDescribeAppBlocks(_ context.Context, body []byte) (any, error) {
	var req describeAppBlocksInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	abs, err := h.Backend.DescribeAppBlocks(req.Arns)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(abs))
	for _, ab := range abs {
		resp = append(resp, appBlockToResponse(ab))
	}

	return map[string]any{"AppBlocks": resp}, nil
}

// --- AppBlockBuilder handlers ---

type createAppBlockBuilderInput struct {
	Tags         map[string]string `json:"Tags"`
	Name         string            `json:"Name"`
	Description  string            `json:"Description"`
	Platform     string            `json:"Platform"`
	InstanceType string            `json:"InstanceType"`
}

func (h *Handler) opCreateAppBlockBuilder(_ context.Context, body []byte) (any, error) {
	var req createAppBlockBuilderInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	bb, err := h.Backend.CreateAppBlockBuilder(req.Name, req.Description, req.Platform, req.InstanceType, req.Tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{"AppBlockBuilder": appBlockBuilderToResponse(bb)}, nil
}

type deleteAppBlockBuilderInput struct {
	Name string `json:"Name"`
}

func (h *Handler) opDeleteAppBlockBuilder(_ context.Context, body []byte) (any, error) {
	var req deleteAppBlockBuilderInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DeleteAppBlockBuilder(req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type describeAppBlockBuildersInput struct {
	Names []string `json:"Names"`
}

func (h *Handler) opDescribeAppBlockBuilders(_ context.Context, body []byte) (any, error) {
	var req describeAppBlockBuildersInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	bbs, err := h.Backend.DescribeAppBlockBuilders(req.Names)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(bbs))
	for _, bb := range bbs {
		resp = append(resp, appBlockBuilderToResponse(bb))
	}

	return map[string]any{"AppBlockBuilders": resp}, nil
}

type appBlockBuilderNameInput struct {
	Name string `json:"Name"`
}

func (h *Handler) opStartAppBlockBuilder(_ context.Context, body []byte) (any, error) {
	var req appBlockBuilderNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.StartAppBlockBuilder(req.Name); err != nil {
		return nil, err
	}

	bbs, err := h.Backend.DescribeAppBlockBuilders([]string{req.Name})
	if err != nil {
		return nil, err
	}

	return map[string]any{"AppBlockBuilder": appBlockBuilderToResponse(bbs[0])}, nil
}

func (h *Handler) opStopAppBlockBuilder(_ context.Context, body []byte) (any, error) {
	var req appBlockBuilderNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.StopAppBlockBuilder(req.Name); err != nil {
		return nil, err
	}

	bbs, err := h.Backend.DescribeAppBlockBuilders([]string{req.Name})
	if err != nil {
		return nil, err
	}

	return map[string]any{"AppBlockBuilder": appBlockBuilderToResponse(bbs[0])}, nil
}

type updateAppBlockBuilderInput struct {
	Name         string `json:"Name"`
	Description  string `json:"Description"`
	InstanceType string `json:"InstanceType"`
}

func (h *Handler) opUpdateAppBlockBuilder(_ context.Context, body []byte) (any, error) {
	var req updateAppBlockBuilderInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	bb, err := h.Backend.UpdateAppBlockBuilder(req.Name, req.Description, req.InstanceType)
	if err != nil {
		return nil, err
	}

	return map[string]any{"AppBlockBuilder": appBlockBuilderToResponse(bb)}, nil
}

type createAppBlockBuilderStreamingURLInput struct {
	AppBlockBuilderName string `json:"AppBlockBuilderName"`
}

func (h *Handler) opCreateAppBlockBuilderStreamingURL(_ context.Context, body []byte) (any, error) {
	var req createAppBlockBuilderStreamingURLInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	url, err := h.Backend.CreateAppBlockBuilderStreamingURL(req.AppBlockBuilderName)
	if err != nil {
		return nil, err
	}

	return map[string]any{"StreamingURL": url}, nil
}

// --- AppBlockBuilder-AppBlock association handlers ---

type appBlockBuilderAppBlockInput struct {
	AppBlockBuilderName string `json:"AppBlockBuilderName"`
	AppBlockArn         string `json:"AppBlockArn"`
}

func (h *Handler) opAssociateAppBlockBuilderAppBlock(_ context.Context, body []byte) (any, error) {
	var req appBlockBuilderAppBlockInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.AssociateAppBlockBuilderAppBlock(req.AppBlockBuilderName, req.AppBlockArn); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) opDisassociateAppBlockBuilderAppBlock(_ context.Context, body []byte) (any, error) {
	var req appBlockBuilderAppBlockInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DisassociateAppBlockBuilderAppBlock(req.AppBlockBuilderName, req.AppBlockArn); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type describeAppBlockBuilderAppBlockAssociationsInput struct {
	AppBlockBuilderName string `json:"AppBlockBuilderName"`
	AppBlockArn         string `json:"AppBlockArn"`
}

func (h *Handler) opDescribeAppBlockBuilderAppBlockAssociations(_ context.Context, body []byte) (any, error) {
	var req describeAppBlockBuilderAppBlockAssociationsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	assocs, err := h.Backend.DescribeAppBlockBuilderAppBlockAssociations(req.AppBlockBuilderName, req.AppBlockArn)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(assocs))
	for _, a := range assocs {
		resp = append(resp, map[string]any{
			"AppBlockBuilderName": a.AppBlockBuilderName,
			"AppBlockArn":         a.AppBlockArn,
			"State":               a.State,
		})
	}

	return map[string]any{"AppBlockBuilderAppBlockAssociations": resp}, nil
}

// --- Response helpers ---

func appBlockToResponse(ab *AppBlock) map[string]any {
	return map[string]any{
		"Name":        ab.Name,
		"Arn":         ab.Arn,
		"Description": ab.Description,
		"State":       ab.State,
		"CreatedTime": ab.CreatedTime.Unix(),
		keyTags:       ab.Tags,
	}
}

func appBlockBuilderToResponse(bb *AppBlockBuilder) map[string]any {
	return map[string]any{
		"Name":         bb.Name,
		"Arn":          bb.Arn,
		"Description":  bb.Description,
		"Platform":     bb.Platform,
		"InstanceType": bb.InstanceType,
		"State":        bb.State,
		"CreatedTime":  bb.CreatedTime.Unix(),
		keyTags:        bb.Tags,
	}
}
