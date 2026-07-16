package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
)

// ---------------------------------------------------------------------------
// Workteam handlers
// ---------------------------------------------------------------------------

// workteamResponseMap builds the AWS wire representation of a Workteam,
// converting timestamps to epoch seconds as required by the aws-json-1.1 protocol.
func workteamResponseMap(w *Workteam) map[string]any {
	resp := map[string]any{
		"WorkteamName":    w.WorkteamName,
		"WorkteamArn":     w.WorkteamArn,
		"Description":     w.Description,
		"CreateDate":      epochSeconds(w.CreateDate),
		"LastUpdatedDate": epochSeconds(w.LastUpdatedDate),
	}

	if w.WorkforceArn != "" {
		resp[keyWorkforceArn] = w.WorkforceArn
	}

	if w.SubDomain != "" {
		resp["SubDomain"] = w.SubDomain
	}

	resp["MemberDefinitions"] = w.MemberDefinitions
	if w.MemberDefinitions == nil {
		resp["MemberDefinitions"] = []MemberDefinition{}
	}

	return resp
}

func (h *Handler) handleCreateWorkteam(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags              []tagObject        `json:"Tags"`
		WorkteamName      string             `json:"WorkteamName"`
		Description       string             `json:"Description"`
		WorkforceName     string             `json:"WorkforceName"`
		MemberDefinitions []MemberDefinition `json:"MemberDefinitions"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkteamName == "" {
		return nil, fmt.Errorf("%w: WorkteamName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateWorkteam(ctx, CreateWorkteamOptions{
		Name:              req.WorkteamName,
		Description:       req.Description,
		WorkforceName:     req.WorkforceName,
		MemberDefinitions: req.MemberDefinitions,
		Tags:              fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"WorkteamArn": result.WorkteamArn})
}

func (h *Handler) handleDescribeWorkteam(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		WorkteamName string `json:"WorkteamName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkteamName == "" {
		return nil, fmt.Errorf("%w: WorkteamName is required", errInvalidRequest)
	}

	result, err := h.Backend.DescribeWorkteam(ctx, req.WorkteamName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Workteam": workteamResponseMap(result)})
}

func (h *Handler) handleUpdateWorkteam(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		WorkteamName      string             `json:"WorkteamName"`
		Description       string             `json:"Description"`
		MemberDefinitions []MemberDefinition `json:"MemberDefinitions"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkteamName == "" {
		return nil, fmt.Errorf("%w: WorkteamName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateWorkteam(ctx, UpdateWorkteamOptions{
		Name:              req.WorkteamName,
		Description:       req.Description,
		MemberDefinitions: req.MemberDefinitions,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Workteam": workteamResponseMap(result)})
}

func (h *Handler) handleDeleteWorkteam(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		WorkteamName string `json:"WorkteamName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkteamName == "" {
		return nil, fmt.Errorf("%w: WorkteamName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteWorkteam(ctx, req.WorkteamName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Success": true})
}

func (h *Handler) handleListWorkteams(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NextToken string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListWorkteams(ctx, req.NextToken)

	summaries := make([]map[string]any, 0, len(items))
	for _, w := range items {
		summaries = append(summaries, workteamResponseMap(w))
	}

	return json.Marshal(map[string]any{
		"Workteams":  summaries,
		keyNextToken: next,
	})
}
