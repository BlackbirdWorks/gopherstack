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

	if w.NotificationConfiguration != nil {
		resp["NotificationConfiguration"] = w.NotificationConfiguration
	}

	if w.WorkerAccessConfiguration != nil {
		resp["WorkerAccessConfiguration"] = w.WorkerAccessConfiguration
	}

	resp["MemberDefinitions"] = w.MemberDefinitions
	if w.MemberDefinitions == nil {
		resp["MemberDefinitions"] = []MemberDefinition{}
	}

	return resp
}

// createWorkteamInput mirrors CreateWorkteamInput (api_op_CreateWorkteam.go:35-83).
type createWorkteamInput struct {
	NotificationConfiguration *NotificationConfiguration `json:"NotificationConfiguration"`
	WorkerAccessConfiguration *WorkerAccessConfiguration `json:"WorkerAccessConfiguration"`
	Tags                      []tagObject                `json:"Tags"`
	WorkteamName              string                     `json:"WorkteamName"`
	Description               string                     `json:"Description"`
	WorkforceName             string                     `json:"WorkforceName"`
	MemberDefinitions         []MemberDefinition         `json:"MemberDefinitions"`
}

func (h *Handler) handleCreateWorkteam(ctx context.Context, body []byte) ([]byte, error) {
	var req createWorkteamInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkteamName == "" {
		return nil, fmt.Errorf("%w: WorkteamName is required", errInvalidRequest)
	}

	result, err := h.Backend.CreateWorkteam(ctx, CreateWorkteamOptions{
		Name:                      req.WorkteamName,
		Description:               req.Description,
		WorkforceName:             req.WorkforceName,
		MemberDefinitions:         req.MemberDefinitions,
		Tags:                      fromTagObjects(req.Tags),
		NotificationConfiguration: req.NotificationConfiguration,
		WorkerAccessConfiguration: req.WorkerAccessConfiguration,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"WorkteamArn": result.WorkteamArn})
}

// describeWorkteamInput mirrors DescribeWorkteamInput (api_op_DescribeWorkteam.go:31-36).
type describeWorkteamInput struct {
	WorkteamName string `json:"WorkteamName"`
}

func (h *Handler) handleDescribeWorkteam(ctx context.Context, body []byte) ([]byte, error) {
	var req describeWorkteamInput

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

// updateWorkteamInput mirrors UpdateWorkteamInput (api_op_UpdateWorkteam.go:28-70).
type updateWorkteamInput struct {
	NotificationConfiguration *NotificationConfiguration `json:"NotificationConfiguration"`
	WorkerAccessConfiguration *WorkerAccessConfiguration `json:"WorkerAccessConfiguration"`
	WorkteamName              string                     `json:"WorkteamName"`
	Description               string                     `json:"Description"`
	MemberDefinitions         []MemberDefinition         `json:"MemberDefinitions"`
}

func (h *Handler) handleUpdateWorkteam(ctx context.Context, body []byte) ([]byte, error) {
	var req updateWorkteamInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.WorkteamName == "" {
		return nil, fmt.Errorf("%w: WorkteamName is required", errInvalidRequest)
	}

	result, err := h.Backend.UpdateWorkteam(ctx, UpdateWorkteamOptions{
		Name:                      req.WorkteamName,
		Description:               req.Description,
		MemberDefinitions:         req.MemberDefinitions,
		NotificationConfiguration: req.NotificationConfiguration,
		WorkerAccessConfiguration: req.WorkerAccessConfiguration,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Workteam": workteamResponseMap(result)})
}

// deleteWorkteamInput mirrors DeleteWorkteamInput (api_op_DeleteWorkteam.go:27-32).
type deleteWorkteamInput struct {
	WorkteamName string `json:"WorkteamName"`
}

func (h *Handler) handleDeleteWorkteam(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteWorkteamInput

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

// listWorkteamsInput mirrors ListWorkteamsInput (api_op_ListWorkteams.go:31-51).
type listWorkteamsInput struct {
	NextToken    string `json:"NextToken"`
	NameContains string `json:"NameContains"`
	SortBy       string `json:"SortBy"`
	SortOrder    string `json:"SortOrder"`
	MaxResults   int32  `json:"MaxResults"`
}

func (h *Handler) handleListWorkteams(ctx context.Context, body []byte) ([]byte, error) {
	var req listWorkteamsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	items, next := h.Backend.ListWorkteams(ctx, req.NextToken, ListWorkteamsFilter{
		NameContains: req.NameContains,
		SortBy:       req.SortBy,
		SortOrder:    req.SortOrder,
		MaxResults:   req.MaxResults,
	})

	summaries := make([]map[string]any, 0, len(items))
	for _, w := range items {
		summaries = append(summaries, workteamResponseMap(w))
	}

	resp := map[string]any{"Workteams": summaries}
	if next != "" {
		resp[keyNextToken] = next
	}

	return json.Marshal(resp)
}
