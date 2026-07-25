package dax

import (
	"encoding/json"
	"fmt"
)

type createParameterGroupRequest struct {
	ParameterGroupName string `json:"ParameterGroupName"`
	Description        string `json:"Description"`
}

type describeParameterGroupsRequest struct {
	NextToken           string   `json:"NextToken"`
	ParameterGroupNames []string `json:"ParameterGroupNames"`
	MaxResults          int      `json:"MaxResults"`
}

type updateParameterGroupRequest struct {
	ParameterGroupName  string               `json:"ParameterGroupName"`
	ParameterNameValues []parameterNameValue `json:"ParameterNameValues"`
}

type parameterNameValue struct {
	ParameterName  string `json:"ParameterName"`
	ParameterValue string `json:"ParameterValue"`
}

type deleteParameterGroupRequest struct {
	ParameterGroupName string `json:"ParameterGroupName"`
}

type describeParametersRequest struct {
	ParameterGroupName string `json:"ParameterGroupName"`
	NextToken          string `json:"NextToken"`
	Source             string `json:"Source"`
	MaxResults         int    `json:"MaxResults"`
}

type describeDefaultParametersRequest struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type resetParameterGroupRequest struct {
	ParameterGroupName string   `json:"ParameterGroupName"`
	ParameterNames     []string `json:"ParameterNames"`
}

type parameterGroupResponse struct {
	ParameterGroupName string `json:"ParameterGroupName"`
	Description        string `json:"Description,omitempty"`
}

type parameterResponse struct {
	ParameterName  string `json:"ParameterName"`
	ParameterValue string `json:"ParameterValue"`
	Description    string `json:"Description,omitempty"`
	Source         string `json:"Source,omitempty"`
	DataType       string `json:"DataType,omitempty"`
	IsModifiable   string `json:"IsModifiable,omitempty"`
	ChangeType     string `json:"ChangeType,omitempty"`
	AllowedValues  string `json:"AllowedValues,omitempty"`
	ParameterType  string `json:"ParameterType,omitempty"`
}

// toParameterResponse converts a Parameter to its JSON response form.
func toParameterResponse(p *Parameter) parameterResponse {
	return parameterResponse{
		ParameterName:  p.ParameterName,
		ParameterValue: p.ParameterValue,
		Description:    p.Description,
		Source:         p.Source,
		DataType:       p.DataType,
		IsModifiable:   p.IsModifiable,
		ChangeType:     p.ChangeType,
		AllowedValues:  p.AllowedValues,
		ParameterType:  p.ParameterType,
	}
}

func (h *Handler) handleCreateParameterGroup(body []byte) (any, error) {
	var req createParameterGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	pg, err := h.Backend.CreateParameterGroup(req.ParameterGroupName, req.Description)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		parameterGroupKey: parameterGroupResponse{
			ParameterGroupName: pg.ParameterGroupName,
			Description:        pg.Description,
		},
	}, nil
}

func (h *Handler) handleDescribeParameterGroups(body []byte) (any, error) {
	var req describeParameterGroupsRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	groups, nextToken, err := h.Backend.DescribeParameterGroups(
		req.ParameterGroupNames,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]parameterGroupResponse, 0, len(groups))
	for _, pg := range groups {
		items = append(items, parameterGroupResponse{
			ParameterGroupName: pg.ParameterGroupName,
			Description:        pg.Description,
		})
	}

	result := map[string]any{
		"ParameterGroups": items,
	}

	if nextToken != "" {
		result["NextToken"] = nextToken
	}

	return result, nil
}

func (h *Handler) handleUpdateParameterGroup(body []byte) (any, error) {
	var req updateParameterGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	pvs := make([]ParameterNameValue, 0, len(req.ParameterNameValues))
	for _, pv := range req.ParameterNameValues {
		pvs = append(pvs, ParameterNameValue(pv))
	}

	pg, err := h.Backend.UpdateParameterGroup(UpdateParameterGroupInput{
		ParameterGroupName:  req.ParameterGroupName,
		ParameterNameValues: pvs,
	})
	if err != nil {
		return nil, err
	}

	return map[string]any{
		parameterGroupKey: parameterGroupResponse{
			ParameterGroupName: pg.ParameterGroupName,
			Description:        pg.Description,
		},
	}, nil
}

func (h *Handler) handleDeleteParameterGroup(body []byte) (any, error) {
	var req deleteParameterGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteParameterGroup(req.ParameterGroupName); err != nil {
		return nil, err
	}

	return map[string]any{
		"DeletionMessage": fmt.Sprintf("ParameterGroup %s deleted", req.ParameterGroupName),
	}, nil
}

func (h *Handler) handleDescribeParameters(body []byte) (any, error) {
	var req describeParametersRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	params, nextToken, err := h.Backend.DescribeParameters(
		req.ParameterGroupName,
		req.MaxResults,
		req.NextToken,
		req.Source,
	)
	if err != nil {
		return nil, err
	}

	items := make([]parameterResponse, 0, len(params))
	for _, p := range params {
		items = append(items, toParameterResponse(p))
	}

	result := map[string]any{
		"Parameters": items,
	}

	if nextToken != "" {
		result["NextToken"] = nextToken
	}

	return result, nil
}

func (h *Handler) handleDescribeDefaultParameters(body []byte) (any, error) {
	var req describeDefaultParametersRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	params, nextToken, err := h.Backend.DescribeDefaultParameters(req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	items := make([]parameterResponse, 0, len(params))
	for _, p := range params {
		items = append(items, toParameterResponse(p))
	}

	result := map[string]any{
		"Parameters": items,
	}

	if nextToken != "" {
		result["NextToken"] = nextToken
	}

	return result, nil
}

func (h *Handler) handleResetParameterGroup(body []byte) (any, error) {
	var req resetParameterGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	pg, err := h.Backend.ResetParameterGroup(req.ParameterGroupName, req.ParameterNames)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		parameterGroupKey: parameterGroupResponse{
			ParameterGroupName: pg.ParameterGroupName,
			Description:        pg.Description,
		},
	}, nil
}
