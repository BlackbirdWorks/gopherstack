package dax

import (
	"encoding/json"
	"fmt"
)

type createSubnetGroupRequest struct {
	SubnetGroupName string   `json:"SubnetGroupName"`
	Description     string   `json:"Description"`
	SubnetIDs       []string `json:"SubnetIds"`
}

type describeSubnetGroupsRequest struct {
	NextToken        string   `json:"NextToken"`
	SubnetGroupNames []string `json:"SubnetGroupNames"`
	MaxResults       int      `json:"MaxResults"`
}

type updateSubnetGroupRequest struct {
	SubnetGroupName string   `json:"SubnetGroupName"`
	Description     string   `json:"Description"`
	SubnetIDs       []string `json:"SubnetIds"`
}

type deleteSubnetGroupRequest struct {
	SubnetGroupName string `json:"SubnetGroupName"`
}

type subnetGroupResponse struct {
	SubnetGroupName string       `json:"SubnetGroupName"`
	Description     string       `json:"Description,omitempty"`
	VpcID           string       `json:"VpcId,omitempty"`
	Subnets         []subnetItem `json:"Subnets,omitempty"`
}

type subnetItem struct {
	SubnetIdentifier       string `json:"SubnetIdentifier"`
	SubnetAvailabilityZone string `json:"SubnetAvailabilityZone"`
}

// toSubnetGroupResponse converts a SubnetGroup to its JSON response form.
func toSubnetGroupResponse(sg *SubnetGroup) subnetGroupResponse {
	items := make([]subnetItem, 0, len(sg.Subnets))

	for _, entry := range sg.Subnets {
		item := subnetItem{
			SubnetIdentifier:       entry.SubnetID,
			SubnetAvailabilityZone: entry.AvailabilityZone,
		}

		items = append(items, item)
	}

	return subnetGroupResponse{
		SubnetGroupName: sg.SubnetGroupName,
		Description:     sg.Description,
		VpcID:           sg.VpcID,
		Subnets:         items,
	}
}

func (h *Handler) handleCreateSubnetGroup(body []byte) (any, error) {
	var req createSubnetGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	sg, err := h.Backend.CreateSubnetGroup(req.SubnetGroupName, req.Description, req.SubnetIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"SubnetGroup": toSubnetGroupResponse(sg),
	}, nil
}

func (h *Handler) handleDescribeSubnetGroups(body []byte) (any, error) {
	var req describeSubnetGroupsRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	groups, nextToken, err := h.Backend.DescribeSubnetGroups(
		req.SubnetGroupNames,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]subnetGroupResponse, 0, len(groups))
	for _, sg := range groups {
		items = append(items, toSubnetGroupResponse(sg))
	}

	result := map[string]any{
		"SubnetGroups": items,
	}

	if nextToken != "" {
		result["NextToken"] = nextToken
	}

	return result, nil
}

func (h *Handler) handleUpdateSubnetGroup(body []byte) (any, error) {
	var req updateSubnetGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	sg, err := h.Backend.UpdateSubnetGroup(UpdateSubnetGroupInput(req))
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"SubnetGroup": toSubnetGroupResponse(sg),
	}, nil
}

func (h *Handler) handleDeleteSubnetGroup(body []byte) (any, error) {
	var req deleteSubnetGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteSubnetGroup(req.SubnetGroupName); err != nil {
		return nil, err
	}

	return map[string]any{
		"DeletionMessage": fmt.Sprintf("SubnetGroup %s deleted", req.SubnetGroupName),
	}, nil
}
