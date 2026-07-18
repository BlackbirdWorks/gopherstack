package shield

import (
	"encoding/json"
	"fmt"
)

// createProtectionGroupRequest is the request body for CreateProtectionGroup.
type createProtectionGroupRequest struct {
	ProtectionGroupID string   `json:"ProtectionGroupId"`
	Aggregation       string   `json:"Aggregation"`
	Pattern           string   `json:"Pattern"`
	ResourceType      string   `json:"ResourceType"`
	Members           []string `json:"Members"`
}

func (h *Handler) handleCreateProtectionGroup(body []byte) error {
	var req createProtectionGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionGroupID == "" {
		return fmt.Errorf("%w: ProtectionGroupId is required", errInvalidRequest)
	}

	if req.Aggregation == "" {
		return fmt.Errorf("%w: Aggregation is required", errInvalidRequest)
	}

	if req.Pattern == "" {
		return fmt.Errorf("%w: Pattern is required", errInvalidRequest)
	}

	_, err := h.Backend.CreateProtectionGroup(
		req.ProtectionGroupID, req.Aggregation, req.Pattern, req.ResourceType, req.Members,
	)

	return err
}

// describeProtectionGroupRequest is the request body for DescribeProtectionGroup.
type describeProtectionGroupRequest struct {
	ProtectionGroupID string `json:"ProtectionGroupId"`
}

func (h *Handler) handleDescribeProtectionGroup(body []byte) ([]byte, error) {
	var req describeProtectionGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionGroupID == "" {
		return nil, fmt.Errorf("%w: ProtectionGroupId is required", errInvalidRequest)
	}

	pg, err := h.Backend.DescribeProtectionGroup(req.ProtectionGroupID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"ProtectionGroup": protectionGroupToMap(pg),
	})
}

// listProtectionGroupsRequest is the request body for ListProtectionGroups.
type listProtectionGroupsRequest struct {
	InclusionFilters *struct {
		ProtectionGroupIDs []string `json:"ProtectionGroupIds"`
		Patterns           []string `json:"Patterns"`
		ResourceTypes      []string `json:"ResourceTypes"`
		Aggregations       []string `json:"Aggregations"`
	} `json:"InclusionFilters,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

func (h *Handler) handleListProtectionGroups(body []byte) ([]byte, error) {
	var req listProtectionGroupsRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	groups := h.Backend.ListProtectionGroups()

	if f := req.InclusionFilters; f != nil {
		groups = applyProtectionGroupFilters(
			groups,
			sliceToSet(f.ProtectionGroupIDs),
			sliceToSet(f.Patterns),
			sliceToSet(f.ResourceTypes),
			sliceToSet(f.Aggregations),
		)
	}

	maxResults := clampMaxResults(req.MaxResults, maxProtectionGroupsPerPage)

	start, err := decodeOffsetToken(req.NextToken)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", errInvalidRequest, err.Error())
	}

	if start >= len(groups) {
		return json.Marshal(map[string]any{"ProtectionGroups": []map[string]any{}})
	}

	end := start + maxResults

	var nextToken string

	if end < len(groups) {
		nextToken = encodeOffsetToken(end)
		groups = groups[start:end]
	} else {
		groups = groups[start:]
	}

	items := make([]map[string]any, 0, len(groups))

	for _, pg := range groups {
		items = append(items, protectionGroupToMap(pg))
	}

	resp := map[string]any{"ProtectionGroups": items}

	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// protectionGroupMatchesFilters returns true if pg passes all filter sets.
func protectionGroupMatchesFilters(
	pg *ProtectionGroup,
	idSet, patternSet, typeSet, aggSet map[string]struct{},
) bool {
	if len(idSet) > 0 {
		if _, ok := idSet[pg.ID]; !ok {
			return false
		}
	}

	if len(patternSet) > 0 {
		if _, ok := patternSet[pg.Pattern]; !ok {
			return false
		}
	}

	if len(typeSet) > 0 {
		if _, ok := typeSet[pg.ResourceType]; !ok {
			return false
		}
	}

	if len(aggSet) > 0 {
		if _, ok := aggSet[pg.Aggregation]; !ok {
			return false
		}
	}

	return true
}

// applyProtectionGroupFilters filters protection groups by the given inclusion filter sets.
func applyProtectionGroupFilters(
	groups []*ProtectionGroup,
	idSet, patternSet, typeSet, aggSet map[string]struct{},
) []*ProtectionGroup {
	if len(idSet) == 0 && len(patternSet) == 0 && len(typeSet) == 0 && len(aggSet) == 0 {
		return groups
	}

	out := groups[:0]

	for _, pg := range groups {
		if protectionGroupMatchesFilters(pg, idSet, patternSet, typeSet, aggSet) {
			out = append(out, pg)
		}
	}

	return out
}

// updateProtectionGroupRequest is the request body for UpdateProtectionGroup.
type updateProtectionGroupRequest struct {
	ProtectionGroupID string   `json:"ProtectionGroupId"`
	Aggregation       string   `json:"Aggregation"`
	Pattern           string   `json:"Pattern"`
	ResourceType      string   `json:"ResourceType"`
	Members           []string `json:"Members"`
}

func (h *Handler) handleUpdateProtectionGroup(body []byte) error {
	var req updateProtectionGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionGroupID == "" {
		return fmt.Errorf("%w: ProtectionGroupId is required", errInvalidRequest)
	}

	if req.Aggregation == "" {
		return fmt.Errorf("%w: Aggregation is required", errInvalidRequest)
	}

	if req.Pattern == "" {
		return fmt.Errorf("%w: Pattern is required", errInvalidRequest)
	}

	return h.Backend.UpdateProtectionGroup(
		req.ProtectionGroupID, req.Aggregation, req.Pattern, req.ResourceType, req.Members,
	)
}

func protectionGroupToMap(pg *ProtectionGroup) map[string]any {
	members := pg.Members
	if members == nil {
		members = []string{}
	}

	return map[string]any{
		"ProtectionGroupId":  pg.ID,
		"ProtectionGroupArn": pg.ProtectionGroupArn,
		"Aggregation":        pg.Aggregation,
		"Pattern":            pg.Pattern,
		"ResourceType":       pg.ResourceType,
		"Members":            members,
		"CreationTime":       floatSeconds(pg.CreationTime),
	}
}

// deleteProtectionGroupRequest is the request body for DeleteProtectionGroup.
type deleteProtectionGroupRequest struct {
	ProtectionGroupID string `json:"ProtectionGroupId"`
}

func (h *Handler) handleDeleteProtectionGroup(body []byte) error {
	var req deleteProtectionGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionGroupID == "" {
		return fmt.Errorf("%w: ProtectionGroupId is required", errInvalidRequest)
	}

	return h.Backend.DeleteProtectionGroup(req.ProtectionGroupID)
}

// listResourcesInProtectionGroupRequest is the request body for ListResourcesInProtectionGroup.
type listResourcesInProtectionGroupRequest struct {
	ProtectionGroupID string `json:"ProtectionGroupId"`
}

func (h *Handler) handleListResourcesInProtectionGroup(body []byte) ([]byte, error) {
	var req listResourcesInProtectionGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionGroupID == "" {
		return nil, fmt.Errorf("%w: ProtectionGroupId is required", errInvalidRequest)
	}

	arns, err := h.Backend.ListResourcesInProtectionGroup(req.ProtectionGroupID)
	if err != nil {
		return nil, err
	}

	if arns == nil {
		arns = []string{}
	}

	return json.Marshal(map[string]any{
		"ResourceArns": arns,
	})
}
