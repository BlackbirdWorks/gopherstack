package xray

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type insightsConfigView struct {
	InsightsEnabled      bool `json:"InsightsEnabled"`
	NotificationsEnabled bool `json:"NotificationsEnabled"`
}

type groupView struct {
	GroupARN              string             `json:"GroupARN"`
	GroupName             string             `json:"GroupName"`
	FilterExpression      string             `json:"FilterExpression"`
	InsightsConfiguration insightsConfigView `json:"InsightsConfiguration"`
}

func toGroupView(g *Group) groupView {
	return groupView{
		GroupARN:         g.GroupARN,
		GroupName:        g.GroupName,
		FilterExpression: g.FilterExpression,
		InsightsConfiguration: insightsConfigView{
			InsightsEnabled:      g.InsightsConfiguration.InsightsEnabled,
			NotificationsEnabled: g.InsightsConfiguration.NotificationsEnabled,
		},
	}
}

type createGroupInput struct {
	GroupName             string             `json:"GroupName"`
	FilterExpression      string             `json:"FilterExpression"`
	InsightsConfiguration insightsConfigView `json:"InsightsConfiguration"`
}

func (h *Handler) handleCreateGroup(_ context.Context, body []byte) ([]byte, error) {
	var in createGroupInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.GroupName == "" {
		return nil, fmt.Errorf("%w: GroupName is required", errInvalidRequest)
	}

	// Validate: NotificationsEnabled=true requires InsightsEnabled=true.
	if in.InsightsConfiguration.NotificationsEnabled && !in.InsightsConfiguration.InsightsEnabled {
		return nil, fmt.Errorf("%w: NotificationsEnabled requires InsightsEnabled to be true", errInvalidRequest)
	}

	ic := InsightsConfiguration{
		InsightsEnabled:      in.InsightsConfiguration.InsightsEnabled,
		NotificationsEnabled: in.InsightsConfiguration.NotificationsEnabled,
	}

	g, err := h.Backend.CreateGroupWithInsights(in.GroupName, in.FilterExpression, ic)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyGroup: toGroupView(g),
	})
}

type getGroupInput struct {
	GroupName string `json:"GroupName"`
	GroupARN  string `json:"GroupARN"`
}

func (h *Handler) handleGetGroup(_ context.Context, body []byte) ([]byte, error) {
	var in getGroupInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.GroupName == "" && in.GroupARN == "" {
		return nil, fmt.Errorf("%w: GroupName or GroupARN is required", errInvalidRequest)
	}

	var (
		g   *Group
		err error
	)

	if in.GroupARN != "" {
		g, err = h.Backend.GetGroupByARN(in.GroupARN)
	} else {
		g, err = h.Backend.GetGroup(in.GroupName)
	}

	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyGroup: toGroupView(g),
	})
}

type getGroupsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

func (h *Handler) handleGetGroups(_ context.Context, body []byte) ([]byte, error) {
	var in getGroupsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	groups := h.Backend.GetGroups()
	views := make([]groupView, 0, len(groups))

	for i := range groups {
		views = append(views, toGroupView(&groups[i]))
	}

	pg := page.New(views, in.NextToken, int(in.MaxResults), defaultGroupsPageSize)
	resp := map[string]any{
		"Groups":     pg.Data,
		keyNextToken: pg.Next,
	}

	return json.Marshal(resp)
}

// updateGroupInput uses pointer fields for FilterExpression and InsightsConfiguration
// so an omitted field can be distinguished from an explicit zero value: the real
// UpdateGroupInput models both as independently optional, and unlike CreateGroup, an
// UpdateGroup call that only wants to flip InsightsConfiguration must not silently wipe
// out an existing FilterExpression (and vice versa).
type updateGroupInput struct {
	FilterExpression      *string             `json:"FilterExpression"`
	InsightsConfiguration *insightsConfigView `json:"InsightsConfiguration"`
	GroupName             string              `json:"GroupName"`
	GroupARN              string              `json:"GroupARN"`
}

func (h *Handler) handleUpdateGroup(_ context.Context, body []byte) ([]byte, error) {
	var in updateGroupInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.GroupName == "" && in.GroupARN == "" {
		return nil, fmt.Errorf("%w: GroupName is required", errInvalidRequest)
	}

	var insights *InsightsConfiguration

	if in.InsightsConfiguration != nil {
		if in.InsightsConfiguration.NotificationsEnabled && !in.InsightsConfiguration.InsightsEnabled {
			return nil, fmt.Errorf("%w: NotificationsEnabled requires InsightsEnabled to be true", errInvalidRequest)
		}

		insights = &InsightsConfiguration{
			InsightsEnabled:      in.InsightsConfiguration.InsightsEnabled,
			NotificationsEnabled: in.InsightsConfiguration.NotificationsEnabled,
		}
	}

	g, err := h.Backend.UpdateGroupByARN(in.GroupName, in.GroupARN, in.FilterExpression, insights)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyGroup: toGroupView(g),
	})
}

type deleteGroupInput struct {
	GroupName string `json:"GroupName"`
	GroupARN  string `json:"GroupARN"`
}

func (h *Handler) handleDeleteGroup(_ context.Context, body []byte) ([]byte, error) {
	var in deleteGroupInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
	}

	if in.GroupName == "" && in.GroupARN == "" {
		return nil, fmt.Errorf("%w: GroupName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteGroupByARN(in.GroupName, in.GroupARN); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}
