package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

// ---- placement groups ----

type placementGroupItem struct {
	GroupName string `xml:"groupName"`
	Strategy  string `xml:"strategy"`
	State     string `xml:"state"`
}

type placementGroupSet struct {
	Items []placementGroupItem `xml:"item"`
}

type describePlacementGroupsResponse struct {
	XMLName           xml.Name          `xml:"DescribePlacementGroupsResponse"`
	Xmlns             string            `xml:"xmlns,attr"`
	RequestID         string            `xml:"requestId"`
	PlacementGroupSet placementGroupSet `xml:"placementGroupSet"`
}

type createPlacementGroupResponse struct {
	XMLName   xml.Name `xml:"CreatePlacementGroupResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type deletePlacementGroupResponse struct {
	XMLName   xml.Name `xml:"DeletePlacementGroupResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleCreatePlacementGroup(vals url.Values, reqID string) (any, error) {
	name := vals.Get("GroupName")
	strategy := vals.Get("Strategy")

	if name == "" {
		return nil, fmt.Errorf("%w: GroupName is required", ErrInvalidParameter)
	}

	if _, err := h.Backend.CreatePlacementGroup(name, strategy); err != nil {
		return nil, err
	}

	return &createPlacementGroupResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribePlacementGroups(vals url.Values, reqID string) (any, error) {
	names := parseMemberList(vals, "GroupName")
	pgs := h.Backend.DescribePlacementGroups(names)

	items := make([]placementGroupItem, 0, len(pgs))
	for _, pg := range pgs {
		items = append(items, placementGroupItem{
			GroupName: pg.Name,
			Strategy:  pg.Strategy,
			State:     pg.State,
		})
	}

	return &describePlacementGroupsResponse{
		Xmlns:             ec2XMLNS,
		RequestID:         reqID,
		PlacementGroupSet: placementGroupSet{Items: items},
	}, nil
}

func (h *Handler) handleDeletePlacementGroup(vals url.Values, reqID string) (any, error) {
	name := vals.Get("GroupName")
	if name == "" {
		return nil, fmt.Errorf("%w: GroupName is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeletePlacementGroup(name); err != nil {
		return nil, err
	}

	return &deletePlacementGroupResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}
