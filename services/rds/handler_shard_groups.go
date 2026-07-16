package rds

import (
	"encoding/xml"
	"net/url"
)

type xmlDBShardGroup struct {
	DBShardGroupIdentifier string  `xml:"DBShardGroupIdentifier"`
	DBClusterIdentifier    string  `xml:"DBClusterIdentifier,omitempty"`
	Status                 string  `xml:"Status,omitempty"`
	Endpoint               string  `xml:"Endpoint,omitempty"`
	MaxACU                 float64 `xml:"MaxACU,omitempty"`
	MinACU                 float64 `xml:"MinACU,omitempty"`
	ComputeRedundancy      int     `xml:"ComputeRedundancy,omitempty"`
}

type xmlDBShardGroupList struct {
	Members []xmlDBShardGroup `xml:"DBShardGroup"`
}

// CreateDBShardGroupOutput, DeleteDBShardGroupOutput, ModifyDBShardGroupOutput, and
// RebootDBShardGroupOutput are flat shapes in the real RDS API (no nested
// <DBShardGroup> wrapper — see the comment on createCustomDBEngineVersionResponse for
// why each field below repeats the full result-element chain). DescribeDBShardGroups
// is different: it returns a real list (DBShardGroups []types.DBShardGroup), so
// describeDBShardGroupsResponse below correctly keeps the xmlDBShardGroupList nesting.
type createDBShardGroupResponse struct {
	XMLName                xml.Name `xml:"CreateDBShardGroupResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	DBShardGroupIdentifier string   `xml:"CreateDBShardGroupResult>DBShardGroupIdentifier"`
	DBClusterIdentifier    string   `xml:"CreateDBShardGroupResult>DBClusterIdentifier,omitempty"`
	Status                 string   `xml:"CreateDBShardGroupResult>Status,omitempty"`
	Endpoint               string   `xml:"CreateDBShardGroupResult>Endpoint,omitempty"`
	MaxACU                 float64  `xml:"CreateDBShardGroupResult>MaxACU,omitempty"`
	MinACU                 float64  `xml:"CreateDBShardGroupResult>MinACU,omitempty"`
	ComputeRedundancy      int      `xml:"CreateDBShardGroupResult>ComputeRedundancy,omitempty"`
}

type deleteDBShardGroupResponse struct {
	XMLName                xml.Name `xml:"DeleteDBShardGroupResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	DBShardGroupIdentifier string   `xml:"DeleteDBShardGroupResult>DBShardGroupIdentifier"`
	DBClusterIdentifier    string   `xml:"DeleteDBShardGroupResult>DBClusterIdentifier,omitempty"`
	Status                 string   `xml:"DeleteDBShardGroupResult>Status,omitempty"`
}

type describeDBShardGroupsResponse struct {
	XMLName       xml.Name            `xml:"DescribeDBShardGroupsResponse"`
	Xmlns         string              `xml:"xmlns,attr"`
	Marker        string              `xml:"DescribeDBShardGroupsResult>Marker,omitempty"`
	DBShardGroups xmlDBShardGroupList `xml:"DescribeDBShardGroupsResult>DBShardGroups"`
}

type modifyDBShardGroupResponse struct {
	XMLName                xml.Name `xml:"ModifyDBShardGroupResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	DBShardGroupIdentifier string   `xml:"ModifyDBShardGroupResult>DBShardGroupIdentifier"`
	DBClusterIdentifier    string   `xml:"ModifyDBShardGroupResult>DBClusterIdentifier,omitempty"`
	Status                 string   `xml:"ModifyDBShardGroupResult>Status,omitempty"`
	MaxACU                 float64  `xml:"ModifyDBShardGroupResult>MaxACU,omitempty"`
	ComputeRedundancy      int      `xml:"ModifyDBShardGroupResult>ComputeRedundancy,omitempty"`
}

type rebootDBShardGroupResponse struct {
	XMLName                xml.Name `xml:"RebootDBShardGroupResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	DBShardGroupIdentifier string   `xml:"RebootDBShardGroupResult>DBShardGroupIdentifier"`
	DBClusterIdentifier    string   `xml:"RebootDBShardGroupResult>DBClusterIdentifier,omitempty"`
	Status                 string   `xml:"RebootDBShardGroupResult>Status,omitempty"`
}

func (h *Handler) handleCreateDBShardGroup(vals url.Values) (any, error) {
	id := vals.Get("DBShardGroupIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	maxACU := parseFloat(vals.Get("MaxACU"))
	minACU := parseFloat(vals.Get("MinACU"))
	computeRedundancy := parseInt(vals.Get("ComputeRedundancy"))
	publiclyAccessible := vals.Get("PubliclyAccessible") == "true"

	sg, err := h.Backend.CreateDBShardGroup(id, clusterID, maxACU, minACU, computeRedundancy, publiclyAccessible)
	if err != nil {
		return nil, err
	}

	return &createDBShardGroupResponse{
		Xmlns:                  rdsXMLNS,
		DBShardGroupIdentifier: sg.DBShardGroupIdentifier,
		DBClusterIdentifier:    sg.DBClusterIdentifier,
		Status:                 sg.Status,
		Endpoint:               sg.Endpoint,
		MaxACU:                 sg.MaxACU,
		MinACU:                 sg.MinACU,
		ComputeRedundancy:      sg.ComputeRedundancy,
	}, nil
}

func (h *Handler) handleDeleteDBShardGroup(vals url.Values) (any, error) {
	id := vals.Get("DBShardGroupIdentifier")

	sg, err := h.Backend.DeleteDBShardGroup(id)
	if err != nil {
		return nil, err
	}

	return &deleteDBShardGroupResponse{
		Xmlns:                  rdsXMLNS,
		DBShardGroupIdentifier: sg.DBShardGroupIdentifier,
		DBClusterIdentifier:    sg.DBClusterIdentifier,
		Status:                 sg.Status,
	}, nil
}

func (h *Handler) handleDescribeDBShardGroups(vals url.Values) (any, error) {
	id := vals.Get("DBShardGroupIdentifier")

	groups, err := h.Backend.DescribeDBShardGroups(id)
	if err != nil {
		return nil, err
	}

	members, marker, err := paginateDescribe(
		vals, groups,
		func(a, b DBShardGroup) bool {
			return a.DBShardGroupIdentifier < b.DBShardGroupIdentifier
		},
		func(sg DBShardGroup) xmlDBShardGroup { return toXMLDBShardGroup(&sg) },
	)
	if err != nil {
		return nil, err
	}

	return &describeDBShardGroupsResponse{
		Xmlns:         rdsXMLNS,
		Marker:        marker,
		DBShardGroups: xmlDBShardGroupList{Members: members},
	}, nil
}

func (h *Handler) handleModifyDBShardGroup(vals url.Values) (any, error) {
	id := vals.Get("DBShardGroupIdentifier")
	maxACU := parseFloat(vals.Get("MaxACU"))
	computeRedundancy := parseInt(vals.Get("ComputeRedundancy"))

	sg, err := h.Backend.ModifyDBShardGroup(id, maxACU, computeRedundancy)
	if err != nil {
		return nil, err
	}

	return &modifyDBShardGroupResponse{
		Xmlns:                  rdsXMLNS,
		DBShardGroupIdentifier: sg.DBShardGroupIdentifier,
		DBClusterIdentifier:    sg.DBClusterIdentifier,
		Status:                 sg.Status,
		MaxACU:                 sg.MaxACU,
		ComputeRedundancy:      sg.ComputeRedundancy,
	}, nil
}

func (h *Handler) handleRebootDBShardGroup(vals url.Values) (any, error) {
	id := vals.Get("DBShardGroupIdentifier")

	sg, err := h.Backend.RebootDBShardGroup(id)
	if err != nil {
		return nil, err
	}

	return &rebootDBShardGroupResponse{
		Xmlns:                  rdsXMLNS,
		DBShardGroupIdentifier: sg.DBShardGroupIdentifier,
		DBClusterIdentifier:    sg.DBClusterIdentifier,
		Status:                 sg.Status,
	}, nil
}

func toXMLDBShardGroup(sg *DBShardGroup) xmlDBShardGroup {
	return xmlDBShardGroup{
		DBShardGroupIdentifier: sg.DBShardGroupIdentifier,
		DBClusterIdentifier:    sg.DBClusterIdentifier,
		Status:                 sg.Status,
		Endpoint:               sg.Endpoint,
		MaxACU:                 sg.MaxACU,
		MinACU:                 sg.MinACU,
		ComputeRedundancy:      sg.ComputeRedundancy,
	}
}
