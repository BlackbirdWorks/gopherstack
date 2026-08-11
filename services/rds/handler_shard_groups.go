package rds

import (
	"encoding/xml"
	"net/url"
)

type xmlDBShardGroup struct {
	DBShardGroupIdentifier string  `xml:"DBShardGroupIdentifier"`
	DBClusterIdentifier    string  `xml:"DBClusterIdentifier,omitempty"`
	DBShardGroupArn        string  `xml:"DBShardGroupArn,omitempty"`
	DBShardGroupResourceID string  `xml:"DBShardGroupResourceId,omitempty"`
	Status                 string  `xml:"Status,omitempty"`
	Endpoint               string  `xml:"Endpoint,omitempty"`
	MaxACU                 float64 `xml:"MaxACU,omitempty"`
	MinACU                 float64 `xml:"MinACU,omitempty"`
	ComputeRedundancy      int     `xml:"ComputeRedundancy,omitempty"`
	PubliclyAccessible     bool    `xml:"PubliclyAccessible,omitempty"`
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
//
// All four of Create/Delete/Modify/RebootDBShardGroupOutput carry the SAME full
// field set as types.DBShardGroup itself (verified against
// aws-sdk-go-v2/service/rds@v1.116.2's api_op_*DBShardGroup.go output structs) --
// not just the identifier/cluster/status subset a shallower reading of each op's
// "primary" fields might suggest. DBShardGroupArn/DBShardGroupResourceId/
// PubliclyAccessible were previously missing from every one of these four
// responses (not just Create), which is why the response structs below repeat
// the full set on each of them rather than just Create.
type createDBShardGroupResponse struct {
	XMLName                xml.Name `xml:"CreateDBShardGroupResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	DBShardGroupIdentifier string   `xml:"CreateDBShardGroupResult>DBShardGroupIdentifier"`
	DBClusterIdentifier    string   `xml:"CreateDBShardGroupResult>DBClusterIdentifier,omitempty"`
	DBShardGroupArn        string   `xml:"CreateDBShardGroupResult>DBShardGroupArn,omitempty"`
	DBShardGroupResourceID string   `xml:"CreateDBShardGroupResult>DBShardGroupResourceId,omitempty"`
	Status                 string   `xml:"CreateDBShardGroupResult>Status,omitempty"`
	Endpoint               string   `xml:"CreateDBShardGroupResult>Endpoint,omitempty"`
	MaxACU                 float64  `xml:"CreateDBShardGroupResult>MaxACU,omitempty"`
	MinACU                 float64  `xml:"CreateDBShardGroupResult>MinACU,omitempty"`
	ComputeRedundancy      int      `xml:"CreateDBShardGroupResult>ComputeRedundancy,omitempty"`
	PubliclyAccessible     bool     `xml:"CreateDBShardGroupResult>PubliclyAccessible,omitempty"`
}

type deleteDBShardGroupResponse struct {
	XMLName                xml.Name `xml:"DeleteDBShardGroupResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	DBShardGroupIdentifier string   `xml:"DeleteDBShardGroupResult>DBShardGroupIdentifier"`
	DBClusterIdentifier    string   `xml:"DeleteDBShardGroupResult>DBClusterIdentifier,omitempty"`
	DBShardGroupArn        string   `xml:"DeleteDBShardGroupResult>DBShardGroupArn,omitempty"`
	DBShardGroupResourceID string   `xml:"DeleteDBShardGroupResult>DBShardGroupResourceId,omitempty"`
	Status                 string   `xml:"DeleteDBShardGroupResult>Status,omitempty"`
	Endpoint               string   `xml:"DeleteDBShardGroupResult>Endpoint,omitempty"`
	MaxACU                 float64  `xml:"DeleteDBShardGroupResult>MaxACU,omitempty"`
	MinACU                 float64  `xml:"DeleteDBShardGroupResult>MinACU,omitempty"`
	ComputeRedundancy      int      `xml:"DeleteDBShardGroupResult>ComputeRedundancy,omitempty"`
	PubliclyAccessible     bool     `xml:"DeleteDBShardGroupResult>PubliclyAccessible,omitempty"`
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
	DBShardGroupArn        string   `xml:"ModifyDBShardGroupResult>DBShardGroupArn,omitempty"`
	DBShardGroupResourceID string   `xml:"ModifyDBShardGroupResult>DBShardGroupResourceId,omitempty"`
	Status                 string   `xml:"ModifyDBShardGroupResult>Status,omitempty"`
	Endpoint               string   `xml:"ModifyDBShardGroupResult>Endpoint,omitempty"`
	MaxACU                 float64  `xml:"ModifyDBShardGroupResult>MaxACU,omitempty"`
	MinACU                 float64  `xml:"ModifyDBShardGroupResult>MinACU,omitempty"`
	ComputeRedundancy      int      `xml:"ModifyDBShardGroupResult>ComputeRedundancy,omitempty"`
	PubliclyAccessible     bool     `xml:"ModifyDBShardGroupResult>PubliclyAccessible,omitempty"`
}

type rebootDBShardGroupResponse struct {
	XMLName                xml.Name `xml:"RebootDBShardGroupResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	DBShardGroupIdentifier string   `xml:"RebootDBShardGroupResult>DBShardGroupIdentifier"`
	DBClusterIdentifier    string   `xml:"RebootDBShardGroupResult>DBClusterIdentifier,omitempty"`
	DBShardGroupArn        string   `xml:"RebootDBShardGroupResult>DBShardGroupArn,omitempty"`
	DBShardGroupResourceID string   `xml:"RebootDBShardGroupResult>DBShardGroupResourceId,omitempty"`
	Status                 string   `xml:"RebootDBShardGroupResult>Status,omitempty"`
	Endpoint               string   `xml:"RebootDBShardGroupResult>Endpoint,omitempty"`
	MaxACU                 float64  `xml:"RebootDBShardGroupResult>MaxACU,omitempty"`
	MinACU                 float64  `xml:"RebootDBShardGroupResult>MinACU,omitempty"`
	ComputeRedundancy      int      `xml:"RebootDBShardGroupResult>ComputeRedundancy,omitempty"`
	PubliclyAccessible     bool     `xml:"RebootDBShardGroupResult>PubliclyAccessible,omitempty"`
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

	h.applyCreateTags(vals, sg.DBShardGroupArn)

	return &createDBShardGroupResponse{
		Xmlns:                  rdsXMLNS,
		DBShardGroupIdentifier: sg.DBShardGroupIdentifier,
		DBClusterIdentifier:    sg.DBClusterIdentifier,
		DBShardGroupArn:        sg.DBShardGroupArn,
		DBShardGroupResourceID: sg.DBShardGroupResourceID,
		Status:                 sg.Status,
		Endpoint:               sg.Endpoint,
		MaxACU:                 sg.MaxACU,
		MinACU:                 sg.MinACU,
		ComputeRedundancy:      sg.ComputeRedundancy,
		PubliclyAccessible:     sg.PubliclyAccessible,
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
		DBShardGroupArn:        sg.DBShardGroupArn,
		DBShardGroupResourceID: sg.DBShardGroupResourceID,
		Status:                 sg.Status,
		Endpoint:               sg.Endpoint,
		MaxACU:                 sg.MaxACU,
		MinACU:                 sg.MinACU,
		ComputeRedundancy:      sg.ComputeRedundancy,
		PubliclyAccessible:     sg.PubliclyAccessible,
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
		DBShardGroupArn:        sg.DBShardGroupArn,
		DBShardGroupResourceID: sg.DBShardGroupResourceID,
		Status:                 sg.Status,
		Endpoint:               sg.Endpoint,
		MaxACU:                 sg.MaxACU,
		MinACU:                 sg.MinACU,
		ComputeRedundancy:      sg.ComputeRedundancy,
		PubliclyAccessible:     sg.PubliclyAccessible,
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
		DBShardGroupArn:        sg.DBShardGroupArn,
		DBShardGroupResourceID: sg.DBShardGroupResourceID,
		Status:                 sg.Status,
		Endpoint:               sg.Endpoint,
		MaxACU:                 sg.MaxACU,
		MinACU:                 sg.MinACU,
		ComputeRedundancy:      sg.ComputeRedundancy,
		PubliclyAccessible:     sg.PubliclyAccessible,
	}, nil
}

func toXMLDBShardGroup(sg *DBShardGroup) xmlDBShardGroup {
	return xmlDBShardGroup{
		DBShardGroupIdentifier: sg.DBShardGroupIdentifier,
		DBClusterIdentifier:    sg.DBClusterIdentifier,
		DBShardGroupArn:        sg.DBShardGroupArn,
		DBShardGroupResourceID: sg.DBShardGroupResourceID,
		PubliclyAccessible:     sg.PubliclyAccessible,
		Status:                 sg.Status,
		Endpoint:               sg.Endpoint,
		MaxACU:                 sg.MaxACU,
		MinACU:                 sg.MinACU,
		ComputeRedundancy:      sg.ComputeRedundancy,
	}
}
