package neptune

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
)

func (h *Handler) handleCreateDBSubnetGroup(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")
	description := vals.Get("DBSubnetGroupDescription")
	vpcID := vals.Get("VpcId")
	subnetIDs := parseSubnetIDMembers(vals)
	sg, err := h.Backend.CreateDBSubnetGroup(ctx, name, description, vpcID, subnetIDs)
	if err != nil {
		return nil, err
	}

	return &createDBSubnetGroupResponse{
		Xmlns:         neptuneXMLNS,
		DBSubnetGroup: toXMLSubnetGroup(sg),
	}, nil
}

func (h *Handler) handleDescribeDBSubnetGroups(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")
	sgs, err := h.Backend.DescribeDBSubnetGroups(ctx, name)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBSubnetGroup, 0, len(sgs))
	for _, sg := range sgs {
		cp := sg
		members = append(members, toXMLSubnetGroup(&cp))
	}

	members, nextMarker := applyNeptuneMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBSubnetGroupsResponse{
		Xmlns: neptuneXMLNS,
		Result: describeDBSubnetGroupsResult{
			DBSubnetGroups: xmlDBSubnetGroupList{Members: members},
			Marker:         nextMarker,
		},
	}, nil
}

func (h *Handler) handleDeleteDBSubnetGroup(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")
	if err := h.Backend.DeleteDBSubnetGroup(ctx, name); err != nil {
		return nil, err
	}

	return &deleteDBSubnetGroupResponse{Xmlns: neptuneXMLNS}, nil
}

func (h *Handler) handleModifyDBSubnetGroup(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")
	description := vals.Get("DBSubnetGroupDescription")
	subnetIDs := parseSubnetIDMembers(vals)
	sg, err := h.Backend.ModifyDBSubnetGroup(ctx, name, description, subnetIDs)
	if err != nil {
		return nil, err
	}

	return &modifyDBSubnetGroupResponse{
		Xmlns:         neptuneXMLNS,
		DBSubnetGroup: toXMLSubnetGroup(sg),
	}, nil
}

func parseSubnetIDMembers(vals url.Values) []string {
	var ids []string
	for i := 1; ; i++ {
		sid := vals.Get(fmt.Sprintf("SubnetIds.member.%d", i))
		if sid == "" {
			return ids
		}
		ids = append(ids, sid)
	}
}

func toXMLSubnetGroup(sg *DBSubnetGroup) xmlDBSubnetGroup {
	subnetMembers := make([]xmlSubnet, 0, len(sg.SubnetIDs))
	for _, id := range sg.SubnetIDs {
		subnetMembers = append(subnetMembers, xmlSubnet{SubnetIdentifier: id})
	}

	return xmlDBSubnetGroup{
		DBSubnetGroupName:        sg.DBSubnetGroupName,
		DBSubnetGroupArn:         sg.DBSubnetGroupArn,
		DBSubnetGroupDescription: sg.DBSubnetGroupDescription,
		VpcID:                    sg.VpcID,
		SubnetGroupStatus:        sg.Status,
		Subnets:                  xmlSubnetList{Members: subnetMembers},
	}
}

type xmlSubnet struct {
	SubnetIdentifier string `xml:"SubnetIdentifier"`
}

type xmlSubnetList struct {
	Members []xmlSubnet `xml:"Subnet"`
}

type xmlDBSubnetGroup struct {
	DBSubnetGroupName        string        `xml:"DBSubnetGroupName"`
	DBSubnetGroupArn         string        `xml:"DBSubnetGroupArn,omitempty"`
	DBSubnetGroupDescription string        `xml:"DBSubnetGroupDescription"`
	VpcID                    string        `xml:"VpcId,omitempty"`
	SubnetGroupStatus        string        `xml:"SubnetGroupStatus"`
	Subnets                  xmlSubnetList `xml:"Subnets"`
}

type xmlDBSubnetGroupList struct {
	Members []xmlDBSubnetGroup `xml:"DBSubnetGroup"`
}

type createDBSubnetGroupResponse struct {
	XMLName       xml.Name         `xml:"CreateDBSubnetGroupResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	DBSubnetGroup xmlDBSubnetGroup `xml:"CreateDBSubnetGroupResult>DBSubnetGroup"`
}

type describeDBSubnetGroupsResult struct {
	Marker         string               `xml:"Marker,omitempty"`
	DBSubnetGroups xmlDBSubnetGroupList `xml:"DBSubnetGroups"`
}

type describeDBSubnetGroupsResponse struct {
	XMLName xml.Name                     `xml:"DescribeDBSubnetGroupsResponse"`
	Xmlns   string                       `xml:"xmlns,attr"`
	Result  describeDBSubnetGroupsResult `xml:"DescribeDBSubnetGroupsResult"`
}

type deleteDBSubnetGroupResponse struct {
	XMLName xml.Name `xml:"DeleteDBSubnetGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type modifyDBSubnetGroupResponse struct {
	XMLName       xml.Name         `xml:"ModifyDBSubnetGroupResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	DBSubnetGroup xmlDBSubnetGroup `xml:"ModifyDBSubnetGroupResult>DBSubnetGroup"`
}

// dispatchSubnetAndClusterParamGroupAction handles DBSubnetGroup and
// DBClusterParameterGroup actions together to keep each switch's cyclomatic
// complexity within lint limits; see dispatch's doc comment for the chaining
// rationale.
func (h *Handler) dispatchSubnetAndClusterParamGroupAction(
	ctx context.Context, action string, vals url.Values,
) (any, error) {
	switch action {
	case "CreateDBSubnetGroup":
		return h.handleCreateDBSubnetGroup(ctx, vals)
	case "DescribeDBSubnetGroups":
		return h.handleDescribeDBSubnetGroups(ctx, vals)
	case "DeleteDBSubnetGroup":
		return h.handleDeleteDBSubnetGroup(ctx, vals)
	case "ModifyDBSubnetGroup":
		return h.handleModifyDBSubnetGroup(ctx, vals)
	case "CreateDBClusterParameterGroup":
		return h.handleCreateDBClusterParameterGroup(ctx, vals)
	case "DescribeDBClusterParameterGroups":
		return h.handleDescribeDBClusterParameterGroups(ctx, vals)
	case "DeleteDBClusterParameterGroup":
		return h.handleDeleteDBClusterParameterGroup(ctx, vals)
	case "ModifyDBClusterParameterGroup":
		return h.handleModifyDBClusterParameterGroup(ctx, vals)
	case "CopyDBClusterParameterGroup":
		return h.handleCopyDBClusterParameterGroup(ctx, vals)
	case "DescribeDBClusterParameters":
		return h.handleDescribeDBClusterParameters(ctx, vals)
	case "ResetDBClusterParameterGroup":
		return h.handleResetDBClusterParameterGroup(ctx, vals)
	case "DescribeEngineDefaultClusterParameters":
		return h.handleDescribeEngineDefaultClusterParameters(ctx, vals)
	default:
		return h.dispatchParameterGroupAction(ctx, action, vals)
	}
}
