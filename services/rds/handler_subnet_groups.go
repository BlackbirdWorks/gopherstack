package rds

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

func (h *Handler) handleCreateDBSubnetGroup(vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")
	description := vals.Get("DBSubnetGroupDescription")
	vpcID := vals.Get("VpcId")
	subnetIDs := parseSubnetIDMembers(vals)

	sg, err := h.Backend.CreateDBSubnetGroup(name, description, vpcID, subnetIDs)
	if err != nil {
		return nil, err
	}

	h.applyCreateTags(vals, sg.DBSubnetGroupArn)

	return &createDBSubnetGroupResponse{
		Xmlns:         rdsXMLNS,
		DBSubnetGroup: toXMLSubnetGroup(sg),
	}, nil
}

func (h *Handler) handleDescribeDBSubnetGroups(vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")

	sgs, err := h.Backend.DescribeDBSubnetGroups(name)
	if err != nil {
		return nil, err
	}
	members, marker, err := paginateDescribe(vals, sgs, func(a, b DBSubnetGroup) bool {
		return a.DBSubnetGroupName < b.DBSubnetGroupName
	}, func(item DBSubnetGroup) xmlDBSubnetGroup {
		cp := item

		return toXMLSubnetGroup(&cp)
	})
	if err != nil {
		return nil, err
	}

	return &describeDBSubnetGroupsResponse{
		Xmlns:          rdsXMLNS,
		DBSubnetGroups: xmlDBSubnetGroupList{Members: members},
		Marker:         marker,
	}, nil
}

func (h *Handler) handleDeleteDBSubnetGroup(vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")

	if err := h.Backend.DeleteDBSubnetGroup(name); err != nil {
		return nil, err
	}

	return &deleteDBSubnetGroupResponse{
		Xmlns: rdsXMLNS,
	}, nil
}

func toXMLSubnetGroup(sg *DBSubnetGroup) xmlDBSubnetGroup {
	subnetMembers := make([]xmlSubnet, 0, len(sg.SubnetIDs))
	for _, id := range sg.SubnetIDs {
		subnetMembers = append(subnetMembers, xmlSubnet{SubnetIdentifier: id})
	}

	return xmlDBSubnetGroup{
		DBSubnetGroupName:        sg.DBSubnetGroupName,
		DBSubnetGroupDescription: sg.DBSubnetGroupDescription,
		DBSubnetGroupArn:         sg.DBSubnetGroupArn,
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
	DBSubnetGroupDescription string        `xml:"DBSubnetGroupDescription"`
	DBSubnetGroupArn         string        `xml:"DBSubnetGroupArn,omitempty"`
	VpcID                    string        `xml:"VpcId"`
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

type deleteDBSubnetGroupResponse struct {
	XMLName xml.Name `xml:"DeleteDBSubnetGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type describeDBSubnetGroupsResponse struct {
	XMLName        xml.Name             `xml:"DescribeDBSubnetGroupsResponse"`
	Xmlns          string               `xml:"xmlns,attr"`
	Marker         string               `xml:"DescribeDBSubnetGroupsResult>Marker,omitempty"`
	DBSubnetGroups xmlDBSubnetGroupList `xml:"DescribeDBSubnetGroupsResult>DBSubnetGroups"`
}

// parseSubnetIDMembers parses SubnetIds.SubnetIdentifier.N form values (AWS query protocol encoding).
func parseSubnetIDMembers(vals url.Values) []string {
	var ids []string
	for i := 1; ; i++ {
		sid := vals.Get(fmt.Sprintf("SubnetIds.SubnetIdentifier.%d", i))
		if sid == "" {
			return ids
		}
		ids = append(ids, sid)
	}
}

func (h *Handler) handleModifyDBSubnetGroup(vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")
	description := vals.Get("DBSubnetGroupDescription")
	var subnetIDs []string
	for i := 1; ; i++ {
		id := vals.Get("SubnetIds.SubnetIdentifier." + strconv.Itoa(i))
		if id == "" {
			break
		}
		subnetIDs = append(subnetIDs, id)
	}
	sg, err := h.Backend.ModifyDBSubnetGroup(name, description, subnetIDs)
	if err != nil {
		return nil, err
	}

	return &modifyDBSubnetGroupResponse{
		Xmlns:         rdsXMLNS,
		DBSubnetGroup: toXMLSubnetGroup(sg),
	}, nil
}

type modifyDBSubnetGroupResponse struct {
	XMLName       xml.Name         `xml:"ModifyDBSubnetGroupResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	DBSubnetGroup xmlDBSubnetGroup `xml:"ModifyDBSubnetGroupResult>DBSubnetGroup"`
}
