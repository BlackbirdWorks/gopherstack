package docdb

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
	tags := parseTags(vals)
	sg, err := h.Backend.CreateDBSubnetGroup(ctx, name, description, vpcID, subnetIDs, tags)
	if err != nil {
		return nil, err
	}

	return &createDBSubnetGroupResponse{
		Xmlns:         docdbXMLNS,
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

	members, nextMarker := applyDocDBMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBSubnetGroupsResponse{
		Xmlns: docdbXMLNS,
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

	return &deleteDBSubnetGroupResponse{Xmlns: docdbXMLNS}, nil
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
		Xmlns:         docdbXMLNS,
		DBSubnetGroup: toXMLSubnetGroup(sg),
	}, nil
}

// parseSubnetIDMembers parses the SubnetIds list. The real aws-sdk-go-v2
// query-protocol serializer (awsAwsquery_serializeDocumentSubnetIdentifierList)
// encodes each element as "SubnetIds.SubnetIdentifier.N", not the generic
// "SubnetIds.member.N" -- unlike most docdb lists, this one's member name is
// not "member". Getting this wrong means every subnet ID a real client sends
// is silently dropped, so CreateDBSubnetGroup/ModifyDBSubnetGroup would always
// persist an empty subnet list.
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

func toXMLSubnetGroup(sg *DBSubnetGroup) xmlDBSubnetGroup {
	subnetMembers := make([]xmlSubnet, 0, len(sg.SubnetIDs))
	for _, id := range sg.SubnetIDs {
		subnetMembers = append(subnetMembers, xmlSubnet{SubnetIdentifier: id, SubnetStatus: "Active"})
	}

	return xmlDBSubnetGroup{
		DBSubnetGroupName:        sg.DBSubnetGroupName,
		DBSubnetGroupDescription: sg.DBSubnetGroupDescription,
		VpcID:                    sg.VpcID,
		SubnetGroupStatus:        sg.Status,
		Subnets:                  xmlSubnetList{Members: subnetMembers},
		DBSubnetGroupArn:         sg.DBSubnetGroupArn,
	}
}

type xmlSubnet struct {
	SubnetIdentifier       string `xml:"SubnetIdentifier"`
	SubnetStatus           string `xml:"SubnetStatus,omitempty"`
	SubnetAvailabilityZone string `xml:"SubnetAvailabilityZone>Name,omitempty"`
}

type xmlSubnetList struct {
	Members []xmlSubnet `xml:"Subnet"`
}

type xmlDBSubnetGroup struct {
	DBSubnetGroupName        string        `xml:"DBSubnetGroupName"`
	DBSubnetGroupDescription string        `xml:"DBSubnetGroupDescription"`
	VpcID                    string        `xml:"VpcId,omitempty"`
	SubnetGroupStatus        string        `xml:"SubnetGroupStatus"`
	DBSubnetGroupArn         string        `xml:"DBSubnetGroupArn,omitempty"`
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
