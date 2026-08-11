package rds

import (
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleAuthorizeDBSecurityGroupIngress(vals url.Values) (any, error) {
	groupName := vals.Get("DBSecurityGroupName")
	cidrIP := vals.Get("CIDRIP")

	sg, err := h.Backend.AuthorizeDBSecurityGroupIngress(groupName, cidrIP)
	if err != nil {
		return nil, err
	}

	return &authorizeDBSecurityGroupIngressResponse{
		Xmlns:           rdsXMLNS,
		DBSecurityGroup: toXMLDBSecurityGroup(sg),
	}, nil
}

func toXMLDBSecurityGroup(sg *DBSecurityGroup) xmlDBSecurityGroup {
	ipRanges := make([]xmlIPRange, 0, len(sg.IPRanges))
	for _, r := range sg.IPRanges {
		ipRanges = append(ipRanges, xmlIPRange(r))
	}

	return xmlDBSecurityGroup{
		DBSecurityGroupName:        sg.DBSecurityGroupName,
		DBSecurityGroupDescription: sg.DBSecurityGroupDescription,
		DBSecurityGroupArn:         sg.DBSecurityGroupArn,
		IPRanges:                   xmlIPRangeList{Members: ipRanges},
	}
}

type xmlIPRange struct {
	CIDRIP string `xml:"CIDRIP"`
	Status string `xml:"Status"`
}

type xmlIPRangeList struct {
	Members []xmlIPRange `xml:"IPRange"`
}

type xmlDBSecurityGroup struct {
	DBSecurityGroupName        string         `xml:"DBSecurityGroupName"`
	DBSecurityGroupDescription string         `xml:"DBSecurityGroupDescription,omitempty"`
	DBSecurityGroupArn         string         `xml:"DBSecurityGroupArn,omitempty"`
	IPRanges                   xmlIPRangeList `xml:"IPRanges"`
}

type authorizeDBSecurityGroupIngressResponse struct {
	XMLName         xml.Name           `xml:"AuthorizeDBSecurityGroupIngressResponse"`
	Xmlns           string             `xml:"xmlns,attr"`
	DBSecurityGroup xmlDBSecurityGroup `xml:"AuthorizeDBSecurityGroupIngressResult>DBSecurityGroup"`
}

func (h *Handler) handleDescribeDBSecurityGroups(vals url.Values) (any, error) {
	name := vals.Get("DBSecurityGroupName")
	groups, err := h.Backend.DescribeDBSecurityGroups(name)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBSecurityGroup, 0, len(groups))
	for i := range groups {
		members = append(members, toXMLDBSecurityGroup(&groups[i]))
	}

	return &describeDBSecurityGroupsResponse{
		Xmlns:            rdsXMLNS,
		DBSecurityGroups: xmlDBSecurityGroupList{Members: members},
	}, nil
}

func (h *Handler) handleDeleteDBSecurityGroup(vals url.Values) (any, error) {
	name := vals.Get("DBSecurityGroupName")
	if err := h.Backend.DeleteDBSecurityGroup(name); err != nil {
		return nil, err
	}

	return &deleteDBSecurityGroupResponse{Xmlns: rdsXMLNS}, nil
}

func (h *Handler) handleRevokeDBSecurityGroupIngress(vals url.Values) (any, error) {
	groupName := vals.Get("DBSecurityGroupName")
	cidrIP := vals.Get("CIDRIP")
	sg, err := h.Backend.RevokeDBSecurityGroupIngress(groupName, cidrIP)
	if err != nil {
		return nil, err
	}

	return &revokeDBSecurityGroupIngressResponse{
		Xmlns:           rdsXMLNS,
		DBSecurityGroup: toXMLDBSecurityGroup(sg),
	}, nil
}

type xmlDBSecurityGroupList struct {
	Members []xmlDBSecurityGroup `xml:"DBSecurityGroup"`
}

type describeDBSecurityGroupsResponse struct {
	XMLName          xml.Name               `xml:"DescribeDBSecurityGroupsResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	DBSecurityGroups xmlDBSecurityGroupList `xml:"DescribeDBSecurityGroupsResult>DBSecurityGroups"`
}

type deleteDBSecurityGroupResponse struct {
	XMLName xml.Name `xml:"DeleteDBSecurityGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type revokeDBSecurityGroupIngressResponse struct {
	XMLName         xml.Name           `xml:"RevokeDBSecurityGroupIngressResponse"`
	Xmlns           string             `xml:"xmlns,attr"`
	DBSecurityGroup xmlDBSecurityGroup `xml:"RevokeDBSecurityGroupIngressResult>DBSecurityGroup"`
}

type createDBSecurityGroupR2Response struct {
	XMLName         xml.Name           `xml:"CreateDBSecurityGroupResponse"`
	Xmlns           string             `xml:"xmlns,attr"`
	DBSecurityGroup xmlDBSecurityGroup `xml:"CreateDBSecurityGroupResult>DBSecurityGroup"`
}

func (h *Handler) handleCreateDBSecurityGroup(vals url.Values) (any, error) {
	name := vals.Get("DBSecurityGroupName")
	description := vals.Get("DBSecurityGroupDescription")
	sg, err := h.Backend.CreateDBSecurityGroup(name, description)
	if err != nil {
		return nil, err
	}

	h.applyCreateTags(vals, sg.DBSecurityGroupArn)

	return &createDBSecurityGroupR2Response{
		Xmlns:           rdsXMLNS,
		DBSecurityGroup: toXMLDBSecurityGroup(sg),
	}, nil
}
