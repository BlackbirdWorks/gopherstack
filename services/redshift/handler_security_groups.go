package redshift

import (
	"encoding/xml"
	"net/url"
)

// ---- CreateClusterSecurityGroup ----

type createClusterSecurityGroupResponse struct {
	XMLName       xml.Name                `xml:"CreateClusterSecurityGroupResponse"`
	Xmlns         string                  `xml:"xmlns,attr"`
	SecurityGroup xmlClusterSecurityGroup `xml:"CreateClusterSecurityGroupResult>ClusterSecurityGroup"`
}

func (h *Handler) handleCreateClusterSecurityGroup(vals url.Values) (any, error) {
	name := vals.Get("ClusterSecurityGroupName")
	description := vals.Get("Description")

	sg, err := h.Backend.CreateClusterSecurityGroup(name, description)
	if err != nil {
		return nil, err
	}

	return &createClusterSecurityGroupResponse{
		Xmlns:         redshiftXMLNS,
		SecurityGroup: securityGroupToXML(sg),
	}, nil
}

// ---- DeleteClusterSecurityGroup ----

type deleteClusterSecurityGroupResponse struct {
	XMLName xml.Name `xml:"DeleteClusterSecurityGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleDeleteClusterSecurityGroup(vals url.Values) (any, error) {
	name := vals.Get("ClusterSecurityGroupName")

	if err := h.Backend.DeleteClusterSecurityGroup(name); err != nil {
		return nil, err
	}

	return &deleteClusterSecurityGroupResponse{Xmlns: redshiftXMLNS}, nil
}

// ---- DescribeClusterSecurityGroups ----

type xmlClusterSecurityGroupList struct {
	Members []xmlClusterSecurityGroup `xml:"ClusterSecurityGroup"`
}

type describeClusterSecurityGroupsResponse struct {
	XMLName        xml.Name                    `xml:"DescribeClusterSecurityGroupsResponse"`
	Xmlns          string                      `xml:"xmlns,attr"`
	SecurityGroups xmlClusterSecurityGroupList `xml:"DescribeClusterSecurityGroupsResult>ClusterSecurityGroups"`
}

func (h *Handler) handleDescribeClusterSecurityGroups(vals url.Values) (any, error) {
	name := vals.Get("ClusterSecurityGroupName")

	groups, err := h.Backend.DescribeClusterSecurityGroups(name)
	if err != nil {
		return nil, err
	}

	members := make([]xmlClusterSecurityGroup, 0, len(groups))
	for _, g := range groups {
		gp := g
		members = append(members, securityGroupToXML(&gp))
	}

	return &describeClusterSecurityGroupsResponse{
		Xmlns:          redshiftXMLNS,
		SecurityGroups: xmlClusterSecurityGroupList{Members: members},
	}, nil
}

// ---- RevokeClusterSecurityGroupIngress ----

type revokeClusterSecurityGroupIngressResponse struct {
	XMLName       xml.Name                `xml:"RevokeClusterSecurityGroupIngressResponse"`
	Xmlns         string                  `xml:"xmlns,attr"`
	SecurityGroup xmlClusterSecurityGroup `xml:"RevokeClusterSecurityGroupIngressResult>ClusterSecurityGroup"`
}

func (h *Handler) handleRevokeClusterSecurityGroupIngress(vals url.Values) (any, error) {
	groupName := vals.Get("ClusterSecurityGroupName")
	cidrIP := vals.Get("CIDRIP")
	ec2GroupName := vals.Get("EC2SecurityGroupName")
	ec2GroupOwnerID := vals.Get("EC2SecurityGroupOwnerId")

	sg, err := h.Backend.RevokeClusterSecurityGroupIngress(groupName, cidrIP, ec2GroupName, ec2GroupOwnerID)
	if err != nil {
		return nil, err
	}

	return &revokeClusterSecurityGroupIngressResponse{
		Xmlns:         redshiftXMLNS,
		SecurityGroup: securityGroupToXML(sg),
	}, nil
}
