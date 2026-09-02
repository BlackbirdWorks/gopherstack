package redshift

import (
	"encoding/xml"
	"net/url"
)

// ----- Endpoint Access -----

// xmlVpcSecurityGroupMembership mirrors types.VpcSecurityGroupMembership. This
// backend applies security group changes synchronously, so Status is always
// "active" (matching the placeholder-status convention used elsewhere in this
// service, e.g. ClusterParameterGroups' "in-sync").
type xmlVpcSecurityGroupMembership struct {
	VpcSecurityGroupID string `xml:"VpcSecurityGroupId"`
	Status             string `xml:"Status"`
}

type endpointAccessXML struct {
	ClusterIdentifier  string                          `xml:"ClusterIdentifier"`
	EndpointName       string                          `xml:"EndpointName"`
	EndpointStatus     string                          `xml:"EndpointStatus"`
	EndpointCreateTime string                          `xml:"EndpointCreateTime,omitempty"`
	SubnetGroupName    string                          `xml:"SubnetGroupName,omitempty"`
	ResourceOwner      string                          `xml:"ResourceOwner,omitempty"`
	VpcSecurityGroups  []xmlVpcSecurityGroupMembership `xml:"VpcSecurityGroups>VpcSecurityGroup,omitempty"`
	Port               int                             `xml:"Port,omitempty"`
}

type createEndpointAccessResponse struct {
	XMLName xml.Name          `xml:"CreateEndpointAccessResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Result  endpointAccessXML `xml:"CreateEndpointAccessResult"`
}

func endpointAccessToXML(ep *EndpointAccess) endpointAccessXML {
	groups := make([]xmlVpcSecurityGroupMembership, 0, len(ep.VpcSecurityGroupIDs))
	for _, id := range ep.VpcSecurityGroupIDs {
		groups = append(groups, xmlVpcSecurityGroupMembership{VpcSecurityGroupID: id, Status: endpointStatusActive})
	}

	return endpointAccessXML{
		ClusterIdentifier:  ep.ClusterIdentifier,
		EndpointName:       ep.EndpointName,
		EndpointStatus:     ep.EndpointStatus,
		EndpointCreateTime: ep.EndpointCreateTime,
		SubnetGroupName:    ep.SubnetGroupName,
		ResourceOwner:      ep.ResourceOwner,
		VpcSecurityGroups:  groups,
		Port:               ep.Port,
	}
}

// handleCreateEndpointAccess implements CreateEndpointAccess. Real
// CreateEndpointAccessInput has no VpcId field -- the request instead carries
// SubnetGroupName, ResourceOwner, and VpcSecurityGroupIds (confirmed against
// aws-sdk-go-v2/service/redshift@v1.62.3's query-protocol serializer).
func (h *Handler) handleCreateEndpointAccess(vals url.Values) (any, error) {
	vpcSecurityGroupIDs := parseStringList(vals, "VpcSecurityGroupIds.VpcSecurityGroupId.")

	ep, err := h.Backend.CreateEndpointAccess(
		vals.Get("ClusterIdentifier"),
		vals.Get("EndpointName"),
		vals.Get("SubnetGroupName"),
		vals.Get("ResourceOwner"),
		vpcSecurityGroupIDs,
	)
	if err != nil {
		return nil, err
	}

	return &createEndpointAccessResponse{
		Xmlns:  redshiftXMLNS,
		Result: endpointAccessToXML(ep),
	}, nil
}

type deleteEndpointAccessResponse struct {
	XMLName xml.Name          `xml:"DeleteEndpointAccessResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Result  endpointAccessXML `xml:"DeleteEndpointAccessResult"`
}

func (h *Handler) handleDeleteEndpointAccess(vals url.Values) (any, error) {
	ep, err := h.Backend.DeleteEndpointAccess(vals.Get("EndpointName"))
	if err != nil {
		return nil, err
	}

	return &deleteEndpointAccessResponse{
		Xmlns:  redshiftXMLNS,
		Result: endpointAccessToXML(ep),
	}, nil
}

type describeEndpointAccessResponse struct {
	XMLName xml.Name `xml:"DescribeEndpointAccessResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		EndpointAccessList []endpointAccessXML `xml:"EndpointAccessList>member"`
	} `xml:"DescribeEndpointAccessResult"`
}

func (h *Handler) handleDescribeEndpointAccess(vals url.Values) (any, error) {
	resourceOwner := vals.Get("ResourceOwner")
	vpcID := vals.Get("VpcId")

	eps, err := h.Backend.DescribeEndpointAccess(
		vals.Get("ClusterIdentifier"),
		vals.Get("EndpointName"),
	)
	if err != nil {
		return nil, err
	}

	members := make([]endpointAccessXML, 0, len(eps))

	for i := range eps {
		if resourceOwner != "" && eps[i].ResourceOwner != resourceOwner {
			continue
		}

		if vpcID != "" && eps[i].VpcID != vpcID {
			continue
		}

		members = append(members, endpointAccessToXML(&eps[i]))
	}

	resp := &describeEndpointAccessResponse{Xmlns: redshiftXMLNS}
	resp.Result.EndpointAccessList = members

	return resp, nil
}

type modifyEndpointAccessResponse struct {
	XMLName xml.Name          `xml:"ModifyEndpointAccessResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Result  endpointAccessXML `xml:"ModifyEndpointAccessResult"`
}

// handleModifyEndpointAccess implements ModifyEndpointAccess. Real
// ModifyEndpointAccessInput only supports changing VpcSecurityGroupIds -- there is
// no VpcId parameter (confirmed against
// aws-sdk-go-v2/service/redshift@v1.62.3's query-protocol serializer).
func (h *Handler) handleModifyEndpointAccess(vals url.Values) (any, error) {
	vpcSecurityGroupIDs := parseStringList(vals, "VpcSecurityGroupIds.VpcSecurityGroupId.")

	ep, err := h.Backend.ModifyEndpointAccess(
		vals.Get("EndpointName"),
		vpcSecurityGroupIDs,
	)
	if err != nil {
		return nil, err
	}

	return &modifyEndpointAccessResponse{
		Xmlns:  redshiftXMLNS,
		Result: endpointAccessToXML(ep),
	}, nil
}
