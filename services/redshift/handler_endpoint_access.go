package redshift

import (
	"encoding/xml"
	"net/url"
)

// ----- Endpoint Access -----

type endpointAccessXML struct {
	ClusterIdentifier  string `xml:"ClusterIdentifier"`
	EndpointName       string `xml:"EndpointName"`
	EndpointStatus     string `xml:"EndpointStatus"`
	EndpointCreateTime string `xml:"EndpointCreateTime,omitempty"`
	Port               int    `xml:"Port,omitempty"`
}

type createEndpointAccessResponse struct {
	XMLName xml.Name          `xml:"CreateEndpointAccessResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Result  endpointAccessXML `xml:"CreateEndpointAccessResult"`
}

func endpointAccessToXML(ep *EndpointAccess) endpointAccessXML {
	return endpointAccessXML{
		ClusterIdentifier:  ep.ClusterIdentifier,
		EndpointName:       ep.EndpointName,
		EndpointStatus:     ep.EndpointStatus,
		EndpointCreateTime: ep.EndpointCreateTime,
		Port:               ep.Port,
	}
}

func (h *Handler) handleCreateEndpointAccess(vals url.Values) (any, error) {
	ep, err := h.Backend.CreateEndpointAccess(
		vals.Get("ClusterIdentifier"),
		vals.Get("EndpointName"),
		vals.Get("VpcId"),
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
	eps, err := h.Backend.DescribeEndpointAccess(
		vals.Get("ClusterIdentifier"),
		vals.Get("EndpointName"),
	)
	if err != nil {
		return nil, err
	}

	members := make([]endpointAccessXML, 0, len(eps))

	for i := range eps {
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

func (h *Handler) handleModifyEndpointAccess(vals url.Values) (any, error) {
	ep, err := h.Backend.ModifyEndpointAccess(
		vals.Get("EndpointName"),
		vals.Get("VpcId"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyEndpointAccessResponse{
		Xmlns:  redshiftXMLNS,
		Result: endpointAccessToXML(ep),
	}, nil
}
