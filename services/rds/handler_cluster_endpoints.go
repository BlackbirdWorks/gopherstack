package rds

import (
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleCreateDBClusterEndpoint(vals url.Values) (any, error) {
	endpointID := vals.Get("DBClusterEndpointIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	endpointType := vals.Get("EndpointType")
	ep, err := h.Backend.CreateDBClusterEndpoint(endpointID, clusterID, endpointType)
	if err != nil {
		return nil, err
	}

	return &createDBClusterEndpointResponse{
		Xmlns:  rdsXMLNS,
		Result: createDBClusterEndpointResult{toXMLClusterEndpointFields(ep)},
	}, nil
}

func (h *Handler) handleDescribeDBClusterEndpoints(vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	endpointID := vals.Get("DBClusterEndpointIdentifier")
	endpoints, err := h.Backend.DescribeDBClusterEndpoints(clusterID, endpointID)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBClusterEndpointFields, 0, len(endpoints))
	for _, ep := range endpoints {
		cp := ep
		members = append(members, toXMLClusterEndpointFields(&cp))
	}

	return &describeDBClusterEndpointsResponse{
		Xmlns:              rdsXMLNS,
		DBClusterEndpoints: xmlDBClusterEndpointList{Members: members},
	}, nil
}

func (h *Handler) handleDeleteDBClusterEndpoint(vals url.Values) (any, error) {
	endpointID := vals.Get("DBClusterEndpointIdentifier")
	ep, err := h.Backend.DeleteDBClusterEndpoint(endpointID)
	if err != nil {
		return nil, err
	}

	return &deleteDBClusterEndpointResponse{
		Xmlns:  rdsXMLNS,
		Result: deleteDBClusterEndpointResult{toXMLClusterEndpointFields(ep)},
	}, nil
}

func toXMLClusterEndpointFields(ep *DBClusterEndpoint) xmlDBClusterEndpointFields {
	return xmlDBClusterEndpointFields{
		DBClusterEndpointIdentifier: ep.DBClusterEndpointIdentifier,
		DBClusterIdentifier:         ep.DBClusterIdentifier,
		EndpointType:                ep.EndpointType,
		Status:                      ep.Status,
		Endpoint:                    ep.Endpoint,
	}
}

// xmlDBClusterEndpointFields contains the individual fields of a cluster endpoint.
// It is used both standalone (for Create/Delete, whose results inline all fields) and
// inside the DescribeDBClusterEndpoints list (as DBClusterEndpointList items).
type xmlDBClusterEndpointFields struct {
	DBClusterEndpointIdentifier string `xml:"DBClusterEndpointIdentifier"`
	DBClusterIdentifier         string `xml:"DBClusterIdentifier"`
	EndpointType                string `xml:"EndpointType"`
	Status                      string `xml:"Status"`
	Endpoint                    string `xml:"Endpoint,omitempty"`
}

type xmlDBClusterEndpointList struct {
	Members []xmlDBClusterEndpointFields `xml:"DBClusterEndpointList"`
}

// createDBClusterEndpointResult wraps fields directly inside CreateDBClusterEndpointResult.
// The SDK deserializes these fields directly (no inner DBClusterEndpoint element).
type createDBClusterEndpointResult struct {
	xmlDBClusterEndpointFields
}

type createDBClusterEndpointResponse struct {
	XMLName xml.Name                      `xml:"CreateDBClusterEndpointResponse"`
	Xmlns   string                        `xml:"xmlns,attr"`
	Result  createDBClusterEndpointResult `xml:"CreateDBClusterEndpointResult"`
}

type describeDBClusterEndpointsResponse struct {
	XMLName            xml.Name                 `xml:"DescribeDBClusterEndpointsResponse"`
	Xmlns              string                   `xml:"xmlns,attr"`
	DBClusterEndpoints xmlDBClusterEndpointList `xml:"DescribeDBClusterEndpointsResult>DBClusterEndpoints"`
}

// deleteDBClusterEndpointResult wraps fields directly inside DeleteDBClusterEndpointResult.
type deleteDBClusterEndpointResult struct {
	xmlDBClusterEndpointFields
}

type deleteDBClusterEndpointResponse struct {
	XMLName xml.Name                      `xml:"DeleteDBClusterEndpointResponse"`
	Xmlns   string                        `xml:"xmlns,attr"`
	Result  deleteDBClusterEndpointResult `xml:"DeleteDBClusterEndpointResult"`
}

func (h *Handler) handleModifyDBClusterEndpoint(vals url.Values) (any, error) {
	endpointID := vals.Get("DBClusterEndpointIdentifier")
	endpointType := vals.Get("EndpointType")
	ep, err := h.Backend.ModifyDBClusterEndpoint(endpointID, endpointType)
	if err != nil {
		return nil, err
	}

	return &modifyDBClusterEndpointResponse{
		Xmlns:  rdsXMLNS,
		Result: modifyDBClusterEndpointResult{toXMLClusterEndpointFields(ep)},
	}, nil
}

type modifyDBClusterEndpointResult struct {
	xmlDBClusterEndpointFields
}

type modifyDBClusterEndpointResponse struct {
	XMLName xml.Name                      `xml:"ModifyDBClusterEndpointResponse"`
	Xmlns   string                        `xml:"xmlns,attr"`
	Result  modifyDBClusterEndpointResult `xml:"ModifyDBClusterEndpointResult"`
}
