package neptune

import (
	"context"
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleCreateDBClusterEndpoint(ctx context.Context, vals url.Values) (any, error) {
	endpointID := vals.Get("DBClusterEndpointIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	endpointType := vals.Get("EndpointType")
	ep, err := h.Backend.CreateDBClusterEndpoint(ctx, endpointID, clusterID, endpointType)
	if err != nil {
		return nil, err
	}

	return &createDBClusterEndpointResponse{
		Xmlns:             neptuneXMLNS,
		DBClusterEndpoint: toXMLClusterEndpoint(ep),
	}, nil
}

func (h *Handler) handleDeleteDBClusterEndpoint(ctx context.Context, vals url.Values) (any, error) {
	endpointID := vals.Get("DBClusterEndpointIdentifier")
	ep, err := h.Backend.DeleteDBClusterEndpoint(ctx, endpointID)
	if err != nil {
		return nil, err
	}

	return &deleteDBClusterEndpointResponse{
		Xmlns:             neptuneXMLNS,
		DBClusterEndpoint: toXMLClusterEndpoint(ep),
	}, nil
}

func (h *Handler) handleDescribeDBClusterEndpoints(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	endpointID := vals.Get("DBClusterEndpointIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	endpoints, err := h.Backend.DescribeDBClusterEndpoints(ctx, endpointID, clusterID)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBClusterEndpoint, 0, len(endpoints))
	for _, ep := range endpoints {
		cp := ep
		members = append(members, toXMLClusterEndpoint(&cp))
	}

	members, nextMarker := applyNeptuneMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBClusterEndpointsResponse{
		Xmlns: neptuneXMLNS,
		Result: describeDBClusterEndpointsResult{
			DBClusterEndpoints: xmlDBClusterEndpointList{Members: members},
			Marker:             nextMarker,
		},
	}, nil
}

func (h *Handler) handleModifyDBClusterEndpoint(ctx context.Context, vals url.Values) (any, error) {
	endpointID := vals.Get("DBClusterEndpointIdentifier")
	endpointType := vals.Get("EndpointType")
	ep, err := h.Backend.ModifyDBClusterEndpoint(ctx, endpointID, endpointType)
	if err != nil {
		return nil, err
	}

	return &modifyDBClusterEndpointResponse{
		Xmlns:             neptuneXMLNS,
		DBClusterEndpoint: toXMLClusterEndpoint(ep),
	}, nil
}

func toXMLClusterEndpoint(ep *DBClusterEndpoint) xmlDBClusterEndpoint {
	staticMembers := make([]xmlSourceID, 0, len(ep.StaticMembers))
	for _, m := range ep.StaticMembers {
		staticMembers = append(staticMembers, xmlSourceID{Member: m})
	}
	excludedMembers := make([]xmlSourceID, 0, len(ep.ExcludedMembers))
	for _, m := range ep.ExcludedMembers {
		excludedMembers = append(excludedMembers, xmlSourceID{Member: m})
	}

	return xmlDBClusterEndpoint{
		DBClusterEndpointIdentifier:         ep.DBClusterEndpointIdentifier,
		DBClusterIdentifier:                 ep.DBClusterIdentifier,
		DBClusterEndpointArn:                ep.DBClusterEndpointArn,
		DBClusterEndpointResourceIdentifier: ep.DBClusterEndpointResourceIdentifier,
		EndpointType:                        ep.EndpointType,
		CustomEndpointType:                  ep.CustomEndpointType,
		Status:                              ep.Status,
		Endpoint:                            ep.Endpoint,
		StaticMembers:                       xmlSourceIDList{Members: staticMembers},
		ExcludedMembers:                     xmlSourceIDList{Members: excludedMembers},
	}
}

type xmlDBClusterEndpoint struct {
	DBClusterEndpointIdentifier         string          `xml:"DBClusterEndpointIdentifier"`
	DBClusterIdentifier                 string          `xml:"DBClusterIdentifier"`
	DBClusterEndpointArn                string          `xml:"DBClusterEndpointArn,omitempty"`
	DBClusterEndpointResourceIdentifier string          `xml:"DBClusterEndpointResourceIdentifier,omitempty"`
	EndpointType                        string          `xml:"EndpointType"`
	CustomEndpointType                  string          `xml:"CustomEndpointType,omitempty"`
	Status                              string          `xml:"Status"`
	Endpoint                            string          `xml:"Endpoint,omitempty"`
	StaticMembers                       xmlSourceIDList `xml:"StaticMembers"`
	ExcludedMembers                     xmlSourceIDList `xml:"ExcludedMembers"`
}

type createDBClusterEndpointResponse struct {
	XMLName           xml.Name             `xml:"CreateDBClusterEndpointResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterEndpoint xmlDBClusterEndpoint `xml:"CreateDBClusterEndpointResult"`
}

// New XML types for the additional operations.

type xmlDBClusterEndpointList struct {
	Members []xmlDBClusterEndpoint `xml:"DBClusterEndpoint"`
}

type describeDBClusterEndpointsResult struct {
	Marker             string                   `xml:"Marker,omitempty"`
	DBClusterEndpoints xmlDBClusterEndpointList `xml:"DBClusterEndpoints"`
}

type describeDBClusterEndpointsResponse struct {
	XMLName xml.Name                         `xml:"DescribeDBClusterEndpointsResponse"`
	Xmlns   string                           `xml:"xmlns,attr"`
	Result  describeDBClusterEndpointsResult `xml:"DescribeDBClusterEndpointsResult"`
}

type deleteDBClusterEndpointResponse struct {
	XMLName           xml.Name             `xml:"DeleteDBClusterEndpointResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterEndpoint xmlDBClusterEndpoint `xml:"DeleteDBClusterEndpointResult"`
}

type modifyDBClusterEndpointResponse struct {
	XMLName           xml.Name             `xml:"ModifyDBClusterEndpointResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterEndpoint xmlDBClusterEndpoint `xml:"ModifyDBClusterEndpointResult"`
}
