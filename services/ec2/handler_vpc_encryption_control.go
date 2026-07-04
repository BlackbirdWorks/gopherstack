package ec2

import (
	"encoding/xml"
	"net/url"
)

// ---- Handler registration ----

func registerVpcEncryptionControlOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateVpcEncryptionControl"] = h.handleCreateVpcEncryptionControl
	ops["DeleteVpcEncryptionControl"] = h.handleDeleteVpcEncryptionControl
	ops["DescribeVpcEncryptionControls"] = h.handleDescribeVpcEncryptionControls
	ops["ModifyVpcEncryptionControl"] = h.handleModifyVpcEncryptionControl
	ops["GetVpcResourcesBlockingEncryptionEnforcement"] = h.handleGetVpcResourcesBlockingEncryptionEnforcement
}

func vpcEncryptionControlSupportedOperations() []string {
	return []string{
		"CreateVpcEncryptionControl",
		"DeleteVpcEncryptionControl",
		"DescribeVpcEncryptionControls",
		"ModifyVpcEncryptionControl",
		"GetVpcResourcesBlockingEncryptionEnforcement",
	}
}

// ---- XML entity types ----

type vpcEncryptionControlExclusionItem struct {
	State string `xml:"state,omitempty"`
}

type vpcEncryptionControlExclusionsItem struct {
	EgressOnlyInternetGateway vpcEncryptionControlExclusionItem `xml:"egressOnlyInternetGateway"`
	ElasticFileSystem         vpcEncryptionControlExclusionItem `xml:"elasticFileSystem"`
	InternetGateway           vpcEncryptionControlExclusionItem `xml:"internetGateway"`
	Lambda                    vpcEncryptionControlExclusionItem `xml:"lambda"`
	NatGateway                vpcEncryptionControlExclusionItem `xml:"natGateway"`
	VirtualPrivateGateway     vpcEncryptionControlExclusionItem `xml:"virtualPrivateGateway"`
	VpcLattice                vpcEncryptionControlExclusionItem `xml:"vpcLattice"`
	VpcPeering                vpcEncryptionControlExclusionItem `xml:"vpcPeering"`
}

type vpcEncryptionControlItem struct {
	VpcEncryptionControlID string                             `xml:"vpcEncryptionControlId"`
	VpcID                  string                             `xml:"vpcId"`
	Mode                   string                             `xml:"mode"`
	State                  string                             `xml:"state"`
	StateMessage           string                             `xml:"stateMessage,omitempty"`
	ResourceExclusions     vpcEncryptionControlExclusionsItem `xml:"resourceExclusions"`
	TagSet                 []simpleTagItem                    `xml:"tagSet>item"`
}

type vpcEncNonCompliantItem struct {
	ID           string `xml:"id"`
	Type         string `xml:"type"`
	Description  string `xml:"description,omitempty"`
	IsExcludable bool   `xml:"isExcludable"`
}

// ---- XML response types ----

type createVpcEncryptionControlResponse struct {
	XMLName              xml.Name                 `xml:"CreateVpcEncryptionControlResponse"`
	Xmlns                string                   `xml:"xmlns,attr"`
	RequestID            string                   `xml:"requestId"`
	VpcEncryptionControl vpcEncryptionControlItem `xml:"vpcEncryptionControl"`
}

type deleteVpcEncryptionControlResponse struct {
	XMLName              xml.Name                 `xml:"DeleteVpcEncryptionControlResponse"`
	Xmlns                string                   `xml:"xmlns,attr"`
	RequestID            string                   `xml:"requestId"`
	VpcEncryptionControl vpcEncryptionControlItem `xml:"vpcEncryptionControl"`
}

type describeVpcEncryptionControlsResponse struct {
	XMLName               xml.Name                   `xml:"DescribeVpcEncryptionControlsResponse"`
	Xmlns                 string                     `xml:"xmlns,attr"`
	RequestID             string                     `xml:"requestId"`
	NextToken             string                     `xml:"nextToken,omitempty"`
	VpcEncryptionControls []vpcEncryptionControlItem `xml:"vpcEncryptionControlSet>item"`
}

type modifyVpcEncryptionControlResponse struct {
	XMLName              xml.Name                 `xml:"ModifyVpcEncryptionControlResponse"`
	Xmlns                string                   `xml:"xmlns,attr"`
	RequestID            string                   `xml:"requestId"`
	VpcEncryptionControl vpcEncryptionControlItem `xml:"vpcEncryptionControl"`
}

// getVpcEncBlockingResourcesResponse is the response for
// GetVpcResourcesBlockingEncryptionEnforcement. The Go type name is shortened from the
// AWS operation name to keep lines within the repo's line-length limit; the XMLName
// tag carries the exact AWS wire root element name.
type getVpcEncBlockingResourcesResponse struct {
	XMLName               xml.Name                 `xml:"GetVpcResourcesBlockingEncryptionEnforcementResponse"`
	Xmlns                 string                   `xml:"xmlns,attr"`
	RequestID             string                   `xml:"requestId"`
	NextToken             string                   `xml:"nextToken,omitempty"`
	NonCompliantResources []vpcEncNonCompliantItem `xml:"nonCompliantResourceSet>item"`
}

// ---- item conversion helpers ----

func vpcEncryptionControlExclusionToItem(s VpcEncryptionControlExclusionState) vpcEncryptionControlExclusionItem {
	return vpcEncryptionControlExclusionItem(s)
}

func vpcEncryptionControlToItem(vec *VpcEncryptionControl) vpcEncryptionControlItem {
	e := vec.ResourceExclusions

	return vpcEncryptionControlItem{
		VpcEncryptionControlID: vec.VpcEncryptionControlID,
		VpcID:                  vec.VpcID,
		Mode:                   vec.Mode,
		State:                  vec.State,
		StateMessage:           vec.StateMessage,
		ResourceExclusions: vpcEncryptionControlExclusionsItem{
			EgressOnlyInternetGateway: vpcEncryptionControlExclusionToItem(e.EgressOnlyInternetGateway),
			ElasticFileSystem:         vpcEncryptionControlExclusionToItem(e.ElasticFileSystem),
			InternetGateway:           vpcEncryptionControlExclusionToItem(e.InternetGateway),
			Lambda:                    vpcEncryptionControlExclusionToItem(e.Lambda),
			NatGateway:                vpcEncryptionControlExclusionToItem(e.NatGateway),
			VirtualPrivateGateway:     vpcEncryptionControlExclusionToItem(e.VirtualPrivateGateway),
			VpcLattice:                vpcEncryptionControlExclusionToItem(e.VpcLattice),
			VpcPeering:                vpcEncryptionControlExclusionToItem(e.VpcPeering),
		},
		TagSet: tagItemsFromMap(vec.Tags),
	}
}

// ---- handlers ----

func (h *Handler) handleCreateVpcEncryptionControl(vals url.Values, reqID string) (any, error) {
	tags := parseTagSpecification(vals, "vpc-encryption-control")

	vec, err := h.Backend.CreateVpcEncryptionControl(vals.Get("VpcId"), tags)
	if err != nil {
		return nil, err
	}

	return &createVpcEncryptionControlResponse{
		Xmlns:                ec2XMLNS,
		RequestID:            reqID,
		VpcEncryptionControl: vpcEncryptionControlToItem(vec),
	}, nil
}

func (h *Handler) handleDeleteVpcEncryptionControl(vals url.Values, reqID string) (any, error) {
	vec, err := h.Backend.DeleteVpcEncryptionControl(vals.Get("VpcEncryptionControlId"))
	if err != nil {
		return nil, err
	}

	return &deleteVpcEncryptionControlResponse{
		Xmlns:                ec2XMLNS,
		RequestID:            reqID,
		VpcEncryptionControl: vpcEncryptionControlToItem(vec),
	}, nil
}

func (h *Handler) handleDescribeVpcEncryptionControls(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VpcEncryptionControlId")
	vpcIDs := parseMemberList(vals, "VpcId")

	vecs := h.Backend.DescribeVpcEncryptionControls(ids, vpcIDs)

	resp := &describeVpcEncryptionControlsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, vec := range vecs {
		resp.VpcEncryptionControls = append(resp.VpcEncryptionControls, vpcEncryptionControlToItem(vec))
	}

	return resp, nil
}

func (h *Handler) handleModifyVpcEncryptionControl(vals url.Values, reqID string) (any, error) {
	exclusions := VpcEncryptionControlExclusionModify{
		EgressOnlyInternetGateway: vals.Get("EgressOnlyInternetGatewayExclusion"),
		ElasticFileSystem:         vals.Get("ElasticFileSystemExclusion"),
		InternetGateway:           vals.Get("InternetGatewayExclusion"),
		Lambda:                    vals.Get("LambdaExclusion"),
		NatGateway:                vals.Get("NatGatewayExclusion"),
		VirtualPrivateGateway:     vals.Get("VirtualPrivateGatewayExclusion"),
		VpcLattice:                vals.Get("VpcLatticeExclusion"),
		VpcPeering:                vals.Get("VpcPeeringExclusion"),
	}

	vec, err := h.Backend.ModifyVpcEncryptionControl(
		vals.Get("VpcEncryptionControlId"),
		vals.Get("Mode"),
		exclusions,
	)
	if err != nil {
		return nil, err
	}

	return &modifyVpcEncryptionControlResponse{
		Xmlns:                ec2XMLNS,
		RequestID:            reqID,
		VpcEncryptionControl: vpcEncryptionControlToItem(vec),
	}, nil
}

func (h *Handler) handleGetVpcResourcesBlockingEncryptionEnforcement(vals url.Values, reqID string) (any, error) {
	resources, err := h.Backend.GetVpcResourcesBlockingEncryptionEnforcement(vals.Get("VpcId"))
	if err != nil {
		return nil, err
	}

	resp := &getVpcEncBlockingResourcesResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, r := range resources {
		resp.NonCompliantResources = append(resp.NonCompliantResources, vpcEncNonCompliantItem{
			ID:           r.ID,
			Type:         r.Type,
			Description:  r.Description,
			IsExcludable: r.IsExcludable,
		})
	}

	return resp, nil
}
