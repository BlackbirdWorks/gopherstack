package rds

import (
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleCreateBlueGreenDeployment(vals url.Values) (any, error) {
	name := vals.Get("BlueGreenDeploymentName")
	source := vals.Get("Source")

	deployment, err := h.Backend.CreateBlueGreenDeployment(name, source)
	if err != nil {
		return nil, err
	}

	return &createBlueGreenDeploymentResponse{
		Xmlns:               rdsXMLNS,
		BlueGreenDeployment: toXMLBlueGreenDeployment(deployment),
	}, nil
}

func toXMLBlueGreenDeployment(d *BlueGreenDeployment) xmlBlueGreenDeployment {
	return xmlBlueGreenDeployment{
		BlueGreenDeploymentIdentifier: d.BlueGreenDeploymentIdentifier,
		BlueGreenDeploymentName:       d.BlueGreenDeploymentName,
		Target:                        d.Target,
		Source:                        d.Source,
		Status:                        d.Status,
	}
}

type xmlBlueGreenDeployment struct {
	BlueGreenDeploymentIdentifier string `xml:"BlueGreenDeploymentIdentifier"`
	BlueGreenDeploymentName       string `xml:"BlueGreenDeploymentName"`
	Source                        string `xml:"Source,omitempty"`
	Target                        string `xml:"Target,omitempty"`
	Status                        string `xml:"Status"`
}

type createBlueGreenDeploymentResponse struct {
	XMLName             xml.Name               `xml:"CreateBlueGreenDeploymentResponse"`
	Xmlns               string                 `xml:"xmlns,attr"`
	BlueGreenDeployment xmlBlueGreenDeployment `xml:"CreateBlueGreenDeploymentResult>BlueGreenDeployment"`
}

func (h *Handler) handleDescribeBlueGreenDeployments(vals url.Values) (any, error) {
	id := vals.Get("BlueGreenDeploymentIdentifier")
	deployments, err := h.Backend.DescribeBlueGreenDeployments(id)
	if err != nil {
		return nil, err
	}
	members := make([]xmlBlueGreenDeployment, 0, len(deployments))
	for i := range deployments {
		members = append(members, toXMLBlueGreenDeployment(&deployments[i]))
	}

	return &describeBlueGreenDeploymentsResponse{
		Xmlns:                rdsXMLNS,
		BlueGreenDeployments: xmlBlueGreenDeploymentList{Members: members},
	}, nil
}

func (h *Handler) handleDeleteBlueGreenDeployment(vals url.Values) (any, error) {
	id := vals.Get("BlueGreenDeploymentIdentifier")
	deployment, err := h.Backend.DeleteBlueGreenDeployment(id)
	if err != nil {
		return nil, err
	}

	return &deleteBlueGreenDeploymentResponse{
		Xmlns:               rdsXMLNS,
		BlueGreenDeployment: toXMLBlueGreenDeployment(deployment),
	}, nil
}

func (h *Handler) handleSwitchoverBlueGreenDeployment(vals url.Values) (any, error) {
	id := vals.Get("BlueGreenDeploymentIdentifier")
	deployment, err := h.Backend.SwitchoverBlueGreenDeployment(id)
	if err != nil {
		return nil, err
	}

	return &switchoverBlueGreenDeploymentResponse{
		Xmlns:               rdsXMLNS,
		BlueGreenDeployment: toXMLBlueGreenDeployment(deployment),
	}, nil
}

type xmlBlueGreenDeploymentList struct {
	Members []xmlBlueGreenDeployment `xml:"BlueGreenDeployment"`
}

type describeBlueGreenDeploymentsResponse struct {
	XMLName              xml.Name                   `xml:"DescribeBlueGreenDeploymentsResponse"`
	Xmlns                string                     `xml:"xmlns,attr"`
	BlueGreenDeployments xmlBlueGreenDeploymentList `xml:"DescribeBlueGreenDeploymentsResult>BlueGreenDeployments"`
}

type deleteBlueGreenDeploymentResponse struct {
	XMLName             xml.Name               `xml:"DeleteBlueGreenDeploymentResponse"`
	Xmlns               string                 `xml:"xmlns,attr"`
	BlueGreenDeployment xmlBlueGreenDeployment `xml:"DeleteBlueGreenDeploymentResult>BlueGreenDeployment"`
}

type switchoverBlueGreenDeploymentResponse struct {
	XMLName             xml.Name               `xml:"SwitchoverBlueGreenDeploymentResponse"`
	Xmlns               string                 `xml:"xmlns,attr"`
	BlueGreenDeployment xmlBlueGreenDeployment `xml:"SwitchoverBlueGreenDeploymentResult>BlueGreenDeployment"`
}
