package redshift

import (
	"encoding/xml"
	"net/url"
)

// ----- Integrations -----

type integrationXML struct {
	IntegrationArn  string `xml:"IntegrationArn"`
	IntegrationName string `xml:"IntegrationName"`
	SourceArn       string `xml:"SourceArn,omitempty"`
	TargetArn       string `xml:"TargetArn,omitempty"`
	Status          string `xml:"Status"`
	Description     string `xml:"Description,omitempty"`
	KmsKeyID        string `xml:"KmsKeyId,omitempty"`
}

func integrationToXML(ig *Integration) integrationXML {
	return integrationXML{
		IntegrationArn:  ig.IntegrationArn,
		IntegrationName: ig.IntegrationName,
		SourceArn:       ig.SourceArn,
		TargetArn:       ig.TargetArn,
		Status:          ig.Status,
		Description:     ig.Description,
		KmsKeyID:        ig.KmsKeyID,
	}
}

type createIntegrationResponse struct {
	XMLName xml.Name       `xml:"CreateIntegrationResponse"`
	Xmlns   string         `xml:"xmlns,attr"`
	Result  integrationXML `xml:"CreateIntegrationResult"`
}

func (h *Handler) handleCreateIntegration(vals url.Values) (any, error) {
	ig, err := h.Backend.CreateIntegration(
		vals.Get("IntegrationName"),
		vals.Get("SourceArn"),
		vals.Get("TargetArn"),
		vals.Get("KmsKeyId"),
		vals.Get("Description"),
	)
	if err != nil {
		return nil, err
	}

	return &createIntegrationResponse{
		Xmlns:  redshiftXMLNS,
		Result: integrationToXML(ig),
	}, nil
}

type deleteIntegrationResponse struct {
	XMLName xml.Name       `xml:"DeleteIntegrationResponse"`
	Xmlns   string         `xml:"xmlns,attr"`
	Result  integrationXML `xml:"DeleteIntegrationResult"`
}

func (h *Handler) handleDeleteIntegration(vals url.Values) (any, error) {
	ig, err := h.Backend.DeleteIntegration(vals.Get("IntegrationArn"))
	if err != nil {
		return nil, err
	}

	return &deleteIntegrationResponse{
		Xmlns:  redshiftXMLNS,
		Result: integrationToXML(ig),
	}, nil
}

type describeIntegrationsResponse struct {
	XMLName xml.Name `xml:"DescribeIntegrationsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		Integrations []integrationXML `xml:"Integrations>Integration"`
	} `xml:"DescribeIntegrationsResult"`
}

func (h *Handler) handleDescribeIntegrations(vals url.Values) (any, error) {
	igs, err := h.Backend.DescribeIntegrations(vals.Get("IntegrationArn"))
	if err != nil {
		return nil, err
	}

	members := make([]integrationXML, 0, len(igs))

	for i := range igs {
		members = append(members, integrationToXML(&igs[i]))
	}

	resp := &describeIntegrationsResponse{Xmlns: redshiftXMLNS}
	resp.Result.Integrations = members

	return resp, nil
}

type describeInboundIntegrationsResponse struct {
	XMLName xml.Name `xml:"DescribeInboundIntegrationsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		InboundIntegrations []integrationXML `xml:"InboundIntegrations>InboundIntegration"`
	} `xml:"DescribeInboundIntegrationsResult"`
}

func (h *Handler) handleDescribeInboundIntegrations(_ url.Values) (any, error) {
	return &describeInboundIntegrationsResponse{Xmlns: redshiftXMLNS}, nil
}

type modifyIntegrationResponse struct {
	XMLName xml.Name       `xml:"ModifyIntegrationResponse"`
	Xmlns   string         `xml:"xmlns,attr"`
	Result  integrationXML `xml:"ModifyIntegrationResult"`
}

func (h *Handler) handleModifyIntegration(vals url.Values) (any, error) {
	ig, err := h.Backend.ModifyIntegration(
		vals.Get("IntegrationArn"),
		vals.Get("Description"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyIntegrationResponse{
		Xmlns:  redshiftXMLNS,
		Result: integrationToXML(ig),
	}, nil
}
