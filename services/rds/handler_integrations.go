package rds

import (
	"encoding/xml"
	"net/url"
)

type xmlIntegration struct {
	IntegrationName        string `xml:"IntegrationName"`
	IntegrationArn         string `xml:"IntegrationArn,omitempty"`
	Status                 string `xml:"Status,omitempty"`
	SourceArn              string `xml:"SourceArn,omitempty"`
	TargetArn              string `xml:"TargetArn,omitempty"`
	DataFilter             string `xml:"DataFilter,omitempty"`
	IntegrationDescription string `xml:"Description,omitempty"`
}

type xmlIntegrationList struct {
	Members []xmlIntegration `xml:"Integration"`
}

// CreateIntegrationOutput, DeleteIntegrationOutput, and ModifyIntegrationOutput are
// flat shapes in the real RDS API (no nested <Integration> wrapper — see the comment
// on createCustomDBEngineVersionResponse for why each field below repeats the full
// result-element chain). DescribeIntegrations is different: it returns a real list,
// so describeIntegrationsResponse below correctly keeps the xmlIntegrationList nesting.
type createIntegrationResponse struct {
	XMLName                xml.Name `xml:"CreateIntegrationResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	IntegrationName        string   `xml:"CreateIntegrationResult>IntegrationName"`
	IntegrationArn         string   `xml:"CreateIntegrationResult>IntegrationArn,omitempty"`
	Status                 string   `xml:"CreateIntegrationResult>Status,omitempty"`
	SourceArn              string   `xml:"CreateIntegrationResult>SourceArn,omitempty"`
	TargetArn              string   `xml:"CreateIntegrationResult>TargetArn,omitempty"`
	DataFilter             string   `xml:"CreateIntegrationResult>DataFilter,omitempty"`
	IntegrationDescription string   `xml:"CreateIntegrationResult>Description,omitempty"`
}

type deleteIntegrationResponse struct {
	XMLName         xml.Name `xml:"DeleteIntegrationResponse"`
	Xmlns           string   `xml:"xmlns,attr"`
	IntegrationName string   `xml:"DeleteIntegrationResult>IntegrationName"`
	IntegrationArn  string   `xml:"DeleteIntegrationResult>IntegrationArn,omitempty"`
	Status          string   `xml:"DeleteIntegrationResult>Status,omitempty"`
	SourceArn       string   `xml:"DeleteIntegrationResult>SourceArn,omitempty"`
	TargetArn       string   `xml:"DeleteIntegrationResult>TargetArn,omitempty"`
}

type describeIntegrationsResponse struct {
	XMLName      xml.Name           `xml:"DescribeIntegrationsResponse"`
	Xmlns        string             `xml:"xmlns,attr"`
	Marker       string             `xml:"DescribeIntegrationsResult>Marker,omitempty"`
	Integrations xmlIntegrationList `xml:"DescribeIntegrationsResult>Integrations"`
}

type modifyIntegrationResponse struct {
	XMLName                xml.Name `xml:"ModifyIntegrationResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	IntegrationName        string   `xml:"ModifyIntegrationResult>IntegrationName"`
	IntegrationArn         string   `xml:"ModifyIntegrationResult>IntegrationArn,omitempty"`
	Status                 string   `xml:"ModifyIntegrationResult>Status,omitempty"`
	SourceArn              string   `xml:"ModifyIntegrationResult>SourceArn,omitempty"`
	TargetArn              string   `xml:"ModifyIntegrationResult>TargetArn,omitempty"`
	DataFilter             string   `xml:"ModifyIntegrationResult>DataFilter,omitempty"`
	IntegrationDescription string   `xml:"ModifyIntegrationResult>Description,omitempty"`
}

func toXMLIntegration(intg *Integration) xmlIntegration {
	return xmlIntegration{
		IntegrationName:        intg.IntegrationName,
		IntegrationArn:         intg.IntegrationArn,
		SourceArn:              intg.SourceArn,
		TargetArn:              intg.TargetArn,
		Status:                 intg.Status,
		DataFilter:             intg.DataFilter,
		IntegrationDescription: intg.IntegrationDescription,
	}
}

func (h *Handler) handleCreateIntegration(vals url.Values) (any, error) {
	name := vals.Get("IntegrationName")
	sourceARN := vals.Get("SourceArn")
	targetARN := vals.Get("TargetArn")
	kmsKeyID := vals.Get("KMSKeyId")
	dataFilter := vals.Get("DataFilter")
	description := vals.Get("Description")

	intg, err := h.Backend.CreateIntegration(name, sourceARN, targetARN, kmsKeyID, dataFilter, description)
	if err != nil {
		return nil, err
	}

	return &createIntegrationResponse{
		Xmlns:                  rdsXMLNS,
		IntegrationName:        intg.IntegrationName,
		IntegrationArn:         intg.IntegrationArn,
		Status:                 intg.Status,
		SourceArn:              intg.SourceArn,
		TargetArn:              intg.TargetArn,
		DataFilter:             intg.DataFilter,
		IntegrationDescription: intg.IntegrationDescription,
	}, nil
}

func (h *Handler) handleDeleteIntegration(vals url.Values) (any, error) {
	identifier := vals.Get("IntegrationIdentifier")

	intg, err := h.Backend.DeleteIntegration(identifier)
	if err != nil {
		return nil, err
	}

	return &deleteIntegrationResponse{
		Xmlns:           rdsXMLNS,
		IntegrationName: intg.IntegrationName,
		IntegrationArn:  intg.IntegrationArn,
		Status:          intg.Status,
		SourceArn:       intg.SourceArn,
		TargetArn:       intg.TargetArn,
	}, nil
}

func (h *Handler) handleDescribeIntegrations(vals url.Values) (any, error) {
	identifier := vals.Get("IntegrationIdentifier")

	integrations, err := h.Backend.DescribeIntegrations(identifier)
	if err != nil {
		return nil, err
	}

	members, marker, err := paginateDescribe(
		vals, integrations,
		func(a, b Integration) bool { return a.IntegrationName < b.IntegrationName },
		func(intg Integration) xmlIntegration { return toXMLIntegration(&intg) },
	)
	if err != nil {
		return nil, err
	}

	return &describeIntegrationsResponse{
		Xmlns:        rdsXMLNS,
		Marker:       marker,
		Integrations: xmlIntegrationList{Members: members},
	}, nil
}

func (h *Handler) handleModifyIntegration(vals url.Values) (any, error) {
	identifier := vals.Get("IntegrationIdentifier")
	dataFilter := vals.Get("DataFilter")
	description := vals.Get("Description")

	intg, err := h.Backend.ModifyIntegration(identifier, dataFilter, description)
	if err != nil {
		return nil, err
	}

	return &modifyIntegrationResponse{
		Xmlns:                  rdsXMLNS,
		IntegrationName:        intg.IntegrationName,
		IntegrationArn:         intg.IntegrationArn,
		Status:                 intg.Status,
		SourceArn:              intg.SourceArn,
		TargetArn:              intg.TargetArn,
		DataFilter:             intg.DataFilter,
		IntegrationDescription: intg.IntegrationDescription,
	}, nil
}
