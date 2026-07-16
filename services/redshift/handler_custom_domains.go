package redshift

import (
	"encoding/xml"
	"net/url"
)

// ----- Custom Domain Association -----

type createCustomDomainAssociationResult struct {
	CustomDomainName           string `xml:"CustomDomainName"`
	CustomDomainCertificateArn string `xml:"CustomDomainCertificateArn"`
	ClusterIdentifier          string `xml:"ClusterIdentifier"`
}

type createCustomDomainAssociationResponse struct {
	XMLName xml.Name                            `xml:"CreateCustomDomainAssociationResponse"`
	Xmlns   string                              `xml:"xmlns,attr"`
	Result  createCustomDomainAssociationResult `xml:"CreateCustomDomainAssociationResult"`
}

func (h *Handler) handleCreateCustomDomainAssociation(vals url.Values) (any, error) {
	assoc, err := h.Backend.CreateCustomDomainAssociation(
		vals.Get("ClusterIdentifier"),
		vals.Get("CustomDomainName"),
		vals.Get("CustomDomainCertificateArn"),
	)
	if err != nil {
		return nil, err
	}

	return &createCustomDomainAssociationResponse{
		Xmlns: redshiftXMLNS,
		Result: createCustomDomainAssociationResult{
			ClusterIdentifier:          assoc.ClusterIdentifier,
			CustomDomainName:           assoc.CustomDomainName,
			CustomDomainCertificateArn: assoc.CustomDomainCertificateArn,
		},
	}, nil
}

type deleteCustomDomainAssociationResponse struct {
	XMLName xml.Name `xml:"DeleteCustomDomainAssociationResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleDeleteCustomDomainAssociation(vals url.Values) (any, error) {
	if err := h.Backend.DeleteCustomDomainAssociation(
		vals.Get("ClusterIdentifier"),
		vals.Get("CustomDomainName"),
	); err != nil {
		return nil, err
	}

	return &deleteCustomDomainAssociationResponse{Xmlns: redshiftXMLNS}, nil
}

type customDomainAssociation struct {
	ClusterIdentifier          string `xml:"ClusterIdentifier"`
	CustomDomainName           string `xml:"CustomDomainName"`
	CustomDomainCertificateArn string `xml:"CustomDomainCertificateArn"`
}

type describeCustomDomainAssociationsResponse struct {
	XMLName xml.Name `xml:"DescribeCustomDomainAssociationsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		CustomDomainAssociations []customDomainAssociation `xml:"CustomDomainAssociations>CustomDomainAssociation"`
	} `xml:"DescribeCustomDomainAssociationsResult"`
}

func (h *Handler) handleDescribeCustomDomainAssociations(vals url.Values) (any, error) {
	assocs, err := h.Backend.DescribeCustomDomainAssociations(
		vals.Get("ClusterIdentifier"),
		vals.Get("CustomDomainName"),
	)
	if err != nil {
		return nil, err
	}

	members := make([]customDomainAssociation, 0, len(assocs))

	for _, a := range assocs {
		members = append(members, customDomainAssociation(a))
	}

	resp := &describeCustomDomainAssociationsResponse{Xmlns: redshiftXMLNS}
	resp.Result.CustomDomainAssociations = members

	return resp, nil
}

type modifyCustomDomainAssociationResponse struct {
	XMLName xml.Name `xml:"ModifyCustomDomainAssociationResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		ClusterIdentifier          string `xml:"ClusterIdentifier"`
		CustomDomainName           string `xml:"CustomDomainName"`
		CustomDomainCertificateArn string `xml:"CustomDomainCertificateArn"`
	} `xml:"ModifyCustomDomainAssociationResult"`
}

func (h *Handler) handleModifyCustomDomainAssociation(vals url.Values) (any, error) {
	assoc, err := h.Backend.ModifyCustomDomainAssociation(
		vals.Get("ClusterIdentifier"),
		vals.Get("CustomDomainName"),
		vals.Get("CustomDomainCertificateArn"),
	)
	if err != nil {
		return nil, err
	}

	resp := &modifyCustomDomainAssociationResponse{Xmlns: redshiftXMLNS}
	resp.Result.ClusterIdentifier = assoc.ClusterIdentifier
	resp.Result.CustomDomainName = assoc.CustomDomainName
	resp.Result.CustomDomainCertificateArn = assoc.CustomDomainCertificateArn

	return resp, nil
}
