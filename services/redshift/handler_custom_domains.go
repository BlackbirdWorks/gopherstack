package redshift

import (
	"encoding/xml"
	"net/url"
)

// ----- Custom Domain Association -----

// createCustomDomainAssociationResult mirrors CreateCustomDomainAssociationOutput,
// including CustomDomainCertExpiryTime (confirmed present against
// aws-sdk-go-v2/service/redshift@v1.65.4/api_op_CreateCustomDomainAssociation.go).
type createCustomDomainAssociationResult struct {
	CustomDomainName           string `xml:"CustomDomainName"`
	CustomDomainCertificateArn string `xml:"CustomDomainCertificateArn"`
	CustomDomainCertExpiryTime string `xml:"CustomDomainCertExpiryTime"`
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
			CustomDomainCertExpiryTime: assoc.CustomDomainCertExpiryTime,
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

// redshift@v1.65.4 deserializers.go:49708,23893 names this field Associations,
// each entry wrapped in <Association>, not <CustomDomainAssociations>/<CustomDomainAssociation>.
type describeCustomDomainAssociationsResponse struct {
	XMLName xml.Name `xml:"DescribeCustomDomainAssociationsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		CustomDomainAssociations []customDomainAssociation `xml:"Associations>Association"`
	} `xml:"DescribeCustomDomainAssociationsResult"`
}

func (h *Handler) handleDescribeCustomDomainAssociations(vals url.Values) (any, error) {
	certificateArn := vals.Get("CustomDomainCertificateArn")

	assocs, err := h.Backend.DescribeCustomDomainAssociations(
		vals.Get("ClusterIdentifier"),
		vals.Get("CustomDomainName"),
	)
	if err != nil {
		return nil, err
	}

	members := make([]customDomainAssociation, 0, len(assocs))

	for _, a := range assocs {
		if certificateArn != "" && a.CustomDomainCertificateArn != certificateArn {
			continue
		}

		members = append(members, customDomainAssociation{
			ClusterIdentifier:          a.ClusterIdentifier,
			CustomDomainName:           a.CustomDomainName,
			CustomDomainCertificateArn: a.CustomDomainCertificateArn,
		})
	}

	resp := &describeCustomDomainAssociationsResponse{Xmlns: redshiftXMLNS}
	resp.Result.CustomDomainAssociations = members

	return resp, nil
}

// modifyCustomDomainAssociationResponse mirrors ModifyCustomDomainAssociationOutput,
// including CustomDomainCertExpiryTime (see createCustomDomainAssociationResult).
type modifyCustomDomainAssociationResponse struct {
	XMLName xml.Name `xml:"ModifyCustomDomainAssociationResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		ClusterIdentifier          string `xml:"ClusterIdentifier"`
		CustomDomainName           string `xml:"CustomDomainName"`
		CustomDomainCertificateArn string `xml:"CustomDomainCertificateArn"`
		CustomDomainCertExpiryTime string `xml:"CustomDomainCertExpiryTime"`
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
	resp.Result.CustomDomainCertExpiryTime = assoc.CustomDomainCertExpiryTime

	return resp, nil
}
