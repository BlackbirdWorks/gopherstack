package docdb

import (
	"context"
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleDescribeCertificates(ctx context.Context, vals url.Values) (any, error) {
	certificateID := vals.Get("CertificateIdentifier")
	certs := h.Backend.DescribeCertificates(ctx, certificateID)
	members := make([]xmlCertificate, 0, len(certs))
	for _, c := range certs {
		cp := c
		members = append(members, toXMLCertificate(&cp))
	}

	members, nextMarker := applyDocDBMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeCertificatesResponse{
		Xmlns: docdbXMLNS,
		Result: describeCertificatesResult{
			Marker:       nextMarker,
			Certificates: xmlCertificateList{Members: members},
		},
	}, nil
}

type xmlCertificate struct {
	CertificateIdentifier string `xml:"CertificateIdentifier"`
	CertificateType       string `xml:"CertificateType"`
	Thumbprint            string `xml:"Thumbprint,omitempty"`
	ValidFrom             string `xml:"ValidFrom,omitempty"`
	ValidTill             string `xml:"ValidTill,omitempty"`
}

type xmlCertificateList struct {
	Members []xmlCertificate `xml:"Certificate"`
}

type describeCertificatesResult struct {
	Marker       string             `xml:"Marker,omitempty"`
	Certificates xmlCertificateList `xml:"Certificates"`
}

type describeCertificatesResponse struct {
	XMLName xml.Name                   `xml:"DescribeCertificatesResponse"`
	Xmlns   string                     `xml:"xmlns,attr"`
	Result  describeCertificatesResult `xml:"DescribeCertificatesResult"`
}

func toXMLCertificate(c *Certificate) xmlCertificate {
	return xmlCertificate{
		CertificateIdentifier: c.CertificateIdentifier,
		CertificateType:       c.CertificateType,
		Thumbprint:            c.Thumbprint,
		ValidFrom:             c.ValidFrom,
		ValidTill:             c.ValidTill,
	}
}
