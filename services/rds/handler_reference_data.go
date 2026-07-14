package rds

import (
	"encoding/xml"
	"net/url"
)

type xmlAccountAttribute struct {
	AttributeName string `xml:"AttributeName"`
	Used          int    `xml:"AccountQuotaName"`
	Max           int    `xml:"Max"`
}

type xmlAccountAttributeList struct {
	Members []xmlAccountAttribute `xml:"AccountQuota"`
}

type describeAccountAttributesResponse struct {
	XMLName           xml.Name                `xml:"DescribeAccountAttributesResponse"`
	Xmlns             string                  `xml:"xmlns,attr"`
	AccountAttributes xmlAccountAttributeList `xml:"DescribeAccountAttributesResult>AccountQuotas"`
}

type xmlCertificate struct {
	CertificateIdentifier string `xml:"CertificateIdentifier"`
	CertificateType       string `xml:"CertificateType"`
	ValidFrom             string `xml:"ValidFrom,omitempty"`
	ValidTill             string `xml:"ValidTill,omitempty"`
	Thumbprint            string `xml:"Thumbprint,omitempty"`
	CustomerOverride      bool   `xml:"CustomerOverride,omitempty"`
}

type xmlCertificateList struct {
	Members []xmlCertificate `xml:"Certificate"`
}

type describeCertificatesResponse struct {
	XMLName      xml.Name           `xml:"DescribeCertificatesResponse"`
	Xmlns        string             `xml:"xmlns,attr"`
	Certificates xmlCertificateList `xml:"DescribeCertificatesResult>Certificates"`
}

type modifyCertificatesResponse struct {
	XMLName     xml.Name       `xml:"ModifyCertificatesResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	Certificate xmlCertificate `xml:"ModifyCertificatesResult>Certificate"`
}

type xmlSourceRegion struct {
	RegionName string `xml:"RegionName"`
	Endpoint   string `xml:"Endpoint,omitempty"`
	Status     string `xml:"Status,omitempty"`
}

type xmlSourceRegionList struct {
	Members []xmlSourceRegion `xml:"SourceRegion"`
}

type describeSourceRegionsResponse struct {
	XMLName       xml.Name            `xml:"DescribeSourceRegionsResponse"`
	Xmlns         string              `xml:"xmlns,attr"`
	SourceRegions xmlSourceRegionList `xml:"DescribeSourceRegionsResult>SourceRegions"`
}

type xmlDBMajorEngineVersion struct {
	Engine             string `xml:"Engine"`
	MajorEngineVersion string `xml:"MajorEngineVersion"`
	Status             string `xml:"Status,omitempty"`
}

type xmlDBMajorEngineVersionList struct {
	Members []xmlDBMajorEngineVersion `xml:"DBMajorEngineVersion"`
}

type describeDBMajorEngineVersionsResponse struct {
	XMLName               xml.Name                    `xml:"DescribeDBMajorEngineVersionsResponse"`
	Xmlns                 string                      `xml:"xmlns,attr"`
	DBMajorEngineVersions xmlDBMajorEngineVersionList `xml:"DescribeDBMajorEngineVersionsResult>DBMajorEngineVersions"`
}

func (h *Handler) handleDescribeAccountAttributes(_ url.Values) (any, error) {
	attrs := h.Backend.DescribeAccountAttributes()
	members := make([]xmlAccountAttribute, 0, len(attrs))
	for _, a := range attrs {
		members = append(members, xmlAccountAttribute(a))
	}

	return &describeAccountAttributesResponse{
		Xmlns:             rdsXMLNS,
		AccountAttributes: xmlAccountAttributeList{Members: members},
	}, nil
}

func (h *Handler) handleDescribeCertificates(vals url.Values) (any, error) {
	certID := vals.Get("CertificateIdentifier")
	certs, err := h.Backend.DescribeCertificates(certID)
	if err != nil {
		return nil, err
	}
	members := make([]xmlCertificate, 0, len(certs))
	for _, c := range certs {
		members = append(members, toXMLCertificate(c))
	}

	return &describeCertificatesResponse{
		Xmlns:        rdsXMLNS,
		Certificates: xmlCertificateList{Members: members},
	}, nil
}

func (h *Handler) handleModifyCertificates(vals url.Values) (any, error) {
	certID := vals.Get("CertificateIdentifier")
	cert, err := h.Backend.ModifyCertificates(certID)
	if err != nil {
		return nil, err
	}

	return &modifyCertificatesResponse{
		Xmlns:       rdsXMLNS,
		Certificate: toXMLCertificate(*cert),
	}, nil
}

func (h *Handler) handleDescribeSourceRegions(vals url.Values) (any, error) {
	regionName := vals.Get("RegionName")
	regions := h.Backend.DescribeSourceRegions(regionName)
	members := make([]xmlSourceRegion, 0, len(regions))
	for _, r := range regions {
		members = append(members, xmlSourceRegion(r))
	}

	return &describeSourceRegionsResponse{
		Xmlns:         rdsXMLNS,
		SourceRegions: xmlSourceRegionList{Members: members},
	}, nil
}

func (h *Handler) handleDescribeDBMajorEngineVersions(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	versions := h.Backend.DescribeDBMajorEngineVersions(engine)
	members := make([]xmlDBMajorEngineVersion, 0, len(versions))
	for _, v := range versions {
		members = append(members, xmlDBMajorEngineVersion(v))
	}

	return &describeDBMajorEngineVersionsResponse{
		Xmlns:                 rdsXMLNS,
		DBMajorEngineVersions: xmlDBMajorEngineVersionList{Members: members},
	}, nil
}

func toXMLCertificate(c Certificate) xmlCertificate {
	return xmlCertificate{
		CertificateIdentifier: c.CertificateIdentifier,
		CertificateType:       c.CertificateType,
		ValidFrom:             c.ValidFrom,
		ValidTill:             c.ValidTill,
		Thumbprint:            c.Thumbprint,
		CustomerOverride:      c.CustomerOverride,
	}
}
