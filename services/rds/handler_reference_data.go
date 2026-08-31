package rds

import (
	"encoding/xml"
	"net/url"
)

type xmlAccountAttribute struct {
	AttributeName string `xml:"AccountQuotaName"`
	Used          int    `xml:"Used"`
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

type xmlServerlessV2FeaturesSupport struct {
	MinCapacity float64 `xml:"MinCapacity"`
	MaxCapacity float64 `xml:"MaxCapacity"`
}

type xmlServerlessV2PlatformVersionInfo struct {
	ServerlessV2FeaturesSupport *xmlServerlessV2FeaturesSupport `xml:"ServerlessV2FeaturesSupport,omitempty"`
	Engine                      string                          `xml:"Engine,omitempty"`
	ServerlessV2PlatformVersion string                          `xml:"ServerlessV2PlatformVersion,omitempty"`
	VersionDescription          string                          `xml:"ServerlessV2PlatformVersionDescription,omitempty"`
	Status                      string                          `xml:"Status,omitempty"`
	IsDefault                   bool                            `xml:"IsDefault,omitempty"`
}

// xmlServerlessV2VersionList wraps the ServerlessV2PlatformVersionInfo member list.
// The list item element is the smithy default "member", not the shape name
// (rds@v1.124.1 deserializers.go: awsAwsquery_deserializeDocumentServerlessV2PlatformVersionList).
type xmlServerlessV2VersionList struct {
	Members []xmlServerlessV2PlatformVersionInfo `xml:"member"`
}

type describeServerlessV2PlatformVersionsResponse struct {
	XMLName  xml.Name                   `xml:"DescribeServerlessV2PlatformVersionsResponse"`
	Xmlns    string                     `xml:"xmlns,attr"`
	Marker   string                     `xml:"DescribeServerlessV2PlatformVersionsResult>Marker,omitempty"`
	Versions xmlServerlessV2VersionList `xml:"DescribeServerlessV2PlatformVersionsResult>ServerlessV2PlatformVersions"`
}

// handleDescribeServerlessV2PlatformVersions handles DescribeServerlessV2PlatformVersions.
// Field names verified against aws-sdk-go-v2/service/rds@v1.124.1's
// api_op_DescribeServerlessV2PlatformVersions.go (request members: DefaultOnly, Engine,
// Filters, IncludeAll, Marker, MaxRecords, ServerlessV2PlatformVersion; response
// members: Marker, ServerlessV2PlatformVersions). Filters isn't supported by this action
// per that file's own doc comment ("This parameter isn't currently supported"), so it is
// accepted-but-ignored the same way ConnectedDatabase etc. are elsewhere in this
// codebase rather than rejected -- there is no evidence real AWS errors on it either.
func (h *Handler) handleDescribeServerlessV2PlatformVersions(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	version := vals.Get("ServerlessV2PlatformVersion")
	defaultOnly := vals.Get("DefaultOnly") == formTrue
	includeAll := vals.Get("IncludeAll") == formTrue

	versions, err := h.Backend.DescribeServerlessV2PlatformVersions(engine, version, defaultOnly, includeAll)
	if err != nil {
		return nil, err
	}

	members, marker, err := paginateDescribe(
		vals, versions,
		func(a, b ServerlessV2PlatformVersionInfo) bool {
			if a.Engine == b.Engine {
				return a.ServerlessV2PlatformVersion < b.ServerlessV2PlatformVersion
			}

			return a.Engine < b.Engine
		},
		toXMLServerlessV2PlatformVersionInfo,
	)
	if err != nil {
		return nil, err
	}

	return &describeServerlessV2PlatformVersionsResponse{
		Xmlns:    rdsXMLNS,
		Marker:   marker,
		Versions: xmlServerlessV2VersionList{Members: members},
	}, nil
}

func toXMLServerlessV2PlatformVersionInfo(v ServerlessV2PlatformVersionInfo) xmlServerlessV2PlatformVersionInfo {
	x := xmlServerlessV2PlatformVersionInfo{
		Engine:                      v.Engine,
		ServerlessV2PlatformVersion: v.ServerlessV2PlatformVersion,
		VersionDescription:          v.ServerlessV2PlatformVersionDescription,
		Status:                      v.Status,
		IsDefault:                   v.IsDefault,
	}

	if v.ServerlessV2FeaturesSupport != nil {
		x.ServerlessV2FeaturesSupport = &xmlServerlessV2FeaturesSupport{
			MinCapacity: v.ServerlessV2FeaturesSupport.MinCapacity,
			MaxCapacity: v.ServerlessV2FeaturesSupport.MaxCapacity,
		}
	}

	return x
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
