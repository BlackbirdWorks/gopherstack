package redshift

import (
	"encoding/xml"
	"net/url"
)

const modelVersion10 = "1.0"

// ---- DescribeAccountAttributes ----

type xmlAccountAttribute struct {
	AttributeName   string `xml:"AttributeName"`
	AttributeValues string `xml:"AttributeValues>AttributeValueTarget>AttributeValue,omitempty"`
}

type xmlAccountAttributeList struct {
	Attributes []xmlAccountAttribute `xml:"AccountAttribute,omitempty"`
}

type xmlDescribeAccountAttributesResult struct {
	AccountAttributeList xmlAccountAttributeList `xml:"AccountAttributeList"`
}

type describeAccountAttributesResponse struct {
	XMLName xml.Name                           `xml:"DescribeAccountAttributesResponse"`
	Xmlns   string                             `xml:"xmlns,attr"`
	Result  xmlDescribeAccountAttributesResult `xml:"DescribeAccountAttributesResult"`
}

func (h *Handler) handleDescribeAccountAttributes(_ url.Values) (any, error) {
	return &describeAccountAttributesResponse{Xmlns: redshiftXMLNS}, nil
}

// ---- DescribeClusterTracks ----

type xmlMaintenanceTrack struct {
	MaintenanceTrackName string `xml:"MaintenanceTrackName"`
	DatabaseVersion      string `xml:"DatabaseVersion,omitempty"`
}

type xmlMaintenanceTracks struct {
	Tracks []xmlMaintenanceTrack `xml:"MaintenanceTrack,omitempty"`
}

type xmlDescribeClusterTracksResult struct {
	MaintenanceTracks xmlMaintenanceTracks `xml:"MaintenanceTracks"`
}

type describeClusterTracksResponse struct {
	XMLName xml.Name                       `xml:"DescribeClusterTracksResponse"`
	Xmlns   string                         `xml:"xmlns,attr"`
	Result  xmlDescribeClusterTracksResult `xml:"DescribeClusterTracksResult"`
}

func (h *Handler) handleDescribeClusterTracks(_ url.Values) (any, error) {
	return &describeClusterTracksResponse{
		Xmlns: redshiftXMLNS,
		Result: xmlDescribeClusterTracksResult{
			MaintenanceTracks: xmlMaintenanceTracks{
				Tracks: []xmlMaintenanceTrack{
					{MaintenanceTrackName: "current", DatabaseVersion: modelVersion10},
					{MaintenanceTrackName: "trailing", DatabaseVersion: modelVersion10},
				},
			},
		},
	}, nil
}

// ---- DescribeClusterVersions ----

type xmlClusterVersion struct {
	ClusterVersion              string `xml:"ClusterVersion"`
	ClusterParameterGroupFamily string `xml:"ClusterParameterGroupFamily"`
	Description                 string `xml:"Description,omitempty"`
}

type xmlClusterVersionList struct {
	Versions []xmlClusterVersion `xml:"ClusterVersion,omitempty"`
}

type xmlDescribeClusterVersionsResult struct {
	ClusterVersions xmlClusterVersionList `xml:"ClusterVersions"`
}

type describeClusterVersionsResponse struct {
	XMLName xml.Name                         `xml:"DescribeClusterVersionsResponse"`
	Xmlns   string                           `xml:"xmlns,attr"`
	Result  xmlDescribeClusterVersionsResult `xml:"DescribeClusterVersionsResult"`
}

func (h *Handler) handleDescribeClusterVersions(_ url.Values) (any, error) {
	return &describeClusterVersionsResponse{
		Xmlns: redshiftXMLNS,
		Result: xmlDescribeClusterVersionsResult{
			ClusterVersions: xmlClusterVersionList{
				Versions: []xmlClusterVersion{
					{
						ClusterVersion:              modelVersion10,
						ClusterParameterGroupFamily: "redshift-1.0",
						Description:                 "Amazon Redshift 1.0",
					},
				},
			},
		},
	}, nil
}

// ---- DescribeOrderableClusterOptions ----

type xmlOrderableClusterOption struct {
	ClusterVersion string `xml:"ClusterVersion"`
	ClusterType    string `xml:"ClusterType"`
	NodeType       string `xml:"NodeType"`
}

type xmlOrderableClusterOptionList struct {
	Options []xmlOrderableClusterOption `xml:"OrderableClusterOption,omitempty"`
}

type xmlDescribeOrderableClusterOptionsResult struct {
	OrderableClusterOptions xmlOrderableClusterOptionList `xml:"OrderableClusterOptions"`
}

type describeOrderableClusterOptionsResponse struct {
	XMLName xml.Name                                 `xml:"DescribeOrderableClusterOptionsResponse"`
	Xmlns   string                                   `xml:"xmlns,attr"`
	Result  xmlDescribeOrderableClusterOptionsResult `xml:"DescribeOrderableClusterOptionsResult"`
}

func (h *Handler) handleDescribeOrderableClusterOptions(_ url.Values) (any, error) {
	return &describeOrderableClusterOptionsResponse{
		Xmlns: redshiftXMLNS,
		Result: xmlDescribeOrderableClusterOptionsResult{
			OrderableClusterOptions: xmlOrderableClusterOptionList{
				Options: []xmlOrderableClusterOption{
					{ClusterVersion: modelVersion10, ClusterType: "multi-node", NodeType: defaultNodeType},
					{ClusterVersion: modelVersion10, ClusterType: "single-node", NodeType: defaultNodeType},
					{ClusterVersion: modelVersion10, ClusterType: "multi-node", NodeType: nodeTypeDC28xlarge},
					{ClusterVersion: modelVersion10, ClusterType: "single-node", NodeType: nodeTypeDC28xlarge},
				},
			},
		},
	}, nil
}

// ---- DescribeStorage ----

type describeStorageResponse struct {
	XMLName                            xml.Name `xml:"DescribeStorageResponse"`
	Xmlns                              string   `xml:"xmlns,attr"`
	TotalBackupSizeInMegaBytes         float64  `xml:"DescribeStorageResult>TotalBackupSizeInMegaBytes"`
	TotalProvisionedStorageInMegaBytes float64  `xml:"DescribeStorageResult>TotalProvisionedStorageInMegaBytes"`
}

func (h *Handler) handleDescribeStorage(_ url.Values) (any, error) {
	return &describeStorageResponse{Xmlns: redshiftXMLNS}, nil
}
