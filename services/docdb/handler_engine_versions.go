package docdb

import (
	"context"
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleDescribeDBEngineVersions(ctx context.Context, vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")
	defaultOnly := vals.Get("DefaultOnly") == stringTrue
	versions := h.Backend.DescribeDBEngineVersions(ctx, engine, engineVersion, defaultOnly)
	members := make([]xmlDBEngineVersion, 0, len(versions))
	for _, v := range versions {
		members = append(members, xmlDBEngineVersion(v))
	}

	members, nextMarker := applyDocDBMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBEngineVersionsResponse{
		Xmlns:            docdbXMLNS,
		Marker:           nextMarker,
		DBEngineVersions: xmlDBEngineVersionList{Members: members},
	}, nil
}

type xmlDBEngineVersion struct {
	Engine              string `xml:"Engine"`
	EngineVersion       string `xml:"EngineVersion"`
	DBEngineDescription string `xml:"DBEngineDescription"`
}

type xmlDBEngineVersionList struct {
	Members []xmlDBEngineVersion `xml:"DBEngineVersion"`
}

type describeDBEngineVersionsResponse struct {
	XMLName          xml.Name               `xml:"DescribeDBEngineVersionsResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	Marker           string                 `xml:"DescribeDBEngineVersionsResult>Marker,omitempty"`
	DBEngineVersions xmlDBEngineVersionList `xml:"DescribeDBEngineVersionsResult>DBEngineVersions"`
}
