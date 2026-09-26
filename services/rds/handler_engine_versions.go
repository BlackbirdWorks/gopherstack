package rds

import (
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleDescribeDBEngineVersions(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")
	versions := h.Backend.DescribeDBEngineVersions(engine, engineVersion)
	versions, err := applyDBEngineVersionFilters(vals, versions)
	if err != nil {
		return nil, err
	}
	if vals.Get("DefaultOnly") == formTrue {
		filtered := make([]DBEngineVersion, 0, len(versions))
		for _, v := range versions {
			if v.IsDefault {
				filtered = append(filtered, v)
			}
		}
		versions = filtered
	}
	members, marker, err := paginateDescribe(vals, versions, func(a, b DBEngineVersion) bool {
		if a.Engine == b.Engine {
			return a.EngineVersion < b.EngineVersion
		}

		return a.Engine < b.Engine
	}, func(v DBEngineVersion) xmlDBEngineVersion {
		out := xmlDBEngineVersion{
			Engine:              v.Engine,
			EngineVersion:       v.EngineVersion,
			DBEngineDescription: v.DBEngineDescription,
			Status:              v.Status,
		}
		if v.ImageID != "" {
			out.Image = &xmlCustomDBEngineVersionAMI{ImageID: v.ImageID, Status: v.Status}
		}

		return out
	})
	if err != nil {
		return nil, err
	}

	return &describeDBEngineVersionsResponse{
		Xmlns:            rdsXMLNS,
		Marker:           marker,
		DBEngineVersions: xmlDBEngineVersionList{Members: members},
	}, nil
}

func (h *Handler) handleDescribeOrderableDBInstanceOptions(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")
	options := h.Backend.DescribeOrderableDBInstanceOptions(engine, engineVersion)
	members, marker, err := paginateDescribe(vals, options, func(a, b OrderableDBInstanceOption) bool {
		if a.Engine != b.Engine {
			return a.Engine < b.Engine
		}
		if a.EngineVersion != b.EngineVersion {
			return a.EngineVersion < b.EngineVersion
		}

		return a.DBInstanceClass < b.DBInstanceClass
	}, func(o OrderableDBInstanceOption) xmlOrderableDBInstanceOption {
		return xmlOrderableDBInstanceOption(o)
	})
	if err != nil {
		return nil, err
	}

	return &describeOrderableDBInstanceOptionsResponse{
		Xmlns: rdsXMLNS,
		Result: describeOrderableDBInstanceOptionsResult{
			Marker:                     marker,
			OrderableDBInstanceOptions: xmlOrderableDBInstanceOptionList{Members: members},
		},
	}, nil
}

type xmlDBEngineVersion struct {
	Image               *xmlCustomDBEngineVersionAMI `xml:"Image,omitempty"`
	Engine              string                       `xml:"Engine"`
	EngineVersion       string                       `xml:"EngineVersion"`
	DBEngineDescription string                       `xml:"DBEngineDescription"`
	Status              string                       `xml:"Status,omitempty"`
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

type xmlOrderableDBInstanceOption struct {
	Engine          string `xml:"Engine"`
	EngineVersion   string `xml:"EngineVersion"`
	DBInstanceClass string `xml:"DBInstanceClass"`
	MultiAZCapable  bool   `xml:"MultiAZCapable"`
}

type xmlOrderableDBInstanceOptionList struct {
	Members []xmlOrderableDBInstanceOption `xml:"OrderableDBInstanceOption"`
}

type describeOrderableDBInstanceOptionsResult struct {
	Marker                     string                           `xml:"Marker,omitempty"`
	OrderableDBInstanceOptions xmlOrderableDBInstanceOptionList `xml:"OrderableDBInstanceOptions"`
}

type describeOrderableDBInstanceOptionsResponse struct {
	XMLName xml.Name                                 `xml:"DescribeOrderableDBInstanceOptionsResponse"`
	Xmlns   string                                   `xml:"xmlns,attr"`
	Result  describeOrderableDBInstanceOptionsResult `xml:"DescribeOrderableDBInstanceOptionsResult"`
}

// CreateCustomDBEngineVersionOutput, DeleteCustomDBEngineVersionOutput, and
// ModifyCustomDBEngineVersionOutput are all flat shapes in the real RDS API — the
// Engine/EngineVersion/Status/DBEngineVersionDescription members sit directly under
// the <XxxResult> element, there is no nested <CustomDBEngineVersion> wrapper (unlike
// e.g. CreateDBInstanceOutput, which does nest under <DBInstance>). Each field below
// therefore carries the full result-element chain individually instead of nesting
// through a shared struct, matching the pattern already used for e.g.
// ModifyCurrentDBClusterCapacityResult below.
type xmlCustomDBEngineVersionAMI struct {
	ImageID string `xml:"ImageId,omitempty"`
	Status  string `xml:"Status,omitempty"`
}

type createCustomDBEngineVersionResponse struct {
	Image *xmlCustomDBEngineVersionAMI `xml:"CreateCustomDBEngineVersionResult>Image,omitempty"`

	XMLName                    xml.Name `xml:"CreateCustomDBEngineVersionResponse"`
	Xmlns                      string   `xml:"xmlns,attr"`
	Engine                     string   `xml:"CreateCustomDBEngineVersionResult>Engine"`
	EngineVersion              string   `xml:"CreateCustomDBEngineVersionResult>EngineVersion"`
	DBEngineVersionArn         string   `xml:"CreateCustomDBEngineVersionResult>DBEngineVersionArn,omitempty"`
	Status                     string   `xml:"CreateCustomDBEngineVersionResult>Status,omitempty"`
	DBEngineVersionDescription string   `xml:"CreateCustomDBEngineVersionResult>DBEngineVersionDescription,omitempty"`
}

type deleteCustomDBEngineVersionResponse struct {
	XMLName       xml.Name `xml:"DeleteCustomDBEngineVersionResponse"`
	Xmlns         string   `xml:"xmlns,attr"`
	Engine        string   `xml:"DeleteCustomDBEngineVersionResult>Engine"`
	EngineVersion string   `xml:"DeleteCustomDBEngineVersionResult>EngineVersion"`
	Status        string   `xml:"DeleteCustomDBEngineVersionResult>Status,omitempty"`
}

type modifyCustomDBEngineVersionResponse struct {
	XMLName                    xml.Name `xml:"ModifyCustomDBEngineVersionResponse"`
	Xmlns                      string   `xml:"xmlns,attr"`
	Engine                     string   `xml:"ModifyCustomDBEngineVersionResult>Engine"`
	EngineVersion              string   `xml:"ModifyCustomDBEngineVersionResult>EngineVersion"`
	Status                     string   `xml:"ModifyCustomDBEngineVersionResult>Status,omitempty"`
	DBEngineVersionDescription string   `xml:"ModifyCustomDBEngineVersionResult>DBEngineVersionDescription,omitempty"`
}

func (h *Handler) handleCreateCustomDBEngineVersion(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")
	description := vals.Get("Description")
	imageID := vals.Get("ImageId")

	cev, err := h.Backend.CreateCustomDBEngineVersion(engine, engineVersion, description, imageID)
	if err != nil {
		return nil, err
	}

	h.applyCreateTags(vals, cev.DBEngineVersionArn)

	resp := &createCustomDBEngineVersionResponse{
		Xmlns:                      rdsXMLNS,
		Engine:                     cev.Engine,
		EngineVersion:              cev.EngineVersion,
		DBEngineVersionArn:         cev.DBEngineVersionArn,
		Status:                     cev.Status,
		DBEngineVersionDescription: cev.Description,
	}
	if cev.ImageID != "" {
		resp.Image = &xmlCustomDBEngineVersionAMI{ImageID: cev.ImageID, Status: cev.Status}
	}

	return resp, nil
}

func (h *Handler) handleDeleteCustomDBEngineVersion(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")

	cev, err := h.Backend.DeleteCustomDBEngineVersion(engine, engineVersion)
	if err != nil {
		return nil, err
	}

	return &deleteCustomDBEngineVersionResponse{
		Xmlns:         rdsXMLNS,
		Engine:        cev.Engine,
		EngineVersion: cev.EngineVersion,
		Status:        cev.Status,
	}, nil
}

func (h *Handler) handleModifyCustomDBEngineVersion(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")
	description := vals.Get("Description")
	status := vals.Get("Status")

	cev, err := h.Backend.ModifyCustomDBEngineVersion(engine, engineVersion, description, status)
	if err != nil {
		return nil, err
	}

	return &modifyCustomDBEngineVersionResponse{
		Xmlns:                      rdsXMLNS,
		Engine:                     cev.Engine,
		EngineVersion:              cev.EngineVersion,
		Status:                     cev.Status,
		DBEngineVersionDescription: cev.Description,
	}, nil
}
