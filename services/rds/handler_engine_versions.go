package rds

import (
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleDescribeDBEngineVersions(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")
	versions := h.Backend.DescribeDBEngineVersions(engine, engineVersion)
	members := make([]xmlDBEngineVersion, 0, len(versions))
	for _, v := range versions {
		members = append(members, xmlDBEngineVersion(v))
	}

	return &describeDBEngineVersionsResponse{
		Xmlns:            rdsXMLNS,
		DBEngineVersions: xmlDBEngineVersionList{Members: members},
	}, nil
}

func (h *Handler) handleDescribeOrderableDBInstanceOptions(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")
	options := h.Backend.DescribeOrderableDBInstanceOptions(engine, engineVersion)
	members := make([]xmlOrderableDBInstanceOption, 0, len(options))
	for _, o := range options {
		members = append(members, xmlOrderableDBInstanceOption(o))
	}

	return &describeOrderableDBInstanceOptionsResponse{
		Xmlns: rdsXMLNS,
		Result: describeOrderableDBInstanceOptionsResult{
			OrderableDBInstanceOptions: xmlOrderableDBInstanceOptionList{Members: members},
		},
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
type createCustomDBEngineVersionResponse struct {
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

	cev, err := h.Backend.CreateCustomDBEngineVersion(engine, engineVersion, description)
	if err != nil {
		return nil, err
	}

	h.applyCreateTags(vals, cev.DBEngineVersionArn)

	return &createCustomDBEngineVersionResponse{
		Xmlns:                      rdsXMLNS,
		Engine:                     cev.Engine,
		EngineVersion:              cev.EngineVersion,
		DBEngineVersionArn:         cev.DBEngineVersionArn,
		Status:                     cev.Status,
		DBEngineVersionDescription: cev.Description,
	}, nil
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
