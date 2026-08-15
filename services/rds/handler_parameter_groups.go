package rds

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

// parseParameterMembers parses Parameters.Parameter.N.ParameterName/ParameterValue form values.
func parseParameterMembers(vals url.Values) []DBParameter {
	var params []DBParameter
	for i := 1; ; i++ {
		name := vals.Get(fmt.Sprintf("Parameters.Parameter.%d.ParameterName", i))
		if name == "" {
			return params
		}
		params = append(params, DBParameter{
			ParameterName:  name,
			ParameterValue: vals.Get(fmt.Sprintf("Parameters.Parameter.%d.ParameterValue", i)),
		})
	}
}

func (h *Handler) handleCreateDBParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	family := vals.Get("DBParameterGroupFamily")
	description := vals.Get("Description")
	pg, err := h.Backend.CreateDBParameterGroup(name, family, description)
	if err != nil {
		return nil, err
	}

	h.applyCreateTags(vals, pg.DBParameterGroupArn)

	return &createDBParameterGroupResponse{
		Xmlns:            rdsXMLNS,
		DBParameterGroup: toXMLParameterGroup(pg),
	}, nil
}

func (h *Handler) handleDescribeDBParameterGroups(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	groups, err := h.Backend.DescribeDBParameterGroups(name)
	if err != nil {
		return nil, err
	}
	members, marker, err := paginateDescribe(vals, groups, func(a, b DBParameterGroup) bool {
		return a.DBParameterGroupName < b.DBParameterGroupName
	}, func(item DBParameterGroup) xmlDBParameterGroup {
		cp := item

		return toXMLParameterGroup(&cp)
	})
	if err != nil {
		return nil, err
	}

	return &describeDBParameterGroupsResponse{
		Xmlns:             rdsXMLNS,
		DBParameterGroups: xmlDBParameterGroupList{Members: members},
		Marker:            marker,
	}, nil
}

func (h *Handler) handleDeleteDBParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	if err := h.Backend.DeleteDBParameterGroup(name); err != nil {
		return nil, err
	}

	return &deleteDBParameterGroupResponse{Xmlns: rdsXMLNS}, nil
}

func (h *Handler) handleModifyDBParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	params := parseParameterMembers(vals)
	pg, err := h.Backend.ModifyDBParameterGroup(name, params)
	if err != nil {
		return nil, err
	}

	return &modifyDBParameterGroupResponse{
		Xmlns:                rdsXMLNS,
		DBParameterGroupName: pg.DBParameterGroupName,
	}, nil
}

func (h *Handler) handleDescribeDBParameters(vals url.Values) (any, error) {
	groupName := vals.Get("DBParameterGroupName")
	params, err := h.Backend.DescribeDBParameters(groupName)
	if err != nil {
		return nil, err
	}
	members, marker, err := paginateDescribe(vals, params, func(a, b DBParameter) bool {
		return a.ParameterName < b.ParameterName
	}, func(item DBParameter) xmlDBParameter {
		return xmlDBParameter(item)
	})
	if err != nil {
		return nil, err
	}

	return &describeDBParametersResponse{
		Xmlns:      rdsXMLNS,
		Parameters: xmlDBParameterList{Members: members},
		Marker:     marker,
	}, nil
}

func (h *Handler) handleResetDBParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	resetAll := vals.Get("ResetAllParameters") == formTrue
	var paramNames []string
	for i := 1; ; i++ {
		pName := vals.Get(fmt.Sprintf("Parameters.Parameter.%d.ParameterName", i))
		if pName == "" {
			break
		}
		paramNames = append(paramNames, pName)
	}
	pg, err := h.Backend.ResetDBParameterGroup(name, resetAll, paramNames)
	if err != nil {
		return nil, err
	}

	return &resetDBParameterGroupResponse{
		Xmlns:                rdsXMLNS,
		DBParameterGroupName: pg.DBParameterGroupName,
	}, nil
}

func toXMLParameterGroup(pg *DBParameterGroup) xmlDBParameterGroup {
	return xmlDBParameterGroup{
		DBParameterGroupName:   pg.DBParameterGroupName,
		DBParameterGroupArn:    pg.DBParameterGroupArn,
		DBParameterGroupFamily: pg.DBParameterGroupFamily,
		Description:            pg.Description,
	}
}

type xmlDBParameterGroup struct {
	DBParameterGroupName   string `xml:"DBParameterGroupName"`
	DBParameterGroupArn    string `xml:"DBParameterGroupArn,omitempty"`
	DBParameterGroupFamily string `xml:"DBParameterGroupFamily"`
	Description            string `xml:"Description"`
}

type xmlDBParameterGroupList struct {
	Members []xmlDBParameterGroup `xml:"DBParameterGroup"`
}

type xmlDBParameter struct {
	ParameterName  string `xml:"ParameterName"`
	ParameterValue string `xml:"ParameterValue,omitempty"`
	Description    string `xml:"Description,omitempty"`
	ApplyType      string `xml:"ApplyType,omitempty"`
	DataType       string `xml:"DataType,omitempty"`
	Source         string `xml:"Source,omitempty"`
	ApplyMethod    string `xml:"ApplyMethod,omitempty"`
	IsModifiable   bool   `xml:"IsModifiable"`
}

type xmlDBParameterList struct {
	Members []xmlDBParameter `xml:"Parameter"`
}

type createDBParameterGroupResponse struct {
	XMLName          xml.Name            `xml:"CreateDBParameterGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	DBParameterGroup xmlDBParameterGroup `xml:"CreateDBParameterGroupResult>DBParameterGroup"`
}

type describeDBParameterGroupsResponse struct {
	XMLName           xml.Name                `xml:"DescribeDBParameterGroupsResponse"`
	Xmlns             string                  `xml:"xmlns,attr"`
	Marker            string                  `xml:"DescribeDBParameterGroupsResult>Marker,omitempty"`
	DBParameterGroups xmlDBParameterGroupList `xml:"DescribeDBParameterGroupsResult>DBParameterGroups"`
}

type deleteDBParameterGroupResponse struct {
	XMLName xml.Name `xml:"DeleteDBParameterGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type modifyDBParameterGroupResponse struct {
	XMLName              xml.Name `xml:"ModifyDBParameterGroupResponse"`
	Xmlns                string   `xml:"xmlns,attr"`
	DBParameterGroupName string   `xml:"ModifyDBParameterGroupResult>DBParameterGroupName"`
}

type resetDBParameterGroupResponse struct {
	XMLName              xml.Name `xml:"ResetDBParameterGroupResponse"`
	Xmlns                string   `xml:"xmlns,attr"`
	DBParameterGroupName string   `xml:"ResetDBParameterGroupResult>DBParameterGroupName"`
}

type describeDBParametersResponse struct {
	XMLName    xml.Name           `xml:"DescribeDBParametersResponse"`
	Xmlns      string             `xml:"xmlns,attr"`
	Marker     string             `xml:"DescribeDBParametersResult>Marker,omitempty"`
	Parameters xmlDBParameterList `xml:"DescribeDBParametersResult>Parameters"`
}

func (h *Handler) handleCopyDBParameterGroup(vals url.Values) (any, error) {
	sourceGroupName := vals.Get("SourceDBParameterGroupIdentifier")
	targetGroupName := vals.Get("TargetDBParameterGroupIdentifier")
	targetDescription := vals.Get("TargetDBParameterGroupDescription")

	pg, err := h.Backend.CopyDBParameterGroup(sourceGroupName, targetGroupName, targetDescription)
	if err != nil {
		return nil, err
	}

	return &copyDBParameterGroupResponse{
		Xmlns:            rdsXMLNS,
		DBParameterGroup: toXMLParameterGroup(pg),
	}, nil
}

type copyDBParameterGroupResponse struct {
	XMLName          xml.Name            `xml:"CopyDBParameterGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	DBParameterGroup xmlDBParameterGroup `xml:"CopyDBParameterGroupResult>DBParameterGroup"`
}

type engineDefaults struct {
	DBParameterGroupFamily string             `xml:"DBParameterGroupFamily"`
	Parameters             xmlDBParameterList `xml:"Parameters"`
}

type describeEngineDefaultParametersResponse struct {
	XMLName xml.Name       `xml:"DescribeEngineDefaultParametersResponse"`
	Xmlns   string         `xml:"xmlns,attr"`
	Result  engineDefaults `xml:"DescribeEngineDefaultParametersResult>EngineDefaults"`
}

func (h *Handler) handleDescribeEngineDefaultParameters(vals url.Values) (any, error) {
	family := vals.Get("DBParameterGroupFamily")
	params := h.Backend.DescribeEngineDefaultParameters(family)
	members := make([]xmlDBParameter, 0, len(params))
	for _, p := range params {
		members = append(members, xmlDBParameter{
			ParameterName:  p.ParameterName,
			ParameterValue: p.ParameterValue,
		})
	}

	return &describeEngineDefaultParametersResponse{
		Xmlns: rdsXMLNS,
		Result: engineDefaults{
			DBParameterGroupFamily: family,
			Parameters:             xmlDBParameterList{Members: members},
		},
	}, nil
}
