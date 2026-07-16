package rds

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

func (h *Handler) handleCreateDBClusterParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	family := vals.Get("DBParameterGroupFamily")
	description := vals.Get("Description")
	pg, err := h.Backend.CreateDBClusterParameterGroup(name, family, description)
	if err != nil {
		return nil, err
	}

	return &createDBClusterParameterGroupResponse{
		Xmlns:            rdsXMLNS,
		DBParameterGroup: toXMLClusterParameterGroup(pg),
	}, nil
}

func (h *Handler) handleDescribeDBClusterParameterGroups(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	groups, err := h.Backend.DescribeDBClusterParameterGroups(name)
	if err != nil {
		return nil, err
	}
	members, marker, err := paginateDescribe(vals, groups, func(a, b DBParameterGroup) bool {
		return a.DBParameterGroupName < b.DBParameterGroupName
	}, func(item DBParameterGroup) xmlDBClusterParameterGroup {
		cp := item

		return toXMLClusterParameterGroup(&cp)
	})
	if err != nil {
		return nil, err
	}

	return &describeDBClusterParameterGroupsResponse{
		Xmlns: rdsXMLNS,
		Result: describeDBClusterParameterGroupsResult{
			DBClusterParameterGroups: xmlDBClusterParameterGroupList{Members: members},
			Marker:                   marker,
		},
	}, nil
}

func toXMLClusterParameterGroup(pg *DBParameterGroup) xmlDBClusterParameterGroup {
	return xmlDBClusterParameterGroup{
		DBClusterParameterGroupName: pg.DBParameterGroupName,
		DBParameterGroupFamily:      pg.DBParameterGroupFamily,
		Description:                 pg.Description,
	}
}

type xmlDBClusterParameterGroup struct {
	DBClusterParameterGroupName string `xml:"DBClusterParameterGroupName"`
	DBParameterGroupFamily      string `xml:"DBParameterGroupFamily"`
	Description                 string `xml:"Description"`
}

type xmlDBClusterParameterGroupList struct {
	Members []xmlDBClusterParameterGroup `xml:"DBClusterParameterGroup"`
}

type createDBClusterParameterGroupResponse struct {
	XMLName          xml.Name                   `xml:"CreateDBClusterParameterGroupResponse"`
	Xmlns            string                     `xml:"xmlns,attr"`
	DBParameterGroup xmlDBClusterParameterGroup `xml:"CreateDBClusterParameterGroupResult>DBClusterParameterGroup"`
}

type describeDBClusterParameterGroupsResult struct {
	Marker                   string                         `xml:"Marker,omitempty"`
	DBClusterParameterGroups xmlDBClusterParameterGroupList `xml:"DBClusterParameterGroups"`
}

type describeDBClusterParameterGroupsResponse struct {
	XMLName xml.Name                               `xml:"DescribeDBClusterParameterGroupsResponse"`
	Xmlns   string                                 `xml:"xmlns,attr"`
	Result  describeDBClusterParameterGroupsResult `xml:"DescribeDBClusterParameterGroupsResult"`
}

func (h *Handler) handleCopyDBClusterParameterGroup(vals url.Values) (any, error) {
	sourceGroupName := vals.Get("SourceDBClusterParameterGroupIdentifier")
	targetGroupName := vals.Get("TargetDBClusterParameterGroupIdentifier")
	targetDescription := vals.Get("TargetDBClusterParameterGroupDescription")

	pg, err := h.Backend.CopyDBClusterParameterGroup(sourceGroupName, targetGroupName, targetDescription)
	if err != nil {
		return nil, err
	}

	return &copyDBClusterParameterGroupResponse{
		Xmlns:            rdsXMLNS,
		DBParameterGroup: toXMLClusterParameterGroup(pg),
	}, nil
}

type copyDBClusterParameterGroupResponse struct {
	XMLName          xml.Name                   `xml:"CopyDBClusterParameterGroupResponse"`
	Xmlns            string                     `xml:"xmlns,attr"`
	DBParameterGroup xmlDBClusterParameterGroup `xml:"CopyDBClusterParameterGroupResult>DBClusterParameterGroup"`
}

func (h *Handler) handleDeleteDBClusterParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	if err := h.Backend.DeleteDBClusterParameterGroup(name); err != nil {
		return nil, err
	}

	return &deleteDBClusterParameterGroupResponse{Xmlns: rdsXMLNS}, nil
}

func (h *Handler) handleModifyDBClusterParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	var params []DBParameter
	for i := 1; ; i++ {
		prefix := "Parameters.Parameter." + strconv.Itoa(i) + "."
		pName := vals.Get(prefix + "ParameterName")
		if pName == "" {
			break
		}
		params = append(params, DBParameter{
			ParameterName:  pName,
			ParameterValue: vals.Get(prefix + "ParameterValue"),
			ApplyType:      vals.Get(prefix + "ApplyType"),
		})
	}
	groupName, err := h.Backend.ModifyDBClusterParameterGroup(name, params)
	if err != nil {
		return nil, err
	}

	return &modifyDBClusterParameterGroupResponse{
		Xmlns:                       rdsXMLNS,
		DBClusterParameterGroupName: groupName,
	}, nil
}

func (h *Handler) handleDescribeDBClusterParameters(vals url.Values) (any, error) {
	groupName := vals.Get("DBClusterParameterGroupName")
	params, err := h.Backend.DescribeDBClusterParameters(groupName)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBParameter, 0, len(params))
	for _, p := range params {
		members = append(members, xmlDBParameter(p))
	}

	return &describeDBClusterParametersResponse{
		Xmlns:      rdsXMLNS,
		Parameters: xmlDBParameterList{Members: members},
	}, nil
}

func (h *Handler) handleResetDBClusterParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	resetAll := vals.Get("ResetAllParameters") == formTrue
	var params []string
	for i := 1; ; i++ {
		pName := vals.Get("Parameters.Parameter." + strconv.Itoa(i) + ".ParameterName")
		if pName == "" {
			break
		}
		params = append(params, pName)
	}
	groupName, err := h.Backend.ResetDBClusterParameterGroup(name, resetAll, params)
	if err != nil {
		return nil, err
	}

	return &resetDBClusterParameterGroupResponse{
		Xmlns:                       rdsXMLNS,
		DBClusterParameterGroupName: groupName,
	}, nil
}

type deleteDBClusterParameterGroupResponse struct {
	XMLName xml.Name `xml:"DeleteDBClusterParameterGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type modifyDBClusterParameterGroupResponse struct {
	XMLName                     xml.Name `xml:"ModifyDBClusterParameterGroupResponse"`
	Xmlns                       string   `xml:"xmlns,attr"`
	DBClusterParameterGroupName string   `xml:"ModifyDBClusterParameterGroupResult>DBClusterParameterGroupName"`
}

type describeDBClusterParametersResponse struct {
	XMLName    xml.Name           `xml:"DescribeDBClusterParametersResponse"`
	Xmlns      string             `xml:"xmlns,attr"`
	Parameters xmlDBParameterList `xml:"DescribeDBClusterParametersResult>Parameters"`
}

type resetDBClusterParameterGroupResponse struct {
	XMLName                     xml.Name `xml:"ResetDBClusterParameterGroupResponse"`
	Xmlns                       string   `xml:"xmlns,attr"`
	DBClusterParameterGroupName string   `xml:"ResetDBClusterParameterGroupResult>DBClusterParameterGroupName"`
}

type describeEngineDefaultClusterParametersResponse struct {
	XMLName    xml.Name           `xml:"DescribeEngineDefaultClusterParametersResponse"`
	Xmlns      string             `xml:"xmlns,attr"`
	Parameters xmlDBParameterList `xml:"DescribeEngineDefaultClusterParametersResult>EngineDefaults>Parameters"`
}

func (h *Handler) handleDescribeEngineDefaultClusterParameters(vals url.Values) (any, error) {
	family := vals.Get("DBParameterGroupFamily")
	params := h.Backend.DescribeEngineDefaultClusterParameters(family)
	members := make([]xmlDBParameter, 0, len(params))
	for _, p := range params {
		members = append(members, xmlDBParameter{
			ParameterName:  p.ParameterName,
			ParameterValue: p.ParameterValue,
		})
	}

	return &describeEngineDefaultClusterParametersResponse{
		Xmlns:      rdsXMLNS,
		Parameters: xmlDBParameterList{Members: members},
	}, nil
}
