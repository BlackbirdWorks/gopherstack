package redshift

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

// ---- Parameter Group XML types ----

type xmlClusterParameterGroup struct {
	ParameterGroupName   string `xml:"ParameterGroupName"`
	ParameterGroupFamily string `xml:"ParameterGroupFamily"`
	Description          string `xml:"Description,omitempty"`
}

type xmlClusterParameterGroupList struct {
	Members []xmlClusterParameterGroup `xml:"ClusterParameterGroup"`
}

type xmlClusterParameter struct {
	ParameterName        string `xml:"ParameterName"`
	ParameterValue       string `xml:"ParameterValue"`
	Description          string `xml:"Description,omitempty"`
	Source               string `xml:"Source,omitempty"`
	DataType             string `xml:"DataType,omitempty"`
	ApplyType            string `xml:"ApplyType,omitempty"`
	MinimumEngineVersion string `xml:"MinimumEngineVersion,omitempty"`
	IsModifiable         bool   `xml:"IsModifiable"`
}

type xmlClusterParameterList struct {
	Members []xmlClusterParameter `xml:"Parameter"`
}

// ---- CreateClusterParameterGroup ----

type createClusterParameterGroupResponse struct {
	XMLName        xml.Name                 `xml:"CreateClusterParameterGroupResponse"`
	Xmlns          string                   `xml:"xmlns,attr"`
	ParameterGroup xmlClusterParameterGroup `xml:"CreateClusterParameterGroupResult>ClusterParameterGroup"`
}

func paramGroupToXML(pg *ClusterParameterGroup) xmlClusterParameterGroup {
	return xmlClusterParameterGroup{
		ParameterGroupName:   pg.ParameterGroupName,
		ParameterGroupFamily: pg.ParameterGroupFamily,
		Description:          pg.Description,
	}
}

func (h *Handler) handleCreateClusterParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("ParameterGroupName")
	family := vals.Get("ParameterGroupFamily")
	description := vals.Get("Description")

	pg, err := h.Backend.CreateClusterParameterGroup(name, family, description)
	if err != nil {
		return nil, err
	}

	return &createClusterParameterGroupResponse{
		Xmlns:          redshiftXMLNS,
		ParameterGroup: paramGroupToXML(pg),
	}, nil
}

// ---- DeleteClusterParameterGroup ----

type deleteClusterParameterGroupResponse struct {
	XMLName xml.Name `xml:"DeleteClusterParameterGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleDeleteClusterParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("ParameterGroupName")

	if err := h.Backend.DeleteClusterParameterGroup(name); err != nil {
		return nil, err
	}

	return &deleteClusterParameterGroupResponse{Xmlns: redshiftXMLNS}, nil
}

// ---- DescribeClusterParameterGroups ----

type describeClusterParameterGroupsResponse struct {
	XMLName         xml.Name                     `xml:"DescribeClusterParameterGroupsResponse"`
	Xmlns           string                       `xml:"xmlns,attr"`
	ParameterGroups xmlClusterParameterGroupList `xml:"DescribeClusterParameterGroupsResult>ParameterGroups"`
}

func (h *Handler) handleDescribeClusterParameterGroups(vals url.Values) (any, error) {
	name := vals.Get("ParameterGroupName")

	groups, err := h.Backend.DescribeClusterParameterGroups(name)
	if err != nil {
		return nil, err
	}

	members := make([]xmlClusterParameterGroup, 0, len(groups))
	for _, g := range groups {
		gp := g
		members = append(members, paramGroupToXML(&gp))
	}

	return &describeClusterParameterGroupsResponse{
		Xmlns:           redshiftXMLNS,
		ParameterGroups: xmlClusterParameterGroupList{Members: members},
	}, nil
}

// ---- DescribeClusterParameters ----

type describeClusterParametersResponse struct {
	XMLName    xml.Name                `xml:"DescribeClusterParametersResponse"`
	Xmlns      string                  `xml:"xmlns,attr"`
	Parameters xmlClusterParameterList `xml:"DescribeClusterParametersResult>Parameters"`
}

func (h *Handler) handleDescribeClusterParameters(vals url.Values) (any, error) {
	groupName := vals.Get("ParameterGroupName")
	source := vals.Get("Source")

	params, err := h.Backend.DescribeClusterParameters(groupName)
	if err != nil {
		return nil, err
	}

	members := make([]xmlClusterParameter, 0, len(params))
	for _, p := range params {
		if source != "" && p.Source != source {
			continue
		}

		members = append(members, xmlClusterParameter(p))
	}

	return &describeClusterParametersResponse{
		Xmlns:      redshiftXMLNS,
		Parameters: xmlClusterParameterList{Members: members},
	}, nil
}

// ---- ModifyClusterParameterGroup ----

type modifyClusterParameterGroupResponse struct {
	XMLName              xml.Name `xml:"ModifyClusterParameterGroupResponse"`
	Xmlns                string   `xml:"xmlns,attr"`
	ParameterGroupName   string   `xml:"ModifyClusterParameterGroupResult>ParameterGroupName"`
	ParameterGroupStatus string   `xml:"ModifyClusterParameterGroupResult>ParameterGroupStatus"`
}

func (h *Handler) handleModifyClusterParameterGroup(vals url.Values) (any, error) {
	groupName := vals.Get("ParameterGroupName")
	params := parseParameterList(vals)

	pg, err := h.Backend.ModifyClusterParameterGroup(groupName, params)
	if err != nil {
		return nil, err
	}

	return &modifyClusterParameterGroupResponse{
		Xmlns:                redshiftXMLNS,
		ParameterGroupName:   pg.ParameterGroupName,
		ParameterGroupStatus: "Your parameter group has been updated",
	}, nil
}

// ---- ResetClusterParameterGroup ----

type resetClusterParameterGroupResponse struct {
	XMLName              xml.Name `xml:"ResetClusterParameterGroupResponse"`
	Xmlns                string   `xml:"xmlns,attr"`
	ParameterGroupName   string   `xml:"ResetClusterParameterGroupResult>ParameterGroupName"`
	ParameterGroupStatus string   `xml:"ResetClusterParameterGroupResult>ParameterGroupStatus"`
}

func (h *Handler) handleResetClusterParameterGroup(vals url.Values) (any, error) {
	groupName := vals.Get("ParameterGroupName")
	resetAll := vals.Get("ResetAllParameters") == paramValueTrue
	params := parseParameterList(vals)

	pg, err := h.Backend.ResetClusterParameterGroup(groupName, resetAll, params)
	if err != nil {
		return nil, err
	}

	return &resetClusterParameterGroupResponse{
		Xmlns:                redshiftXMLNS,
		ParameterGroupName:   pg.ParameterGroupName,
		ParameterGroupStatus: "Your parameter group has been updated",
	}, nil
}

// ---- DescribeDefaultClusterParameters ----

type xmlDefaultClusterParametersResult struct {
	Family     string                  `xml:"DefaultClusterParameters>ParameterGroupFamily"`
	Parameters xmlClusterParameterList `xml:"DefaultClusterParameters>Parameters"`
}

type describeDefaultClusterParametersResponse struct {
	XMLName xml.Name                          `xml:"DescribeDefaultClusterParametersResponse"`
	Xmlns   string                            `xml:"xmlns,attr"`
	Result  xmlDefaultClusterParametersResult `xml:"DescribeDefaultClusterParametersResult"`
}

func (h *Handler) handleDescribeDefaultClusterParameters(vals url.Values) (any, error) {
	family := vals.Get("ParameterGroupFamily")

	params, err := h.Backend.DescribeDefaultClusterParameters(family)
	if err != nil {
		return nil, err
	}

	members := make([]xmlClusterParameter, 0, len(params))
	for _, p := range params {
		members = append(members, xmlClusterParameter{
			ParameterName:  p.ParameterName,
			ParameterValue: p.ParameterValue,
			Description:    p.Description,
			Source:         p.Source,
			DataType:       p.DataType,
			ApplyType:      p.ApplyType,
			IsModifiable:   p.IsModifiable,
		})
	}

	return &describeDefaultClusterParametersResponse{
		Xmlns: redshiftXMLNS,
		Result: xmlDefaultClusterParametersResult{
			Family:     family,
			Parameters: xmlClusterParameterList{Members: members},
		},
	}, nil
}

// parseParameterList extracts Parameters.Parameter.N.ParameterName/ParameterValue from form values.
func parseParameterList(vals url.Values) []ClusterParameter {
	var params []ClusterParameter

	for i := 1; i <= maxListItems; i++ {
		prefix := "Parameters.Parameter." + strconv.Itoa(i) + "."
		name := vals.Get(prefix + "ParameterName")

		if name == "" {
			break
		}

		params = append(params, ClusterParameter{
			ParameterName:  name,
			ParameterValue: vals.Get(prefix + "ParameterValue"),
			ApplyType:      vals.Get(prefix + "ApplyType"),
		})
	}

	return params
}
