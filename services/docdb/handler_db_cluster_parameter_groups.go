package docdb

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
)

func (h *Handler) handleCreateDBClusterParameterGroup(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	family := vals.Get("DBParameterGroupFamily")
	description := vals.Get("Description")
	tags := parseTags(vals)
	pg, err := h.Backend.CreateDBClusterParameterGroup(ctx, name, family, description, tags)
	if err != nil {
		return nil, err
	}

	return &createDBClusterParameterGroupResponse{
		Xmlns:                   docdbXMLNS,
		DBClusterParameterGroup: toXMLParameterGroup(pg),
	}, nil
}

func (h *Handler) handleDescribeDBClusterParameterGroups(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	groups, err := h.Backend.DescribeDBClusterParameterGroups(ctx, name)
	if err != nil {
		return nil, err
	}

	paged, nextMarker := applyDocDBMarker(groups, vals.Get("Marker"), vals.Get("MaxRecords"))
	members := make([]xmlDBClusterParameterGroup, 0, len(paged))
	for _, pg := range paged {
		cp := pg
		members = append(members, toXMLParameterGroup(&cp))
	}

	return &describeDBClusterParameterGroupsResponse{
		Xmlns: docdbXMLNS,
		Result: describeDBClusterParameterGroupsResult{
			Marker:                   nextMarker,
			DBClusterParameterGroups: xmlDBClusterParameterGroupList{Members: members},
		},
	}, nil
}

func (h *Handler) handleDeleteDBClusterParameterGroup(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	if err := h.Backend.DeleteDBClusterParameterGroup(ctx, name); err != nil {
		return nil, err
	}

	return &deleteDBClusterParameterGroupResponse{Xmlns: docdbXMLNS}, nil
}

func (h *Handler) handleModifyDBClusterParameterGroup(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	parameters := parseDBClusterParameters(vals)
	pg, err := h.Backend.ModifyDBClusterParameterGroup(ctx, name, parameters)
	if err != nil {
		return nil, err
	}

	return &modifyDBClusterParameterGroupResponse{
		Xmlns:                       docdbXMLNS,
		DBClusterParameterGroupName: pg.DBClusterParameterGroupName,
	}, nil
}

func (h *Handler) handleCopyDBClusterParameterGroup(ctx context.Context, vals url.Values) (any, error) {
	sourceGroupName := vals.Get("SourceDBClusterParameterGroupIdentifier")
	targetName := vals.Get("TargetDBClusterParameterGroupIdentifier")
	targetDescription := vals.Get("TargetDBClusterParameterGroupDescription")
	pg, err := h.Backend.CopyDBClusterParameterGroup(ctx, sourceGroupName, targetName, targetDescription)
	if err != nil {
		return nil, err
	}

	return &copyDBClusterParameterGroupResponse{
		Xmlns:                   docdbXMLNS,
		DBClusterParameterGroup: toXMLParameterGroup(pg),
	}, nil
}

func (h *Handler) handleDescribeDBClusterParameters(ctx context.Context, vals url.Values) (any, error) {
	groupName := vals.Get("DBClusterParameterGroupName")
	params, err := h.Backend.DescribeDBClusterParameters(ctx, groupName, vals.Get("Source"))
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBClusterParameter, 0, len(params))
	for _, p := range params {
		cp := p
		members = append(members, toXMLDBClusterParameter(&cp))
	}

	return &describeDBClusterParametersResponse{
		Xmlns: docdbXMLNS,
		Result: describeDBClusterParametersResult{
			Parameters: xmlDBClusterParameterList{Members: members},
		},
	}, nil
}

func (h *Handler) handleDescribeEngineDefaultClusterParameters(ctx context.Context, vals url.Values) (any, error) {
	family := vals.Get("DBParameterGroupFamily")
	params := h.Backend.DescribeEngineDefaultClusterParameters(ctx, family)
	members := make([]xmlDBClusterParameter, 0, len(params))
	for _, p := range params {
		cp := p
		members = append(members, toXMLDBClusterParameter(&cp))
	}

	return &describeEngineDefaultClusterParametersResponse{
		Xmlns: docdbXMLNS,
		Result: describeEngineDefaultClusterParametersResult{
			EngineDefaults: xmlEngineDefaults{
				DBParameterGroupFamily: family,
				Parameters:             xmlDBClusterParameterList{Members: members},
			},
		},
	}, nil
}

func (h *Handler) handleResetDBClusterParameterGroup(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	resetAll := vals.Get("ResetAllParameters") == stringTrue
	paramNames := parseDBClusterParameterNames(vals)
	pg, err := h.Backend.ResetDBClusterParameterGroup(ctx, name, resetAll, paramNames)
	if err != nil {
		return nil, err
	}

	return &resetDBClusterParameterGroupResponse{
		Xmlns:                       docdbXMLNS,
		DBClusterParameterGroupName: pg.DBClusterParameterGroupName,
	}, nil
}

func toXMLParameterGroup(pg *DBClusterParameterGroup) xmlDBClusterParameterGroup {
	return xmlDBClusterParameterGroup{
		DBClusterParameterGroupName: pg.DBClusterParameterGroupName,
		DBParameterGroupFamily:      pg.DBParameterGroupFamily,
		Description:                 pg.Description,
		DBClusterParameterGroupArn:  pg.DBClusterParameterGroupArn,
	}
}

type xmlDBClusterParameterGroup struct {
	DBClusterParameterGroupName string `xml:"DBClusterParameterGroupName"`
	DBParameterGroupFamily      string `xml:"DBParameterGroupFamily"`
	Description                 string `xml:"Description"`
	DBClusterParameterGroupArn  string `xml:"DBClusterParameterGroupArn,omitempty"`
}

type xmlDBClusterParameterGroupList struct {
	Members []xmlDBClusterParameterGroup `xml:"DBClusterParameterGroup"`
}

type createDBClusterParameterGroupResponse struct {
	XMLName                 xml.Name                   `xml:"CreateDBClusterParameterGroupResponse"`
	Xmlns                   string                     `xml:"xmlns,attr"`
	DBClusterParameterGroup xmlDBClusterParameterGroup `xml:"CreateDBClusterParameterGroupResult>DBClusterParameterGroup"`
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

type deleteDBClusterParameterGroupResponse struct {
	XMLName xml.Name `xml:"DeleteDBClusterParameterGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type modifyDBClusterParameterGroupResponse struct {
	XMLName                     xml.Name `xml:"ModifyDBClusterParameterGroupResponse"`
	Xmlns                       string   `xml:"xmlns,attr"`
	DBClusterParameterGroupName string   `xml:"ModifyDBClusterParameterGroupResult>DBClusterParameterGroupName"`
}

type copyDBClusterParameterGroupResponse struct {
	XMLName                 xml.Name                   `xml:"CopyDBClusterParameterGroupResponse"`
	Xmlns                   string                     `xml:"xmlns,attr"`
	DBClusterParameterGroup xmlDBClusterParameterGroup `xml:"CopyDBClusterParameterGroupResult>DBClusterParameterGroup"`
}

type xmlDBClusterParameter struct {
	ParameterName  string `xml:"ParameterName"`
	ParameterValue string `xml:"ParameterValue,omitempty"`
	Description    string `xml:"Description,omitempty"`
	Source         string `xml:"Source,omitempty"`
	ApplyType      string `xml:"ApplyType,omitempty"`
	ApplyMethod    string `xml:"ApplyMethod,omitempty"`
	DataType       string `xml:"DataType,omitempty"`
	IsModifiable   bool   `xml:"IsModifiable"`
}

type xmlDBClusterParameterList struct {
	Members []xmlDBClusterParameter `xml:"Parameter"`
}

type describeDBClusterParametersResult struct {
	Parameters xmlDBClusterParameterList `xml:"Parameters"`
}

type describeDBClusterParametersResponse struct {
	XMLName xml.Name                          `xml:"DescribeDBClusterParametersResponse"`
	Xmlns   string                            `xml:"xmlns,attr"`
	Result  describeDBClusterParametersResult `xml:"DescribeDBClusterParametersResult"`
}

type xmlEngineDefaults struct {
	DBParameterGroupFamily string                    `xml:"DBParameterGroupFamily"`
	Parameters             xmlDBClusterParameterList `xml:"Parameters"`
}

type describeEngineDefaultClusterParametersResult struct {
	EngineDefaults xmlEngineDefaults `xml:"EngineDefaults"`
}

type describeEngineDefaultClusterParametersResponse struct {
	XMLName xml.Name                                     `xml:"DescribeEngineDefaultClusterParametersResponse"`
	Xmlns   string                                       `xml:"xmlns,attr"`
	Result  describeEngineDefaultClusterParametersResult `xml:"DescribeEngineDefaultClusterParametersResult"`
}

type resetDBClusterParameterGroupResponse struct {
	XMLName                     xml.Name `xml:"ResetDBClusterParameterGroupResponse"`
	Xmlns                       string   `xml:"xmlns,attr"`
	DBClusterParameterGroupName string   `xml:"ResetDBClusterParameterGroupResult>DBClusterParameterGroupName"`
}

func toXMLDBClusterParameter(p *DBClusterParameter) xmlDBClusterParameter {
	return xmlDBClusterParameter{
		ParameterName:  p.ParameterName,
		ParameterValue: p.ParameterValue,
		Description:    p.Description,
		Source:         p.Source,
		ApplyType:      p.ApplyType,
		ApplyMethod:    p.ApplyMethod,
		DataType:       p.DataType,
		IsModifiable:   p.IsModifiable,
	}
}

// parseDBClusterParameters parses Parameters.Parameter.N.ParameterName +
// ParameterValue form values. The real aws-sdk-go-v2 query-protocol
// serializer (awsAwsquery_serializeDocumentParametersList) encodes each
// element as "Parameters.Parameter.N", not the generic "Parameters.member.N"
// -- getting this wrong means ModifyDBClusterParameterGroup silently ignores
// every parameter a real client sends.
func parseDBClusterParameters(vals url.Values) map[string]string {
	params := make(map[string]string)
	for i := 1; ; i++ {
		pName := vals.Get(fmt.Sprintf("Parameters.Parameter.%d.ParameterName", i))
		if pName == "" {
			break
		}
		pValue := vals.Get(fmt.Sprintf("Parameters.Parameter.%d.ParameterValue", i))
		params[pName] = pValue
	}

	return params
}

// parseDBClusterParameterNames parses just the ParameterName half of the
// same Parameters.Parameter.N.* wire shape parseDBClusterParameters reads
// (ResetDBClusterParameterGroup's per-parameter reset form only needs the
// name -- a real client resetting specific parameters sends
// ParameterName+ApplyMethod, not ParameterValue).
func parseDBClusterParameterNames(vals url.Values) []string {
	var names []string
	for i := 1; ; i++ {
		pName := vals.Get(fmt.Sprintf("Parameters.Parameter.%d.ParameterName", i))
		if pName == "" {
			break
		}
		names = append(names, pName)
	}

	return names
}
