package neptune

import (
	"context"
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleCopyDBParameterGroup(ctx context.Context, vals url.Values) (any, error) {
	sourceName := vals.Get("SourceDBParameterGroupIdentifier")
	targetName := vals.Get("TargetDBParameterGroupIdentifier")
	targetDescription := vals.Get("TargetDBParameterGroupDescription")
	pg, err := h.Backend.CopyDBParameterGroup(ctx, sourceName, targetName, targetDescription)
	if err != nil {
		return nil, err
	}

	return &copyDBParameterGroupResponse{
		Xmlns:            neptuneXMLNS,
		DBParameterGroup: toXMLDBParameterGroup(pg),
	}, nil
}

func (h *Handler) handleCreateDBParameterGroup(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	family := vals.Get("DBParameterGroupFamily")
	description := vals.Get("Description")
	pg, err := h.Backend.CreateDBParameterGroup(ctx, name, family, description)
	if err != nil {
		return nil, err
	}

	return &createDBParameterGroupResponse{
		Xmlns:            neptuneXMLNS,
		DBParameterGroup: toXMLDBParameterGroup(pg),
	}, nil
}

func (h *Handler) handleDeleteDBParameterGroup(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	if err := h.Backend.DeleteDBParameterGroup(ctx, name); err != nil {
		return nil, err
	}

	return &deleteDBParameterGroupResponse{Xmlns: neptuneXMLNS}, nil
}

func (h *Handler) handleDescribeDBParameterGroups(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	name := vals.Get("DBParameterGroupName")
	groups, err := h.Backend.DescribeDBParameterGroups(ctx, name)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBParameterGroup, 0, len(groups))
	for _, pg := range groups {
		cp := pg
		members = append(members, toXMLDBParameterGroup(&cp))
	}

	members, nextMarker := applyNeptuneMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBParameterGroupsResponse{
		Xmlns: neptuneXMLNS,
		Result: describeDBParameterGroupsResult{
			DBParameterGroups: xmlDBParameterGroupList{Members: members},
			Marker:            nextMarker,
		},
	}, nil
}

func (h *Handler) handleDescribeDBParameters(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	if name != "" {
		if _, err := h.Backend.DescribeDBParameterGroups(ctx, name); err != nil {
			return nil, err
		}
	}

	return &describeDBParametersResponse{
		Xmlns: neptuneXMLNS,
		Result: describeDBParametersResult{
			Parameters: xmlParameterList{},
		},
	}, nil
}

func (h *Handler) handleModifyDBParameterGroup(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	pg, err := h.Backend.ModifyDBParameterGroup(ctx, name)
	if err != nil {
		return nil, err
	}

	return &modifyDBParameterGroupResponse{
		Xmlns:                neptuneXMLNS,
		DBParameterGroupName: pg.DBParameterGroupName,
	}, nil
}

func (h *Handler) handleResetDBParameterGroup(ctx context.Context, vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	pg, err := h.Backend.ResetDBParameterGroup(ctx, name)
	if err != nil {
		return nil, err
	}

	return &resetDBParameterGroupResponse{
		Xmlns:                neptuneXMLNS,
		DBParameterGroupName: pg.DBParameterGroupName,
	}, nil
}

func (h *Handler) handleDescribeEngineDefaultParameters(
	_ context.Context,
	vals url.Values,
) (any, error) {
	family := vals.Get("DBParameterGroupFamily")
	if family == "" {
		family = pgFamilyNeptune13
	}

	return &describeEngineDefaultParametersResponse{
		Xmlns: neptuneXMLNS,
		Result: describeEngineDefaultParametersResult{
			EngineDefaults: xmlEngineDefaults{
				DBParameterGroupFamily: family,
				Parameters:             xmlParameterList{},
			},
		},
	}, nil
}

func toXMLDBParameterGroup(pg *DBParameterGroup) xmlDBParameterGroup {
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

type copyDBParameterGroupResponse struct {
	XMLName          xml.Name            `xml:"CopyDBParameterGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	DBParameterGroup xmlDBParameterGroup `xml:"CopyDBParameterGroupResult>DBParameterGroup"`
}

type createDBParameterGroupResponse struct {
	XMLName          xml.Name            `xml:"CreateDBParameterGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	DBParameterGroup xmlDBParameterGroup `xml:"CreateDBParameterGroupResult>DBParameterGroup"`
}

type xmlDBParameterGroupList struct {
	Members []xmlDBParameterGroup `xml:"DBParameterGroup"`
}

type describeDBParameterGroupsResult struct {
	Marker            string                  `xml:"Marker,omitempty"`
	DBParameterGroups xmlDBParameterGroupList `xml:"DBParameterGroups"`
}

type describeDBParameterGroupsResponse struct {
	XMLName xml.Name                        `xml:"DescribeDBParameterGroupsResponse"`
	Xmlns   string                          `xml:"xmlns,attr"`
	Result  describeDBParameterGroupsResult `xml:"DescribeDBParameterGroupsResult"`
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

type xmlParameterList struct {
	Members []xmlParameter `xml:"Parameter"`
}

type xmlParameter struct {
	ParameterName  string `xml:"ParameterName"`
	ParameterValue string `xml:"ParameterValue,omitempty"`
	Description    string `xml:"Description,omitempty"`
}

type describeDBParametersResult struct {
	Marker     string           `xml:"Marker,omitempty"`
	Parameters xmlParameterList `xml:"Parameters"`
}

type describeDBParametersResponse struct {
	XMLName xml.Name                   `xml:"DescribeDBParametersResponse"`
	Xmlns   string                     `xml:"xmlns,attr"`
	Result  describeDBParametersResult `xml:"DescribeDBParametersResult"`
}

type xmlEngineDefaults struct {
	DBParameterGroupFamily string           `xml:"DBParameterGroupFamily"`
	Parameters             xmlParameterList `xml:"Parameters"`
}

type describeEngineDefaultParametersResult struct {
	EngineDefaults xmlEngineDefaults `xml:"EngineDefaults"`
}

type describeEngineDefaultParametersResponse struct {
	XMLName xml.Name                              `xml:"DescribeEngineDefaultParametersResponse"`
	Xmlns   string                                `xml:"xmlns,attr"`
	Result  describeEngineDefaultParametersResult `xml:"DescribeEngineDefaultParametersResult"`
}

// dispatchParameterGroupAction handles DBParameterGroup (non-cluster) actions;
// see dispatch's doc comment for the chaining rationale.
func (h *Handler) dispatchParameterGroupAction(
	ctx context.Context, action string, vals url.Values,
) (any, error) {
	switch action {
	case "CreateDBParameterGroup":
		return h.handleCreateDBParameterGroup(ctx, vals)
	case "DeleteDBParameterGroup":
		return h.handleDeleteDBParameterGroup(ctx, vals)
	case "DescribeDBParameterGroups":
		return h.handleDescribeDBParameterGroups(ctx, vals)
	case "DescribeDBParameters":
		return h.handleDescribeDBParameters(ctx, vals)
	case "ModifyDBParameterGroup":
		return h.handleModifyDBParameterGroup(ctx, vals)
	case "ResetDBParameterGroup":
		return h.handleResetDBParameterGroup(ctx, vals)
	case "CopyDBParameterGroup":
		return h.handleCopyDBParameterGroup(ctx, vals)
	case "DescribeEngineDefaultParameters":
		return h.handleDescribeEngineDefaultParameters(ctx, vals)
	default:
		return h.dispatchSnapshotAndEndpointAction(ctx, action, vals)
	}
}
