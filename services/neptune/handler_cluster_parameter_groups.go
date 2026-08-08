package neptune

import (
	"context"
	"encoding/xml"
	"net/url"
)

func (h *Handler) handleCreateDBClusterParameterGroup(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	family := vals.Get("DBParameterGroupFamily")
	description := vals.Get("Description")
	tags := parseTagEntries(vals)
	if err := validateTagEntries(tags); err != nil {
		return nil, err
	}
	pg, err := h.Backend.CreateDBClusterParameterGroup(ctx, name, family, description)
	if err != nil {
		return nil, err
	}
	if len(tags) > 0 {
		_ = h.Backend.AddTagsToResource(ctx, pg.DBClusterParameterGroupArn, tags)
	}

	return &createDBClusterParameterGroupResponse{
		Xmlns:                   neptuneXMLNS,
		DBClusterParameterGroup: toXMLParameterGroup(pg),
	}, nil
}

func (h *Handler) handleDescribeDBClusterParameterGroups(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	groups, err := h.Backend.DescribeDBClusterParameterGroups(ctx, name)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBClusterParameterGroup, 0, len(groups))
	for _, pg := range groups {
		cp := pg
		members = append(members, toXMLParameterGroup(&cp))
	}

	members, nextMarker := applyNeptuneMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBClusterParameterGroupsResponse{
		Xmlns: neptuneXMLNS,
		Result: describeDBClusterParameterGroupsResult{
			DBClusterParameterGroups: xmlDBClusterParameterGroupList{Members: members},
			Marker:                   nextMarker,
		},
	}, nil
}

func (h *Handler) handleDeleteDBClusterParameterGroup(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	if err := h.Backend.DeleteDBClusterParameterGroup(ctx, name); err != nil {
		return nil, err
	}

	return &deleteDBClusterParameterGroupResponse{Xmlns: neptuneXMLNS}, nil
}

func (h *Handler) handleModifyDBClusterParameterGroup(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	params := parseParameterEntries(vals)
	pg, err := h.Backend.ModifyDBClusterParameterGroup(ctx, name, params)
	if err != nil {
		return nil, err
	}

	return &modifyDBClusterParameterGroupResponse{
		Xmlns:                       neptuneXMLNS,
		DBClusterParameterGroupName: pg.DBClusterParameterGroupName,
	}, nil
}

func (h *Handler) handleCopyDBClusterParameterGroup(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	sourceName := vals.Get("SourceDBClusterParameterGroupIdentifier")
	targetName := vals.Get("TargetDBClusterParameterGroupIdentifier")
	targetDescription := vals.Get("TargetDBClusterParameterGroupDescription")
	pg, err := h.Backend.CopyDBClusterParameterGroup(ctx, sourceName, targetName, targetDescription)
	if err != nil {
		return nil, err
	}

	return &copyDBClusterParameterGroupResponse{
		Xmlns:                   neptuneXMLNS,
		DBClusterParameterGroup: toXMLParameterGroup(pg),
	}, nil
}

func (h *Handler) handleDescribeDBClusterParameters(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	params, err := h.Backend.DescribeDBClusterParameters(ctx, name)
	if err != nil {
		return nil, err
	}
	members := make([]xmlParameter, 0, len(params))
	for _, p := range params {
		members = append(members, toXMLParameter(p))
	}

	return &describeDBClusterParametersResponse{
		Xmlns: neptuneXMLNS,
		Result: describeDBClusterParametersResult{
			Parameters: xmlParameterList{Members: members},
		},
	}, nil
}

func (h *Handler) handleResetDBClusterParameterGroup(
	ctx context.Context,
	vals url.Values,
) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	params := parseParameterEntries(vals)
	resetAll := vals.Get("ResetAllParameters") == formTrue
	pg, err := h.Backend.ResetDBClusterParameterGroup(ctx, name, resetAll, params)
	if err != nil {
		return nil, err
	}

	return &resetDBClusterParameterGroupResponse{
		Xmlns:                       neptuneXMLNS,
		DBClusterParameterGroupName: pg.DBClusterParameterGroupName,
	}, nil
}

func (h *Handler) handleDescribeEngineDefaultClusterParameters(
	_ context.Context,
	vals url.Values,
) (any, error) {
	family := vals.Get("DBParameterGroupFamily")
	if family == "" {
		family = pgFamilyNeptune13
	}
	catalog := neptuneParameterCatalog()
	members := make([]xmlParameter, 0, len(catalog))
	for _, p := range catalog {
		members = append(members, toXMLParameter(p))
	}

	return &describeEngineDefaultClusterParametersResponse{
		Xmlns: neptuneXMLNS,
		Result: describeEngineDefaultClusterParametersResult{
			EngineDefaults: xmlEngineDefaults{
				DBParameterGroupFamily: family,
				Parameters:             xmlParameterList{Members: members},
			},
		},
	}, nil
}

func toXMLParameterGroup(pg *DBClusterParameterGroup) xmlDBClusterParameterGroup {
	return xmlDBClusterParameterGroup{
		DBClusterParameterGroupName: pg.DBClusterParameterGroupName,
		DBClusterParameterGroupArn:  pg.DBClusterParameterGroupArn,
		DBParameterGroupFamily:      pg.DBParameterGroupFamily,
		Description:                 pg.Description,
	}
}

type xmlDBClusterParameterGroup struct {
	DBClusterParameterGroupName string `xml:"DBClusterParameterGroupName"`
	DBClusterParameterGroupArn  string `xml:"DBClusterParameterGroupArn,omitempty"`
	DBParameterGroupFamily      string `xml:"DBParameterGroupFamily"`
	Description                 string `xml:"Description"`
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

type describeDBClusterParametersResult struct {
	Marker     string           `xml:"Marker,omitempty"`
	Parameters xmlParameterList `xml:"Parameters"`
}

type describeDBClusterParametersResponse struct {
	XMLName xml.Name                          `xml:"DescribeDBClusterParametersResponse"`
	Xmlns   string                            `xml:"xmlns,attr"`
	Result  describeDBClusterParametersResult `xml:"DescribeDBClusterParametersResult"`
}

type resetDBClusterParameterGroupResponse struct {
	XMLName                     xml.Name `xml:"ResetDBClusterParameterGroupResponse"`
	Xmlns                       string   `xml:"xmlns,attr"`
	DBClusterParameterGroupName string   `xml:"ResetDBClusterParameterGroupResult>DBClusterParameterGroupName"`
}

type describeEngineDefaultClusterParametersResult struct {
	EngineDefaults xmlEngineDefaults `xml:"EngineDefaults"`
}

type describeEngineDefaultClusterParametersResponse struct {
	XMLName xml.Name                                     `xml:"DescribeEngineDefaultClusterParametersResponse"`
	Xmlns   string                                       `xml:"xmlns,attr"`
	Result  describeEngineDefaultClusterParametersResult `xml:"DescribeEngineDefaultClusterParametersResult"`
}
