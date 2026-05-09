package rds

import (
	"encoding/xml"
	"net/url"
	"strconv"
	"strings"
)

// dispatchExtended7 routes the new RDS operations added in refinement 1.
// Split from dispatchExtended6 to keep cyclomatic complexity within limits.
func (h *Handler) dispatchExtended7(action string, vals url.Values) (any, error) {
	switch action {
	case "CreateEventSubscription":
		return h.handleCreateEventSubscription(vals)
	case "DeleteEventSubscription":
		return h.handleDeleteEventSubscription(vals)
	case "DescribeEventSubscriptions":
		return h.handleDescribeEventSubscriptions(vals)
	case "ModifyEventSubscription":
		return h.handleModifyEventSubscription(vals)
	case "DescribeEvents":
		return h.handleDescribeEvents(vals)
	case "DescribeEventCategories":
		return h.handleDescribeEventCategories(vals)
	case "DescribeBlueGreenDeployments":
		return h.handleDescribeBlueGreenDeployments(vals)
	case "DeleteBlueGreenDeployment":
		return h.handleDeleteBlueGreenDeployment(vals)
	case "SwitchoverBlueGreenDeployment":
		return h.handleSwitchoverBlueGreenDeployment(vals)
	case "DescribeDBSecurityGroups":
		return h.handleDescribeDBSecurityGroups(vals)
	default:
		return h.dispatchExtended8(action, vals)
	}
}

// dispatchExtended8 routes additional RDS operations added in refinement 1.
// Split from dispatchExtended7 to keep cyclomatic complexity within limits.
func (h *Handler) dispatchExtended8(action string, vals url.Values) (any, error) {
	switch action {
	case "DeleteDBSecurityGroup":
		return h.handleDeleteDBSecurityGroup(vals)
	case "RevokeDBSecurityGroupIngress":
		return h.handleRevokeDBSecurityGroupIngress(vals)
	case "FailoverDBCluster":
		return h.handleFailoverDBCluster(vals)
	case "RebootDBCluster":
		return h.handleRebootDBCluster(vals)
	case "DeleteDBClusterParameterGroup":
		return h.handleDeleteDBClusterParameterGroup(vals)
	case "ModifyDBClusterParameterGroup":
		return h.handleModifyDBClusterParameterGroup(vals)
	case "DescribeDBClusterParameters":
		return h.handleDescribeDBClusterParameters(vals)
	case "ResetDBClusterParameterGroup":
		return h.handleResetDBClusterParameterGroup(vals)
	case "ModifyDBSubnetGroup":
		return h.handleModifyDBSubnetGroup(vals)
	case "ModifyDBClusterEndpoint":
		return h.handleModifyDBClusterEndpoint(vals)
	default:
		return h.dispatchExtended9(action, vals)
	}
}

func (h *Handler) handleCreateEventSubscription(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	snsTopicARN := vals.Get("SnsTopicArn")
	sourceType := vals.Get("SourceType")
	var sourceIDs []string
	for i := 1; ; i++ {
		id := vals.Get("SourceIds.member." + strconv.Itoa(i))
		if id == "" {
			break
		}
		sourceIDs = append(sourceIDs, id)
	}
	var eventCategories []string
	for i := 1; ; i++ {
		cat := vals.Get("EventCategories.member." + strconv.Itoa(i))
		if cat == "" {
			break
		}
		eventCategories = append(eventCategories, cat)
	}
	sub, err := h.Backend.CreateEventSubscription(name, snsTopicARN, sourceType, sourceIDs, eventCategories)
	if err != nil {
		return nil, err
	}

	return &createEventSubscriptionResponse{
		Xmlns:             rdsXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleDeleteEventSubscription(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	sub, err := h.Backend.DeleteEventSubscription(name)
	if err != nil {
		return nil, err
	}

	return &deleteEventSubscriptionResponse{
		Xmlns:             rdsXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleDescribeEventSubscriptions(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	subs, err := h.Backend.DescribeEventSubscriptions(name)
	if err != nil {
		return nil, err
	}
	members := make([]xmlEventSubscription, 0, len(subs))
	for i := range subs {
		members = append(members, toXMLEventSubscription(&subs[i]))
	}

	return &describeEventSubscriptionsResponse{
		Xmlns:                  rdsXMLNS,
		EventSubscriptionsList: xmlEventSubscriptionList{Members: members},
	}, nil
}

func (h *Handler) handleModifyEventSubscription(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	snsTopicARN := vals.Get("SnsTopicArn")
	sourceType := vals.Get("SourceType")
	var sourceIDs []string
	for i := 1; ; i++ {
		id := vals.Get("SourceIds.member." + strconv.Itoa(i))
		if id == "" {
			break
		}
		sourceIDs = append(sourceIDs, id)
	}
	var eventCategories []string
	for i := 1; ; i++ {
		cat := vals.Get("EventCategories.member." + strconv.Itoa(i))
		if cat == "" {
			break
		}
		eventCategories = append(eventCategories, cat)
	}
	var enabled *bool
	if v := vals.Get("Enabled"); v != "" {
		b := strings.EqualFold(v, "true")
		enabled = &b
	}
	sub, err := h.Backend.ModifyEventSubscription(name, snsTopicARN, sourceType, sourceIDs, eventCategories, enabled)
	if err != nil {
		return nil, err
	}

	return &modifyEventSubscriptionResponse{
		Xmlns:             rdsXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleDescribeEvents(vals url.Values) (any, error) {
	sourceID := vals.Get("SourceIdentifier")
	sourceType := vals.Get("SourceType")
	durationMinutes := 0
	if v := vals.Get("Duration"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			durationMinutes = n
		}
	}
	events, err := h.Backend.DescribeEvents(sourceID, sourceType, durationMinutes)
	if err != nil {
		return nil, err
	}
	members := make([]xmlEvent, 0, len(events))
	for _, ev := range events {
		members = append(members, xmlEvent{
			SourceIdentifier: ev.SourceIdentifier,
			SourceType:       ev.SourceType,
			Message:          ev.Message,
			Date:             ev.CreatedAt.Format("2006-01-02T15:04:05Z"),
		})
	}

	return &describeEventsResponse{
		Xmlns:  rdsXMLNS,
		Events: xmlEventList{Members: members},
	}, nil
}

func (h *Handler) handleDescribeEventCategories(vals url.Values) (any, error) {
	sourceType := vals.Get("SourceType")
	cats, err := h.Backend.DescribeEventCategories(sourceType)
	if err != nil {
		return nil, err
	}
	catMap := xmlEventCategoriesMap{
		SourceType:      sourceType,
		EventCategories: xmlStringList{Members: cats},
	}

	return &describeEventCategoriesResponse{
		Xmlns:                  rdsXMLNS,
		EventCategoriesMapList: xmlEventCategoriesMapList{Members: []xmlEventCategoriesMap{catMap}},
	}, nil
}

func (h *Handler) handleDescribeBlueGreenDeployments(vals url.Values) (any, error) {
	id := vals.Get("BlueGreenDeploymentIdentifier")
	deployments, err := h.Backend.DescribeBlueGreenDeployments(id)
	if err != nil {
		return nil, err
	}
	members := make([]xmlBlueGreenDeployment, 0, len(deployments))
	for i := range deployments {
		members = append(members, toXMLBlueGreenDeployment(&deployments[i]))
	}

	return &describeBlueGreenDeploymentsResponse{
		Xmlns:                rdsXMLNS,
		BlueGreenDeployments: xmlBlueGreenDeploymentList{Members: members},
	}, nil
}

func (h *Handler) handleDeleteBlueGreenDeployment(vals url.Values) (any, error) {
	id := vals.Get("BlueGreenDeploymentIdentifier")
	deployment, err := h.Backend.DeleteBlueGreenDeployment(id)
	if err != nil {
		return nil, err
	}

	return &deleteBlueGreenDeploymentResponse{
		Xmlns:               rdsXMLNS,
		BlueGreenDeployment: toXMLBlueGreenDeployment(deployment),
	}, nil
}

func (h *Handler) handleSwitchoverBlueGreenDeployment(vals url.Values) (any, error) {
	id := vals.Get("BlueGreenDeploymentIdentifier")
	deployment, err := h.Backend.SwitchoverBlueGreenDeployment(id)
	if err != nil {
		return nil, err
	}

	return &switchoverBlueGreenDeploymentResponse{
		Xmlns:               rdsXMLNS,
		BlueGreenDeployment: toXMLBlueGreenDeployment(deployment),
	}, nil
}

func (h *Handler) handleDescribeDBSecurityGroups(vals url.Values) (any, error) {
	name := vals.Get("DBSecurityGroupName")
	groups, err := h.Backend.DescribeDBSecurityGroups(name)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBSecurityGroup, 0, len(groups))
	for i := range groups {
		members = append(members, toXMLDBSecurityGroup(&groups[i]))
	}

	return &describeDBSecurityGroupsResponse{
		Xmlns:            rdsXMLNS,
		DBSecurityGroups: xmlDBSecurityGroupList{Members: members},
	}, nil
}

func (h *Handler) handleDeleteDBSecurityGroup(vals url.Values) (any, error) {
	name := vals.Get("DBSecurityGroupName")
	if err := h.Backend.DeleteDBSecurityGroup(name); err != nil {
		return nil, err
	}

	return &deleteDBSecurityGroupResponse{Xmlns: rdsXMLNS}, nil
}

func (h *Handler) handleRevokeDBSecurityGroupIngress(vals url.Values) (any, error) {
	groupName := vals.Get("DBSecurityGroupName")
	cidrIP := vals.Get("CIDRIP")
	sg, err := h.Backend.RevokeDBSecurityGroupIngress(groupName, cidrIP)
	if err != nil {
		return nil, err
	}

	return &revokeDBSecurityGroupIngressResponse{
		Xmlns:           rdsXMLNS,
		DBSecurityGroup: toXMLDBSecurityGroup(sg),
	}, nil
}

func (h *Handler) handleFailoverDBCluster(vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	targetDBInstanceIdentifier := vals.Get("TargetDBInstanceIdentifier")
	cluster, err := h.Backend.FailoverDBCluster(clusterID, targetDBInstanceIdentifier)
	if err != nil {
		return nil, err
	}

	return &failoverDBClusterResponse{
		Xmlns:     rdsXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleRebootDBCluster(vals url.Values) (any, error) {
	clusterID := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.RebootDBCluster(clusterID)
	if err != nil {
		return nil, err
	}

	return &rebootDBClusterResponse{
		Xmlns:     rdsXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
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

func (h *Handler) handleModifyDBSubnetGroup(vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")
	description := vals.Get("DBSubnetGroupDescription")
	var subnetIDs []string
	for i := 1; ; i++ {
		id := vals.Get("SubnetIds.member." + strconv.Itoa(i))
		if id == "" {
			break
		}
		subnetIDs = append(subnetIDs, id)
	}
	sg, err := h.Backend.ModifyDBSubnetGroup(name, description, subnetIDs)
	if err != nil {
		return nil, err
	}

	return &modifyDBSubnetGroupResponse{
		Xmlns:         rdsXMLNS,
		DBSubnetGroup: toXMLSubnetGroup(sg),
	}, nil
}

func (h *Handler) handleModifyDBClusterEndpoint(vals url.Values) (any, error) {
	endpointID := vals.Get("DBClusterEndpointIdentifier")
	endpointType := vals.Get("EndpointType")
	ep, err := h.Backend.ModifyDBClusterEndpoint(endpointID, endpointType)
	if err != nil {
		return nil, err
	}

	return &modifyDBClusterEndpointResponse{
		Xmlns:  rdsXMLNS,
		Result: modifyDBClusterEndpointResult{toXMLClusterEndpointFields(ep)},
	}, nil
}

// ---- XML response types for refinement1 ----

type createEventSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"CreateEventSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"CreateEventSubscriptionResult>EventSubscription"`
}

type deleteEventSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"DeleteEventSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"DeleteEventSubscriptionResult>EventSubscription"`
}

type xmlEventSubscriptionList struct {
	Members []xmlEventSubscription `xml:"EventSubscription"`
}

type describeEventSubscriptionsResponse struct {
	XMLName                xml.Name                 `xml:"DescribeEventSubscriptionsResponse"`
	Xmlns                  string                   `xml:"xmlns,attr"`
	EventSubscriptionsList xmlEventSubscriptionList `xml:"DescribeEventSubscriptionsResult>EventSubscriptionsList"`
}

type modifyEventSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"ModifyEventSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"ModifyEventSubscriptionResult>EventSubscription"`
}

type xmlEvent struct {
	SourceIdentifier string `xml:"SourceIdentifier,omitempty"`
	SourceType       string `xml:"SourceType,omitempty"`
	Message          string `xml:"Message,omitempty"`
	Date             string `xml:"Date,omitempty"`
}

type xmlEventList struct {
	Members []xmlEvent `xml:"Event"`
}

type describeEventsResponse struct {
	XMLName xml.Name     `xml:"DescribeEventsResponse"`
	Xmlns   string       `xml:"xmlns,attr"`
	Events  xmlEventList `xml:"DescribeEventsResult>Events"`
}

type xmlStringList struct {
	Members []string `xml:"member"`
}

type xmlEventCategoriesMap struct {
	SourceType      string        `xml:"SourceType,omitempty"`
	EventCategories xmlStringList `xml:"EventCategories"`
}

type xmlEventCategoriesMapList struct {
	Members []xmlEventCategoriesMap `xml:"EventCategoriesMap"`
}

type describeEventCategoriesResponse struct {
	XMLName                xml.Name                  `xml:"DescribeEventCategoriesResponse"`
	Xmlns                  string                    `xml:"xmlns,attr"`
	EventCategoriesMapList xmlEventCategoriesMapList `xml:"DescribeEventCategoriesResult>EventCategoriesMapList"`
}

type xmlBlueGreenDeploymentList struct {
	Members []xmlBlueGreenDeployment `xml:"BlueGreenDeployment"`
}

type describeBlueGreenDeploymentsResponse struct {
	XMLName              xml.Name                   `xml:"DescribeBlueGreenDeploymentsResponse"`
	Xmlns                string                     `xml:"xmlns,attr"`
	BlueGreenDeployments xmlBlueGreenDeploymentList `xml:"DescribeBlueGreenDeploymentsResult>BlueGreenDeployments"`
}

type deleteBlueGreenDeploymentResponse struct {
	XMLName             xml.Name               `xml:"DeleteBlueGreenDeploymentResponse"`
	Xmlns               string                 `xml:"xmlns,attr"`
	BlueGreenDeployment xmlBlueGreenDeployment `xml:"DeleteBlueGreenDeploymentResult>BlueGreenDeployment"`
}

type switchoverBlueGreenDeploymentResponse struct {
	XMLName             xml.Name               `xml:"SwitchoverBlueGreenDeploymentResponse"`
	Xmlns               string                 `xml:"xmlns,attr"`
	BlueGreenDeployment xmlBlueGreenDeployment `xml:"SwitchoverBlueGreenDeploymentResult>BlueGreenDeployment"`
}

type xmlDBSecurityGroupList struct {
	Members []xmlDBSecurityGroup `xml:"DBSecurityGroup"`
}

type describeDBSecurityGroupsResponse struct {
	XMLName          xml.Name               `xml:"DescribeDBSecurityGroupsResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	DBSecurityGroups xmlDBSecurityGroupList `xml:"DescribeDBSecurityGroupsResult>DBSecurityGroups"`
}

type deleteDBSecurityGroupResponse struct {
	XMLName xml.Name `xml:"DeleteDBSecurityGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type revokeDBSecurityGroupIngressResponse struct {
	XMLName         xml.Name           `xml:"RevokeDBSecurityGroupIngressResponse"`
	Xmlns           string             `xml:"xmlns,attr"`
	DBSecurityGroup xmlDBSecurityGroup `xml:"RevokeDBSecurityGroupIngressResult>DBSecurityGroup"`
}

type failoverDBClusterResponse struct {
	XMLName   xml.Name     `xml:"FailoverDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"FailoverDBClusterResult>DBCluster"`
}

type rebootDBClusterResponse struct {
	XMLName   xml.Name     `xml:"RebootDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"RebootDBClusterResult>DBCluster"`
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

type modifyDBSubnetGroupResponse struct {
	XMLName       xml.Name         `xml:"ModifyDBSubnetGroupResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	DBSubnetGroup xmlDBSubnetGroup `xml:"ModifyDBSubnetGroupResult>DBSubnetGroup"`
}

type modifyDBClusterEndpointResult struct {
	xmlDBClusterEndpointFields
}

type modifyDBClusterEndpointResponse struct {
	XMLName xml.Name                      `xml:"ModifyDBClusterEndpointResponse"`
	Xmlns   string                        `xml:"xmlns,attr"`
	Result  modifyDBClusterEndpointResult `xml:"ModifyDBClusterEndpointResult"`
}
