package dax

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	daxService         = "dax"
	daxTargetPrefix    = "AmazonDAXV3."
	daxMatchPriority   = service.PriorityHeaderExact
	clusterResponseKey = "Cluster"
	parameterGroupKey  = "ParameterGroup"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the HTTP handler for the Amazon DAX API.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new DAX handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset clears handler state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "DAX" }

// GetSupportedOperations returns the list of supported DAX operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateCluster",
		"DescribeClusters",
		"UpdateCluster",
		"DeleteCluster",
		"IncreaseReplicationFactor",
		"DecreaseReplicationFactor",
		"RebootNode",
		"TagResource",
		"UntagResource",
		"ListTags",
		"CreateParameterGroup",
		"DescribeParameterGroups",
		"UpdateParameterGroup",
		"DeleteParameterGroup",
		"DescribeParameters",
		"DescribeDefaultParameters",
		"ResetParameterGroup",
		"CreateSubnetGroup",
		"DescribeSubnetGroups",
		"UpdateSubnetGroup",
		"DeleteSubnetGroup",
		"DescribeEvents",
	}
}

// RouteMatcher returns a function that matches DAX API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), daxTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return daxMatchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, daxTargetPrefix)
}

// ExtractResource extracts the resource from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return h.ExtractOperation(c)
}

// Handler returns the Echo handler function for DAX requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "dax: failed to read request body", "error", err)

			return c.JSON(
				http.StatusBadRequest,
				daxError("SerializationException", "failed to read request body"),
			)
		}

		operation := h.ExtractOperation(c)
		if operation == "" {
			return c.JSON(
				http.StatusBadRequest,
				daxError("InvalidAction", "missing X-Amz-Target header"),
			)
		}

		resp, handlerErr := h.dispatch(ctx, operation, body)
		if handlerErr != nil {
			status, errBody := h.mapError(handlerErr)

			return c.JSON(status, errBody)
		}

		return c.JSON(http.StatusOK, resp)
	}
}

// dispatch routes the DAX operation to the appropriate handler function.
//
//nolint:cyclop // routing switch covers all DAX operations
func (h *Handler) dispatch(
	ctx context.Context,
	operation string,
	body []byte,
) (any, error) {
	_ = ctx // reserved for future logging/tracing use

	switch operation {
	case "CreateCluster":
		return h.handleCreateCluster(body)
	case "DescribeClusters":
		return h.handleDescribeClusters(body)
	case "UpdateCluster":
		return h.handleUpdateCluster(body)
	case "DeleteCluster":
		return h.handleDeleteCluster(body)
	case "IncreaseReplicationFactor":
		return h.handleIncreaseReplicationFactor(body)
	case "DecreaseReplicationFactor":
		return h.handleDecreaseReplicationFactor(body)
	case "RebootNode":
		return h.handleRebootNode(body)
	case "TagResource":
		return h.handleTagResource(body)
	case "UntagResource":
		return h.handleUntagResource(body)
	case "ListTags":
		return h.handleListTags(body)
	case "CreateParameterGroup":
		return h.handleCreateParameterGroup(body)
	case "DescribeParameterGroups":
		return h.handleDescribeParameterGroups(body)
	case "UpdateParameterGroup":
		return h.handleUpdateParameterGroup(body)
	case "DeleteParameterGroup":
		return h.handleDeleteParameterGroup(body)
	case "DescribeParameters":
		return h.handleDescribeParameters(body)
	case "DescribeDefaultParameters":
		return h.handleDescribeDefaultParameters(body)
	case "ResetParameterGroup":
		return h.handleResetParameterGroup(body)
	case "CreateSubnetGroup":
		return h.handleCreateSubnetGroup(body)
	case "DescribeSubnetGroups":
		return h.handleDescribeSubnetGroups(body)
	case "UpdateSubnetGroup":
		return h.handleUpdateSubnetGroup(body)
	case "DeleteSubnetGroup":
		return h.handleDeleteSubnetGroup(body)
	case "DescribeEvents":
		return h.handleDescribeEvents(body)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, operation)
	}
}

// ---- request/response types ----

type createClusterRequest struct {
	Tags                          map[string]string `json:"Tags"`
	NodeType                      string            `json:"NodeType"`
	ClusterName                   string            `json:"ClusterName"`
	Description                   string            `json:"Description"`
	IamRoleArn                    string            `json:"IamRoleArn"`
	SubnetGroupName               string            `json:"SubnetGroupName"`
	PreferredMaintenanceWindow    string            `json:"PreferredMaintenanceWindow"`
	ParameterGroupName            string            `json:"ParameterGroupName"`
	NotificationTopicArn          string            `json:"NotificationTopicArn"`
	ClusterEndpointEncryptionType string            `json:"ClusterEndpointEncryptionType"`
	AvailabilityZones             []string          `json:"AvailabilityZones"`
	SecurityGroupIDs              []string          `json:"SecurityGroupIds"`
	ReplicationFactor             int               `json:"ReplicationFactor"`
	SSESpecification              struct {
		Enabled bool `json:"Enabled"`
	} `json:"SSESpecification"`
}

type describeClustersRequest struct {
	NextToken    string   `json:"NextToken"`
	ClusterNames []string `json:"ClusterNames"`
	MaxResults   int      `json:"MaxResults"`
}

type updateClusterRequest struct {
	Description                *string  `json:"Description"`
	ClusterName                string   `json:"ClusterName"`
	PreferredMaintenanceWindow string   `json:"PreferredMaintenanceWindow"`
	ParameterGroupName         string   `json:"ParameterGroupName"`
	NotificationTopicArn       string   `json:"NotificationTopicArn"`
	NotificationTopicStatus    string   `json:"NotificationTopicStatus"`
	SecurityGroupIDs           []string `json:"SecurityGroupIds"`
}

type deleteClusterRequest struct {
	ClusterName string `json:"ClusterName"`
}

type increaseReplicationFactorRequest struct {
	ClusterName          string   `json:"ClusterName"`
	AvailabilityZones    []string `json:"AvailabilityZones"`
	NewReplicationFactor int      `json:"NewReplicationFactor"`
}

type decreaseReplicationFactorRequest struct {
	ClusterName          string   `json:"ClusterName"`
	NodeIDsToRemove      []string `json:"NodeIdsToRemove"`
	NewReplicationFactor int      `json:"NewReplicationFactor"`
}

type rebootNodeRequest struct {
	ClusterName string `json:"ClusterName"`
	NodeID      string `json:"NodeId"`
}

type tagResourceRequest struct {
	ResourceName string    `json:"ResourceName"`
	Tags         []tagItem `json:"Tags"`
}

type untagResourceRequest struct {
	ResourceName string   `json:"ResourceName"`
	TagKeys      []string `json:"TagKeys"`
}

type listTagsRequest struct {
	ResourceName string `json:"ResourceName"`
	NextToken    string `json:"NextToken"`
}

type createParameterGroupRequest struct {
	ParameterGroupName string `json:"ParameterGroupName"`
	Description        string `json:"Description"`
}

type describeParameterGroupsRequest struct {
	NextToken           string   `json:"NextToken"`
	ParameterGroupNames []string `json:"ParameterGroupNames"`
	MaxResults          int      `json:"MaxResults"`
}

type updateParameterGroupRequest struct {
	ParameterGroupName  string               `json:"ParameterGroupName"`
	ParameterNameValues []parameterNameValue `json:"ParameterNameValues"`
}

type parameterNameValue struct {
	ParameterName  string `json:"ParameterName"`
	ParameterValue string `json:"ParameterValue"`
}

type deleteParameterGroupRequest struct {
	ParameterGroupName string `json:"ParameterGroupName"`
}

type describeParametersRequest struct {
	ParameterGroupName string `json:"ParameterGroupName"`
	NextToken          string `json:"NextToken"`
	MaxResults         int    `json:"MaxResults"`
}

type describeDefaultParametersRequest struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type resetParameterGroupRequest struct {
	ParameterGroupName string   `json:"ParameterGroupName"`
	ParameterNames     []string `json:"ParameterNames"`
}

type createSubnetGroupRequest struct {
	SubnetGroupName string   `json:"SubnetGroupName"`
	Description     string   `json:"Description"`
	SubnetIDs       []string `json:"SubnetIds"`
}

type describeSubnetGroupsRequest struct {
	NextToken        string   `json:"NextToken"`
	SubnetGroupNames []string `json:"SubnetGroupNames"`
	MaxResults       int      `json:"MaxResults"`
}

type updateSubnetGroupRequest struct {
	SubnetGroupName string   `json:"SubnetGroupName"`
	Description     string   `json:"Description"`
	SubnetIDs       []string `json:"SubnetIds"`
}

type deleteSubnetGroupRequest struct {
	SubnetGroupName string `json:"SubnetGroupName"`
}

type describeEventsRequest struct {
	SourceName string `json:"SourceName"`
	SourceType string `json:"SourceType"`
	StartTime  string `json:"StartTime"`
	EndTime    string `json:"EndTime"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type tagItem struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// ---- cluster response helpers ----

type clusterResponse struct {
	ParameterGroup                *paramGroupStatus       `json:"ParameterGroup,omitempty"`
	SSEDescription                *sseDescResponse        `json:"SSEDescription,omitempty"`
	Endpoint                      *endpointResponse       `json:"ClusterDiscoveryEndpoint,omitempty"`
	NotificationConfiguration     *notificationConfigResp `json:"NotificationConfiguration,omitempty"`
	ClusterName                   string                  `json:"ClusterName"`
	ClusterArn                    string                  `json:"ClusterArn"`
	Description                   string                  `json:"Description,omitempty"`
	NodeType                      string                  `json:"NodeType"`
	Status                        string                  `json:"ClusterStatus"`
	SubnetGroup                   string                  `json:"SubnetGroup,omitempty"`
	IamRoleArn                    string                  `json:"IamRoleArn,omitempty"`
	PreferredMaintenanceWindow    string                  `json:"PreferredMaintenanceWindow,omitempty"`
	ClusterEndpointEncryptionType string                  `json:"ClusterEndpointEncryptionType,omitempty"`
	Nodes                         []nodeResponse          `json:"Nodes,omitempty"`
	SecurityGroups                []securityGroupResp     `json:"SecurityGroups,omitempty"`
	Tags                          []tagItem               `json:"Tags,omitempty"`
	TotalNodes                    int                     `json:"TotalNodes"`
	ActiveNodes                   int                     `json:"ActiveNodes"`
}

type endpointResponse struct {
	Address string `json:"Address"`
	URL     string `json:"URL,omitempty"`
	Port    int    `json:"Port"`
}

type nodeResponse struct {
	Endpoint             *endpointResponse `json:"Endpoint,omitempty"`
	NodeID               string            `json:"NodeId"`
	NodeStatus           string            `json:"NodeStatus"`
	AvailabilityZone     string            `json:"AvailabilityZone,omitempty"`
	NodeCreateTime       string            `json:"NodeCreateTime,omitempty"`
	ParameterGroupStatus string            `json:"ParameterGroupStatus,omitempty"`
}

type paramGroupStatus struct {
	ParameterGroupName   string   `json:"ParameterGroupName"`
	ParameterApplyStatus string   `json:"ParameterApplyStatus,omitempty"`
	NodeIDsToReboot      []string `json:"NodeIDsToReboot,omitempty"`
}

type sseDescResponse struct {
	Status string `json:"Status"`
}

type securityGroupResp struct {
	SecurityGroupIdentifier string `json:"SecurityGroupIdentifier"`
	Status                  string `json:"Status"`
}

type notificationConfigResp struct {
	TopicArn    string `json:"TopicArn"`
	TopicStatus string `json:"TopicStatus"`
}

type parameterGroupResponse struct {
	ParameterGroupName string `json:"ParameterGroupName"`
	Description        string `json:"Description,omitempty"`
}

type parameterResponse struct {
	ParameterName  string `json:"ParameterName"`
	ParameterValue string `json:"ParameterValue"`
	Description    string `json:"Description,omitempty"`
	Source         string `json:"Source,omitempty"`
	DataType       string `json:"DataType,omitempty"`
	IsModifiable   string `json:"IsModifiable,omitempty"`
	ChangeType     string `json:"ChangeType,omitempty"`
}

type subnetGroupResponse struct {
	SubnetGroupName string       `json:"SubnetGroupName"`
	Description     string       `json:"Description,omitempty"`
	VpcID           string       `json:"VpcId,omitempty"`
	Subnets         []subnetItem `json:"Subnets,omitempty"`
}

type subnetItem struct {
	SubnetIdentifier       string `json:"SubnetIdentifier"`
	SubnetAvailabilityZone struct {
		Name string `json:"Name"`
	} `json:"SubnetAvailabilityZone"`
}

type eventResponse struct {
	SourceName string `json:"SourceName"`
	SourceType string `json:"SourceType"`
	Message    string `json:"Message"`
	Date       string `json:"Date"`
}

// toClusterResponse converts a Cluster to its JSON response form.
func toClusterResponse(c *Cluster) clusterResponse {
	resp := clusterResponse{
		ClusterName:                   c.ClusterName,
		ClusterArn:                    c.ClusterArn,
		Description:                   c.Description,
		NodeType:                      c.NodeType,
		Status:                        c.Status,
		IamRoleArn:                    c.IamRoleArn,
		SubnetGroup:                   c.SubnetGroupName,
		ActiveNodes:                   c.ActiveNodes,
		TotalNodes:                    c.TotalNodes,
		PreferredMaintenanceWindow:    c.PreferredMaintenanceWindow,
		ClusterEndpointEncryptionType: c.ClusterEndpointEncryptionType,
		ParameterGroup: &paramGroupStatus{
			ParameterGroupName:   c.ParameterGroup.ParameterGroupName,
			ParameterApplyStatus: c.ParameterGroup.ParameterApplyStatus,
		},
		SSEDescription: &sseDescResponse{Status: c.SSEDescription.Status},
	}

	if c.Endpoint != nil {
		resp.Endpoint = &endpointResponse{
			Address: c.Endpoint.Address,
			Port:    c.Endpoint.Port,
			URL:     c.Endpoint.URL,
		}
	}

	if c.NotificationConfiguration != nil {
		resp.NotificationConfiguration = &notificationConfigResp{
			TopicArn:    c.NotificationConfiguration.TopicArn,
			TopicStatus: c.NotificationConfiguration.TopicStatus,
		}
	}

	for _, sg := range c.SecurityGroupIDs {
		resp.SecurityGroups = append(resp.SecurityGroups, securityGroupResp{
			SecurityGroupIdentifier: sg,
			Status:                  "active",
		})
	}

	for _, n := range c.Nodes {
		nr := nodeResponse{
			NodeID:               n.NodeID,
			NodeStatus:           n.NodeStatus,
			AvailabilityZone:     n.AvailabilityZone,
			NodeCreateTime:       n.CreateTime.Format(time.RFC3339),
			ParameterGroupStatus: n.ParameterGroupStatus,
		}

		if n.Endpoint != nil {
			nr.Endpoint = &endpointResponse{
				Address: n.Endpoint.Address,
				Port:    n.Endpoint.Port,
				URL:     n.Endpoint.URL,
			}
		}

		resp.Nodes = append(resp.Nodes, nr)
	}

	for k, v := range c.Tags {
		resp.Tags = append(resp.Tags, tagItem{Key: k, Value: v})
	}

	return resp
}

// toSubnetGroupResponse converts a SubnetGroup to its JSON response form.
func toSubnetGroupResponse(sg *SubnetGroup) subnetGroupResponse {
	items := make([]subnetItem, 0, len(sg.Subnets))

	for _, entry := range sg.Subnets {
		item := subnetItem{
			SubnetIdentifier: entry.SubnetID,
		}
		item.SubnetAvailabilityZone.Name = entry.AvailabilityZone

		items = append(items, item)
	}

	return subnetGroupResponse{
		SubnetGroupName: sg.SubnetGroupName,
		Description:     sg.Description,
		VpcID:           sg.VpcID,
		Subnets:         items,
	}
}

// toParameterResponse converts a Parameter to its JSON response form.
func toParameterResponse(p *Parameter) parameterResponse {
	return parameterResponse{
		ParameterName:  p.ParameterName,
		ParameterValue: p.ParameterValue,
		Description:    p.Description,
		Source:         p.Source,
		DataType:       p.DataType,
		IsModifiable:   p.IsModifiable,
		ChangeType:     p.ChangeType,
	}
}

// ---- handlers ----

func (h *Handler) handleCreateCluster(body []byte) (any, error) {
	var req createClusterRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	cluster, err := h.Backend.CreateCluster(CreateClusterInput{
		ClusterName:                   req.ClusterName,
		NodeType:                      req.NodeType,
		Description:                   req.Description,
		IamRoleArn:                    req.IamRoleArn,
		ReplicationFactor:             req.ReplicationFactor,
		AvailabilityZones:             req.AvailabilityZones,
		SubnetGroupName:               req.SubnetGroupName,
		SecurityGroupIDs:              req.SecurityGroupIDs,
		PreferredMaintenanceWindow:    req.PreferredMaintenanceWindow,
		ParameterGroupName:            req.ParameterGroupName,
		NotificationTopicArn:          req.NotificationTopicArn,
		ClusterEndpointEncryptionType: req.ClusterEndpointEncryptionType,
		Tags:                          req.Tags,
		SSESpecificationEnabled:       req.SSESpecification.Enabled,
	})
	if err != nil {
		return nil, err
	}

	return map[string]any{
		clusterResponseKey: toClusterResponse(cluster),
	}, nil
}

func (h *Handler) handleDescribeClusters(body []byte) (any, error) {
	var req describeClustersRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	clusters, nextToken, err := h.Backend.DescribeClusters(
		req.ClusterNames,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]clusterResponse, 0, len(clusters))
	for _, c := range clusters {
		items = append(items, toClusterResponse(c))
	}

	result := map[string]any{
		"Clusters": items,
	}

	if nextToken != "" {
		result["NextToken"] = nextToken
	}

	return result, nil
}

func (h *Handler) handleUpdateCluster(body []byte) (any, error) {
	var req updateClusterRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	cluster, err := h.Backend.UpdateCluster(UpdateClusterInput(req))
	if err != nil {
		return nil, err
	}

	return map[string]any{
		clusterResponseKey: toClusterResponse(cluster),
	}, nil
}

func (h *Handler) handleDeleteCluster(body []byte) (any, error) {
	var req deleteClusterRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	cluster, err := h.Backend.DeleteCluster(req.ClusterName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		clusterResponseKey: toClusterResponse(cluster),
	}, nil
}

func (h *Handler) handleIncreaseReplicationFactor(body []byte) (any, error) {
	var req increaseReplicationFactorRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	cluster, err := h.Backend.IncreaseReplicationFactor(IncreaseReplicationFactorInput(req))
	if err != nil {
		return nil, err
	}

	return map[string]any{
		clusterResponseKey: toClusterResponse(cluster),
	}, nil
}

func (h *Handler) handleDecreaseReplicationFactor(body []byte) (any, error) {
	var req decreaseReplicationFactorRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	cluster, err := h.Backend.DecreaseReplicationFactor(DecreaseReplicationFactorInput(req))
	if err != nil {
		return nil, err
	}

	return map[string]any{
		clusterResponseKey: toClusterResponse(cluster),
	}, nil
}

func (h *Handler) handleRebootNode(body []byte) (any, error) {
	var req rebootNodeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	cluster, err := h.Backend.RebootNode(req.ClusterName, req.NodeID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		clusterResponseKey: toClusterResponse(cluster),
	}, nil
}

func (h *Handler) handleTagResource(body []byte) (any, error) {
	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	tags := make(map[string]string, len(req.Tags))
	for _, t := range req.Tags {
		tags[t.Key] = t.Value
	}

	if err := h.Backend.TagResource(req.ResourceName, tags); err != nil {
		return nil, err
	}

	return map[string]any{
		"Tags": req.Tags,
	}, nil
}

func (h *Handler) handleUntagResource(body []byte) (any, error) {
	var req untagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UntagResource(req.ResourceName, req.TagKeys); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) handleListTags(body []byte) (any, error) {
	var req listTagsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	tags, nextToken, err := h.Backend.ListTags(req.ResourceName, req.NextToken)
	if err != nil {
		return nil, err
	}

	items := make([]tagItem, 0, len(tags))
	for k, v := range tags {
		items = append(items, tagItem{Key: k, Value: v})
	}

	result := map[string]any{
		"Tags": items,
	}

	if nextToken != "" {
		result["NextToken"] = nextToken
	}

	return result, nil
}

func (h *Handler) handleCreateParameterGroup(body []byte) (any, error) {
	var req createParameterGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	pg, err := h.Backend.CreateParameterGroup(req.ParameterGroupName, req.Description)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		parameterGroupKey: parameterGroupResponse{
			ParameterGroupName: pg.ParameterGroupName,
			Description:        pg.Description,
		},
	}, nil
}

func (h *Handler) handleDescribeParameterGroups(body []byte) (any, error) {
	var req describeParameterGroupsRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	groups, nextToken, err := h.Backend.DescribeParameterGroups(
		req.ParameterGroupNames,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]parameterGroupResponse, 0, len(groups))
	for _, pg := range groups {
		items = append(items, parameterGroupResponse{
			ParameterGroupName: pg.ParameterGroupName,
			Description:        pg.Description,
		})
	}

	result := map[string]any{
		"ParameterGroups": items,
	}

	if nextToken != "" {
		result["NextToken"] = nextToken
	}

	return result, nil
}

func (h *Handler) handleUpdateParameterGroup(body []byte) (any, error) {
	var req updateParameterGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	pvs := make([]ParameterNameValue, 0, len(req.ParameterNameValues))
	for _, pv := range req.ParameterNameValues {
		pvs = append(pvs, ParameterNameValue(pv))
	}

	pg, err := h.Backend.UpdateParameterGroup(UpdateParameterGroupInput{
		ParameterGroupName:  req.ParameterGroupName,
		ParameterNameValues: pvs,
	})
	if err != nil {
		return nil, err
	}

	return map[string]any{
		parameterGroupKey: parameterGroupResponse{
			ParameterGroupName: pg.ParameterGroupName,
			Description:        pg.Description,
		},
	}, nil
}

func (h *Handler) handleDeleteParameterGroup(body []byte) (any, error) {
	var req deleteParameterGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteParameterGroup(req.ParameterGroupName); err != nil {
		return nil, err
	}

	return map[string]any{
		"DeletionMessage": fmt.Sprintf("ParameterGroup %s deleted", req.ParameterGroupName),
	}, nil
}

func (h *Handler) handleDescribeParameters(body []byte) (any, error) {
	var req describeParametersRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	params, nextToken, err := h.Backend.DescribeParameters(req.ParameterGroupName, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	items := make([]parameterResponse, 0, len(params))
	for _, p := range params {
		items = append(items, toParameterResponse(p))
	}

	result := map[string]any{
		"Parameters": items,
	}

	if nextToken != "" {
		result["NextToken"] = nextToken
	}

	return result, nil
}

func (h *Handler) handleDescribeDefaultParameters(body []byte) (any, error) {
	var req describeDefaultParametersRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	params, nextToken, err := h.Backend.DescribeDefaultParameters(req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	items := make([]parameterResponse, 0, len(params))
	for _, p := range params {
		items = append(items, toParameterResponse(p))
	}

	result := map[string]any{
		"Parameters": items,
	}

	if nextToken != "" {
		result["NextToken"] = nextToken
	}

	return result, nil
}

func (h *Handler) handleResetParameterGroup(body []byte) (any, error) {
	var req resetParameterGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	pg, err := h.Backend.ResetParameterGroup(req.ParameterGroupName, req.ParameterNames)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		parameterGroupKey: parameterGroupResponse{
			ParameterGroupName: pg.ParameterGroupName,
			Description:        pg.Description,
		},
	}, nil
}

func (h *Handler) handleCreateSubnetGroup(body []byte) (any, error) {
	var req createSubnetGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	sg, err := h.Backend.CreateSubnetGroup(req.SubnetGroupName, req.Description, req.SubnetIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"SubnetGroup": toSubnetGroupResponse(sg),
	}, nil
}

func (h *Handler) handleDescribeSubnetGroups(body []byte) (any, error) {
	var req describeSubnetGroupsRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	groups, nextToken, err := h.Backend.DescribeSubnetGroups(
		req.SubnetGroupNames,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]subnetGroupResponse, 0, len(groups))
	for _, sg := range groups {
		items = append(items, toSubnetGroupResponse(sg))
	}

	result := map[string]any{
		"SubnetGroups": items,
	}

	if nextToken != "" {
		result["NextToken"] = nextToken
	}

	return result, nil
}

func (h *Handler) handleUpdateSubnetGroup(body []byte) (any, error) {
	var req updateSubnetGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	sg, err := h.Backend.UpdateSubnetGroup(UpdateSubnetGroupInput(req))
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"SubnetGroup": toSubnetGroupResponse(sg),
	}, nil
}

func (h *Handler) handleDeleteSubnetGroup(body []byte) (any, error) {
	var req deleteSubnetGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteSubnetGroup(req.SubnetGroupName); err != nil {
		return nil, err
	}

	return map[string]any{
		"DeletionMessage": fmt.Sprintf("SubnetGroup %s deleted", req.SubnetGroupName),
	}, nil
}

func (h *Handler) handleDescribeEvents(body []byte) (any, error) {
	var req describeEventsRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	var startTime, endTime *time.Time

	if req.StartTime != "" {
		t, err := time.Parse(time.RFC3339, req.StartTime)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid StartTime format", errInvalidRequest)
		}

		startTime = &t
	}

	if req.EndTime != "" {
		t, err := time.Parse(time.RFC3339, req.EndTime)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid EndTime format", errInvalidRequest)
		}

		endTime = &t
	}

	events, nextToken, err := h.Backend.DescribeEvents(
		req.SourceName,
		req.SourceType,
		startTime,
		endTime,
		req.MaxResults,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]eventResponse, 0, len(events))
	for _, ev := range events {
		items = append(items, eventResponse{
			SourceName: ev.SourceName,
			SourceType: ev.SourceType,
			Message:    ev.Message,
			Date:       ev.Date.Format(time.RFC3339),
		})
	}

	result := map[string]any{
		"Events": items,
	}

	if nextToken != "" {
		result["NextToken"] = nextToken
	}

	return result, nil
}

// mapError maps a backend error to an HTTP status code and error body.
// Specific sentinel errors take priority over their parent error categories.
//
//nolint:cyclop // exhaustive error mapping requires many cases
func (h *Handler) mapError(err error) (int, map[string]any) {
	// Specific not-found variants.
	switch {
	case errors.Is(err, ErrClusterNotFound):
		return http.StatusBadRequest, daxError("ClusterNotFoundFault", err.Error())
	case errors.Is(err, ErrParameterGroupNotFound):
		return http.StatusBadRequest, daxError("ParameterGroupNotFoundFault", err.Error())
	case errors.Is(err, ErrSubnetGroupNotFound):
		return http.StatusBadRequest, daxError("SubnetGroupNotFoundFault", err.Error())
	case errors.Is(err, ErrTagNotFound):
		return http.StatusBadRequest, daxError("TagNotFoundFault", err.Error())
	case errors.Is(err, ErrNodeNotFound):
		return http.StatusBadRequest, daxError("NodeNotFoundFault", err.Error())

	// Specific conflict variants.
	case errors.Is(err, ErrClusterAlreadyExists):
		return http.StatusBadRequest, daxError("ClusterAlreadyExistsFault", err.Error())
	case errors.Is(err, ErrParameterGroupAlreadyExists):
		return http.StatusBadRequest, daxError("ParameterGroupAlreadyExistsFault", err.Error())
	case errors.Is(err, ErrSubnetGroupAlreadyExists):
		return http.StatusBadRequest, daxError("SubnetGroupAlreadyExistsFault", err.Error())
	case errors.Is(err, ErrInvalidClusterState):
		return http.StatusBadRequest, daxError("InvalidClusterStateFault", err.Error())

	// Specific invalid parameter variants.
	case errors.Is(err, ErrInvalidARN):
		return http.StatusBadRequest, daxError("InvalidARNFault", err.Error())
	case errors.Is(err, ErrInvalidParameterValue):
		return http.StatusBadRequest, daxError("InvalidParameterValueException", err.Error())
	case errors.Is(err, ErrInvalidParameterCombination):
		return http.StatusBadRequest, daxError("InvalidParameterCombinationException", err.Error())

	// Generic fallbacks.
	case errors.Is(err, awserr.ErrNotFound):
		return http.StatusBadRequest, daxError("ResourceNotFoundException", err.Error())
	case errors.Is(err, awserr.ErrConflict):
		return http.StatusBadRequest, daxError("InvalidClusterStateFault", err.Error())
	case errors.Is(err, awserr.ErrInvalidParameter):
		return http.StatusBadRequest, daxError("InvalidParameterValueException", err.Error())
	case errors.Is(err, errUnknownAction):
		return http.StatusBadRequest, daxError("InvalidAction", err.Error())
	case errors.Is(err, errInvalidRequest):
		return http.StatusBadRequest, daxError("SerializationException", err.Error())
	default:
		return http.StatusInternalServerError, daxError("InternalFailure", err.Error())
	}
}

// daxError builds a standard DAX JSON error body.
func daxError(code, message string) map[string]any {
	return map[string]any{
		"__type":  code,
		"message": message,
	}
}

// Snapshot and Restore are delegated to the backend.

// Snapshot returns the backend state as JSON bytes.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore restores backend state from JSON bytes.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
