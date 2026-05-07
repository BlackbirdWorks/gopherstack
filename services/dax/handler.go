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
		"TagResource",
		"UntagResource",
		"ListTags",
		"CreateParameterGroup",
		"DescribeParameterGroups",
		"DeleteParameterGroup",
		"CreateSubnetGroup",
		"DescribeSubnetGroups",
		"DeleteSubnetGroup",
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
func (h *Handler) dispatch(ctx context.Context, operation string, body []byte) (any, error) {
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
	case "DeleteParameterGroup":
		return h.handleDeleteParameterGroup(body)
	case "CreateSubnetGroup":
		return h.handleCreateSubnetGroup(body)
	case "DescribeSubnetGroups":
		return h.handleDescribeSubnetGroups(body)
	case "DeleteSubnetGroup":
		return h.handleDeleteSubnetGroup(body)
	default:
		return nil, fmt.Errorf("%w: %s", errUnknownAction, operation)
	}
}

// ---- request/response types ----

type createClusterRequest struct {
	Tags                       map[string]string `json:"Tags"`
	NodeType                   string            `json:"NodeType"`
	ClusterName                string            `json:"ClusterName"`
	Description                string            `json:"Description"`
	IamRoleArn                 string            `json:"IamRoleArn"`
	SubnetGroupName            string            `json:"SubnetGroupName"`
	PreferredMaintenanceWindow string            `json:"PreferredMaintenanceWindow"`
	ParameterGroupName         string            `json:"ParameterGroupName"`
	AvailabilityZones          []string          `json:"AvailabilityZones"`
	SecurityGroupIDs           []string          `json:"SecurityGroupIds"`
	ReplicationFactor          int               `json:"ReplicationFactor"`
	SSESpecification           struct {
		Enabled bool `json:"Enabled"`
	} `json:"SSESpecification"`
}

type describeClustersRequest struct {
	NextToken    string   `json:"NextToken"`
	ClusterNames []string `json:"ClusterNames"`
	MaxResults   int      `json:"MaxResults"`
}

type updateClusterRequest struct {
	ClusterName                string   `json:"ClusterName"`
	Description                string   `json:"Description"`
	PreferredMaintenanceWindow string   `json:"PreferredMaintenanceWindow"`
	ParameterGroupName         string   `json:"ParameterGroupName"`
	NotificationTopicArn       string   `json:"NotificationTopicArn"`
	NotificationTopicStatus    string   `json:"NotificationTopicStatus"`
	SecurityGroupIDs           []string `json:"SecurityGroupIds"`
}

type deleteClusterRequest struct {
	ClusterName string `json:"ClusterName"`
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

type deleteParameterGroupRequest struct {
	ParameterGroupName string `json:"ParameterGroupName"`
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

type deleteSubnetGroupRequest struct {
	SubnetGroupName string `json:"SubnetGroupName"`
}

type tagItem struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// ---- cluster response helpers ----

type clusterResponse struct {
	ParameterGroup             *paramGroupStatus   `json:"ParameterGroup,omitempty"`
	SSEDescription             *sseDescResponse    `json:"SSEDescription,omitempty"`
	Endpoint                   *endpointResponse   `json:"ClusterDiscoveryEndpoint,omitempty"`
	ClusterName                string              `json:"ClusterName"`
	ClusterArn                 string              `json:"ClusterArn"`
	Description                string              `json:"Description,omitempty"`
	NodeType                   string              `json:"NodeType"`
	Status                     string              `json:"ClusterStatus"`
	SubnetGroup                string              `json:"SubnetGroup,omitempty"`
	IamRoleArn                 string              `json:"IamRoleArn,omitempty"`
	PreferredMaintenanceWindow string              `json:"PreferredMaintenanceWindow,omitempty"`
	Nodes                      []nodeResponse      `json:"Nodes,omitempty"`
	SecurityGroups             []securityGroupResp `json:"SecurityGroups,omitempty"`
	Tags                       []tagItem           `json:"Tags,omitempty"`
	TotalNodes                 int                 `json:"TotalNodes"`
	ActiveNodes                int                 `json:"ActiveNodes"`
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

type parameterGroupResponse struct {
	ParameterGroupName string `json:"ParameterGroupName"`
	Description        string `json:"Description,omitempty"`
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

// toClusterResponse converts a Cluster to its JSON response form.
func toClusterResponse(c *Cluster) clusterResponse {
	resp := clusterResponse{
		ClusterName:                c.ClusterName,
		ClusterArn:                 c.ClusterArn,
		Description:                c.Description,
		NodeType:                   c.NodeType,
		Status:                     c.Status,
		IamRoleArn:                 c.IamRoleArn,
		SubnetGroup:                c.SubnetGroupName,
		ActiveNodes:                c.ActiveNodes,
		TotalNodes:                 c.TotalNodes,
		PreferredMaintenanceWindow: c.PreferredMaintenanceWindow,
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

// ---- handlers ----

func (h *Handler) handleCreateCluster(body []byte) (any, error) {
	var req createClusterRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	cluster, err := h.Backend.CreateCluster(CreateClusterInput{
		ClusterName:                req.ClusterName,
		NodeType:                   req.NodeType,
		Description:                req.Description,
		IamRoleArn:                 req.IamRoleArn,
		ReplicationFactor:          req.ReplicationFactor,
		AvailabilityZones:          req.AvailabilityZones,
		SubnetGroupName:            req.SubnetGroupName,
		SecurityGroupIDs:           req.SecurityGroupIDs,
		PreferredMaintenanceWindow: req.PreferredMaintenanceWindow,
		ParameterGroupName:         req.ParameterGroupName,
		Tags:                       req.Tags,
		SSESpecificationEnabled:    req.SSESpecification.Enabled,
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

	cluster, err := h.Backend.UpdateCluster(
		UpdateClusterInput{ //nolint:staticcheck // struct types differ due to json tags, conversion not possible
			ClusterName:                req.ClusterName,
			Description:                req.Description,
			PreferredMaintenanceWindow: req.PreferredMaintenanceWindow,
			SecurityGroupIDs:           req.SecurityGroupIDs,
			ParameterGroupName:         req.ParameterGroupName,
			NotificationTopicArn:       req.NotificationTopicArn,
			NotificationTopicStatus:    req.NotificationTopicStatus,
		},
	)
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
		"ParameterGroup": parameterGroupResponse{
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

func toSubnetGroupResponse(sg *SubnetGroup) subnetGroupResponse {
	items := make([]subnetItem, 0, len(sg.SubnetIDs))

	for _, id := range sg.SubnetIDs {
		item := subnetItem{
			SubnetIdentifier: id,
		}
		item.SubnetAvailabilityZone.Name = "us-east-1a" // placeholder

		items = append(items, item)
	}

	return subnetGroupResponse{
		SubnetGroupName: sg.SubnetGroupName,
		Description:     sg.Description,
		VpcID:           sg.VpcID,
		Subnets:         items,
	}
}

// mapError maps a backend error to an HTTP status code and error body.
func (h *Handler) mapError(err error) (int, map[string]any) {
	switch {
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
