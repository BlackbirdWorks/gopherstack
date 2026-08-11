package dax

import (
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

type createClusterRequest struct {
	Tags                          []tagItem `json:"Tags"`
	NodeType                      string    `json:"NodeType"`
	ClusterName                   string    `json:"ClusterName"`
	Description                   string    `json:"Description"`
	IamRoleArn                    string    `json:"IamRoleArn"`
	SubnetGroupName               string    `json:"SubnetGroupName"`
	PreferredMaintenanceWindow    string    `json:"PreferredMaintenanceWindow"`
	ParameterGroupName            string    `json:"ParameterGroupName"`
	NotificationTopicArn          string    `json:"NotificationTopicArn"`
	ClusterEndpointEncryptionType string    `json:"ClusterEndpointEncryptionType"`
	NetworkType                   string    `json:"NetworkType"`
	AvailabilityZones             []string  `json:"AvailabilityZones"`
	SecurityGroupIDs              []string  `json:"SecurityGroupIds"`
	ReplicationFactor             int       `json:"ReplicationFactor"`
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
	Status                        string                  `json:"Status"`
	SubnetGroup                   string                  `json:"SubnetGroup,omitempty"`
	IamRoleArn                    string                  `json:"IamRoleArn,omitempty"`
	PreferredMaintenanceWindow    string                  `json:"PreferredMaintenanceWindow,omitempty"`
	ClusterEndpointEncryptionType string                  `json:"ClusterEndpointEncryptionType,omitempty"`
	NetworkType                   string                  `json:"NetworkType,omitempty"`
	Nodes                         []nodeResponse          `json:"Nodes,omitempty"`
	SecurityGroups                []securityGroupResp     `json:"SecurityGroups,omitempty"`
	NodeIDsToRemove               []string                `json:"NodeIdsToRemove,omitempty"`
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
	ParameterGroupStatus string            `json:"ParameterGroupStatus,omitempty"`
	// NodeCreateTime is epoch seconds (float64), matching the DAX awsjson1.1
	// wire format -- the real SDK deserializer parses this field with
	// smithytime.ParseEpochSeconds and rejects RFC3339 strings.
	NodeCreateTime float64 `json:"NodeCreateTime,omitempty"`
}

type paramGroupStatus struct {
	ParameterGroupName   string `json:"ParameterGroupName"`
	ParameterApplyStatus string `json:"ParameterApplyStatus,omitempty"`
	// NodeIDsToReboot: the wire key casing is "NodeIdsToReboot" (lowercase
	// "ds"), not "NodeIDsToReboot" -- the client's hand-rolled JSON
	// deserializer matches on the exact key string. The Go field name keeps
	// the ID-initialism spelling per project convention; only the json tag
	// needs to match the wire format.
	NodeIDsToReboot []string `json:"NodeIdsToReboot,omitempty"`
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
		NetworkType:                   c.NetworkType,
		NodeIDsToRemove:               c.NodeIDsToRemove,
		ParameterGroup: &paramGroupStatus{
			ParameterGroupName:   c.ParameterGroup.ParameterGroupName,
			ParameterApplyStatus: c.ParameterGroup.ParameterApplyStatus,
			NodeIDsToReboot:      c.ParameterGroup.NodeIDsToReboot,
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
			NodeCreateTime:       awstime.Epoch(n.CreateTime),
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

	return resp
}

// ---- handlers ----

func (h *Handler) handleCreateCluster(body []byte) (any, error) {
	var req createClusterRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := validateTagItems(req.Tags); err != nil {
		return nil, err
	}

	tags := make(map[string]string, len(req.Tags))
	for _, t := range req.Tags {
		tags[t.Key] = t.Value
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
		NetworkType:                   req.NetworkType,
		Tags:                          tags,
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
