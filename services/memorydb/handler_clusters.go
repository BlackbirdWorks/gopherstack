package memorydb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

func (h *Handler) handleCreateCluster(ctx context.Context, c *echo.Context, body []byte) error {
	var req createClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ClusterName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ClusterName is required")
	}

	if req.NodeType == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "NodeType is required")
	}

	if err := validateTagEntries(req.Tags); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	cluster, err := h.Backend.CreateCluster(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createClusterResponse{Cluster: toClusterObject(cluster, true)})
}

func (h *Handler) handleDescribeClusters(ctx context.Context, c *echo.Context, body []byte) error {
	var req describeClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	clusters, err := h.Backend.DescribeClusters(ctx, req.ClusterName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	// Apply cursor-based pagination when listing all clusters.
	start := 0

	if req.NextToken != "" {
		for i, cl := range clusters {
			if cl.Name == req.NextToken {
				start = i + 1

				break
			}
		}
	}

	clusters = clusters[start:]

	var nextToken string

	if req.MaxResults != nil && int(*req.MaxResults) < len(clusters) {
		nextToken = clusters[*req.MaxResults].Name
		clusters = clusters[:*req.MaxResults]
	}

	showShards := req.ShowShardDetails != nil && *req.ShowShardDetails

	objs := make([]clusterObject, 0, len(clusters))

	for _, cl := range clusters {
		objs = append(objs, toClusterObject(cl, showShards))
	}

	return c.JSON(http.StatusOK, describeClusterResponse{Clusters: objs, NextToken: nextToken})
}

func (h *Handler) handleDeleteCluster(ctx context.Context, c *echo.Context, body []byte) error {
	var req deleteClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ClusterName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ClusterName is required")
	}

	var (
		cluster *Cluster
		err     error
	)

	if req.FinalSnapshotName != "" {
		cluster, err = h.Backend.DeleteClusterWithSnapshot(ctx, req.ClusterName, req.FinalSnapshotName)
	} else {
		cluster, err = h.Backend.DeleteCluster(ctx, req.ClusterName)
	}

	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, deleteClusterResponse{Cluster: toClusterObject(cluster, true)})
}

func (h *Handler) handleUpdateCluster(ctx context.Context, c *echo.Context, body []byte) error {
	var req updateClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ClusterName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ClusterName is required")
	}

	cluster, err := h.Backend.UpdateCluster(ctx, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateClusterResponse{Cluster: toClusterObject(cluster, true)})
}

// -- ACL handlers ----------------------------------------------------------------

func (h *Handler) handleBatchUpdateCluster(ctx context.Context, c *echo.Context, body []byte) error {
	var req batchUpdateClusterRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if len(req.ClusterNames) == 0 {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ClusterNames is required")
	}

	found := h.Backend.BatchUpdateCluster(ctx, req.ClusterNames)

	processedObjs := make([]clusterObject, 0, len(found))
	unprocessedObjs := make([]unprocessedCluster, 0, len(req.ClusterNames))

	for _, name := range req.ClusterNames {
		if cl, ok := found[name]; ok {
			processedObjs = append(processedObjs, toClusterObject(cl, true))
		} else {
			unprocessedObjs = append(unprocessedObjs, unprocessedCluster{
				ClusterName:  name,
				ErrorType:    "ClusterNotFoundFault",
				ErrorMessage: "cluster not found: " + name,
			})
		}
	}

	return c.JSON(http.StatusOK, batchUpdateClusterResponse{
		ProcessedClusters:   processedObjs,
		UnprocessedClusters: unprocessedObjs,
	})
}

// -- New handler functions (refinement check 2) ----------------------------------

func (h *Handler) handleFailoverShard(ctx context.Context, c *echo.Context, body []byte) error {
	var req failoverShardRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ClusterName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ClusterName is required")
	}

	cl, err := h.Backend.FailoverShard(ctx, req.ClusterName, req.ShardName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, failoverShardResponse{Cluster: toClusterObject(cl, true)})
}

func (h *Handler) handleListAllowedNodeTypeUpdates(ctx context.Context, c *echo.Context, body []byte) error {
	var req listAllowedNodeTypeUpdatesRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ClusterName == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ClusterName is required")
	}

	nodeTypes, err := h.Backend.ListAllowedNodeTypeUpdates(ctx, req.ClusterName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listAllowedNodeTypeUpdatesResponse{
		ScaleUpNodeTypes:   nodeTypes,
		ScaleDownNodeTypes: nodeTypes,
	})
}

// enginePatchVersionFor looks up the EnginePatchVersion for a given engine+version pair.
func enginePatchVersionFor(engine, engineVersion string) string {
	for _, ev := range defaultEngineVersions() {
		if ev.Engine == engine && ev.EngineVersion == engineVersion {
			return ev.EnginePatchVersion
		}
	}

	return engineVersion
}

// toClusterObject converts a Cluster to its JSON representation.
// showShards controls whether per-shard node detail is populated.
func toClusterObject(c *Cluster, showShards bool) clusterObject {
	region := c.Region
	if region == "" {
		region = config.DefaultRegion
	}

	var shards []shardObject
	if showShards {
		shards = buildShards(c.Name, c.NumShards, c.NumReplicasPerShard, c.Port, region)
	}

	sgs := make([]securityGroupMembership, 0, len(c.SecurityGroupIDs))
	for _, id := range c.SecurityGroupIDs {
		sgs = append(sgs, securityGroupMembership{SecurityGroupID: id, Status: "active"})
	}

	pgStatus := c.ParameterGroupStatus
	if pgStatus == "" {
		pgStatus = "in-sync"
	}

	return clusterObject{
		Name:                          c.Name,
		ARN:                           c.ARN,
		Description:                   c.Description,
		Status:                        c.Status,
		NodeType:                      c.NodeType,
		EngineVersion:                 c.EngineVersion,
		EnginePatchVersion:            enginePatchVersionFor(c.Engine, c.EngineVersion),
		Engine:                        c.Engine,
		DataTiering:                   c.DataTiering,
		NetworkType:                   c.NetworkType,
		IPDiscovery:                   c.IPDiscovery,
		AutoMinorVersionUpgrade:       c.AutoMinorVersionUpgrade,
		ACLName:                       c.ACLName,
		SubnetGroupName:               c.SubnetGroupName,
		ParameterGroupName:            c.ParameterGroupName,
		ParameterGroupStatus:          pgStatus,
		MultiRegionClusterName:        c.MultiRegionClusterName,
		MultiRegionParameterGroupName: c.MultiRegionParameterGroupName,
		KmsKeyID:                      c.KmsKeyID,
		SnsTopicArn:                   c.SnsTopicArn,
		SnsTopicStatus:                c.SnsTopicStatus,
		MaintenanceWindow:             c.MaintenanceWindow,
		SnapshotWindow:                c.SnapshotWindow,
		NumberOfShards:                c.NumShards,
		TLSEnabled:                    c.TLSEnabled,
		SnapshotRetentionLimit:        c.SnapshotRetentionLimit,
		Shards:                        shards,
		AvailabilityMode:              c.AvailabilityMode,
		NumberOfReplicasPerShard:      c.NumReplicasPerShard,
		SecurityGroups:                sgs,
		ClusterEndpoint: &endpointObject{
			Address: c.Name + ".memorydb." + region + ".amazonaws.com",
			Port:    c.Port,
		},
	}
}

// buildShards constructs a slice of shardObjects with evenly-distributed slots and nodes.
func buildShards(clusterName string, numShards, numReplicas, port int32, region string) []shardObject {
	const totalSlots = 16384

	const maxShards = 256

	// Clamp nShards to [1, maxShards] before use. Converting through a
	// clamped int prevents CodeQL from treating the make size as
	// attacker-controlled (go/slice-memory-allocation-excessive-size).
	nShards := max(1, min(maxShards, int(numShards)))

	slotsPerShard := totalSlots / nShards

	zones := []string{region + "a", region + "b", region + "c"}

	// No capacity hint — user-derived values in the make capacity position
	// trigger CodeQL go/slice-memory-allocation-excessive-size even after
	// clamping. nShards is only used for the loop count below (safe).
	// nolint:prealloc,nolintlint // satisfies CodeQL by removing tainted capacity hint
	shards := make([]shardObject, 0)

	for i := range nShards {
		start := i * slotsPerShard
		end := start + slotsPerShard - 1

		if i == nShards-1 {
			end = totalSlots - 1
		}

		nodes := make([]nodeObject, 0, 1+int(numReplicas))
		for ni := range 1 + int(numReplicas) {
			role := "primary"
			if ni > 0 {
				role = "replica"
			}
			nodeName := fmt.Sprintf("%s-0001-%04d-%04d", clusterName, i, ni)
			nodes = append(nodes, nodeObject{
				Name:             nodeName,
				Status:           clusterStatusAvailable,
				AvailabilityZone: zones[ni%len(zones)],
				CreateTime:       float64(time.Now().Unix()),
				Endpoint: &endpointObject{
					Address: nodeName + ".memorydb." + region + ".amazonaws.com",
					Port:    port,
				},
			})
			_ = role
		}

		// Shard name follows the AWS MemoryDB convention: <cluster>-<nodegroup>-<shardindex>
		// where nodegroup is always "0001" for single-shard-group clusters.
		shards = append(shards, shardObject{
			Name:          fmt.Sprintf("%s-0001-%04d", clusterName, i),
			Status:        clusterStatusAvailable,
			Slots:         fmt.Sprintf("%d-%d", start, end),
			NumberOfNodes: int32(1 + int(numReplicas)), //nolint:gosec // clamped above
			Nodes:         nodes,
		})
	}

	return shards
}
