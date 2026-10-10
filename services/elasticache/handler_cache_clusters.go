package elasticache

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
)

// cacheEndpoint is the XML representation of a cache node endpoint.
type cacheEndpoint struct {
	Address string `xml:"Address"`
	Port    int    `xml:"Port"`
}

// cacheNode is the XML representation of a cache node.
type cacheNode struct {
	CacheNodeID              string        `xml:"CacheNodeId"`
	CacheNodeStatus          string        `xml:"CacheNodeStatus"`
	CacheNodeCreateTime      string        `xml:"CacheNodeCreateTime"`
	CustomerAvailabilityZone string        `xml:"CustomerAvailabilityZone"`
	Endpoint                 cacheEndpoint `xml:"Endpoint"`
}

// cacheNodes is the XML container for cache nodes.
type cacheNodes struct {
	CacheNode []cacheNode `xml:"CacheNode"`
}

// cacheSecurityGroupMembershipXML is the XML representation of a cache
// security group membership on a cluster or replication group.
type cacheSecurityGroupMembershipXML struct {
	CacheSecurityGroupName string `xml:"CacheSecurityGroupName"`
	Status                 string `xml:"Status"`
}

// cacheSecurityGroupsXML is the XML container for cache security group memberships.
type cacheSecurityGroupsXML struct {
	CacheSecurityGroup []cacheSecurityGroupMembershipXML `xml:"CacheSecurityGroup"`
}

// notificationConfigXML is the XML representation of a cluster's SNS notification topic.
type notificationConfigXML struct {
	TopicArn    string `xml:"TopicArn"`
	TopicStatus string `xml:"TopicStatus,omitempty"`
}

// securityGroupMembershipXML is one VPC security group on a cluster.
type securityGroupMembershipXML struct {
	SecurityGroupID string `xml:"SecurityGroupId"`
	Status          string `xml:"Status"`
}

// securityGroupsXML is the XML container for VPC security group memberships.
type securityGroupsXML struct {
	Member []securityGroupMembershipXML `xml:"member"`
}

// cacheClusterXML is the XML representation of a cache cluster.
type cacheClusterXML struct {
	AutoMinorVersionUpgrade    *bool                  `xml:"AutoMinorVersionUpgrade,omitempty"`
	NotificationConfiguration  *notificationConfigXML `xml:"NotificationConfiguration,omitempty"`
	SecurityGroups             *securityGroupsXML     `xml:"SecurityGroups,omitempty"`
	LogDeliveryConfigurations  *logDeliveryConfigsXML `xml:"LogDeliveryConfigurations,omitempty"`
	NetworkType                string                 `xml:"NetworkType,omitempty"`
	IPDiscovery                string                 `xml:"IpDiscovery,omitempty"`
	CacheParameterGroupName    string                 `xml:"CacheParameterGroup>CacheParameterGroupName,omitempty"`
	PreferredMaintenanceWindow string                 `xml:"PreferredMaintenanceWindow,omitempty"`
	CacheNodeType              string                 `xml:"CacheNodeType"`
	CacheSubnetGroupName       string                 `xml:"CacheSubnetGroupName,omitempty"`
	PreferredOutpostArn        string                 `xml:"PreferredOutpostArn,omitempty"`
	ConfigurationEndpoint      *cacheEndpoint         `xml:"ConfigurationEndpoint,omitempty"`
	Engine                     string                 `xml:"Engine"`
	EngineVersion              string                 `xml:"EngineVersion"`
	ARN                        string                 `xml:"ARN"`
	CacheClusterStatus         string                 `xml:"CacheClusterStatus"`
	CreatedAt                  string                 `xml:"CacheClusterCreateTime,omitempty"`
	CacheClusterID             string                 `xml:"CacheClusterId"`
	ReplicationGroupID         string                 `xml:"ReplicationGroupId,omitempty"`
	SnapshotWindow             string                 `xml:"SnapshotWindow,omitempty"`
	PreferredAvailabilityZone  string                 `xml:"PreferredAvailabilityZone,omitempty"`
	CacheNodes                 cacheNodes             `xml:"CacheNodes"`
	CacheSecurityGroups        cacheSecurityGroupsXML `xml:"CacheSecurityGroups"`
	NumCacheNodes              int                    `xml:"NumCacheNodes"`
	SnapshotRetentionLimit     int                    `xml:"SnapshotRetentionLimit,omitempty"`
	TransitEncryptionEnabled   bool                   `xml:"TransitEncryptionEnabled"`
	AtRestEncryptionEnabled    bool                   `xml:"AtRestEncryptionEnabled"`
	AuthTokenEnabled           bool                   `xml:"AuthTokenEnabled"`
}

// parseNumCacheNodes parses NumCacheNodes, falling back to def when unset or
// non-positive. Split out of createCacheCluster to keep its cyclomatic
// complexity down.
func parseNumCacheNodes(form url.Values, def int) int {
	s := form.Get("NumCacheNodes")
	if s == "" {
		return def
	}

	if n, err := strconv.Atoi(s); err == nil && n > 0 {
		return n
	}

	return def
}

// clusterCreatePlan is CreateCacheCluster's validated request.
type clusterCreatePlan struct {
	engine        string
	nodeType      string
	params        clusterCreateParams
	numCacheNodes int
}

// planClusterCreate validates a CreateCacheCluster request before anything is created.
// ok is false when an error response was already written.
func (h *Handler) planClusterCreate(
	ctx context.Context, c *echo.Context, form url.Values,
) (clusterCreatePlan, bool, error) {
	plan := clusterCreatePlan{
		engine:        form.Get("Engine"),
		nodeType:      form.Get("CacheNodeType"),
		numCacheNodes: parseNumCacheNodes(form, 1),
	}

	var restoreErr error

	plan.engine, plan.nodeType, restoreErr = h.applySnapshotDefaults(
		ctx, form.Get("SnapshotName"), plan.engine, plan.nodeType,
	)
	if restoreErr != nil {
		return plan, false, snapshotDefaultsErrorResponse(c, restoreErr)
	}

	var paramErr error

	plan.params, paramErr = parseClusterCreateParams(form, plan.engine, plan.numCacheNodes)
	if paramErr != nil {
		status, code, _ := paramErrorCode(paramErr)

		return plan, false, xmlError(c, status, code, paramErr.Error())
	}

	// A nonexistent ReplicationGroupId is rejected before creating anything:
	// ReplicationGroupNotFoundFault is in CreateCacheCluster's modeled error
	// list, and real AWS never materializes the cluster in that case.
	if rgID := form.Get("ReplicationGroupId"); rgID != "" {
		if rgErr := h.Backend.CheckReplicaAttach(ctx, rgID, plan.engine); rgErr != nil {
			return plan, false, h.attachErrorResponse(c, rgErr)
		}
	}

	return plan, true, nil
}

// attachErrorResponse maps a replication-group attach error to its wire fault.
func (h *Handler) attachErrorResponse(c *echo.Context, err error) error {
	if status, code, ok := paramErrorCode(err); ok {
		return xmlError(c, status, code, err.Error())
	}

	return h.replicationGroupErrorResponse(c, err)
}

func (h *Handler) createCacheCluster(ctx context.Context, c *echo.Context, form url.Values) error {
	id := form.Get("CacheClusterId")
	if id == "" {
		return xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "CacheClusterId is required")
	}

	if err := firstError(
		validateCacheID("CacheClusterId", id, maxCacheClusterIDLen),
		validateCacheEngine(form.Get("Engine")),
		validateCacheNodeType(form.Get("CacheNodeType")),
		validateNumCacheNodesRaw(form.Get("NumCacheNodes")),
	); err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidParameterValue", err.Error())
	}

	plan, ok, planErr := h.planClusterCreate(ctx, c, form)
	if !ok {
		return planErr
	}

	cluster, err := h.Backend.CreateClusterWithOptions(ctx,
		id,
		plan.engine,
		plan.nodeType,
		form.Get("CacheParameterGroupName"),
		form.Get("PreferredMaintenanceWindow"),
		form.Get("SnapshotWindow"),
		plan.numCacheNodes,
		plan.params.port,
	)
	if err != nil {
		return mapClusterCreateErr(c, err)
	}

	h.applyCreateTimeTags(ctx, form, cluster.ARN)

	if sgErr := h.applyClusterSubnetGroup(ctx, form, id, cluster); sgErr != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", sgErr.Error())
	}

	if azErr := h.Backend.SetClusterPlacement(ctx, id, plan.params.placement); azErr != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", azErr.Error())
	}

	if srErr := h.applyClusterSnapshotRetentionLimit(ctx, form, id, cluster); srErr != nil {
		return snapshotRetentionLimitErrorResponse(c, srErr)
	}

	if rgErr := h.applyClusterReplicationGroup(ctx, form, id, cluster); rgErr != nil {
		return h.attachErrorResponse(c, rgErr)
	}

	settled, setErr := h.Backend.ApplyClusterSettings(ctx, id, clusterSettingsFromForm(form))
	if setErr != nil {
		if errors.Is(setErr, ErrCacheSecurityGroupNotFound) {
			return xmlError(c, http.StatusNotFound, "CacheSecurityGroupNotFound", "Cache security group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", setErr.Error())
	}

	type result struct {
		XMLName      xml.Name        `xml:"CreateCacheClusterResponse"`
		Xmlns        string          `xml:"xmlns,attr"`
		CacheCluster cacheClusterXML `xml:"CreateCacheClusterResult>CacheCluster"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:        elasticacheNS,
		CacheCluster: clusterToXML(settled, settled.Status),
	})
}

// mapClusterCreateErr maps a CreateClusterWithOptions failure to its wire fault.
func mapClusterCreateErr(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrClusterAlreadyExists):
		return xmlError(c, http.StatusBadRequest, "CacheClusterAlreadyExists", "Cache cluster already exists")
	case errors.Is(err, ErrParameterGroupNotFound):
		return xmlError(c, http.StatusNotFound, "CacheParameterGroupNotFound", "Cache parameter group not found")
	case errors.Is(err, ErrInvalidParameterGroupFamily):
		return xmlError(c, http.StatusBadRequest, "InvalidParameterValue", err.Error())
	}

	return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
}

// mapClusterModifyErr maps a ModifyCluster failure to its wire fault.
func mapClusterModifyErr(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrClusterNotFound):
		return xmlError(c, http.StatusNotFound, "CacheClusterNotFound", "Cache cluster not found")
	case errors.Is(err, ErrParameterGroupNotFound):
		return xmlError(c, http.StatusNotFound, "CacheParameterGroupNotFound", "Cache parameter group not found")
	case errors.Is(err, ErrClusterNotAvailable):
		return xmlError(c, http.StatusBadRequest, "InvalidCacheClusterState", err.Error())
	case errors.Is(err, ErrCacheSecurityGroupNotFound):
		return xmlError(c, http.StatusNotFound, "CacheSecurityGroupNotFound", "Cache security group not found")
	}

	if status, code, ok := paramErrorCode(err); ok {
		return xmlError(c, status, code, err.Error())
	}

	return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
}

// applyClusterSubnetGroup records CacheSubnetGroupName on a just-created cluster,
// if the caller supplied one. Split out of createCacheCluster to keep its
// cognitive complexity down. Returns the raw backend error (nil on success)
// rather than an xmlError, for the same reason as checkReplicationGroupExists:
// the caller must map and return it directly, not through a bubbled-up
// nil-on-success value -- see replicationGroupErrorResponse.
func (h *Handler) applyClusterSubnetGroup(
	ctx context.Context, form url.Values, id string, cluster *Cluster,
) error {
	subnetGroupName := form.Get("CacheSubnetGroupName")
	if subnetGroupName == "" {
		return nil
	}

	if err := h.Backend.SetClusterSubnetGroupName(ctx, id, subnetGroupName); err != nil {
		return err
	}

	cluster.SubnetGroupName = subnetGroupName

	return nil
}

// errInvalidSnapshotRetentionLimit is applyClusterSnapshotRetentionLimit's
// sentinel for a non-integer SnapshotRetentionLimit, distinguished by
// snapshotRetentionLimitErrorResponse from a plain backend failure.
var errInvalidSnapshotRetentionLimit = errors.New("SnapshotRetentionLimit must be an integer")

// applyClusterSnapshotRetentionLimit records SnapshotRetentionLimit on a
// just-created cluster, if the caller supplied it. AWS documents 0 as a
// meaningful explicit value ("automatic backups are disabled"), so presence
// is checked on the raw form value, not on the parsed int being non-zero.
// Split out of createCacheCluster/modifyCacheCluster to keep their cognitive
// complexity down. Returns a raw error (nil on success) rather than an
// xmlError -- see applyClusterSubnetGroup.
func (h *Handler) applyClusterSnapshotRetentionLimit(
	ctx context.Context, form url.Values, id string, cluster *Cluster,
) error {
	s := form.Get("SnapshotRetentionLimit")
	if s == "" {
		return nil
	}

	n, parseErr := strconv.Atoi(s)
	if parseErr != nil {
		return errInvalidSnapshotRetentionLimit
	}

	if err := h.Backend.SetClusterSnapshotRetentionLimit(ctx, id, &n); err != nil {
		return err
	}

	cluster.SnapshotRetentionLimit = n

	return nil
}

// snapshotRetentionLimitErrorResponse maps a raw applyClusterSnapshotRetentionLimit
// error to its wire fault. Must be called as
// "return snapshotRetentionLimitErrorResponse(...)" directly at the error
// site, not stored and conditionally re-returned -- see
// replicationGroupErrorResponse for why.
func snapshotRetentionLimitErrorResponse(c *echo.Context, err error) error {
	if errors.Is(err, errInvalidSnapshotRetentionLimit) {
		return xmlError(c, http.StatusBadRequest, "InvalidParameterValue", err.Error())
	}

	return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
}

// replicationGroupErrorResponse maps a raw replication-group lookup error to
// its wire fault. Must be called as "return h.replicationGroupErrorResponse(...)"
// directly at the error site, not stored and conditionally re-returned:
// xmlError/xmlResp return nil on a successful write, so a caller checking a
// bubbled-up "if storedErr != nil" would never see the rejection and would
// fall through past it.
func (h *Handler) replicationGroupErrorResponse(c *echo.Context, err error) error {
	if errors.Is(err, ErrReplicationGroupNotFound) {
		return xmlError(c, http.StatusNotFound, "ReplicationGroupNotFoundFault", "Replication group not found")
	}

	return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
}

// applyClusterReplicationGroup attaches a just-created cluster to an existing
// replication group as a read replica, if the caller supplied
// ReplicationGroupId (api_op_CreateCacheCluster.go ReplicationGroupId doc:
// "the cluster is added to the specified replication group as a read
// replica"). Returns the raw backend error (nil on success, in which case
// cluster is mutated in place) rather than an xmlError, for the same reason
// as checkReplicationGroupExists: createCacheCluster must map and return it
// directly, not through a bubbled-up nil-on-success value.
func (h *Handler) applyClusterReplicationGroup(
	ctx context.Context, form url.Values, id string, cluster *Cluster,
) error {
	replicationGroupID := form.Get("ReplicationGroupId")
	if replicationGroupID == "" {
		return nil
	}

	if err := h.Backend.SetClusterReplicationGroupID(ctx, id, replicationGroupID); err != nil {
		return err
	}

	cluster.ReplicationGroupID = replicationGroupID

	return nil
}

// errSnapshotNotFoundForRestore is applySnapshotDefaults' sentinel for a
// missing/invalid restore snapshot, wrapped with the snapshot name.
var errSnapshotNotFoundForRestore = errors.New("cache cluster snapshot not found")

// applySnapshotDefaults implements AWS's CreateCacheCluster-from-snapshot restore:
// when snapshotName is set, the snapshot must exist and its engine/node type
// become the defaults for whichever of engine/nodeType the caller left blank.
// Split out of createCacheCluster to keep its cognitive complexity down.
// Returns a raw error (nil on success) rather than an xmlError -- see
// applyClusterSubnetGroup; the caller must map and return it via
// snapshotDefaultsErrorResponse, not through a bubbled-up nil-on-success value.
func (h *Handler) applySnapshotDefaults(
	ctx context.Context, snapshotName, engine, nodeType string,
) (string, string, error) {
	if snapshotName == "" {
		return engine, nodeType, nil
	}

	snaps, snapErr := h.Backend.DescribeSnapshots(ctx, snapshotName, "", "", "", "", 0)
	if snapErr != nil || len(snaps.Data) == 0 {
		return "", "", fmt.Errorf("%w: %s", errSnapshotNotFoundForRestore, snapshotName)
	}

	src := snaps.Data[0]
	if engine == "" {
		engine = src.Engine
	}

	if nodeType == "" {
		nodeType = src.NodeType
	}

	return engine, nodeType, nil
}

// snapshotDefaultsErrorResponse maps a raw applySnapshotDefaults error to its
// wire fault. Must be called as "return snapshotDefaultsErrorResponse(...)"
// directly at the error site, not stored and conditionally re-returned -- see
// replicationGroupErrorResponse for why.
//
// SnapshotNotFoundFault isn't in CreateCacheCluster's modeled error list
// (api-2.json), so aws-sdk-go-v2 has no case for it in this operation's
// error deserializer and would fall back to a generic error; AWS instead
// surfaces a missing/invalid snapshot here as InvalidParameterValue, which
// the SDK does model for this op.
func snapshotDefaultsErrorResponse(c *echo.Context, err error) error {
	return xmlError(c, http.StatusBadRequest, "InvalidParameterValue", err.Error())
}

func (h *Handler) deleteCacheCluster(ctx context.Context, c *echo.Context, form url.Values) error {
	id := form.Get("CacheClusterId")
	clusters, descErr := h.Backend.DescribeClusters(ctx, id, "", 0, false)
	if descErr != nil {
		if errors.Is(descErr, ErrClusterNotFound) {
			return xmlError(c, http.StatusNotFound, "CacheClusterNotFound", "Cache cluster not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", descErr.Error())
	}
	if len(clusters.Data) == 0 {
		return xmlError(c, http.StatusNotFound, "CacheClusterNotFound", "Cache cluster not found")
	}

	cl := clusters.Data[0]

	// api-2.json SnapshotFeatureNotSupportedFault: "Creating a snapshot of a
	// cluster that is running Memcached rather than Valkey or Redis OSS" is
	// unsupported, and DeleteCacheCluster takes the final snapshot as part
	// of the delete.
	if form.Get("FinalSnapshotIdentifier") != "" && cl.Engine == engineMemcached {
		return xmlError(c, http.StatusBadRequest, "SnapshotFeatureNotSupportedFault",
			"Snapshots not supported for Memcached engine")
	}

	undoSnapshot, snapErr := h.takeFinalSnapshot(ctx, form, id, "")
	if snapErr != nil {
		return finalSnapshotErrorResponse(c, snapErr)
	}

	if err := h.Backend.DeleteCluster(ctx, id); err != nil {
		undoSnapshot()

		if errors.Is(err, ErrClusterNotFound) {
			return xmlError(c, http.StatusNotFound, "CacheClusterNotFound", "Cache cluster not found")
		}
		if errors.Is(err, ErrClusterNotAvailable) || errors.Is(err, ErrClusterInReplicationGroup) {
			return xmlError(c, http.StatusBadRequest, "InvalidCacheClusterState", err.Error())
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName      xml.Name        `xml:"DeleteCacheClusterResponse"`
		Xmlns        string          `xml:"xmlns,attr"`
		CacheCluster cacheClusterXML `xml:"DeleteCacheClusterResult>CacheCluster"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:        elasticacheNS,
		CacheCluster: clusterToXML(&cl, "deleting"),
	})
}

func (h *Handler) describeCacheClusters(ctx context.Context, c *echo.Context, form url.Values) error {
	id := form.Get("CacheClusterId")
	marker, maxRecords, err := parsePaginationChecked(c, form)
	if err != nil {
		return err
	}
	notInRG := strings.EqualFold(form.Get("ShowCacheClustersNotInReplicationGroups"), "true")

	p, err := h.Backend.DescribeClusters(ctx, id, marker, maxRecords, notInRG)
	if err != nil {
		if errors.Is(err, ErrClusterNotFound) {
			return xmlError(c, http.StatusNotFound, "CacheClusterNotFound", "Cache cluster not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type cacheClusters struct {
		CacheCluster []cacheClusterXML `xml:"CacheCluster"`
	}
	type result struct {
		XMLName       xml.Name      `xml:"DescribeCacheClustersResponse"`
		Xmlns         string        `xml:"xmlns,attr"`
		Marker        string        `xml:"DescribeCacheClustersResult>Marker,omitempty"`
		CacheClusters cacheClusters `xml:"DescribeCacheClustersResult>CacheClusters"`
	}

	items := make([]cacheClusterXML, 0, len(p.Data))
	for i := range p.Data {
		items = append(items, clusterToXML(&p.Data[i], p.Data[i].Status))
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:         elasticacheNS,
		Marker:        p.Next,
		CacheClusters: cacheClusters{CacheCluster: items},
	})
}

func notificationConfigToXML(cl *Cluster) *notificationConfigXML {
	if cl.NotificationTopicArn == "" {
		return nil
	}

	return &notificationConfigXML{TopicArn: cl.NotificationTopicArn, TopicStatus: cl.NotificationTopicStatus}
}

func securityGroupsToXML(ids []string) *securityGroupsXML {
	if len(ids) == 0 {
		return nil
	}

	out := &securityGroupsXML{Member: make([]securityGroupMembershipXML, 0, len(ids))}
	for _, id := range ids {
		out.Member = append(out.Member, securityGroupMembershipXML{SecurityGroupID: id, Status: statusActive})
	}

	return out
}

// clusterSettingsFromForm reads the optional Create/ModifyCacheCluster members.
func clusterSettingsFromForm(form url.Values) ClusterSettings {
	return ClusterSettings{
		AutoMinorVersionUpgrade:   optionalBool(form, "AutoMinorVersionUpgrade"),
		NotificationTopicArn:      form.Get("NotificationTopicArn"),
		NotificationTopicStatus:   form.Get("NotificationTopicStatus"),
		NetworkType:               form.Get("NetworkType"),
		IPDiscovery:               form.Get("IpDiscovery"),
		SecurityGroupIDs:          parseRepeatedField(form, "SecurityGroupIds.SecurityGroupId"),
		CacheSecurityGroupNames:   parseRepeatedField(form, "CacheSecurityGroupNames.CacheSecurityGroupName"),
		LogDeliveryConfigurations: parseLogDeliveryConfigs(form),
	}
}

// clusterToXML converts a Cluster to its XML representation with the given status.
func clusterToXML(cl *Cluster, status string) cacheClusterXML {
	region := cl.Region
	if region == "" {
		region = config.DefaultRegion
	}

	ids := nodeIDsOf(cl)
	n := len(ids)

	nodes := make([]cacheNode, 0, n)
	for i, nodeID := range ids {
		nodes = append(nodes, cacheNode{
			CacheNodeID:              nodeID,
			CacheNodeStatus:          status,
			CacheNodeCreateTime:      cl.CreatedAt.UTC().Format(time.RFC3339),
			CustomerAvailabilityZone: nodeAvailabilityZone(cl, i, region),
			Endpoint: cacheEndpoint{
				Address: cl.Endpoint,
				Port:    cl.Port,
			},
		})
	}

	var configEndpoint *cacheEndpoint
	if cl.Engine == engineMemcached {
		configEndpoint = &cacheEndpoint{Address: cl.Endpoint, Port: cl.Port}
	}

	return cacheClusterXML{
		CacheSubnetGroupName:       cl.SubnetGroupName,
		PreferredOutpostArn:        cl.PreferredOutpostArn,
		ConfigurationEndpoint:      configEndpoint,
		AutoMinorVersionUpgrade:    cl.AutoMinorVersionUpgrade,
		NotificationConfiguration:  notificationConfigToXML(cl),
		SecurityGroups:             securityGroupsToXML(cl.SecurityGroupIDs),
		LogDeliveryConfigurations:  logDeliveryConfigsToXML(cl.LogDeliveryConfigurations),
		NetworkType:                cl.NetworkType,
		IPDiscovery:                cl.IPDiscovery,
		CacheClusterID:             cl.ClusterID,
		CacheClusterStatus:         status,
		CacheNodeType:              cl.NodeType,
		Engine:                     cl.Engine,
		EngineVersion:              cl.EngineVersion,
		NumCacheNodes:              n,
		ARN:                        cl.ARN,
		CacheParameterGroupName:    cl.CacheParameterGroupName,
		ReplicationGroupID:         cl.ReplicationGroupID,
		PreferredMaintenanceWindow: cl.PreferredMaintenanceWindow,
		SnapshotWindow:             cl.SnapshotWindow,
		PreferredAvailabilityZone:  clusterAvailabilityZone(cl),
		SnapshotRetentionLimit:     cl.SnapshotRetentionLimit,
		TransitEncryptionEnabled:   cl.TransitEncryptionEnabled,
		AtRestEncryptionEnabled:    cl.AtRestEncryptionEnabled,
		AuthTokenEnabled:           cl.AuthTokenEnabled,
		CreatedAt:                  cl.CreatedAt.UTC().Format(time.RFC3339),
		CacheNodes: cacheNodes{
			CacheNode: nodes,
		},
		CacheSecurityGroups: cacheSecurityGroupsXML{
			CacheSecurityGroup: cacheSecurityGroupMembershipsXML(cl.CacheSecurityGroupNames),
		},
	}
}

// cacheSecurityGroupMembershipsXML builds the XML membership list for a set
// of cache security group names, all reported "active" (this backend has no
// asynchronous authorization workflow to produce any other status).
func cacheSecurityGroupMembershipsXML(names []string) []cacheSecurityGroupMembershipXML {
	if len(names) == 0 {
		return nil
	}
	out := make([]cacheSecurityGroupMembershipXML, 0, len(names))
	for _, n := range names {
		out = append(out, cacheSecurityGroupMembershipXML{CacheSecurityGroupName: n, Status: "active"})
	}

	return out
}

// nodeAvailabilityZone returns the AZ node i is placed in: the single
// PreferredAvailabilityZone when set, the i-th (cycled) entry of
// PreferredAvailabilityZones for a Memcached multi-node cluster, or the
// synthetic region+"a" fallback when the caller specified neither.
func nodeAvailabilityZone(cl *Cluster, i int, region string) string {
	if i < len(cl.CacheNodeAZs) {
		return cl.CacheNodeAZs[i]
	}

	if cl.PreferredAvailabilityZone != "" {
		return cl.PreferredAvailabilityZone
	}
	if len(cl.PreferredAvailabilityZones) > 0 {
		return cl.PreferredAvailabilityZones[i%len(cl.PreferredAvailabilityZones)]
	}

	return region + "a"
}

// clusterAvailabilityZone returns CacheCluster.PreferredAvailabilityZone: the
// AZ name the cluster is located in, or "Multiple" when
// PreferredAvailabilityZones names more than one distinct zone -- see
// elasticache@v1.56.4 types.CacheCluster's own doc comment.
func clusterAvailabilityZone(cl *Cluster) string {
	if cl.PreferredAvailabilityZone != "" {
		return cl.PreferredAvailabilityZone
	}
	if len(cl.PreferredAvailabilityZones) == 0 {
		if len(distinctStrings(cl.CacheNodeAZs)) > 1 {
			return "Multiple"
		}

		return ""
	}
	for _, az := range cl.PreferredAvailabilityZones[1:] {
		if az != cl.PreferredAvailabilityZones[0] {
			return "Multiple"
		}
	}

	return cl.PreferredAvailabilityZones[0]
}

func (h *Handler) modifyCacheCluster(ctx context.Context, c *echo.Context, form url.Values) error {
	id := form.Get("CacheClusterId")
	nodeType := form.Get("CacheNodeType")
	paramGroupName := form.Get("CacheParameterGroupName")
	engineVersion := form.Get("EngineVersion")
	maintenanceWindow := form.Get("PreferredMaintenanceWindow")
	snapshotWindow := form.Get("SnapshotWindow")
	numCacheNodes := 0

	if s := form.Get("NumCacheNodes"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			numCacheNodes = n
		}
	}

	opts := &ModifyClusterOptions{
		AuthToken:               form.Get("AuthToken"),
		AuthTokenUpdateStrategy: form.Get("AuthTokenUpdateStrategy"),
		AZMode:                  form.Get("AZMode"),
		CacheSecurityGroupNames: parseRepeatedField(form, "CacheSecurityGroupNames.CacheSecurityGroupName"),
		CacheNodeIDsToRemove:    parseRepeatedField(form, "CacheNodeIdsToRemove.CacheNodeId"),
		NewAvailabilityZones:    parseRepeatedField(form, "NewAvailabilityZones.PreferredAvailabilityZone"),
		ApplyImmediately:        strings.EqualFold(form.Get("ApplyImmediately"), "true"),
	}

	cluster, err := h.Backend.ModifyCluster(ctx,
		id,
		nodeType,
		paramGroupName,
		engineVersion,
		maintenanceWindow,
		snapshotWindow,
		numCacheNodes,
		opts,
	)
	if err != nil {
		return mapClusterModifyErr(c, err)
	}

	if srErr := h.applyClusterSnapshotRetentionLimit(ctx, form, id, cluster); srErr != nil {
		return snapshotRetentionLimitErrorResponse(c, srErr)
	}

	settled, setErr := h.Backend.ApplyClusterSettings(ctx, id, clusterSettingsFromForm(form))
	if setErr != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", setErr.Error())
	}

	cluster = mergeClusterSettings(cluster, settled)

	type result struct {
		XMLName      xml.Name        `xml:"ModifyCacheClusterResponse"`
		Xmlns        string          `xml:"xmlns,attr"`
		CacheCluster cacheClusterXML `xml:"ModifyCacheClusterResult>CacheCluster"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:        elasticacheNS,
		CacheCluster: clusterToXML(cluster, cluster.Status),
	})
}

func (h *Handler) rebootCacheCluster(ctx context.Context, c *echo.Context, form url.Values) error {
	clusterID := form.Get("CacheClusterId")
	nodeIDs := parseRepeatedField(form, "CacheNodeIdsToReboot.CacheNodeId")

	cl, err := h.Backend.RebootCacheCluster(ctx, clusterID, nodeIDs)
	if err != nil {
		if errors.Is(err, ErrClusterNotFound) {
			return xmlError(c, http.StatusNotFound, "CacheClusterNotFound", "Cache cluster not found")
		}
		if errors.Is(err, ErrClusterNotAvailable) {
			return xmlError(c, http.StatusBadRequest, "InvalidCacheClusterState", err.Error())
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName      xml.Name        `xml:"RebootCacheClusterResponse"`
		Xmlns        string          `xml:"xmlns,attr"`
		CacheCluster cacheClusterXML `xml:"RebootCacheClusterResult>CacheCluster"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:        elasticacheNS,
		CacheCluster: clusterToXML(cl, cl.Status),
	})
}

func (h *Handler) listAllowedNodeTypeModifications(ctx context.Context, c *echo.Context, form url.Values) error {
	clusterID := form.Get("CacheClusterId")
	replicationGroupID := form.Get("ReplicationGroupId")

	scaleUp, scaleDown, err := h.Backend.ListAllowedNodeTypeModifications(ctx, clusterID, replicationGroupID)
	if err != nil {
		if errors.Is(err, ErrNodeTypeModSourceRequired) {
			return xmlError(c, http.StatusBadRequest, "InvalidParameterCombination", err.Error())
		}
		if errors.Is(err, ErrClusterNotFound) {
			return xmlError(c, http.StatusNotFound, "CacheClusterNotFound", "Cache cluster not found")
		}
		if errors.Is(err, ErrReplicationGroupNotFound) {
			return xmlError(c, http.StatusNotFound, "ReplicationGroupNotFoundFault", "Replication group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type scaleModsXML struct {
		Member []string `xml:"member"`
	}

	type result struct {
		XMLName                xml.Name     `xml:"ListAllowedNodeTypeModificationsResponse"`
		Xmlns                  string       `xml:"xmlns,attr"`
		ScaleUpModifications   scaleModsXML `xml:"ListAllowedNodeTypeModificationsResult>ScaleUpModifications"`
		ScaleDownModifications scaleModsXML `xml:"ListAllowedNodeTypeModificationsResult>ScaleDownModifications"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                  elasticacheNS,
		ScaleUpModifications:   scaleModsXML{Member: scaleUp},
		ScaleDownModifications: scaleModsXML{Member: scaleDown},
	})
}

// mergeClusterSettings copies the members ApplyClusterSettings owns from settled onto cluster.
func mergeClusterSettings(cluster, settled *Cluster) *Cluster {
	out := *cluster
	out.AutoMinorVersionUpgrade = settled.AutoMinorVersionUpgrade
	out.NotificationTopicArn = settled.NotificationTopicArn
	out.NotificationTopicStatus = settled.NotificationTopicStatus
	out.NetworkType = settled.NetworkType
	out.IPDiscovery = settled.IPDiscovery
	out.SecurityGroupIDs = settled.SecurityGroupIDs
	out.CacheSecurityGroupNames = settled.CacheSecurityGroupNames
	out.LogDeliveryConfigurations = settled.LogDeliveryConfigurations

	return &out
}
