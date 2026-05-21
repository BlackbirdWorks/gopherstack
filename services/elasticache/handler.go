package elasticache

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	ownerElasticacheStub = "elasticache-stub"
)

const (
	elasticacheVersion = "2015-02-02"
	elasticacheNS      = "http://elasticache.amazonaws.com/doc/2015-02-02/"
	unknownOp          = "Unknown"
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

// cacheClusterXML is the XML representation of a cache cluster.
type cacheClusterXML struct {
	CacheParameterGroupName    string     `xml:"CacheParameterGroup>CacheParameterGroupName,omitempty"`
	PreferredMaintenanceWindow string     `xml:"PreferredMaintenanceWindow,omitempty"`
	CacheNodeType              string     `xml:"CacheNodeType"`
	Engine                     string     `xml:"Engine"`
	EngineVersion              string     `xml:"EngineVersion"`
	ARN                        string     `xml:"ARN"`
	CacheClusterStatus         string     `xml:"CacheClusterStatus"`
	CreatedAt                  string     `xml:"CacheClusterCreateTime,omitempty"`
	CacheClusterID             string     `xml:"CacheClusterId"`
	ReplicationGroupID         string     `xml:"ReplicationGroupId,omitempty"`
	SnapshotWindow             string     `xml:"SnapshotWindow,omitempty"`
	CacheNodes                 cacheNodes `xml:"CacheNodes"`
	NumCacheNodes              int        `xml:"NumCacheNodes"`
	TransitEncryptionEnabled   bool       `xml:"TransitEncryptionEnabled"`
	AtRestEncryptionEnabled    bool       `xml:"AtRestEncryptionEnabled"`
}

// Handler is the Echo HTTP handler for ElastiCache operations.
type Handler struct {
	Backend   StorageBackend
	AccountID string
	Region    string
}

// NewHandler creates a new ElastiCache handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "ElastiCache" }

// GetSupportedOperations returns all supported ElastiCache operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateCacheCluster",
		"DeleteCacheCluster",
		"DescribeCacheClusters",
		"ModifyCacheCluster",
		"ListTagsForResource",
		"AddTagsToResource",
		"RemoveTagsFromResource",
		"CreateReplicationGroup",
		"DeleteReplicationGroup",
		"DescribeReplicationGroups",
		"ModifyReplicationGroup",
		"TestFailover",
		"CreateCacheParameterGroup",
		"DeleteCacheParameterGroup",
		"DescribeCacheParameterGroups",
		"ModifyCacheParameterGroup",
		"ResetCacheParameterGroup",
		"DescribeCacheParameters",
		"CreateCacheSubnetGroup",
		"DeleteCacheSubnetGroup",
		"DescribeCacheSubnetGroups",
		"ModifyCacheSubnetGroup",
		"CreateSnapshot",
		"DeleteSnapshot",
		"DescribeSnapshots",
		"CopySnapshot",
		"DescribeEvents",
		// New ops
		"CreateCacheSecurityGroup",
		"AuthorizeCacheSecurityGroupIngress",
		"CreateGlobalReplicationGroup",
		"CreateServerlessCache",
		"CreateServerlessCacheSnapshot",
		"CopyServerlessCacheSnapshot",
		"CreateUser",
		"BatchApplyUpdateAction",
		"BatchStopUpdateAction",
		"CompleteMigration",
		// Ops2
		"DeleteUser",
		"DescribeUsers",
		"ModifyUser",
		"CreateUserGroup",
		"DeleteUserGroup",
		"DescribeUserGroups",
		"ModifyUserGroup",
		"DeleteGlobalReplicationGroup",
		"DescribeGlobalReplicationGroups",
		"DisassociateGlobalReplicationGroup",
		"FailoverGlobalReplicationGroup",
		"IncreaseNodeGroupsInGlobalReplicationGroup",
		"DecreaseNodeGroupsInGlobalReplicationGroup",
		"ModifyGlobalReplicationGroup",
		"RebalanceSlotsInGlobalReplicationGroup",
		"DescribeReservedCacheNodes",
		"DescribeReservedCacheNodesOfferings",
		"PurchaseReservedCacheNodesOffering",
		"DeleteServerlessCache",
		"DeleteServerlessCacheSnapshot",
		"DescribeServerlessCaches",
		"DescribeServerlessCacheSnapshots",
		"ExportServerlessCacheSnapshot",
		"ModifyServerlessCache",
		"StartMigration",
		"TestMigration",
		"IncreaseReplicaCount",
		"DecreaseReplicaCount",
		"ModifyReplicationGroupShardConfiguration",
		"DescribeCacheEngineVersions",
		"RebootCacheCluster",
		"DeleteCacheSecurityGroup",
		"DescribeCacheSecurityGroups",
		"RevokeCacheSecurityGroupIngress",
		"DescribeEngineDefaultParameters",
		"DescribeServiceUpdates",
		"DescribeUpdateActions",
		"ListAllowedNodeTypeModifications",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "elasticache" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this ElastiCache instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a matcher for ElastiCache query-protocol requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if r.Method != http.MethodPost {
			return false
		}
		ct := r.Header.Get("Content-Type")
		if !strings.Contains(ct, "application/x-www-form-urlencoded") {
			return false
		}
		body, err := httputils.ReadBody(r)
		if err != nil {
			return false
		}
		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return false
		}

		return vals.Get("Version") == elasticacheVersion &&
			slices.Contains(h.GetSupportedOperations(), vals.Get("Action"))
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathSubdomain }

// ExtractOperation extracts the Action from the form body.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return unknownOp
	}
	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return unknownOp
	}
	action := vals.Get("Action")
	if action == "" {
		return unknownOp
	}

	return action
}

// ExtractResource extracts the primary resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}
	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return ""
	}
	for _, key := range []string{
		"CacheClusterId",
		"ReplicationGroupId",
		"CacheParameterGroupName",
		"CacheSubnetGroupName",
		"SnapshotName",
		"ResourceName",
		"CacheSecurityGroupName",
		"GlobalReplicationGroupIdSuffix",
		"ServerlessCacheName",
		"ServerlessCacheSnapshotName",
		"UserId",
	} {
		if v := vals.Get(key); v != "" {
			return v
		}
	}

	return ""
}

type elasticacheActionFn func(c *echo.Context, form url.Values) error

func (h *Handler) dispatchTable() map[string]elasticacheActionFn {
	return map[string]elasticacheActionFn{
		"CreateCacheCluster":           h.createCacheCluster,
		"DeleteCacheCluster":           h.deleteCacheCluster,
		"DescribeCacheClusters":        h.describeCacheClusters,
		"ModifyCacheCluster":           h.modifyCacheCluster,
		"ListTagsForResource":          h.listTagsForResource,
		"AddTagsToResource":            h.addTagsToResource,
		"RemoveTagsFromResource":       h.removeTagsFromResource,
		"CreateReplicationGroup":       h.createReplicationGroup,
		"DeleteReplicationGroup":       h.deleteReplicationGroup,
		"DescribeReplicationGroups":    h.describeReplicationGroups,
		"ModifyReplicationGroup":       h.modifyReplicationGroup,
		"TestFailover":                 h.testFailoverReplicationGroup,
		"CreateCacheParameterGroup":    h.createCacheParameterGroup,
		"DeleteCacheParameterGroup":    h.deleteCacheParameterGroup,
		"DescribeCacheParameterGroups": h.describeCacheParameterGroups,
		"ModifyCacheParameterGroup":    h.modifyCacheParameterGroup,
		"ResetCacheParameterGroup":     h.resetCacheParameterGroup,
		"DescribeCacheParameters":      h.describeCacheParameters,
		"CreateCacheSubnetGroup":       h.createCacheSubnetGroup,
		"DeleteCacheSubnetGroup":       h.deleteCacheSubnetGroup,
		"DescribeCacheSubnetGroups":    h.describeCacheSubnetGroups,
		"ModifyCacheSubnetGroup":       h.modifyCacheSubnetGroup,
		"CreateSnapshot":               h.createSnapshot,
		"DeleteSnapshot":               h.deleteSnapshot,
		"DescribeSnapshots":            h.describeSnapshots,
		"CopySnapshot":                 h.copySnapshot,
		"DescribeEvents":               h.describeEvents,
		// New ops
		"CreateCacheSecurityGroup":           h.createCacheSecurityGroup,
		"AuthorizeCacheSecurityGroupIngress": h.authorizeCacheSecurityGroupIngress,
		"CreateGlobalReplicationGroup":       h.createGlobalReplicationGroup,
		"CreateServerlessCache":              h.createServerlessCache,
		"CreateServerlessCacheSnapshot":      h.createServerlessCacheSnapshot,
		"CopyServerlessCacheSnapshot":        h.copyServerlessCacheSnapshot,
		"CreateUser":                         h.createUser,
		"BatchApplyUpdateAction":             h.batchApplyUpdateAction,
		"BatchStopUpdateAction":              h.batchStopUpdateAction,
		"CompleteMigration":                  h.completeMigration,
		// Ops2
		"DeleteUser":                                 h.deleteUser,
		"DescribeUsers":                              h.describeUsers,
		"ModifyUser":                                 h.modifyUser,
		"CreateUserGroup":                            h.createUserGroup,
		"DeleteUserGroup":                            h.deleteUserGroup,
		"DescribeUserGroups":                         h.describeUserGroups,
		"ModifyUserGroup":                            h.modifyUserGroup,
		"DeleteGlobalReplicationGroup":               h.deleteGlobalReplicationGroup,
		"DescribeGlobalReplicationGroups":            h.describeGlobalReplicationGroups,
		"DisassociateGlobalReplicationGroup":         h.disassociateGlobalReplicationGroup,
		"FailoverGlobalReplicationGroup":             h.failoverGlobalReplicationGroup,
		"IncreaseNodeGroupsInGlobalReplicationGroup": h.increaseNodeGroupsInGlobalReplicationGroup,
		"DecreaseNodeGroupsInGlobalReplicationGroup": h.decreaseNodeGroupsInGlobalReplicationGroup,
		"ModifyGlobalReplicationGroup":               h.modifyGlobalReplicationGroup,
		"RebalanceSlotsInGlobalReplicationGroup":     h.rebalanceSlotsInGlobalReplicationGroup,
		"DescribeReservedCacheNodes":                 h.describeReservedCacheNodes,
		"DescribeReservedCacheNodesOfferings":        h.describeReservedCacheNodesOfferings,
		"PurchaseReservedCacheNodesOffering":         h.purchaseReservedCacheNodesOffering,
		"DeleteServerlessCache":                      h.deleteServerlessCache,
		"DeleteServerlessCacheSnapshot":              h.deleteServerlessCacheSnapshot,
		"DescribeServerlessCaches":                   h.describeServerlessCaches,
		"DescribeServerlessCacheSnapshots":           h.describeServerlessCacheSnapshots,
		"ExportServerlessCacheSnapshot":              h.exportServerlessCacheSnapshot,
		"ModifyServerlessCache":                      h.modifyServerlessCache,
		"StartMigration":                             h.startMigration,
		"TestMigration":                              h.testMigration,
		"IncreaseReplicaCount":                       h.increaseReplicaCount,
		"DecreaseReplicaCount":                       h.decreaseReplicaCount,
		"ModifyReplicationGroupShardConfiguration":   h.modifyReplicationGroupShardConfiguration,
		"DescribeCacheEngineVersions":                h.describeCacheEngineVersions,
		"RebootCacheCluster":                         h.rebootCacheCluster,
		"DeleteCacheSecurityGroup":                   h.deleteCacheSecurityGroup,
		"DescribeCacheSecurityGroups":                h.describeCacheSecurityGroups,
		"RevokeCacheSecurityGroupIngress":            h.revokeCacheSecurityGroupIngress,
		"DescribeEngineDefaultParameters":            h.describeEngineDefaultParameters,
		"DescribeServiceUpdates":                     h.describeServiceUpdates,
		"DescribeUpdateActions":                      h.describeUpdateActions,
		"ListAllowedNodeTypeModifications":           h.listAllowedNodeTypeModifications,
	}
}

// Handler returns the Echo handler function for ElastiCache requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return c.String(http.StatusBadRequest, "cannot read body")
		}
		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return c.String(http.StatusBadRequest, "cannot parse form")
		}
		action := vals.Get("Action")
		fn, ok := h.dispatchTable()[action]
		if !ok {
			return c.String(http.StatusBadRequest, "unknown action: "+action)
		}

		return fn(c, vals)
	}
}

// parsePagination extracts Marker and MaxRecords from query form values.
func parsePagination(form url.Values) (string, int) {
	marker := form.Get("Marker")
	maxRecords := 0

	if s := form.Get("MaxRecords"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			maxRecords = n
		}
	}

	return marker, maxRecords
}

// parseSubnetIDs extracts a list of subnet IDs from query form values.
func parseSubnetIDs(form url.Values) []string {
	var ids []string
	for i := 1; ; i++ {
		id := form.Get(fmt.Sprintf("SubnetIds.SubnetIdentifier.%d", i))
		if id == "" {
			break
		}
		ids = append(ids, id)
	}

	return ids
}

func (h *Handler) createCacheCluster(c *echo.Context, form url.Values) error {
	id := form.Get("CacheClusterId")
	if id == "" {
		return xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "CacheClusterId is required")
	}

	engine := form.Get("Engine")
	nodeType := form.Get("CacheNodeType")
	paramGroupName := form.Get("CacheParameterGroupName")
	maintenanceWindow := form.Get("PreferredMaintenanceWindow")
	snapshotWindow := form.Get("SnapshotWindow")
	numCacheNodes := 1

	if s := form.Get("NumCacheNodes"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			numCacheNodes = n
		}
	}

	cluster, err := h.Backend.CreateClusterWithOptions(
		id,
		engine,
		nodeType,
		paramGroupName,
		maintenanceWindow,
		snapshotWindow,
		numCacheNodes,
		0,
	)
	if err != nil {
		if errors.Is(err, ErrClusterAlreadyExists) {
			return xmlError(c, http.StatusBadRequest, "CacheClusterAlreadyExists", "Cache cluster already exists")
		}
		if errors.Is(err, ErrParameterGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheParameterGroupNotFound", "Cache parameter group not found")
		}
		if errors.Is(err, ErrInvalidParameterGroupFamily) {
			return xmlError(c, http.StatusBadRequest, "InvalidParameterGroupFamily", err.Error())
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName      xml.Name        `xml:"CreateCacheClusterResponse"`
		Xmlns        string          `xml:"xmlns,attr"`
		CacheCluster cacheClusterXML `xml:"CreateCacheClusterResult>CacheCluster"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:        elasticacheNS,
		CacheCluster: clusterToXML(cluster, cluster.Status),
	})
}

func (h *Handler) deleteCacheCluster(c *echo.Context, form url.Values) error {
	id := form.Get("CacheClusterId")
	clusters, descErr := h.Backend.DescribeClusters(id, "", 0)
	if descErr != nil {
		if errors.Is(descErr, ErrClusterNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheClusterNotFound", "Cache cluster not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", descErr.Error())
	}
	cl := clusters.Data[0]
	if err := h.Backend.DeleteCluster(id); err != nil {
		if errors.Is(err, ErrClusterNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheClusterNotFound", "Cache cluster not found")
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

func (h *Handler) describeCacheClusters(c *echo.Context, form url.Values) error {
	id := form.Get("CacheClusterId")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeClusters(id, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrClusterNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheClusterNotFound", "Cache cluster not found")
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

func (h *Handler) listTagsForResource(c *echo.Context, form url.Values) error {
	arn := form.Get("ResourceName")
	tags, err := h.Backend.ListTagsForResource(arn)
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidARN", err.Error())
	}

	type tag struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type tagList struct {
		Tag []tag `xml:"Tag"`
	}
	type result struct {
		XMLName xml.Name `xml:"ListTagsForResourceResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
		TagList tagList  `xml:"ListTagsForResourceResult>TagList"`
	}

	items := make([]tag, 0, len(tags))
	for k, v := range tags {
		items = append(items, tag{Key: k, Value: v})
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:   elasticacheNS,
		TagList: tagList{Tag: items},
	})
}

func (h *Handler) createReplicationGroup(c *echo.Context, form url.Values) error {
	opts := parseCreateReplicationGroupOpts(form)

	rg, err := h.Backend.CreateReplicationGroupFull(opts)
	if err != nil {
		return mapReplicationGroupCreateErr(c, err)
	}

	type result struct {
		XMLName          xml.Name            `xml:"CreateReplicationGroupResponse"`
		Xmlns            string              `xml:"xmlns,attr"`
		ReplicationGroup replicationGroupXML `xml:"CreateReplicationGroupResult>ReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:            elasticacheNS,
		ReplicationGroup: rgToXML(*rg),
	})
}

// parseCreateReplicationGroupOpts extracts all create options from a form submission.
func parseCreateReplicationGroupOpts(form url.Values) ReplicationGroupCreateOpts {
	opts := ReplicationGroupCreateOpts{
		ID:                    form.Get("ReplicationGroupId"),
		Description:           form.Get("ReplicationGroupDescription"),
		ParameterGroupName:    form.Get("CacheParameterGroupName"),
		MaintenanceWindow:     form.Get("PreferredMaintenanceWindow"),
		SnapshotWindow:        form.Get("SnapshotWindow"),
		AuthToken:             form.Get("AuthToken"),
		KmsKeyID:              form.Get("KmsKeyId"),
		NotificationTopicArn:  form.Get("NotificationTopicArn"),
		TransitEncryptionMode: form.Get("TransitEncryptionMode"),
		Engine:                form.Get("Engine"),
		EngineVersion:         form.Get("EngineVersion"),
		CacheNodeType:         form.Get("CacheNodeType"),
	}

	opts.AuthTokenEnabled = !strings.EqualFold(form.Get("AuthToken"), "") ||
		strings.EqualFold(form.Get("AuthTokenEnabled"), "true")
	opts.AtRestEncryptionEnabled = strings.EqualFold(form.Get("AtRestEncryptionEnabled"), "true")
	opts.TransitEncryptionEnabled = strings.EqualFold(form.Get("TransitEncryptionEnabled"), "true")
	opts.ClusterModeEnabled = strings.EqualFold(form.Get("ClusterModeEnabled"), "true") ||
		strings.EqualFold(form.Get("ClusterMode"), "enabled")
	opts.DataTieringEnabled = strings.EqualFold(form.Get("DataTieringEnabled"), "true")
	opts.MultiAZEnabled = strings.EqualFold(form.Get("MultiAZEnabled"), "true")
	opts.AutomaticFailoverEnabled = strings.EqualFold(form.Get("AutomaticFailoverEnabled"), "true")

	if s := form.Get("SnapshotRetentionLimit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			opts.SnapshotRetentionLimit = n
		}
	}

	if s := form.Get("NumNodeGroups"); s != "" {
		if n, err := strconv.ParseInt(s, 10, 32); err == nil {
			opts.NumNodeGroups = int32(n)
		}
	}

	if s := form.Get("ReplicasPerNodeGroup"); s != "" {
		if n, err := strconv.ParseInt(s, 10, 32); err == nil {
			opts.ReplicasPerNodeGroup = int32(n)
		}
	}

	// Parse UserGroupIds.
	for i := 1; ; i++ {
		id := form.Get(fmt.Sprintf("UserGroupIds.member.%d", i))
		if id == "" {
			break
		}
		opts.UserGroupIDs = append(opts.UserGroupIDs, id)
	}

	// Parse Tags.
	tags := make(map[string]string)
	for i := 1; ; i++ {
		key := form.Get(fmt.Sprintf("Tags.Tag.%d.Key", i))
		if key == "" {
			break
		}
		val := form.Get(fmt.Sprintf("Tags.Tag.%d.Value", i))
		tags[key] = val
	}
	if len(tags) > 0 {
		opts.Tags = tags
	}

	return opts
}

// mapReplicationGroupCreateErr maps backend errors to XML error responses.
func mapReplicationGroupCreateErr(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrReplicationGroupAlreadyExists):
		return xmlError(c, http.StatusBadRequest, "ReplicationGroupAlreadyExists", "Replication group already exists")
	case errors.Is(err, ErrParameterGroupNotFound):
		return xmlError(c, http.StatusBadRequest, "CacheParameterGroupNotFound", "Cache parameter group not found")
	case errors.Is(err, ErrDataTieringInvalid):
		return xmlError(c, http.StatusBadRequest, "InvalidParameterValue", err.Error())
	case errors.Is(err, ErrAuthTokenRequiredForMode):
		return xmlError(c, http.StatusBadRequest, "InvalidParameterCombination", err.Error())
	default:
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}
}

func (h *Handler) deleteReplicationGroup(c *echo.Context, form url.Values) error {
	id := form.Get("ReplicationGroupId")
	rgs, descErr := h.Backend.DescribeReplicationGroups(id, "", 0)
	if descErr != nil {
		if errors.Is(descErr, ErrReplicationGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "ReplicationGroupNotFound", "Replication group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", descErr.Error())
	}
	rg := rgs.Data[0]
	if err := h.Backend.DeleteReplicationGroup(id); err != nil {
		if errors.Is(err, ErrReplicationGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "ReplicationGroupNotFound", "Replication group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type replicationGroup struct {
		ReplicationGroupID string `xml:"ReplicationGroupId"`
		Description        string `xml:"Description"`
		Status             string `xml:"Status"`
		ARN                string `xml:"ARN"`
	}
	type result struct {
		XMLName          xml.Name         `xml:"DeleteReplicationGroupResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ReplicationGroup replicationGroup `xml:"DeleteReplicationGroupResult>ReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns: elasticacheNS,
		ReplicationGroup: replicationGroup{
			ReplicationGroupID: rg.ReplicationGroupID,
			Description:        rg.Description,
			Status:             "deleting",
			ARN:                rg.ARN,
		},
	})
}

// nodeGroupNodeXML is the XML for a single node within a node group.
type nodeGroupNodeXML struct {
	CacheClusterID            string        `xml:"CacheClusterId,omitempty"`
	CacheNodeID               string        `xml:"CacheNodeId,omitempty"`
	CurrentRole               string        `xml:"CurrentRole,omitempty"`
	PreferredAvailabilityZone string        `xml:"PreferredAvailabilityZone,omitempty"`
	ReadEndpoint              cacheEndpoint `xml:"ReadEndpoint,omitempty"`
}

// nodeGroupXML is the XML representation of a shard / node group.
type nodeGroupXML struct {
	NodeGroupID     string             `xml:"NodeGroupId"`
	Status          string             `xml:"Status"`
	Slots           string             `xml:"Slots,omitempty"`
	NodeGroupMembers nodeGroupMembersXML `xml:"NodeGroupMembers"`
}

type nodeGroupMembersXML struct {
	NodeGroupMember []nodeGroupNodeXML `xml:"NodeGroupMember"`
}

type nodeGroupsListXML struct {
	NodeGroup []nodeGroupXML `xml:"NodeGroup"`
}

// rgPendingModifiedXML is the XML for pending replication group changes.
type rgPendingModifiedXML struct {
	NumCacheNodes           *int32 `xml:"NumCacheNodes,omitempty"`
	CacheNodeType           string `xml:"CacheNodeType,omitempty"`
	EngineVersion           string `xml:"EngineVersion,omitempty"`
	AuthTokenStatus         string `xml:"AuthTokenStatus,omitempty"`
	AutomaticFailoverStatus string `xml:"AutomaticFailoverStatus,omitempty"`
}

// rgUserGroupIdsXML holds UserGroupId list in the XML response.
type rgUserGroupIdsXML struct {
	UserGroupId []string `xml:"member"`
}

// replicationGroupXML is the XML representation of a single replication group.
type replicationGroupXML struct {
	PendingModifiedValues      *rgPendingModifiedXML `xml:"PendingModifiedValues,omitempty"`
	NodeGroups                 *nodeGroupsListXML    `xml:"NodeGroups,omitempty"`
	UserGroupIds               *rgUserGroupIdsXML    `xml:"UserGroupIds,omitempty"`
	ReplicationGroupID         string                `xml:"ReplicationGroupId"`
	Description                string                `xml:"Description"`
	Status                     string                `xml:"Status"`
	ARN                        string                `xml:"ARN"`
	Engine                     string                `xml:"Engine,omitempty"`
	CacheParameterGroupName    string                `xml:"CacheParameterGroupName,omitempty"`
	AutomaticFailover          string                `xml:"AutomaticFailover,omitempty"`
	MultiAZ                    string                `xml:"MultiAZ,omitempty"`
	CacheNodeType              string                `xml:"CacheNodeType,omitempty"`
	SnapshotWindow             string                `xml:"SnapshotWindow,omitempty"`
	PreferredMaintenanceWindow string                `xml:"PreferredMaintenanceWindow,omitempty"`
	EngineVersion              string                `xml:"EngineVersion,omitempty"`
	CreatedAt                  string                `xml:"CreatingDate,omitempty"`
	KmsKeyID                   string                `xml:"KmsKeyId,omitempty"`
	NotificationTopicArn       string                `xml:"NotificationTopicArn,omitempty"`
	TransitEncryptionMode      string                `xml:"TransitEncryptionMode,omitempty"`
	DataTiering                string                `xml:"DataTiering,omitempty"`
	SnapshotRetentionLimit     int                   `xml:"SnapshotRetentionLimit,omitempty"`
	NumCacheClusters           int                   `xml:"NumCacheClusters,omitempty"`
	ClusterEnabled             bool                  `xml:"ClusterEnabled,omitempty"`
	AuthTokenEnabled           bool                  `xml:"AuthTokenEnabled,omitempty"`
	AtRestEncryptionEnabled    bool                  `xml:"AtRestEncryptionEnabled,omitempty"`
	TransitEncryptionEnabled   bool                  `xml:"TransitEncryptionEnabled,omitempty"`
}

// dataTieringStatus converts a bool to the AWS DataTieringStatus string.
func dataTieringStatus(enabled bool) string {
	if enabled {
		return statusEnabled
	}

	return ""
}

// nodeGroupsToXML converts backend NodeGroups to XML.
func nodeGroupsToXML(ngs []NodeGroup) *nodeGroupsListXML {
	if len(ngs) == 0 {
		return nil
	}

	xmlNGs := make([]nodeGroupXML, 0, len(ngs))
	for _, ng := range ngs {
		members := make([]nodeGroupNodeXML, 0)
		if ng.PrimaryNode != nil {
			members = append(members, nodeGroupNodeXML{
				CacheClusterID:            ng.PrimaryNode.CacheClusterID,
				CacheNodeID:               ng.PrimaryNode.CacheNodeID,
				CurrentRole:               "primary",
				PreferredAvailabilityZone: ng.PrimaryNode.PreferredAvailabilityZone,
			})
		}
		for _, r := range ng.Replicas {
			members = append(members, nodeGroupNodeXML{
				CacheClusterID:            r.CacheClusterID,
				CacheNodeID:               r.CacheNodeID,
				CurrentRole:               "replica",
				PreferredAvailabilityZone: r.PreferredAvailabilityZone,
			})
		}
		xmlNGs = append(xmlNGs, nodeGroupXML{
			NodeGroupID:      ng.NodeGroupID,
			Status:           ng.Status,
			Slots:            ng.Slots,
			NodeGroupMembers: nodeGroupMembersXML{NodeGroupMember: members},
		})
	}

	return &nodeGroupsListXML{NodeGroup: xmlNGs}
}

// pendingToXML converts RGPendingModifiedValues to XML.
func pendingToXML(p *RGPendingModifiedValues) *rgPendingModifiedXML {
	if p == nil {
		return nil
	}

	x := &rgPendingModifiedXML{
		CacheNodeType:           p.CacheNodeType,
		EngineVersion:           p.EngineVersion,
		AuthTokenStatus:         p.AuthTokenStatus,
		AutomaticFailoverStatus: p.AutomaticFailoverStatus,
	}
	if p.ReplicaCount != nil {
		rc := *p.ReplicaCount
		x.NumCacheNodes = &rc
	}

	return x
}

// rgToXML converts a ReplicationGroup to its XML representation.
func rgToXML(rg ReplicationGroup) replicationGroupXML {
	multiAZ := statusDisabled
	if rg.MultiAZEnabled {
		multiAZ = statusEnabled
	}

	autoFailover := rg.AutomaticFailover
	if autoFailover == "" {
		autoFailover = statusDisabled
	}

	numCacheClusters := int(rg.ReplicaCount) + 1
	if numCacheClusters <= 1 && !rg.ClusterModeEnabled {
		numCacheClusters = 1
	}

	var userGroupIds *rgUserGroupIdsXML
	if len(rg.UserGroupIds) > 0 {
		userGroupIds = &rgUserGroupIdsXML{UserGroupId: rg.UserGroupIds}
	}

	return replicationGroupXML{
		ReplicationGroupID:         rg.ReplicationGroupID,
		Description:                rg.Description,
		Status:                     rg.Status,
		ARN:                        rg.ARN,
		Engine:                     rg.Engine,
		CacheParameterGroupName:    rg.CacheParameterGroupName,
		AutomaticFailover:          autoFailover,
		MultiAZ:                    multiAZ,
		CacheNodeType:              rg.CacheNodeType,
		SnapshotWindow:             rg.SnapshotWindow,
		PreferredMaintenanceWindow: rg.PreferredMaintenanceWindow,
		EngineVersion:              rg.EngineVersion,
		CreatedAt:                  rg.CreatedAt.UTC().Format(time.RFC3339),
		KmsKeyID:                   rg.KmsKeyID,
		NotificationTopicArn:       rg.NotificationTopicArn,
		TransitEncryptionMode:      rg.TransitEncryptionMode,
		SnapshotRetentionLimit:     rg.SnapshotRetentionLimit,
		NumCacheClusters:           numCacheClusters,
		ClusterEnabled:             rg.ClusterModeEnabled,
		AuthTokenEnabled:           rg.AuthTokenEnabled,
		AtRestEncryptionEnabled:    rg.AtRestEncryptionEnabled,
		TransitEncryptionEnabled:   rg.TransitEncryptionEnabled,
		DataTiering:                dataTieringStatus(rg.DataTieringEnabled),
		NodeGroups:                 nodeGroupsToXML(rg.NodeGroups),
		PendingModifiedValues:      pendingToXML(rg.PendingModifiedValues),
		UserGroupIds:               userGroupIds,
	}
}

// describeReplicationGroupsResultXML is the XML result for DescribeReplicationGroups.
type describeReplicationGroupsResultXML struct {
	XMLName           xml.Name                 `xml:"DescribeReplicationGroupsResponse"`
	Xmlns             string                   `xml:"xmlns,attr"`
	Marker            string                   `xml:"DescribeReplicationGroupsResult>Marker,omitempty"`
	ReplicationGroups replicationGroupsListXML `xml:"DescribeReplicationGroupsResult>ReplicationGroups"`
}

// replicationGroupsListXML holds the list of replication groups.
type replicationGroupsListXML struct {
	ReplicationGroup []replicationGroupXML `xml:"ReplicationGroup"`
}

func (h *Handler) describeReplicationGroups(c *echo.Context, form url.Values) error {
	id := form.Get("ReplicationGroupId")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeReplicationGroups(id, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrReplicationGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "ReplicationGroupNotFound", "Replication group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	items := make([]replicationGroupXML, 0, len(p.Data))
	for _, rg := range p.Data {
		items = append(items, rgToXML(rg))
	}

	return xmlResp(c, http.StatusOK, describeReplicationGroupsResultXML{
		Xmlns:             elasticacheNS,
		Marker:            p.Next,
		ReplicationGroups: replicationGroupsListXML{ReplicationGroup: items},
	})
}

// clusterToXML converts a Cluster to its XML representation with the given status.
func clusterToXML(cl *Cluster, status string) cacheClusterXML {
	n := cl.NumCacheNodes
	if n <= 0 {
		n = 1
	}

	nodes := make([]cacheNode, 0, n)
	for i := range n {
		nodeID := fmt.Sprintf("%04d", i+1)
		nodes = append(nodes, cacheNode{
			CacheNodeID:              nodeID,
			CacheNodeStatus:          status,
			CacheNodeCreateTime:      cl.CreatedAt.UTC().Format(time.RFC3339),
			CustomerAvailabilityZone: "us-east-1a",
			Endpoint: cacheEndpoint{
				Address: cl.Endpoint,
				Port:    cl.Port,
			},
		})
	}

	return cacheClusterXML{
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
		TransitEncryptionEnabled:   cl.TransitEncryptionEnabled,
		AtRestEncryptionEnabled:    cl.AtRestEncryptionEnabled,
		CreatedAt:                  cl.CreatedAt.UTC().Format(time.RFC3339),
		CacheNodes: cacheNodes{
			CacheNode: nodes,
		},
	}
}

func (h *Handler) modifyCacheCluster(c *echo.Context, form url.Values) error {
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

	cluster, err := h.Backend.ModifyCluster(
		id,
		nodeType,
		paramGroupName,
		engineVersion,
		maintenanceWindow,
		snapshotWindow,
		numCacheNodes,
	)
	if err != nil {
		if errors.Is(err, ErrClusterNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheClusterNotFound", "Cache cluster not found")
		}
		if errors.Is(err, ErrParameterGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheParameterGroupNotFound", "Cache parameter group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

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

func (h *Handler) modifyReplicationGroup(c *echo.Context, form url.Values) error {
	id := form.Get("ReplicationGroupId")
	opts := parseModifyReplicationGroupOpts(form)

	rg, err := h.Backend.ModifyReplicationGroupFull(id, opts)
	if err != nil {
		return mapReplicationGroupModifyErr(c, err)
	}

	type result struct {
		XMLName          xml.Name            `xml:"ModifyReplicationGroupResponse"`
		Xmlns            string              `xml:"xmlns,attr"`
		ReplicationGroup replicationGroupXML `xml:"ModifyReplicationGroupResult>ReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:            elasticacheNS,
		ReplicationGroup: rgToXML(*rg),
	})
}

// parseModifyReplicationGroupOpts extracts all modify options from a form.
func parseModifyReplicationGroupOpts(form url.Values) ReplicationGroupModifyOpts {
	opts := ReplicationGroupModifyOpts{
		Description:             form.Get("ReplicationGroupDescription"),
		ParameterGroupName:      form.Get("CacheParameterGroupName"),
		EngineVersion:           form.Get("EngineVersion"),
		CacheNodeType:           form.Get("CacheNodeType"),
		MaintenanceWindow:       form.Get("PreferredMaintenanceWindow"),
		SnapshotWindow:          form.Get("SnapshotWindow"),
		AuthToken:               form.Get("AuthToken"),
		AuthTokenUpdateStrategy: form.Get("AuthTokenUpdateStrategy"),
		NotificationTopicArn:    form.Get("NotificationTopicArn"),
		TransitEncryptionMode:   form.Get("TransitEncryptionMode"),
		ApplyImmediately:        strings.EqualFold(form.Get("ApplyImmediately"), "true"),
	}

	if s := form.Get("AutomaticFailoverEnabled"); s != "" {
		v := strings.EqualFold(s, "true")
		opts.AutomaticFailoverEnabled = &v
	}

	if s := form.Get("MultiAZEnabled"); s != "" {
		v := strings.EqualFold(s, "true")
		opts.MultiAZEnabled = &v
	}

	if s := form.Get("SnapshotRetentionLimit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			opts.SnapshotRetentionLimit = &n
		}
	}

	if s := form.Get("ReplicaCount"); s != "" {
		if n, err := strconv.ParseInt(s, 10, 32); err == nil {
			rc := int32(n)
			opts.ReplicaCount = &rc
		}
	}

	// Parse UserGroupIds to add/remove.
	for i := 1; ; i++ {
		id := form.Get(fmt.Sprintf("UserGroupIdsToAdd.member.%d", i))
		if id == "" {
			break
		}
		opts.UserGroupIdsToAdd = append(opts.UserGroupIdsToAdd, id)
	}
	for i := 1; ; i++ {
		id := form.Get(fmt.Sprintf("UserGroupIdsToRemove.member.%d", i))
		if id == "" {
			break
		}
		opts.UserGroupIdsToRemove = append(opts.UserGroupIdsToRemove, id)
	}

	return opts
}

// mapReplicationGroupModifyErr maps backend errors to XML error responses.
func mapReplicationGroupModifyErr(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrReplicationGroupNotFound):
		return xmlError(c, http.StatusBadRequest, "ReplicationGroupNotFound", "Replication group not found")
	case errors.Is(err, ErrParameterGroupNotFound):
		return xmlError(c, http.StatusBadRequest, "CacheParameterGroupNotFound", "Cache parameter group not found")
	default:
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}
}

// cacheParameterGroupXML is the XML representation of a cache parameter group.
type cacheParameterGroupXML struct {
	ARN                       string `xml:"ARN"`
	CacheParameterGroupFamily string `xml:"CacheParameterGroupFamily"`
	CacheParameterGroupName   string `xml:"CacheParameterGroupName"`
	Description               string `xml:"Description"`
	IsGlobal                  bool   `xml:"IsGlobal"`
}

func paramGroupToXML(pg *CacheParameterGroup) cacheParameterGroupXML {
	return cacheParameterGroupXML{
		ARN:                       pg.ARN,
		CacheParameterGroupFamily: pg.Family,
		CacheParameterGroupName:   pg.Name,
		Description:               pg.Description,
		IsGlobal:                  pg.IsGlobal,
	}
}

func (h *Handler) createCacheParameterGroup(c *echo.Context, form url.Values) error {
	name := form.Get("CacheParameterGroupName")
	family := form.Get("CacheParameterGroupFamily")
	desc := form.Get("Description")

	pg, err := h.Backend.CreateParameterGroup(name, family, desc)
	if err != nil {
		if errors.Is(err, ErrParameterGroupAlreadyExists) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"CacheParameterGroupAlreadyExists",
				"Cache parameter group already exists",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName             xml.Name               `xml:"CreateCacheParameterGroupResponse"`
		Xmlns               string                 `xml:"xmlns,attr"`
		CacheParameterGroup cacheParameterGroupXML `xml:"CreateCacheParameterGroupResult>CacheParameterGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:               elasticacheNS,
		CacheParameterGroup: paramGroupToXML(pg),
	})
}

func (h *Handler) deleteCacheParameterGroup(c *echo.Context, form url.Values) error {
	name := form.Get("CacheParameterGroupName")

	if err := h.Backend.DeleteParameterGroup(name); err != nil {
		if errors.Is(err, ErrParameterGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheParameterGroupNotFound", "Cache parameter group not found")
		}
		if errors.Is(err, ErrParameterGroupDefaultNotModifiable) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"InvalidCacheParameterGroupState",
				"The default parameter group cannot be deleted",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName   xml.Name `xml:"DeleteCacheParameterGroupResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return xmlResp(c, http.StatusOK, result{Xmlns: elasticacheNS, RequestID: ownerElasticacheStub})
}

// describeCacheParameterGroupsResultXML is the XML result for DescribeCacheParameterGroups.
type describeCacheParameterGroupsResultXML struct {
	XMLName              xml.Name                    `xml:"DescribeCacheParameterGroupsResponse"`
	Xmlns                string                      `xml:"xmlns,attr"`
	Marker               string                      `xml:"DescribeCacheParameterGroupsResult>Marker,omitempty"`
	CacheParameterGroups cacheParameterGroupsListXML `xml:"DescribeCacheParameterGroupsResult>CacheParameterGroups"`
}

// cacheParameterGroupsListXML holds the list of cache parameter groups.
type cacheParameterGroupsListXML struct {
	CacheParameterGroup []cacheParameterGroupXML `xml:"CacheParameterGroup"`
}

func (h *Handler) describeCacheParameterGroups(c *echo.Context, form url.Values) error {
	name := form.Get("CacheParameterGroupName")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeParameterGroups(name, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrParameterGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheParameterGroupNotFound", "Cache parameter group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	items := make([]cacheParameterGroupXML, 0, len(p.Data))
	for i := range p.Data {
		items = append(items, paramGroupToXML(&p.Data[i]))
	}

	return xmlResp(c, http.StatusOK, describeCacheParameterGroupsResultXML{
		Xmlns:                elasticacheNS,
		Marker:               p.Next,
		CacheParameterGroups: cacheParameterGroupsListXML{CacheParameterGroup: items},
	})
}

func (h *Handler) modifyCacheParameterGroup(c *echo.Context, form url.Values) error {
	name := form.Get("CacheParameterGroupName")

	params := make(map[string]string)

	for i := 1; ; i++ {
		pname := form.Get(fmt.Sprintf("ParameterNameValues.ParameterNameValue.%d.ParameterName", i))
		if pname == "" {
			break
		}
		pval := form.Get(fmt.Sprintf("ParameterNameValues.ParameterNameValue.%d.ParameterValue", i))
		params[pname] = pval
	}

	pg, err := h.Backend.ModifyParameterGroup(name, params)
	if err != nil {
		if errors.Is(err, ErrParameterGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheParameterGroupNotFound", "Cache parameter group not found")
		}
		if errors.Is(err, ErrParameterGroupDefaultNotModifiable) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"InvalidCacheParameterGroupState",
				"The default parameter group cannot be modified",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                 xml.Name `xml:"ModifyCacheParameterGroupResponse"`
		Xmlns                   string   `xml:"xmlns,attr"`
		CacheParameterGroupName string   `xml:"ModifyCacheParameterGroupResult>CacheParameterGroupName"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                   elasticacheNS,
		CacheParameterGroupName: pg.Name,
	})
}

func (h *Handler) resetCacheParameterGroup(c *echo.Context, form url.Values) error {
	name := form.Get("CacheParameterGroupName")
	resetAll := form.Get("ResetAllParameters") == "true"

	var paramNames []string
	if !resetAll {
		for i := 1; ; i++ {
			pname := form.Get(fmt.Sprintf("ParameterNameValues.ParameterNameValue.%d.ParameterName", i))
			if pname == "" {
				break
			}
			paramNames = append(paramNames, pname)
		}
	}

	pg, err := h.Backend.ResetParameterGroup(name, paramNames, resetAll)
	if err != nil {
		if errors.Is(err, ErrParameterGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheParameterGroupNotFound", "Cache parameter group not found")
		}
		if errors.Is(err, ErrParameterGroupDefaultNotModifiable) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"InvalidCacheParameterGroupState",
				"The default parameter group cannot be reset",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                 xml.Name `xml:"ResetCacheParameterGroupResponse"`
		Xmlns                   string   `xml:"xmlns,attr"`
		CacheParameterGroupName string   `xml:"ResetCacheParameterGroupResult>CacheParameterGroupName"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                   elasticacheNS,
		CacheParameterGroupName: pg.Name,
	})
}

// parameterXML is the XML representation of a single cache parameter.
type parameterXML struct {
	ParameterName  string `xml:"ParameterName"`
	ParameterValue string `xml:"ParameterValue"`
	DataType       string `xml:"DataType"`
	IsModifiable   bool   `xml:"IsModifiable"`
}

// describeCacheParametersResultXML is the XML result for DescribeCacheParameters.
type describeCacheParametersResultXML struct {
	XMLName    xml.Name          `xml:"DescribeCacheParametersResponse"`
	Xmlns      string            `xml:"xmlns,attr"`
	Marker     string            `xml:"DescribeCacheParametersResult>Marker,omitempty"`
	Parameters parametersListXML `xml:"DescribeCacheParametersResult>Parameters"`
}

// parametersListXML holds the list of parameters.
type parametersListXML struct {
	Parameter []parameterXML `xml:"Parameter"`
}

// buildParameterItems converts CacheParameter backend items to XML.
func buildParameterItems(params []CacheParameter) []parameterXML {
	items := make([]parameterXML, 0, len(params))
	for _, param := range params {
		items = append(items, parameterXML{
			ParameterName:  param.Name,
			ParameterValue: param.Value,
			DataType:       param.DataType,
			IsModifiable:   param.IsModifiable,
		})
	}

	return items
}

func (h *Handler) describeCacheParameters(c *echo.Context, form url.Values) error {
	name := form.Get("CacheParameterGroupName")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeParameters(name, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrParameterGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheParameterGroupNotFound", "Cache parameter group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return xmlResp(c, http.StatusOK, describeCacheParametersResultXML{
		Xmlns:      elasticacheNS,
		Marker:     p.Next,
		Parameters: parametersListXML{Parameter: buildParameterItems(p.Data)},
	})
}

// cacheSubnetGroupXML is the XML representation of a cache subnet group.
type cacheSubnetGroupXML struct {
	ARN                         string     `xml:"ARN"`
	CacheSubnetGroupName        string     `xml:"CacheSubnetGroupName"`
	CacheSubnetGroupDescription string     `xml:"CacheSubnetGroupDescription"`
	VpcID                       string     `xml:"VpcId"`
	Subnets                     subnetsXML `xml:"Subnets"`
}

type subnetXML struct {
	SubnetIdentifier string `xml:"SubnetIdentifier"`
}

type subnetsXML struct {
	Subnet []subnetXML `xml:"Subnet"`
}

func subnetGroupToXML(sg *CacheSubnetGroup) cacheSubnetGroupXML {
	subnets := make([]subnetXML, 0, len(sg.SubnetIDs))
	for _, id := range sg.SubnetIDs {
		subnets = append(subnets, subnetXML{SubnetIdentifier: id})
	}

	return cacheSubnetGroupXML{
		ARN:                         sg.ARN,
		CacheSubnetGroupName:        sg.Name,
		CacheSubnetGroupDescription: sg.Description,
		VpcID:                       sg.VpcID,
		Subnets:                     subnetsXML{Subnet: subnets},
	}
}

func (h *Handler) createCacheSubnetGroup(c *echo.Context, form url.Values) error {
	name := form.Get("CacheSubnetGroupName")
	desc := form.Get("CacheSubnetGroupDescription")
	subnetIDs := parseSubnetIDs(form)

	sg, err := h.Backend.CreateSubnetGroup(name, desc, subnetIDs)
	if err != nil {
		if errors.Is(err, ErrSubnetGroupAlreadyExists) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"CacheSubnetGroupAlreadyExists",
				"Cache subnet group already exists",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName          xml.Name            `xml:"CreateCacheSubnetGroupResponse"`
		Xmlns            string              `xml:"xmlns,attr"`
		CacheSubnetGroup cacheSubnetGroupXML `xml:"CreateCacheSubnetGroupResult>CacheSubnetGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:            elasticacheNS,
		CacheSubnetGroup: subnetGroupToXML(sg),
	})
}

func (h *Handler) deleteCacheSubnetGroup(c *echo.Context, form url.Values) error {
	name := form.Get("CacheSubnetGroupName")

	if err := h.Backend.DeleteSubnetGroup(name); err != nil {
		if errors.Is(err, ErrSubnetGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheSubnetGroupNotFound", "Cache subnet group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName   xml.Name `xml:"DeleteCacheSubnetGroupResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return xmlResp(c, http.StatusOK, result{Xmlns: elasticacheNS, RequestID: ownerElasticacheStub})
}

// describeCacheSubnetGroupsResultXML is the XML result for DescribeCacheSubnetGroups.
type describeCacheSubnetGroupsResultXML struct {
	XMLName           xml.Name                 `xml:"DescribeCacheSubnetGroupsResponse"`
	Xmlns             string                   `xml:"xmlns,attr"`
	Marker            string                   `xml:"DescribeCacheSubnetGroupsResult>Marker,omitempty"`
	CacheSubnetGroups cacheSubnetGroupsListXML `xml:"DescribeCacheSubnetGroupsResult>CacheSubnetGroups"`
}

// cacheSubnetGroupsListXML holds the list of cache subnet groups.
type cacheSubnetGroupsListXML struct {
	CacheSubnetGroup []cacheSubnetGroupXML `xml:"CacheSubnetGroup"`
}

func (h *Handler) describeCacheSubnetGroups(c *echo.Context, form url.Values) error {
	name := form.Get("CacheSubnetGroupName")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeSubnetGroups(name, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrSubnetGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheSubnetGroupNotFound", "Cache subnet group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	items := make([]cacheSubnetGroupXML, 0, len(p.Data))
	for i := range p.Data {
		items = append(items, subnetGroupToXML(&p.Data[i]))
	}

	return xmlResp(c, http.StatusOK, describeCacheSubnetGroupsResultXML{
		Xmlns:             elasticacheNS,
		Marker:            p.Next,
		CacheSubnetGroups: cacheSubnetGroupsListXML{CacheSubnetGroup: items},
	})
}

func (h *Handler) modifyCacheSubnetGroup(c *echo.Context, form url.Values) error {
	name := form.Get("CacheSubnetGroupName")
	desc := form.Get("CacheSubnetGroupDescription")
	subnetIDs := parseSubnetIDs(form)

	sg, err := h.Backend.ModifySubnetGroup(name, desc, subnetIDs)
	if err != nil {
		if errors.Is(err, ErrSubnetGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheSubnetGroupNotFound", "Cache subnet group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName          xml.Name            `xml:"ModifyCacheSubnetGroupResponse"`
		Xmlns            string              `xml:"xmlns,attr"`
		CacheSubnetGroup cacheSubnetGroupXML `xml:"ModifyCacheSubnetGroupResult>CacheSubnetGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:            elasticacheNS,
		CacheSubnetGroup: subnetGroupToXML(sg),
	})
}

// snapshotXML is the XML representation of a cache snapshot.
type snapshotXML struct {
	ARN                string `xml:"ARN"`
	SnapshotName       string `xml:"SnapshotName"`
	CacheClusterID     string `xml:"CacheClusterId,omitempty"`
	ReplicationGroupID string `xml:"ReplicationGroupId,omitempty"`
	SnapshotStatus     string `xml:"SnapshotStatus"`
	Engine             string `xml:"Engine,omitempty"`
	EngineVersion      string `xml:"EngineVersion,omitempty"`
	CacheNodeType      string `xml:"CacheNodeType,omitempty"`
	SnapshotSource     string `xml:"SnapshotSource"`
	SnapshotCreateTime string `xml:"SnapshotCreateTime,omitempty"`
}

func snapshotToXML(snap *CacheSnapshot) snapshotXML {
	return snapshotXML{
		ARN:                snap.ARN,
		SnapshotName:       snap.SnapshotName,
		CacheClusterID:     snap.CacheClusterID,
		ReplicationGroupID: snap.ReplicationGroupID,
		SnapshotStatus:     snap.Status,
		Engine:             snap.Engine,
		EngineVersion:      snap.EngineVersion,
		CacheNodeType:      snap.NodeType,
		SnapshotSource:     snap.SnapshotSource,
		SnapshotCreateTime: snap.CreatedAt.UTC().Format(time.RFC3339),
	}
}

func (h *Handler) createSnapshot(c *echo.Context, form url.Values) error {
	snapshotName := form.Get("SnapshotName")
	clusterID := form.Get("CacheClusterId")
	replicationGroupID := form.Get("ReplicationGroupId")

	snap, err := h.Backend.CreateSnapshot(snapshotName, clusterID, replicationGroupID)
	if err != nil {
		if errors.Is(err, ErrInvalidSnapshotSource) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"InvalidParameterCombination",
				ErrInvalidSnapshotSource.Error(),
			)
		}
		if errors.Is(err, ErrSnapshotAlreadyExists) {
			return xmlError(c, http.StatusBadRequest, "SnapshotAlreadyExistsFault", "Snapshot already exists")
		}
		if errors.Is(err, ErrClusterNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheClusterNotFound", "Cache cluster not found")
		}
		if errors.Is(err, ErrReplicationGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "ReplicationGroupNotFound", "Replication group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName  xml.Name    `xml:"CreateSnapshotResponse"`
		Xmlns    string      `xml:"xmlns,attr"`
		Snapshot snapshotXML `xml:"CreateSnapshotResult>Snapshot"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:    elasticacheNS,
		Snapshot: snapshotToXML(snap),
	})
}

func (h *Handler) deleteSnapshot(c *echo.Context, form url.Values) error {
	snapshotName := form.Get("SnapshotName")

	snap, err := h.Backend.DeleteSnapshot(snapshotName)
	if err != nil {
		if errors.Is(err, ErrSnapshotNotFound) {
			return xmlError(c, http.StatusBadRequest, "SnapshotNotFoundFault", "Snapshot not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName  xml.Name    `xml:"DeleteSnapshotResponse"`
		Xmlns    string      `xml:"xmlns,attr"`
		Snapshot snapshotXML `xml:"DeleteSnapshotResult>Snapshot"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:    elasticacheNS,
		Snapshot: snapshotToXML(snap),
	})
}

func (h *Handler) describeSnapshots(c *echo.Context, form url.Values) error {
	snapshotName := form.Get("SnapshotName")
	clusterID := form.Get("CacheClusterId")
	replicationGroupID := form.Get("ReplicationGroupId")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeSnapshots(snapshotName, clusterID, replicationGroupID, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrSnapshotNotFound) {
			return xmlError(c, http.StatusBadRequest, "SnapshotNotFoundFault", "Snapshot not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type snapshots struct {
		Snapshot []snapshotXML `xml:"Snapshot"`
	}
	type result struct {
		XMLName   xml.Name  `xml:"DescribeSnapshotsResponse"`
		Xmlns     string    `xml:"xmlns,attr"`
		Marker    string    `xml:"DescribeSnapshotsResult>Marker,omitempty"`
		Snapshots snapshots `xml:"DescribeSnapshotsResult>Snapshots"`
	}

	items := make([]snapshotXML, 0, len(p.Data))
	for i := range p.Data {
		items = append(items, snapshotToXML(&p.Data[i]))
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:     elasticacheNS,
		Marker:    p.Next,
		Snapshots: snapshots{Snapshot: items},
	})
}

func (h *Handler) copySnapshot(c *echo.Context, form url.Values) error {
	sourceSnapshotName := form.Get("SourceSnapshotName")
	targetSnapshotName := form.Get("TargetSnapshotName")

	snap, err := h.Backend.CopySnapshot(sourceSnapshotName, targetSnapshotName)
	if err != nil {
		if errors.Is(err, ErrSnapshotNotFound) {
			return xmlError(c, http.StatusBadRequest, "SnapshotNotFoundFault", "Source snapshot not found")
		}
		if errors.Is(err, ErrSnapshotAlreadyExists) {
			return xmlError(c, http.StatusBadRequest, "SnapshotAlreadyExistsFault", "Target snapshot already exists")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName  xml.Name    `xml:"CopySnapshotResponse"`
		Xmlns    string      `xml:"xmlns,attr"`
		Snapshot snapshotXML `xml:"CopySnapshotResult>Snapshot"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:    elasticacheNS,
		Snapshot: snapshotToXML(snap),
	})
}

func (h *Handler) addTagsToResource(c *echo.Context, form url.Values) error {
	resourceARN := form.Get("ResourceName")

	newTags := make(map[string]string)
	for i := 1; ; i++ {
		key := form.Get(fmt.Sprintf("Tags.Tag.%d.Key", i))
		if key == "" {
			break
		}
		val := form.Get(fmt.Sprintf("Tags.Tag.%d.Value", i))
		newTags[key] = val
	}

	if err := h.Backend.AddTagsToResource(resourceARN, newTags); err != nil {
		if errors.Is(err, ErrResourceNotFound) {
			return xmlError(c, http.StatusBadRequest, "InvalidARN", err.Error())
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type tag struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type tagList struct {
		Tag []tag `xml:"Tag"`
	}
	type result struct {
		XMLName xml.Name `xml:"AddTagsToResourceResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
		TagList tagList  `xml:"AddTagsToResourceResult>TagList"`
	}

	items := make([]tag, 0, len(newTags))
	for k, v := range newTags {
		items = append(items, tag{Key: k, Value: v})
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:   elasticacheNS,
		TagList: tagList{Tag: items},
	})
}

func (h *Handler) removeTagsFromResource(c *echo.Context, form url.Values) error {
	resourceARN := form.Get("ResourceName")

	var tagKeys []string
	for i := 1; ; i++ {
		key := form.Get(fmt.Sprintf("TagKeys.member.%d", i))
		if key == "" {
			break
		}
		tagKeys = append(tagKeys, key)
	}

	if err := h.Backend.RemoveTagsFromResource(resourceARN, tagKeys); err != nil {
		if errors.Is(err, ErrResourceNotFound) {
			return xmlError(c, http.StatusBadRequest, "InvalidARN", err.Error())
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type tag struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type tagList struct {
		Tag []tag `xml:"Tag"`
	}
	type result struct {
		XMLName xml.Name `xml:"RemoveTagsFromResourceResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
		TagList tagList  `xml:"RemoveTagsFromResourceResult>TagList"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:   elasticacheNS,
		TagList: tagList{Tag: []tag{}},
	})
}

func (h *Handler) testFailoverReplicationGroup(c *echo.Context, form url.Values) error {
	id := form.Get("ReplicationGroupId")
	nodeGroupID := form.Get("NodeGroupId")

	rg, err := h.Backend.FailoverReplicationGroup(id, nodeGroupID)
	if err != nil {
		if errors.Is(err, ErrReplicationGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "ReplicationGroupNotFound", "Replication group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName          xml.Name            `xml:"TestFailoverResponse"`
		Xmlns            string              `xml:"xmlns,attr"`
		ReplicationGroup replicationGroupXML `xml:"TestFailoverResult>ReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:            elasticacheNS,
		ReplicationGroup: rgToXML(*rg),
	})
}

func (h *Handler) describeEvents(c *echo.Context, form url.Values) error {
	sourceIdentifier := form.Get("SourceIdentifier")
	sourceType := form.Get("SourceType")
	marker, maxRecords := parsePagination(form)

	var startTime, endTime time.Time

	if s := form.Get("StartTime"); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			startTime = t
		}
	}

	if s := form.Get("EndTime"); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			endTime = t
		}
	}

	duration := 0
	if s := form.Get("Duration"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			duration = n
		}
	}

	p, err := h.Backend.DescribeEvents(sourceIdentifier, sourceType, marker, startTime, endTime, duration, maxRecords)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type eventXML struct {
		Date             string `xml:"Date"`
		SourceIdentifier string `xml:"SourceIdentifier"`
		SourceType       string `xml:"SourceType"`
		Message          string `xml:"Message"`
	}
	type eventsList struct {
		Event []eventXML `xml:"Event"`
	}
	type result struct {
		XMLName xml.Name   `xml:"DescribeEventsResponse"`
		Xmlns   string     `xml:"xmlns,attr"`
		Marker  string     `xml:"DescribeEventsResult>Marker,omitempty"`
		Events  eventsList `xml:"DescribeEventsResult>Events"`
	}

	items := make([]eventXML, 0, len(p.Data))
	for _, e := range p.Data {
		items = append(items, eventXML{
			Date:             e.Date.UTC().Format(time.RFC3339),
			SourceIdentifier: e.SourceIdentifier,
			SourceType:       e.SourceType,
			Message:          e.Message,
		})
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:  elasticacheNS,
		Marker: p.Next,
		Events: eventsList{Event: items},
	})
}

// Reset clears all backend state.
func (h *Handler) Reset() {
	type resetter interface{ Reset() }
	if r, ok := h.Backend.(resetter); ok {
		r.Reset()
	}
}

func xmlResp(c *echo.Context, status int, v any) error {
	data, err := xml.Marshal(v)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	c.Response().Header().Set("Content-Type", "text/xml; charset=utf-8")
	c.Response().WriteHeader(status)
	_, _ = c.Response().Write([]byte(xml.Header))
	_, _ = c.Response().Write(data)

	return nil
}

// xmlErrorDetail holds the error code and message for an ElastiCache XML error.
type xmlErrorDetail struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

type xmlErrorResp struct {
	XMLName   xml.Name       `xml:"ErrorResponse"`
	Error     xmlErrorDetail `xml:"Error"`
	RequestID string         `xml:"RequestId"`
}

func xmlError(c *echo.Context, status int, code, message string) error {
	resp := xmlErrorResp{}
	resp.Error.Code = code
	resp.Error.Message = message
	resp.RequestID = ownerElasticacheStub

	return xmlResp(c, status, resp)
}
