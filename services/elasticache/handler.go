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
	CacheClusterID          string     `xml:"CacheClusterId"`
	CacheClusterStatus      string     `xml:"CacheClusterStatus"`
	CacheNodeType           string     `xml:"CacheNodeType"`
	Engine                  string     `xml:"Engine"`
	EngineVersion           string     `xml:"EngineVersion"`
	ARN                     string     `xml:"ARN"`
	CacheParameterGroupName string     `xml:"CacheParameterGroup>CacheParameterGroupName,omitempty"`
	CacheNodes              cacheNodes `xml:"CacheNodes"`
	NumCacheNodes           int        `xml:"NumCacheNodes"`
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
		"CreateReplicationGroup",
		"DeleteReplicationGroup",
		"DescribeReplicationGroups",
		"ModifyReplicationGroup",
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
		"CreateReplicationGroup":       h.createReplicationGroup,
		"DeleteReplicationGroup":       h.deleteReplicationGroup,
		"DescribeReplicationGroups":    h.describeReplicationGroups,
		"ModifyReplicationGroup":       h.modifyReplicationGroup,
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
			return c.String(http.StatusBadRequest, fmt.Sprintf("unknown action: %s", action))
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
	engine := form.Get("Engine")
	nodeType := form.Get("CacheNodeType")
	paramGroupName := form.Get("CacheParameterGroupName")

	var cluster *Cluster
	var err error

	if paramGroupName != "" {
		cluster, err = h.Backend.CreateClusterWithOptions(id, engine, nodeType, paramGroupName, 0)
	} else {
		cluster, err = h.Backend.CreateCluster(id, engine, nodeType, 0)
	}

	if err != nil {
		if errors.Is(err, ErrClusterAlreadyExists) {
			return xmlError(c, http.StatusBadRequest, "CacheClusterAlreadyExists", "Cache cluster already exists")
		}
		if errors.Is(err, ErrParameterGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheParameterGroupNotFound", "Cache parameter group not found")
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
	id := form.Get("ReplicationGroupId")
	desc := form.Get("ReplicationGroupDescription")
	paramGroupName := form.Get("CacheParameterGroupName")

	var rg *ReplicationGroup
	var err error

	if paramGroupName != "" {
		rg, err = h.Backend.CreateReplicationGroupWithOptions(id, desc, paramGroupName)
	} else {
		rg, err = h.Backend.CreateReplicationGroup(id, desc)
	}

	if err != nil {
		if errors.Is(err, ErrReplicationGroupAlreadyExists) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"ReplicationGroupAlreadyExists",
				"Replication group already exists",
			)
		}
		if errors.Is(err, ErrParameterGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheParameterGroupNotFound", "Cache parameter group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
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

// replicationGroupXML is the XML representation of a single replication group.
type replicationGroupXML struct {
	ReplicationGroupID      string `xml:"ReplicationGroupId"`
	Description             string `xml:"Description"`
	Status                  string `xml:"Status"`
	ARN                     string `xml:"ARN"`
	CacheParameterGroupName string `xml:"CacheParameterGroupName,omitempty"`
}

// rgToXML converts a ReplicationGroup to its XML representation.
func rgToXML(rg ReplicationGroup) replicationGroupXML {
	return replicationGroupXML{
		ReplicationGroupID:      rg.ReplicationGroupID,
		Description:             rg.Description,
		Status:                  rg.Status,
		ARN:                     rg.ARN,
		CacheParameterGroupName: rg.CacheParameterGroupName,
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
	return cacheClusterXML{
		CacheClusterID:          cl.ClusterID,
		CacheClusterStatus:      status,
		CacheNodeType:           cl.NodeType,
		Engine:                  cl.Engine,
		EngineVersion:           cl.EngineVersion,
		NumCacheNodes:           cl.NumCacheNodes,
		ARN:                     cl.ARN,
		CacheParameterGroupName: cl.CacheParameterGroupName,
		CacheNodes: cacheNodes{
			CacheNode: []cacheNode{{
				CacheNodeID:              "0001",
				CacheNodeStatus:          status,
				CacheNodeCreateTime:      time.Now().UTC().Format(time.RFC3339),
				CustomerAvailabilityZone: "us-east-1a",
				Endpoint: cacheEndpoint{
					Address: cl.Endpoint,
					Port:    cl.Port,
				},
			}},
		},
	}
}

func (h *Handler) modifyCacheCluster(c *echo.Context, form url.Values) error {
	id := form.Get("CacheClusterId")
	nodeType := form.Get("CacheNodeType")
	paramGroupName := form.Get("CacheParameterGroupName")

	cluster, err := h.Backend.ModifyCluster(id, nodeType, paramGroupName)
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
	desc := form.Get("ReplicationGroupDescription")
	paramGroupName := form.Get("CacheParameterGroupName")

	rg, err := h.Backend.ModifyReplicationGroup(id, desc, paramGroupName)
	if err != nil {
		if errors.Is(err, ErrReplicationGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "ReplicationGroupNotFound", "Replication group not found")
		}
		if errors.Is(err, ErrParameterGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheParameterGroupNotFound", "Cache parameter group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
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

	return xmlResp(c, http.StatusOK, result{Xmlns: elasticacheNS, RequestID: "elasticache-stub"})
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

	return xmlResp(c, http.StatusOK, result{Xmlns: elasticacheNS, RequestID: "elasticache-stub"})
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
	resp.RequestID = "elasticache-stub"

	return xmlResp(c, status, resp)
}
