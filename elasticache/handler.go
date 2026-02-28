package elasticache

import (
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
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
	CacheClusterID     string     `xml:"CacheClusterId"`
	CacheClusterStatus string     `xml:"CacheClusterStatus"`
	CacheNodeType      string     `xml:"CacheNodeType"`
	Engine             string     `xml:"Engine"`
	EngineVersion      string     `xml:"EngineVersion"`
	ARN                string     `xml:"ARN"`
	CacheNodes         cacheNodes `xml:"CacheNodes"`
	NumCacheNodes      int        `xml:"NumCacheNodes"`
}

// Handler is the Echo HTTP handler for ElastiCache operations.
type Handler struct {
	Backend   StorageBackend
	Logger    *slog.Logger
	AccountID string
	Region    string
}

// NewHandler creates a new ElastiCache handler.
func NewHandler(backend StorageBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
}

// Name returns the service name.
func (h *Handler) Name() string { return "ElastiCache" }

// GetSupportedOperations returns all supported ElastiCache operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateCacheCluster",
		"DeleteCacheCluster",
		"DescribeCacheClusters",
		"ListTagsForResource",
		"CreateReplicationGroup",
		"DeleteReplicationGroup",
		"DescribeReplicationGroups",
	}
}

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
		body, err := httputil.ReadBody(r)
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
	body, err := httputil.ReadBody(c.Request())
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
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}
	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return ""
	}
	for _, key := range []string{"CacheClusterId", "ReplicationGroupId", "ResourceName"} {
		if v := vals.Get(key); v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function for ElastiCache requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			return c.String(http.StatusBadRequest, "cannot read body")
		}
		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return c.String(http.StatusBadRequest, "cannot parse form")
		}
		action := vals.Get("Action")
		switch action {
		case "CreateCacheCluster":
			return h.createCacheCluster(c, vals)
		case "DeleteCacheCluster":
			return h.deleteCacheCluster(c, vals)
		case "DescribeCacheClusters":
			return h.describeCacheClusters(c, vals)
		case "ListTagsForResource":
			return h.listTagsForResource(c, vals)
		case "CreateReplicationGroup":
			return h.createReplicationGroup(c, vals)
		case "DeleteReplicationGroup":
			return h.deleteReplicationGroup(c, vals)
		case "DescribeReplicationGroups":
			return h.describeReplicationGroups(c, vals)
		default:
			return c.String(http.StatusBadRequest, fmt.Sprintf("unknown action: %s", action))
		}
	}
}

func (h *Handler) createCacheCluster(c *echo.Context, form url.Values) error {
	id := form.Get("CacheClusterId")
	engine := form.Get("Engine")
	nodeType := form.Get("CacheNodeType")

	cluster, err := h.Backend.CreateCluster(id, engine, nodeType, 0)
	if err != nil {
		if errors.Is(err, ErrClusterAlreadyExists) {
			return xmlError(c, http.StatusBadRequest, "CacheClusterAlreadyExists", "Cache cluster already exists")
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
	clusters, descErr := h.Backend.DescribeClusters(id)
	if descErr != nil {
		if errors.Is(descErr, ErrClusterNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheClusterNotFound", "Cache cluster not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", descErr.Error())
	}
	cl := clusters[0]
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
	clusters, err := h.Backend.DescribeClusters(id)
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
		CacheClusters cacheClusters `xml:"DescribeCacheClustersResult>CacheClusters"`
	}

	items := make([]cacheClusterXML, 0, len(clusters))
	for i := range clusters {
		items = append(items, clusterToXML(&clusters[i], clusters[i].Status))
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:         elasticacheNS,
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
	rg, err := h.Backend.CreateReplicationGroup(id, desc)
	if err != nil {
		if errors.Is(err, ErrReplicationGroupAlreadyExists) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"ReplicationGroupAlreadyExists",
				"Replication group already exists",
			)
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
		XMLName          xml.Name         `xml:"CreateReplicationGroupResponse"`
		Xmlns            string           `xml:"xmlns,attr"`
		ReplicationGroup replicationGroup `xml:"CreateReplicationGroupResult>ReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns: elasticacheNS,
		ReplicationGroup: replicationGroup{
			ReplicationGroupID: rg.ReplicationGroupID,
			Description:        rg.Description,
			Status:             rg.Status,
			ARN:                rg.ARN,
		},
	})
}

func (h *Handler) deleteReplicationGroup(c *echo.Context, form url.Values) error {
	id := form.Get("ReplicationGroupId")
	rgs, descErr := h.Backend.DescribeReplicationGroups(id)
	if descErr != nil {
		if errors.Is(descErr, ErrReplicationGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "ReplicationGroupNotFound", "Replication group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", descErr.Error())
	}
	rg := rgs[0]
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

func (h *Handler) describeReplicationGroups(c *echo.Context, form url.Values) error {
	id := form.Get("ReplicationGroupId")
	rgs, err := h.Backend.DescribeReplicationGroups(id)
	if err != nil {
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
	type replicationGroups struct {
		ReplicationGroup []replicationGroup `xml:"ReplicationGroup"`
	}
	type result struct {
		XMLName           xml.Name          `xml:"DescribeReplicationGroupsResponse"`
		Xmlns             string            `xml:"xmlns,attr"`
		ReplicationGroups replicationGroups `xml:"DescribeReplicationGroupsResult>ReplicationGroups"`
	}

	items := make([]replicationGroup, 0, len(rgs))
	for _, rg := range rgs {
		items = append(items, replicationGroup{
			ReplicationGroupID: rg.ReplicationGroupID,
			Description:        rg.Description,
			Status:             rg.Status,
			ARN:                rg.ARN,
		})
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:             elasticacheNS,
		ReplicationGroups: replicationGroups{ReplicationGroup: items},
	})
}

// clusterToXML converts a Cluster to its XML representation with the given status.
func clusterToXML(cl *Cluster, status string) cacheClusterXML {
	return cacheClusterXML{
		CacheClusterID:     cl.ClusterID,
		CacheClusterStatus: status,
		CacheNodeType:      cl.NodeType,
		Engine:             cl.Engine,
		EngineVersion:      cl.EngineVersion,
		NumCacheNodes:      cl.NumCacheNodes,
		ARN:                cl.ARN,
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

type xmlErrorResp struct {
	XMLName xml.Name `xml:"ErrorResponse"`
	Error   struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	} `xml:"Error"`
	RequestID string `xml:"RequestId"`
}

func xmlError(c *echo.Context, status int, code, message string) error {
	resp := xmlErrorResp{}
	resp.Error.Code = code
	resp.Error.Message = message
	resp.RequestID = "elasticache-stub"

	return xmlResp(c, status, resp)
}
