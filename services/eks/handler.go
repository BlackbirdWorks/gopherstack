package eks

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	eksMatchPriority = service.PriorityPathVersioned

	pathClusters = "/clusters"
	pathEKSTags  = "/tags/"
)

// Handler is the Echo HTTP handler for AWS EKS operations (REST-JSON protocol).
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new EKS handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "EKS" }

// GetSupportedOperations returns the list of supported EKS operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateCluster",
		"DescribeCluster",
		"ListClusters",
		"DeleteCluster",
		"CreateNodegroup",
		"DescribeNodegroup",
		"ListNodegroups",
		"DeleteNodegroup",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "eks" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this EKS instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches AWS EKS REST requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return path == pathClusters ||
			strings.HasPrefix(path, pathClusters+"/") ||
			strings.HasPrefix(path, pathEKSTags+"arn:aws:eks:")
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return eksMatchPriority }

// eksRoute holds the parsed information from an EKS REST request path.
type eksRoute struct {
	clusterName   string
	nodegroupName string
	resourceARN   string
	operation     string
}

// parseEKSPath maps HTTP method + path to an operation name and resource identifiers.
//
//nolint:cyclop // path parsing requires many cases
func parseEKSPath(method, rawPath string) eksRoute {
	path, _ := url.PathUnescape(rawPath)

	// /tags/{resourceArn}
	if after, ok := strings.CutPrefix(path, pathEKSTags); ok {
		resourceARN := after

		switch method {
		case http.MethodPost:
			return eksRoute{operation: "TagResource", resourceARN: resourceARN}
		case http.MethodDelete:
			return eksRoute{operation: "UntagResource", resourceARN: resourceARN}
		case http.MethodGet:
			return eksRoute{operation: "ListTagsForResource", resourceARN: resourceARN}
		}

		return eksRoute{operation: "Unknown"}
	}

	// /clusters and /clusters/{name}/...
	if !strings.HasPrefix(path, pathClusters) {
		return eksRoute{operation: "Unknown"}
	}

	rest := strings.TrimPrefix(path, pathClusters)

	// /clusters
	if rest == "" {
		switch method {
		case http.MethodPost:
			return eksRoute{operation: "CreateCluster"}
		case http.MethodGet:
			return eksRoute{operation: "ListClusters"}
		}

		return eksRoute{operation: "Unknown"}
	}

	// /clusters/{name}[/...]
	rest = strings.TrimPrefix(rest, "/")

	const maxPathParts = 3

	parts := strings.SplitN(rest, "/", maxPathParts)
	clusterName := parts[0]

	// /clusters/{name}
	if len(parts) == 1 {
		switch method {
		case http.MethodGet:
			return eksRoute{operation: "DescribeCluster", clusterName: clusterName}
		case http.MethodDelete:
			return eksRoute{operation: "DeleteCluster", clusterName: clusterName}
		}

		return eksRoute{operation: "Unknown"}
	}

	// /clusters/{name}/node-groups[/{nodegroupName}]
	if parts[1] != "node-groups" {
		return eksRoute{operation: "Unknown"}
	}

	const nodeGroupPathParts = 2

	if len(parts) == nodeGroupPathParts {
		switch method {
		case http.MethodPost:
			return eksRoute{operation: "CreateNodegroup", clusterName: clusterName}
		case http.MethodGet:
			return eksRoute{operation: "ListNodegroups", clusterName: clusterName}
		}

		return eksRoute{operation: "Unknown"}
	}

	// /clusters/{name}/node-groups/{nodegroupName}
	nodegroupName := parts[2]
	switch method {
	case http.MethodGet:
		return eksRoute{operation: "DescribeNodegroup", clusterName: clusterName, nodegroupName: nodegroupName}
	case http.MethodDelete:
		return eksRoute{operation: "DeleteNodegroup", clusterName: clusterName, nodegroupName: nodegroupName}
	}

	return eksRoute{operation: "Unknown"}
}

// ExtractOperation extracts the EKS operation name from the REST path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := parseEKSPath(c.Request().Method, c.Request().URL.Path)

	return r.operation
}

// ExtractResource extracts the primary resource identifier from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := parseEKSPath(c.Request().Method, c.Request().URL.Path)
	if r.clusterName != "" {
		return r.clusterName
	}

	return r.resourceARN
}

// Handler returns the Echo handler function for EKS requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		log := logger.Load(c.Request().Context())
		route := parseEKSPath(c.Request().Method, c.Request().URL.Path)

		log.Debug("eks request", "operation", route.operation, "cluster", route.clusterName)

		var body []byte
		if c.Request().Body != nil {
			decoder := json.NewDecoder(c.Request().Body)
			var raw json.RawMessage
			if err := decoder.Decode(&raw); err == nil {
				body = raw
			}
		}

		return h.dispatch(c, route, body)
	}
}

func (h *Handler) dispatch(c *echo.Context, route eksRoute, body []byte) error {
	switch route.operation {
	case "CreateCluster":
		return h.handleCreateCluster(c, body)
	case "DescribeCluster":
		return h.handleDescribeCluster(c, route.clusterName)
	case "ListClusters":
		return h.handleListClusters(c)
	case "DeleteCluster":
		return h.handleDeleteCluster(c, route.clusterName)
	case "CreateNodegroup":
		return h.handleCreateNodegroup(c, route.clusterName, body)
	case "DescribeNodegroup":
		return h.handleDescribeNodegroup(c, route.clusterName, route.nodegroupName)
	case "ListNodegroups":
		return h.handleListNodegroups(c, route.clusterName)
	case "DeleteNodegroup":
		return h.handleDeleteNodegroup(c, route.clusterName, route.nodegroupName)
	case "TagResource":
		return h.handleTagResource(c, route.resourceARN, body)
	case "UntagResource":
		return h.handleUntagResource(c, route.resourceARN)
	case "ListTagsForResource":
		return h.handleListTagsForResource(c, route.resourceARN)
	default:
		return c.JSON(http.StatusNotFound, errResp("ResourceNotFoundException", "unknown operation: "+route.operation))
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errResp("ResourceNotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errResp("ResourceInUseException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errResp("InternalFailure", err.Error()))
	}
}

func errResp(code, msg string) map[string]string {
	return map[string]string{"code": code, "message": msg}
}

// clusterToJSON converts a Cluster to a JSON-serializable map.
func clusterToJSON(c *Cluster) map[string]any {
	m := map[string]any{
		"name":            c.Name,
		"arn":             c.ARN,
		"status":          c.Status,
		"version":         c.Version,
		"createdAt":       c.CreatedAt.Unix(),
		"platformVersion": c.PlatformVersion,
	}
	if c.Endpoint != "" {
		m["endpoint"] = c.Endpoint
	}
	if c.RoleARN != "" {
		m["roleArn"] = c.RoleARN
	}

	return m
}

// nodegroupToJSON converts a Nodegroup to a JSON-serializable map.
func nodegroupToJSON(ng *Nodegroup) map[string]any {
	m := map[string]any{
		"nodegroupName": ng.NodegroupName,
		"clusterName":   ng.ClusterName,
		"nodegroupArn":  ng.ARN,
		"status":        ng.Status,
		"createdAt":     ng.CreatedAt.Unix(),
		"scalingConfig": map[string]any{
			"desiredSize": ng.DesiredSize,
			"minSize":     ng.MinSize,
			"maxSize":     ng.MaxSize,
		},
	}
	if ng.AMIType != "" {
		m["amiType"] = ng.AMIType
	}
	if ng.CapacityType != "" {
		m["capacityType"] = ng.CapacityType
	}
	if len(ng.InstanceTypes) > 0 {
		m["instanceTypes"] = ng.InstanceTypes
	}
	if ng.NodeRole != "" {
		m["nodeRole"] = ng.NodeRole
	}
	if ng.Version != "" {
		m["version"] = ng.Version
	}

	return m
}

// --- Cluster handlers ---

type createClusterBody struct {
	Tags    map[string]string `json:"tags"`
	Name    string            `json:"name"`
	Version string            `json:"version"`
	RoleArn string            `json:"roleArn"`
}

func (h *Handler) handleCreateCluster(c *echo.Context, body []byte) error {
	var in createClusterBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "name is required"))
	}

	cluster, err := h.Backend.CreateCluster(in.Name, in.Version, in.RoleArn, in.Tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"cluster": clusterToJSON(cluster),
	})
}

func (h *Handler) handleDescribeCluster(c *echo.Context, name string) error {
	cluster, err := h.Backend.DescribeCluster(name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"cluster": clusterToJSON(cluster),
	})
}

func (h *Handler) handleListClusters(c *echo.Context) error {
	names := h.Backend.ListClusters()

	return c.JSON(http.StatusOK, map[string]any{
		"clusters": names,
	})
}

func (h *Handler) handleDeleteCluster(c *echo.Context, name string) error {
	cluster, err := h.Backend.DeleteCluster(name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"cluster": clusterToJSON(cluster),
	})
}

// --- Nodegroup handlers ---

type scalingConfigJSON struct {
	DesiredSize int32 `json:"desiredSize"`
	MinSize     int32 `json:"minSize"`
	MaxSize     int32 `json:"maxSize"`
}

type createNodegroupBody struct {
	Tags          map[string]string `json:"tags"`
	NodegroupName string            `json:"nodegroupName"`
	NodeRole      string            `json:"nodeRole"`
	AMIType       string            `json:"amiType"`
	CapacityType  string            `json:"capacityType"`
	Version       string            `json:"version"`
	InstanceTypes []string          `json:"instanceTypes"`
	ScalingConfig scalingConfigJSON `json:"scalingConfig"`
}

func (h *Handler) handleCreateNodegroup(c *echo.Context, clusterName string, body []byte) error {
	var in createNodegroupBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.NodegroupName == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "nodegroupName is required"))
	}

	ng, err := h.Backend.CreateNodegroup(
		clusterName, in.NodegroupName, in.NodeRole,
		in.AMIType, in.CapacityType, in.Version,
		in.InstanceTypes,
		in.ScalingConfig.DesiredSize, in.ScalingConfig.MinSize, in.ScalingConfig.MaxSize,
		in.Tags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"nodegroup": nodegroupToJSON(ng),
	})
}

func (h *Handler) handleDescribeNodegroup(c *echo.Context, clusterName, nodegroupName string) error {
	ng, err := h.Backend.DescribeNodegroup(clusterName, nodegroupName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"nodegroup": nodegroupToJSON(ng),
	})
}

func (h *Handler) handleListNodegroups(c *echo.Context, clusterName string) error {
	names, err := h.Backend.ListNodegroups(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"nodegroups": names,
	})
}

func (h *Handler) handleDeleteNodegroup(c *echo.Context, clusterName, nodegroupName string) error {
	ng, err := h.Backend.DeleteNodegroup(clusterName, nodegroupName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"nodegroup": nodegroupToJSON(ng),
	})
}

// --- Tag handlers ---

type tagResourceBody struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string, body []byte) error {
	var in tagResourceBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.Tags == nil {
		in.Tags = make(map[string]string)
	}

	if err := h.Backend.TagResource(resourceARN, in.Tags); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string) error {
	// The EKS UntagResource API uses query parameters "tagKeys" to specify which tags to remove.
	// For simplicity in our stub, we accept the call and return success.
	_ = resourceARN

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	t, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"tags": t,
	})
}
