package eks

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	eksMatchPriority = service.PriorityPathVersioned

	pathClusters     = "/clusters"
	pathEKSTags      = "/tags/"
	pathCapabilities = "/capabilities"
	pathSubscriptions = "/subscriptions"
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
		"UpdateNodegroupConfig",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
		"AssociateAccessPolicy",
		"AssociateEncryptionConfig",
		"AssociateIdentityProviderConfig",
		"CreateAccessEntry",
		"CreateAddon",
		"CreateCapability",
		"CreateEksAnywhereSubscription",
		"CreateFargateProfile",
		"CreatePodIdentityAssociation",
		"DeleteAccessEntry",
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
			strings.HasPrefix(path, pathEKSTags+"arn:aws:eks:") ||
			path == pathCapabilities ||
			path == pathSubscriptions
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return eksMatchPriority }

// eksRoute holds the parsed information from an EKS REST request path.
type eksRoute struct {
	clusterName   string
	nodegroupName string
	principalARN  string
	resourceARN   string
	operation     string
}

// parseNodegroupRoute returns the route for /clusters/{name}/node-groups[/{ng}] paths.
func parseNodegroupRoute(method, clusterName string, parts []string) eksRoute {
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

	nodegroupName := parts[2]

	switch method {
	case http.MethodGet:
		return eksRoute{operation: "DescribeNodegroup", clusterName: clusterName, nodegroupName: nodegroupName}
	case http.MethodDelete:
		return eksRoute{operation: "DeleteNodegroup", clusterName: clusterName, nodegroupName: nodegroupName}
	case http.MethodPost:
		return eksRoute{operation: "UpdateNodegroupConfig", clusterName: clusterName, nodegroupName: nodegroupName}
	}

	return eksRoute{operation: "Unknown"}
}

// parseAccessEntryRoute returns the route for /clusters/{name}/access-entries[/{principalArn}[/access-policies]] paths.
func parseAccessEntryRoute(method, clusterName string, parts []string) eksRoute {
	const accessEntryParts = 2

	if len(parts) == accessEntryParts {
		if method == http.MethodPost {
			return eksRoute{operation: "CreateAccessEntry", clusterName: clusterName}
		}

		return eksRoute{operation: "Unknown"}
	}

	// parts[2] may be principalArn or principalArn/access-policies
	tail := parts[2]

	if strings.HasSuffix(tail, "/access-policies") {
		principalARN := strings.TrimSuffix(tail, "/access-policies")
		if method == http.MethodPost {
			return eksRoute{operation: "AssociateAccessPolicy", clusterName: clusterName, principalARN: principalARN}
		}

		return eksRoute{operation: "Unknown"}
	}

	// plain principalArn
	switch method {
	case http.MethodDelete:
		return eksRoute{operation: "DeleteAccessEntry", clusterName: clusterName, principalARN: tail}
	}

	return eksRoute{operation: "Unknown"}
}

// parseAddonRoute returns the route for /clusters/{name}/addons[/{addonName}] paths.
func parseAddonRoute(method, clusterName string, parts []string) eksRoute {
	const addonParts = 2

	if len(parts) == addonParts {
		if method == http.MethodPost {
			return eksRoute{operation: "CreateAddon", clusterName: clusterName}
		}

		return eksRoute{operation: "Unknown"}
	}

	return eksRoute{operation: "Unknown"}
}

// parseFargateProfileRoute returns the route for /clusters/{name}/fargate-profiles[/{profileName}] paths.
func parseFargateProfileRoute(method, clusterName string, parts []string) eksRoute {
	const fargateProfileParts = 2

	if len(parts) == fargateProfileParts {
		if method == http.MethodPost {
			return eksRoute{operation: "CreateFargateProfile", clusterName: clusterName}
		}

		return eksRoute{operation: "Unknown"}
	}

	return eksRoute{operation: "Unknown"}
}

// parseEKSPath maps HTTP method + path to an operation name and resource identifiers.
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

	// /capabilities
	if path == pathCapabilities {
		if method == http.MethodPost {
			return eksRoute{operation: "CreateCapability"}
		}

		return eksRoute{operation: "Unknown"}
	}

	// /subscriptions
	if path == pathSubscriptions {
		if method == http.MethodPost {
			return eksRoute{operation: "CreateEksAnywhereSubscription"}
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
	if parts[1] == "node-groups" {
		return parseNodegroupRoute(method, clusterName, parts)
	}

	// /clusters/{name}/access-entries[/{principalArn}[/access-policies]]
	if parts[1] == "access-entries" {
		return parseAccessEntryRoute(method, clusterName, parts)
	}

	// /clusters/{name}/encryption-config/associate
	if parts[1] == "encryption-config" && len(parts) == maxPathParts && parts[2] == "associate" {
		if method == http.MethodPost {
			return eksRoute{operation: "AssociateEncryptionConfig", clusterName: clusterName}
		}

		return eksRoute{operation: "Unknown"}
	}

	// /clusters/{name}/identity-provider-configs/associate
	if parts[1] == "identity-provider-configs" && len(parts) == maxPathParts && parts[2] == "associate" {
		if method == http.MethodPost {
			return eksRoute{operation: "AssociateIdentityProviderConfig", clusterName: clusterName}
		}

		return eksRoute{operation: "Unknown"}
	}

	// /clusters/{name}/addons[/{addonName}]
	if parts[1] == "addons" {
		return parseAddonRoute(method, clusterName, parts)
	}

	// /clusters/{name}/fargate-profiles[/{profileName}]
	if parts[1] == "fargate-profiles" {
		return parseFargateProfileRoute(method, clusterName, parts)
	}

	// /clusters/{name}/pod-identity-associations
	if parts[1] == "pod-identity-associations" {
		if method == http.MethodPost {
			return eksRoute{operation: "CreatePodIdentityAssociation", clusterName: clusterName}
		}

		return eksRoute{operation: "Unknown"}
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
	case "UpdateNodegroupConfig":
		return h.handleUpdateNodegroupConfig(c, route.clusterName, route.nodegroupName, body)
	case "TagResource":
		return h.handleTagResource(c, route.resourceARN, body)
	case "UntagResource":
		return h.handleUntagResource(c, route.resourceARN)
	case "ListTagsForResource":
		return h.handleListTagsForResource(c, route.resourceARN)
	case "CreateAccessEntry":
		return h.handleCreateAccessEntry(c, route.clusterName, body)
	case "DeleteAccessEntry":
		return h.handleDeleteAccessEntry(c, route.clusterName, route.principalARN)
	case "AssociateAccessPolicy":
		return h.handleAssociateAccessPolicy(c, route.clusterName, route.principalARN, body)
	case "AssociateEncryptionConfig":
		return h.handleAssociateEncryptionConfig(c, route.clusterName, body)
	case "AssociateIdentityProviderConfig":
		return h.handleAssociateIdentityProviderConfig(c, route.clusterName, body)
	case "CreateAddon":
		return h.handleCreateAddon(c, route.clusterName, body)
	case "CreateCapability":
		return h.handleCreateCapability(c, body)
	case "CreateEksAnywhereSubscription":
		return h.handleCreateEksAnywhereSubscription(c, body)
	case "CreateFargateProfile":
		return h.handleCreateFargateProfile(c, route.clusterName, body)
	case "CreatePodIdentityAssociation":
		return h.handleCreatePodIdentityAssociation(c, route.clusterName, body)
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
	tagKeys := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return h.handleError(c, err)
	}

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

type updateNodegroupConfigInput struct {
	ScalingConfig *struct {
		DesiredSize *int32 `json:"desiredSize,omitempty"`
		MinSize     *int32 `json:"minSize,omitempty"`
		MaxSize     *int32 `json:"maxSize,omitempty"`
	} `json:"scalingConfig,omitempty"`
}

func (h *Handler) handleUpdateNodegroupConfig(
	c *echo.Context,
	clusterName, nodegroupName string,
	body []byte,
) error {
	var in updateNodegroupConfigInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
		}
	}

	var desiredSize, minSize, maxSize *int32
	if in.ScalingConfig != nil {
		desiredSize = in.ScalingConfig.DesiredSize
		minSize = in.ScalingConfig.MinSize
		maxSize = in.ScalingConfig.MaxSize
	}

	ng, err := h.Backend.UpdateNodegroupConfig(clusterName, nodegroupName, desiredSize, minSize, maxSize)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"update": map[string]any{
			"id":            uuid.NewString()[:8],
			"status":        "InProgress",
			"type":          "ConfigUpdate",
			"createdAt":     float64(time.Now().Unix()),
			"clusterName":   clusterName,
			"nodegroupName": ng.NodegroupName,
		},
	})
}

// --- New operation handlers ---

type createAccessEntryBody struct {
	Tags         map[string]string `json:"tags"`
	PrincipalArn string            `json:"principalArn"`
	Type         string            `json:"type"`
	Username     string            `json:"username"`
}

func (h *Handler) handleCreateAccessEntry(c *echo.Context, clusterName string, body []byte) error {
	var in createAccessEntryBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.PrincipalArn == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "principalArn is required"))
	}

	entry, err := h.Backend.CreateAccessEntry(clusterName, in.PrincipalArn, in.Type, in.Username, in.Tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"accessEntry": map[string]any{
			"clusterName":    entry.ClusterName,
			"principalArn":   entry.PrincipalARN,
			"accessEntryArn": entry.ARN,
			"type":           entry.Type,
			"username":       entry.Username,
			"createdAt":      entry.CreatedAt.Unix(),
		},
	})
}

func (h *Handler) handleDeleteAccessEntry(c *echo.Context, clusterName, principalARN string) error {
	if err := h.Backend.DeleteAccessEntry(clusterName, principalARN); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

type associateAccessPolicyBody struct {
	AccessScope map[string]any `json:"accessScope"`
	PolicyArn   string         `json:"policyArn"`
}

func (h *Handler) handleAssociateAccessPolicy(c *echo.Context, clusterName, principalARN string, body []byte) error {
	var in associateAccessPolicyBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.PolicyArn == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "policyArn is required"))
	}

	assoc, err := h.Backend.AssociateAccessPolicy(clusterName, principalARN, in.PolicyArn, in.AccessScope)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"associatedAccessPolicy": map[string]any{
			"clusterName":  assoc.ClusterName,
			"principalArn": assoc.PrincipalARN,
			"policyArn":    assoc.PolicyARN,
			"associatedAt": assoc.AssociatedAt.Unix(),
			"accessScope":  assoc.AccessScope,
		},
	})
}

type encryptionConfigItem struct {
	Provider  map[string]string `json:"provider"`
	Resources []string          `json:"resources"`
}

type associateEncryptionConfigBody struct {
	EncryptionConfig []encryptionConfigItem `json:"encryptionConfig"`
}

func (h *Handler) handleAssociateEncryptionConfig(c *echo.Context, clusterName string, body []byte) error {
	var in associateEncryptionConfigBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	configs := make([]EncryptionConfig, len(in.EncryptionConfig))
	for i, ec := range in.EncryptionConfig {
		configs[i] = EncryptionConfig{Provider: ec.Provider, Resources: ec.Resources}
	}

	result, err := h.Backend.AssociateEncryptionConfig(clusterName, configs)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"update": map[string]any{
			"id":          uuid.NewString()[:8],
			"status":      "InProgress",
			"type":        "AssociateEncryptionConfig",
			"clusterName": clusterName,
			"params": map[string]any{
				"encryptionConfig": result,
			},
		},
	})
}

type oidcConfigJSON struct {
	ClientID                           string `json:"clientId"`
	GroupsClaim                        string `json:"groupsClaim,omitempty"`
	GroupsPrefix                       string `json:"groupsPrefix,omitempty"`
	IssuerURL                          string `json:"issuerUrl"`
	UsernameClaim                      string `json:"usernameClaim,omitempty"`
	UsernamePrefix                     string `json:"usernamePrefix,omitempty"`
}

type associateIdentityProviderConfigBody struct {
	Tags map[string]string  `json:"tags"`
	Oidc *oidcConfigJSON    `json:"oidc"`
}

func (h *Handler) handleAssociateIdentityProviderConfig(c *echo.Context, clusterName string, body []byte) error {
	var in associateIdentityProviderConfigBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.Oidc == nil || in.Oidc.IssuerURL == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "oidc.issuerUrl is required"))
	}

	if in.Oidc.ClientID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "oidc.clientId is required"))
	}

	params := map[string]string{
		"issuerUrl": in.Oidc.IssuerURL,
		"clientId":  in.Oidc.ClientID,
	}
	if in.Oidc.UsernameClaim != "" {
		params["usernameClaim"] = in.Oidc.UsernameClaim
	}
	if in.Oidc.GroupsClaim != "" {
		params["groupsClaim"] = in.Oidc.GroupsClaim
	}

	// Use clientId as the config name (unique per cluster).
	configName := in.Oidc.ClientID

	cfg, err := h.Backend.AssociateIdentityProviderConfig(clusterName, "oidc", configName, params, in.Tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"update": map[string]any{
			"id":          uuid.NewString()[:8],
			"status":      "InProgress",
			"type":        "AssociateIdentityProviderConfig",
			"clusterName": clusterName,
		},
		"tags": cfg.Tags.Clone(),
	})
}

type createAddonBody struct {
	Tags                  map[string]string `json:"tags"`
	AddonName             string            `json:"addonName"`
	AddonVersion          string            `json:"addonVersion"`
	ServiceAccountRoleArn string            `json:"serviceAccountRoleArn"`
}

func (h *Handler) handleCreateAddon(c *echo.Context, clusterName string, body []byte) error {
	var in createAddonBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.AddonName == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "addonName is required"))
	}

	addon, err := h.Backend.CreateAddon(clusterName, in.AddonName, in.AddonVersion, in.ServiceAccountRoleArn, in.Tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"addon": map[string]any{
			"clusterName":           addon.ClusterName,
			"addonName":             addon.AddonName,
			"addonArn":              addon.ARN,
			"addonVersion":          addon.AddonVersion,
			"status":                addon.Status,
			"serviceAccountRoleArn": addon.ServiceAccountRoleARN,
			"createdAt":             addon.CreatedAt.Unix(),
		},
	})
}

type createCapabilityBody struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

func (h *Handler) handleCreateCapability(c *echo.Context, body []byte) error {
	var in createCapabilityBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "name is required"))
	}

	cap, err := h.Backend.CreateCapability(in.Name, in.Version)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"capability": map[string]any{
			"name":    cap.Name,
			"version": cap.Version,
			"status":  cap.Status,
		},
	})
}

type createEksAnywhereSubscriptionBody struct {
	Tags            map[string]string `json:"tags"`
	Name            string            `json:"name"`
	LicenseType     string            `json:"licenseType"`
	LicenseQuantity int32             `json:"licenseQuantity"`
}

func (h *Handler) handleCreateEksAnywhereSubscription(c *echo.Context, body []byte) error {
	var in createEksAnywhereSubscriptionBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "name is required"))
	}

	sub, err := h.Backend.CreateEksAnywhereSubscription(in.Name, in.LicenseQuantity, in.LicenseType, in.Tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"subscription": map[string]any{
			"id":              sub.ID,
			"arn":             sub.ARN,
			"name":            sub.Name,
			"status":          sub.Status,
			"licenseType":     sub.LicenseType,
			"licenseQuantity": sub.LicenseQuantity,
			"createdAt":       sub.CreatedAt.Unix(),
		},
	})
}

type fargateProfileSelectorJSON struct {
	Namespace string            `json:"namespace"`
	Labels    map[string]string `json:"labels,omitempty"`
}

type createFargateProfileBody struct {
	Tags                map[string]string            `json:"tags"`
	FargateProfileName  string                       `json:"fargateProfileName"`
	PodExecutionRoleArn string                       `json:"podExecutionRoleArn"`
	Selectors           []fargateProfileSelectorJSON `json:"selectors"`
}

func (h *Handler) handleCreateFargateProfile(c *echo.Context, clusterName string, body []byte) error {
	var in createFargateProfileBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.FargateProfileName == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "fargateProfileName is required"))
	}

	selectors := make([]FargateProfileSelector, len(in.Selectors))
	for i, s := range in.Selectors {
		selectors[i] = FargateProfileSelector{Namespace: s.Namespace, Labels: s.Labels}
	}

	profile, err := h.Backend.CreateFargateProfile(clusterName, in.FargateProfileName, in.PodExecutionRoleArn, selectors, in.Tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"fargateProfile": map[string]any{
			"clusterName":         profile.ClusterName,
			"fargateProfileName":  profile.FargateProfileName,
			"fargateProfileArn":   profile.ARN,
			"podExecutionRoleArn": profile.PodExecutionRoleARN,
			"status":              profile.Status,
			"selectors":           profile.Selectors,
			"createdAt":           profile.CreatedAt.Unix(),
		},
	})
}

type createPodIdentityAssociationBody struct {
	Tags           map[string]string `json:"tags"`
	Namespace      string            `json:"namespace"`
	ServiceAccount string            `json:"serviceAccount"`
	RoleArn        string            `json:"roleArn"`
}

func (h *Handler) handleCreatePodIdentityAssociation(c *echo.Context, clusterName string, body []byte) error {
	var in createPodIdentityAssociationBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.Namespace == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "namespace is required"))
	}

	if in.ServiceAccount == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "serviceAccount is required"))
	}

	assoc, err := h.Backend.CreatePodIdentityAssociation(clusterName, in.Namespace, in.ServiceAccount, in.RoleArn, in.Tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"association": map[string]any{
			"clusterName":    assoc.ClusterName,
			"associationId":  assoc.AssociationID,
			"associationArn": assoc.ARN,
			"namespace":      assoc.Namespace,
			"serviceAccount": assoc.ServiceAccount,
			"roleArn":        assoc.RoleARN,
			"createdAt":      assoc.CreatedAt.Unix(),
		},
	})
}
