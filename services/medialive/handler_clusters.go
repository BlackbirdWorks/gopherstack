package medialive

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// --- Cluster path classification ---

// classifyClusterPath classifies paths under /prod/clusters.
// resource is one of:
//   - clusterID (single cluster ops)
//   - clusterID (for node-list/create ops, nodeID is embedded in resource)
//   - "clusterID/nodeID" (compound, for node CRUD)
func classifyClusterPath(method, path string) (string, string, bool) {
	if path == pathClusters {
		return classifyClusterRoot(method)
	}

	after, ok := strings.CutPrefix(path, pathClusters+"/")
	if !ok {
		return "", "", false
	}

	// Split into at most 4 parts: clusterId/subpath/nodeId/state
	parts := strings.SplitN(after, "/", pathSegmentsDeepSub)

	clusterID := parts[0]
	if clusterID == "" {
		return "", "", false
	}

	switch len(parts) {
	case pathSegmentsID:
		return classifyClusterIDOnly(method, clusterID)
	case pathSegmentsSub:
		return classifyClusterSubpath(method, clusterID, parts[1])
	case pathSegmentsNamed:
		return classifyClusterNodePath(method, clusterID, parts[1], parts[2])
	case pathSegmentsDeepSub:
		return classifyClusterNodeStatePath(method, clusterID, parts[1], parts[2], parts[3])
	}

	return "", "", false
}

func classifyClusterRoot(method string) (string, string, bool) {
	switch method {
	case http.MethodGet:
		return opListClusters, "", true
	case http.MethodPost:
		return opCreateCluster, "", true
	}

	return "", "", false
}

func classifyClusterIDOnly(method, clusterID string) (string, string, bool) {
	switch method {
	case http.MethodGet:
		return opDescribeCluster, clusterID, true
	case http.MethodPut:
		return opUpdateCluster, clusterID, true
	case http.MethodDelete:
		return opDeleteCluster, clusterID, true
	}

	return "", "", false
}

func classifyClusterSubpath(method, clusterID, sub string) (string, string, bool) {
	switch {
	case sub == subAlerts && method == http.MethodGet:
		return opListClusterAlerts, clusterID, true
	case sub == subNodeRegistrationScript && method == http.MethodPost:
		return opCreateNodeRegistrationScript, clusterID, true
	case sub == subNodes && method == http.MethodGet:
		return opListNodes, clusterID, true
	case sub == subNodes && method == http.MethodPost:
		return opCreateNode, clusterID, true
	case sub == subChannelPlacementGroups && method == http.MethodGet:
		return opListChannelPlacementGroups, clusterID, true
	case sub == subChannelPlacementGroups && method == http.MethodPost:
		return opCreateChannelPlacementGroup, clusterID, true
	}

	return "", "", false
}

// classifyClusterNodePath handles /prod/clusters/{id}/nodes/{nodeId}.
// resource is compound "clusterID/nodeID".
func classifyClusterNodePath(method, clusterID, sub, nodeID string) (string, string, bool) {
	if nodeID == "" {
		return "", "", false
	}

	compound := clusterID + "/" + nodeID

	if sub == subChannelPlacementGroups {
		switch method {
		case http.MethodGet:
			return opDescribeChannelPlacementGroup, compound, true
		case http.MethodPut:
			return opUpdateChannelPlacementGroup, compound, true
		case http.MethodDelete:
			return opDeleteChannelPlacementGroup, compound, true
		}

		return "", "", false
	}

	if sub != subNodes {
		return "", "", false
	}

	switch method {
	case http.MethodGet:
		return opDescribeNode, compound, true
	case http.MethodPut:
		return opUpdateNode, compound, true
	case http.MethodDelete:
		return opDeleteNode, compound, true
	}

	return "", "", false
}

// classifyClusterNodeStatePath handles /prod/clusters/{id}/nodes/{nodeId}/state.
func classifyClusterNodeStatePath(
	method, clusterID, sub, nodeID, extra string,
) (string, string, bool) {
	if sub != subNodes || nodeID == "" || extra != subState {
		return "", "", false
	}

	if method != http.MethodPut {
		return "", "", false
	}

	compound := clusterID + "/" + nodeID

	return opUpdateNodeState, compound, true
}

// splitClusterNode splits the compound resource "clusterID/nodeID".
func splitClusterNode(resource string) (string, string) {
	before, after, _ := strings.Cut(resource, "/")

	return before, after
}

// --- Cluster handlers ---

// interfaceMappingOutput mirrors types.InterfaceMapping's wire shape
// (lowerCamel "logicalInterfaceName"/"networkId").
type interfaceMappingOutput struct {
	LogicalInterfaceName string `json:"logicalInterfaceName"`
	NetworkID            string `json:"networkId"`
}

// clusterNetworkSettingsOutput mirrors types.ClusterNetworkSettings.
type clusterNetworkSettingsOutput struct {
	DefaultRoute      string                   `json:"defaultRoute,omitempty"`
	InterfaceMappings []interfaceMappingOutput `json:"interfaceMappings,omitempty"`
}

func toClusterNetworkSettingsOutput(ns ClusterNetworkSettings) *clusterNetworkSettingsOutput {
	if !ns.hasNetworkSettings() {
		return nil
	}

	mappings := make([]interfaceMappingOutput, 0, len(ns.InterfaceMappings))
	for _, m := range ns.InterfaceMappings {
		mappings = append(mappings, interfaceMappingOutput(m))
	}

	return &clusterNetworkSettingsOutput{
		DefaultRoute:      ns.DefaultRoute,
		InterfaceMappings: mappings,
	}
}

// clusterOutput mirrors DescribeClusterOutput/CreateClusterOutput/
// UpdateClusterOutput exactly. The real API has NO "tags" field on this
// shape (verified against aws-sdk-go-v2/service/medialive@v1.101.4's
// awsRestjson1_deserializeOpDocumentDescribeClusterOutput) even though
// CreateClusterInput accepts tags -- tags for a Cluster only surface via
// ListTagsForResource. "channelIds" is derived live from Channel.
// AnywhereSettings.ClusterID (see channelIDsForCluster); "networkSettings"
// is omitted entirely (nil, matching a real never-configured Cluster) until
// the caller sets one via Create/UpdateCluster.
type clusterOutput struct {
	NetworkSettings *clusterNetworkSettingsOutput `json:"networkSettings,omitempty"`
	Arn             string                        `json:"arn"`
	ID              string                        `json:"id"`
	Name            string                        `json:"name"`
	ClusterType     string                        `json:"clusterType"`
	InstanceRoleArn string                        `json:"instanceRoleArn"`
	State           string                        `json:"state"`
	ChannelIDs      []string                      `json:"channelIds"`
}

func toClusterOutput(c *Cluster) clusterOutput {
	channelIDs := c.ChannelIDs
	if channelIDs == nil {
		channelIDs = []string{}
	}

	return clusterOutput{
		Arn:             c.ARN,
		ID:              c.ID,
		Name:            c.Name,
		ClusterType:     c.ClusterType,
		InstanceRoleArn: c.InstanceRoleArn,
		State:           c.State,
		ChannelIDs:      channelIDs,
		NetworkSettings: toClusterNetworkSettingsOutput(c.NetworkSettings),
	}
}

// extractInterfaceMappings parses a raw "interfaceMappings" array
// (lowerCamel "logicalInterfaceName"/"networkId" per item -- verified
// against types.InterfaceMapping's serializer).
func extractInterfaceMappings(raw []any) []InterfaceMapping {
	mappings := make([]InterfaceMapping, 0, len(raw))

	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		name, _ := m["logicalInterfaceName"].(string)
		networkID, _ := m["networkId"].(string)
		mappings = append(mappings, InterfaceMapping{
			LogicalInterfaceName: name,
			NetworkID:            networkID,
		})
	}

	return mappings
}

// extractClusterNetworkSettings parses the "networkSettings" request-body
// object shared by CreateClusterInput/UpdateClusterInput. The second return
// reports whether the key was present at all, so UpdateCluster can
// distinguish "omitted" (leave unchanged) from an explicit empty object.
func extractClusterNetworkSettings(body map[string]any) (ClusterNetworkSettings, bool) {
	raw, ok := body["networkSettings"].(map[string]any)
	if !ok {
		return ClusterNetworkSettings{}, false
	}

	defaultRoute, _ := raw["defaultRoute"].(string)

	var mappings []InterfaceMapping
	if rawMappings, hasMappings := raw["interfaceMappings"].([]any); hasMappings {
		mappings = extractInterfaceMappings(rawMappings)
	}

	return ClusterNetworkSettings{DefaultRoute: defaultRoute, InterfaceMappings: mappings}, true
}

func (h *Handler) handleCreateCluster(c *echo.Context, body map[string]any) error {
	name, _ := body["name"].(string)
	clusterType, _ := body["clusterType"].(string)
	instanceRoleArn, _ := body["instanceRoleArn"].(string)
	networkSettings, _ := extractClusterNetworkSettings(body)
	tags := extractTags(body)

	cl, err := h.Backend.CreateCluster(name, clusterType, instanceRoleArn, networkSettings, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, toClusterOutput(cl))
}

func (h *Handler) handleDescribeCluster(c *echo.Context, clusterID string) error {
	cl, err := h.Backend.DescribeCluster(clusterID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toClusterOutput(cl))
}

func (h *Handler) handleUpdateCluster(
	c *echo.Context,
	clusterID string,
	body map[string]any,
) error {
	name, _ := body["name"].(string)
	networkSettings, hasNetworkSettings := extractClusterNetworkSettings(body)

	cl, err := h.Backend.UpdateCluster(clusterID, name, networkSettings, hasNetworkSettings)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toClusterOutput(cl))
}

func (h *Handler) handleDeleteCluster(c *echo.Context, clusterID string) error {
	cl, err := h.Backend.DeleteCluster(clusterID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toClusterOutput(cl))
}

func (h *Handler) handleListClusters(c *echo.Context) error {
	summaries, nextToken, err := h.Backend.ListClusters(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		channelIDs := s.ChannelIDs
		if channelIDs == nil {
			channelIDs = []string{}
		}

		item := map[string]any{
			keyArn:            s.ARN,
			keyID:             s.ID,
			keyName:           s.Name,
			keyState:          s.State,
			"clusterType":     s.ClusterType,
			"instanceRoleArn": s.InstanceRoleArn,
			"channelIds":      channelIDs,
		}
		if ns := toClusterNetworkSettingsOutput(s.NetworkSettings); ns != nil {
			item["networkSettings"] = ns
		}

		out = append(out, item)
	}

	resp := map[string]any{"clusters": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListClusterAlerts(c *echo.Context, clusterID string) error {
	alerts, nextToken, err := h.Backend.ListClusterAlerts(clusterID, 0, "")
	if err != nil {
		return respondErr(c, err)
	}

	resp := map[string]any{keyAlerts: alerts}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleCreateNodeRegistrationScript(c *echo.Context, clusterID string) error {
	script, err := h.Backend.CreateNodeRegistrationScript(clusterID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{"nodeRegistrationScript": script})
}
