package eks

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// dispatchNodegroupOps handles nodegroup CRUD and version/config update operations.
func (h *Handler) dispatchNodegroupOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opCreateNodegroup:
		return true, h.handleCreateNodegroup(c, route.clusterName, body)
	case opDescribeNodegroup:
		return true, h.handleDescribeNodegroup(c, route.clusterName, route.nodegroupName)
	case opListNodegroups:
		return true, h.handleListNodegroups(c, route.clusterName)
	case opDeleteNodegroup:
		return true, h.handleDeleteNodegroup(c, route.clusterName, route.nodegroupName)
	case opUpdateNodegroupConfig:
		return true, h.handleUpdateNodegroupConfig(c, route.clusterName, route.nodegroupName, body)
	case opUpdateNodegroupVersion:
		return true, h.handleUpdateNodegroupVersion(c, route.clusterName, route.nodegroupName, body)
	}

	return false, nil
}

// parseNodegroupRoute returns the route for /clusters/{name}/node-groups[/{ng}[/update-version]] paths.
func parseNodegroupRoute(method, clusterName string, parts []string) eksRoute {
	const nodeGroupPathParts = 2

	if len(parts) == nodeGroupPathParts {
		switch method {
		case http.MethodPost:
			return eksRoute{operation: opCreateNodegroup, clusterName: clusterName}
		case http.MethodGet:
			return eksRoute{operation: opListNodegroups, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	tail := parts[2]

	if before, ok := strings.CutSuffix(tail, "/update-version"); ok {
		if method == http.MethodPost {
			return eksRoute{operation: opUpdateNodegroupVersion, clusterName: clusterName, nodegroupName: before}
		}

		return eksRoute{operation: opUnknown}
	}

	// Real path is /clusters/{clusterName}/node-groups/{nodegroupName}/update-config
	// (POST), NOT a bare-path PUT -- verified against the SDK serializer.
	if before, ok := strings.CutSuffix(tail, "/update-config"); ok {
		if method == http.MethodPost {
			return eksRoute{operation: opUpdateNodegroupConfig, clusterName: clusterName, nodegroupName: before}
		}

		return eksRoute{operation: opUnknown}
	}

	switch method {
	case http.MethodGet:
		return eksRoute{operation: opDescribeNodegroup, clusterName: clusterName, nodegroupName: tail}
	case http.MethodDelete:
		return eksRoute{operation: opDeleteNodegroup, clusterName: clusterName, nodegroupName: tail}
	}

	return eksRoute{operation: opUnknown}
}

// nodegroupToJSON converts a Nodegroup to a JSON-serializable map.
func nodegroupToJSON(ng *Nodegroup) map[string]any {
	m := map[string]any{
		"nodegroupName": ng.NodegroupName,
		keyClusterName:  ng.ClusterName,
		"nodegroupArn":  ng.ARN,
		keyStatusField:  ng.Status,
		keyCreatedAt:    ng.CreatedAt.Unix(),
		"scalingConfig": map[string]any{
			"desiredSize": ng.DesiredSize,
			"minSize":     ng.MinSize,
			"maxSize":     ng.MaxSize,
		},
		keyHealth: map[string]any{"issues": []any{}},
	}
	appendNodegroupCoreFields(ng, m)
	appendNodegroupOptionalFields(ng, m)

	return m
}

func appendNodegroupCoreFields(ng *Nodegroup, m map[string]any) {
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
		m[keyVersion] = ng.Version
	}
	if ng.ReleaseVersion != "" {
		m["releaseVersion"] = ng.ReleaseVersion
	}
}

func appendNodegroupOptionalFields(ng *Nodegroup, m map[string]any) {
	if len(ng.Subnets) > 0 {
		m["subnets"] = ng.Subnets
	}
	if len(ng.Labels) > 0 {
		m["labels"] = ng.Labels
	}
	if len(ng.Taints) > 0 {
		m["taints"] = ng.Taints
	}
	if ng.DiskSize > 0 {
		m["diskSize"] = ng.DiskSize
	}
	if ng.RemoteAccess != nil {
		m["remoteAccess"] = remoteAccessToJSON(ng.RemoteAccess)
	}
	if ng.LaunchTemplate != nil {
		m["launchTemplate"] = launchTemplateToJSON(ng.LaunchTemplate)
	}
	if ng.Resources != nil && len(ng.Resources.AutoScalingGroups) > 0 {
		m["resources"] = nodegroupResourcesToJSON(ng.Resources)
	}
	if ng.UpdateConfig != nil {
		uc := map[string]any{}
		if ng.UpdateConfig.MaxUnavailable != nil {
			uc["maxUnavailable"] = *ng.UpdateConfig.MaxUnavailable
		}

		if ng.UpdateConfig.MaxUnavailablePercentage != nil {
			uc["maxUnavailablePercentage"] = *ng.UpdateConfig.MaxUnavailablePercentage
		}

		m["updateConfig"] = uc
	}
	if ng.Tags != nil {
		m[keyTags] = ng.Tags.Clone()
	} else {
		m[keyTags] = map[string]string{}
	}
}

func remoteAccessToJSON(ra *RemoteAccess) map[string]any {
	m := map[string]any{}
	if ra.EC2SSHKey != "" {
		m["ec2SshKey"] = ra.EC2SSHKey
	}
	if len(ra.SourceSecurityGroups) > 0 {
		m["sourceSecurityGroups"] = ra.SourceSecurityGroups
	}

	return m
}

func launchTemplateToJSON(lt *LaunchTemplate) map[string]any {
	m := map[string]any{}
	if lt.ID != "" {
		m["id"] = lt.ID
	}
	if lt.Name != "" {
		m["name"] = lt.Name
	}
	if lt.Version != "" {
		m["version"] = lt.Version
	}

	return m
}

func nodegroupResourcesToJSON(res *NodegroupResources) map[string]any {
	asgs := make([]map[string]any, len(res.AutoScalingGroups))
	for i, asg := range res.AutoScalingGroups {
		asgs[i] = map[string]any{"name": asg.Name}
	}

	return map[string]any{"autoScalingGroups": asgs}
}

type scalingConfigJSON struct {
	DesiredSize int32 `json:"desiredSize"`
	MinSize     int32 `json:"minSize"`
	MaxSize     int32 `json:"maxSize"`
}

type nodegroupTaintJSON struct {
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Effect string `json:"effect"`
}

type remoteAccessJSON struct {
	EC2SSHKey            string   `json:"ec2SshKey,omitempty"`
	SourceSecurityGroups []string `json:"sourceSecurityGroups,omitempty"`
}

type launchTemplateJSON struct {
	ID      string `json:"id,omitempty"`
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

type nodegroupUpdateConfigJSON struct {
	MaxUnavailable           *int32 `json:"maxUnavailable,omitempty"`
	MaxUnavailablePercentage *int32 `json:"maxUnavailablePercentage,omitempty"`
}

type createNodegroupBody struct {
	Tags           map[string]string          `json:"tags"`
	Labels         map[string]string          `json:"labels"`
	RemoteAccess   *remoteAccessJSON          `json:"remoteAccess"`
	LaunchTemplate *launchTemplateJSON        `json:"launchTemplate"`
	UpdateConfig   *nodegroupUpdateConfigJSON `json:"updateConfig"`
	NodegroupName  string                     `json:"nodegroupName"`
	NodeRole       string                     `json:"nodeRole"`
	AMIType        string                     `json:"amiType"`
	CapacityType   string                     `json:"capacityType"`
	Version        string                     `json:"version"`
	ReleaseVersion string                     `json:"releaseVersion"`
	InstanceTypes  []string                   `json:"instanceTypes"`
	Subnets        []string                   `json:"subnets"`
	Taints         []nodegroupTaintJSON       `json:"taints"`
	ScalingConfig  scalingConfigJSON          `json:"scalingConfig"`
	DiskSize       int32                      `json:"diskSize"`
}

func (h *Handler) handleCreateNodegroup(c *echo.Context, clusterName string, body []byte) error {
	var in createNodegroupBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.NodegroupName == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "nodegroupName is required"))
	}

	if in.NodeRole == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "nodeRole is required"))
	}

	if len(in.Subnets) == 0 && in.LaunchTemplate == nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "subnets is required"))
	}

	if err := validateTagMap(in.Tags, 0); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException",
			"tag key must be 1-128 chars, value 0-256 chars, max 50 tags per resource"))
	}

	taints := make([]NodegroupTaint, len(in.Taints))
	for i, t := range in.Taints {
		taints[i] = NodegroupTaint(t)
	}

	var remoteAccess *RemoteAccess
	if in.RemoteAccess != nil {
		remoteAccess = &RemoteAccess{
			EC2SSHKey:            in.RemoteAccess.EC2SSHKey,
			SourceSecurityGroups: in.RemoteAccess.SourceSecurityGroups,
		}
	}

	var lt *LaunchTemplate
	if in.LaunchTemplate != nil {
		lt = &LaunchTemplate{
			ID:      in.LaunchTemplate.ID,
			Name:    in.LaunchTemplate.Name,
			Version: in.LaunchTemplate.Version,
		}
	}

	var ngUpdateCfg *NodegroupUpdateConfig
	if in.UpdateConfig != nil {
		ngUpdateCfg = &NodegroupUpdateConfig{
			MaxUnavailable:           in.UpdateConfig.MaxUnavailable,
			MaxUnavailablePercentage: in.UpdateConfig.MaxUnavailablePercentage,
		}
	}

	ng, err := h.Backend.CreateNodegroup(
		clusterName, in.NodegroupName, in.NodeRole,
		in.AMIType, in.CapacityType, in.Version, in.ReleaseVersion,
		in.InstanceTypes,
		in.ScalingConfig.DesiredSize, in.ScalingConfig.MinSize, in.ScalingConfig.MaxSize,
		NodegroupInput{
			Labels:         in.Labels,
			RemoteAccess:   remoteAccess,
			LaunchTemplate: lt,
			Subnets:        in.Subnets,
			Taints:         taints,
			DiskSize:       in.DiskSize,
			UpdateConfig:   ngUpdateCfg,
		},
		in.Tags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyNodegroup: nodegroupToJSON(ng),
	})
}

func (h *Handler) handleDescribeNodegroup(c *echo.Context, clusterName, nodegroupName string) error {
	ng, err := h.Backend.DescribeNodegroup(clusterName, nodegroupName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyNodegroup: nodegroupToJSON(ng),
	})
}

func (h *Handler) handleListNodegroups(c *echo.Context, clusterName string) error {
	names, err := h.Backend.ListNodegroups(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	maxResults, nextToken := eksPaginationParams(c)
	p := page.New(names, nextToken, maxResults, eksDefaultPageSize)

	return c.JSON(http.StatusOK, eksPageResponse("nodegroups", p))
}

func (h *Handler) handleDeleteNodegroup(c *echo.Context, clusterName, nodegroupName string) error {
	ng, err := h.Backend.DeleteNodegroup(clusterName, nodegroupName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyNodegroup: nodegroupToJSON(ng),
	})
}

type updateNodegroupScalingConfigJSON struct {
	DesiredSize *int32 `json:"desiredSize,omitempty"`
	MinSize     *int32 `json:"minSize,omitempty"`
	MaxSize     *int32 `json:"maxSize,omitempty"`
}

type updateNodegroupLabelsPayload struct {
	AddOrUpdateLabels map[string]string `json:"addOrUpdateLabels,omitempty"`
	RemoveLabels      []string          `json:"removeLabels,omitempty"`
}

type updateNodegroupTaintsPayload struct {
	AddOrUpdateTaints []nodegroupTaintJSON `json:"addOrUpdateTaints,omitempty"`
	RemoveTaints      []nodegroupTaintJSON `json:"removeTaints,omitempty"`
}

type updateNodegroupUpdateConfigJSON struct {
	MaxUnavailable           *int32 `json:"maxUnavailable,omitempty"`
	MaxUnavailablePercentage *int32 `json:"maxUnavailablePercentage,omitempty"`
}

type updateNodegroupConfigInput struct {
	ScalingConfig *updateNodegroupScalingConfigJSON `json:"scalingConfig,omitempty"`
	Labels        *updateNodegroupLabelsPayload     `json:"labels,omitempty"`
	Taints        *updateNodegroupTaintsPayload     `json:"taints,omitempty"`
	UpdateConfig  *updateNodegroupUpdateConfigJSON  `json:"updateConfig,omitempty"`
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

	upd := NodegroupConfigUpdate{}
	if in.ScalingConfig != nil {
		upd.DesiredSize = in.ScalingConfig.DesiredSize
		upd.MinSize = in.ScalingConfig.MinSize
		upd.MaxSize = in.ScalingConfig.MaxSize
	}

	if in.Labels != nil {
		upd.AddOrUpdateLabels = in.Labels.AddOrUpdateLabels
		upd.RemoveLabels = in.Labels.RemoveLabels
	}

	if in.Taints != nil {
		for _, t := range in.Taints.AddOrUpdateTaints {
			upd.AddOrUpdateTaints = append(upd.AddOrUpdateTaints, NodegroupTaint(t))
		}

		for _, t := range in.Taints.RemoveTaints {
			upd.RemoveTaints = append(upd.RemoveTaints, NodegroupTaint(t))
		}
	}

	if in.UpdateConfig != nil {
		upd.UpdateConfig = &NodegroupUpdateConfig{
			MaxUnavailable:           in.UpdateConfig.MaxUnavailable,
			MaxUnavailablePercentage: in.UpdateConfig.MaxUnavailablePercentage,
		}
	}

	ng, err := h.Backend.UpdateNodegroupConfig(clusterName, nodegroupName, upd)
	if err != nil {
		return h.handleError(c, err)
	}

	now := time.Now().UTC()
	u := &Update{
		ID:          uuid.NewString()[:8],
		ClusterName: clusterName,
		Status:      statusInProgress,
		Type:        "ConfigUpdate",
		CreatedAt:   now,
	}
	h.Backend.StoreUpdate(u)

	return c.JSON(http.StatusOK, map[string]any{
		keyUpdate: map[string]any{
			"id":            u.ID,
			keyStatusField:  u.Status,
			keyType:         u.Type,
			keyCreatedAt:    float64(now.Unix()),
			keyClusterName:  clusterName,
			"nodegroupName": ng.NodegroupName,
		},
	})
}

type updateNodegroupVersionBody struct {
	Version string `json:"version"`
}

func (h *Handler) handleUpdateNodegroupVersion(c *echo.Context, clusterName, nodegroupName string, body []byte) error {
	var in updateNodegroupVersionBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
		}
	}

	update, err := h.Backend.UpdateNodegroupVersion(clusterName, nodegroupName, in.Version)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyUpdate: updateToJSON(update),
	})
}
