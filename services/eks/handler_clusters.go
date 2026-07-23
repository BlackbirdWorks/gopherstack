package eks

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// dispatchClusterOps handles cluster CRUD and cluster-registration operations.
func (h *Handler) dispatchClusterOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opCreateCluster:
		return true, h.handleCreateCluster(c, body)
	case opDescribeCluster:
		return true, h.handleDescribeCluster(c, route.clusterName)
	case opListClusters:
		return true, h.handleListClusters(c)
	case opDeleteCluster:
		return true, h.handleDeleteCluster(c, route.clusterName)
	case opRegisterCluster:
		return true, h.handleRegisterCluster(c, body)
	case opDeregisterCluster:
		return true, h.handleDeregisterCluster(c, route.clusterName)
	case opDescribeClusterVersions:
		return true, h.handleDescribeClusterVersions(c)
	}

	return false, nil
}

// parseClusterRegistrationsEKSPath returns the route for the global (not
// cluster-nested) RegisterCluster/DeregisterCluster paths: POST
// /cluster-registrations and DELETE /cluster-registrations/{name} --
// verified against the SDK serializer.
func parseClusterRegistrationsEKSPath(method, path string) (eksRoute, bool) {
	if path == pathClusterRegistrations {
		if method == http.MethodPost {
			return eksRoute{operation: opRegisterCluster}, true
		}

		return eksRoute{operation: opUnknown}, true
	}

	if after, ok := strings.CutPrefix(path, pathClusterRegistrations+"/"); ok {
		if method == http.MethodDelete {
			return eksRoute{operation: opDeregisterCluster, clusterName: after}, true
		}

		return eksRoute{operation: opUnknown}, true
	}

	return eksRoute{}, false
}

// clusterVpcConfigJSON converts a VpcConfig to a JSON-serializable map.
func clusterVpcConfigJSON(v *VpcConfig) map[string]any {
	vpc := map[string]any{
		"subnetIds":             v.SubnetIDs,
		"securityGroupIds":      v.SecurityGroupIDs,
		"endpointPrivateAccess": v.EndpointPrivateAccess,
		"endpointPublicAccess":  v.EndpointPublicAccess,
		"publicAccessCidrs":     v.PublicAccessCIDRs,
	}
	if v.ClusterSecurityGroupID != "" {
		vpc["clusterSecurityGroupId"] = v.ClusterSecurityGroupID
	}
	if v.VpcID != "" {
		vpc["vpcId"] = v.VpcID
	}

	return vpc
}

// clusterToJSON converts a Cluster to a JSON-serializable map.
func clusterToJSON(c *Cluster) map[string]any {
	m := map[string]any{
		keyName:           c.Name,
		keyArn:            c.ARN,
		keyStatusField:    c.Status,
		keyVersion:        c.Version,
		keyCreatedAt:      c.CreatedAt.Unix(),
		"platformVersion": c.PlatformVersion,
		keyTags:           clusterTagsMap(c),
	}
	appendClusterCoreFields(c, m)
	appendClusterOptionalInfra(c, m)

	return m
}

func appendClusterCoreFields(c *Cluster, m map[string]any) {
	if c.Endpoint != "" {
		m["endpoint"] = c.Endpoint
	}
	if c.RoleARN != "" {
		m["roleArn"] = c.RoleARN
	}
	if c.OIDCIssuer != "" {
		m["identity"] = map[string]any{"oidc": map[string]string{"issuer": c.OIDCIssuer}}
	}
	if c.VpcConfig != nil {
		m["resourcesVpcConfig"] = clusterVpcConfigJSON(c.VpcConfig)
	}
	if net := clusterNetConfigJSON(c.KubernetesNetworkConfig); net != nil {
		m["kubernetesNetworkConfig"] = net
	}
	if len(c.ClusterLogging) > 0 {
		m["logging"] = map[string]any{"clusterLogging": clusterLoggingJSON(c.ClusterLogging)}
	}
	if len(c.EncryptionConfig) > 0 {
		m["encryptionConfig"] = c.EncryptionConfig
	}
}

func appendClusterOptionalInfra(c *Cluster, m map[string]any) {
	if c.AccessConfig != nil {
		m["accessConfig"] = map[string]any{
			"authenticationMode":                      c.AccessConfig.AuthenticationMode,
			"bootstrapClusterCreatorAdminPermissions": c.AccessConfig.BootstrapClusterCreatorAdminPermissions,
		}
	}
	if c.ComputeConfig != nil {
		m["computeConfig"] = clusterComputeConfigJSON(c.ComputeConfig)
	}
	if c.StorageConfig != nil && c.StorageConfig.BlockStorage != nil {
		m["storageConfig"] = map[string]any{
			"blockStorage": map[string]any{keyEnabled: c.StorageConfig.BlockStorage.Enabled},
		}
	}
	if c.NetworkingConfig != nil && c.NetworkingConfig.ElasticLoadBalancing != nil {
		m["networkingConfig"] = map[string]any{
			"elasticLoadBalancing": map[string]any{keyEnabled: c.NetworkingConfig.ElasticLoadBalancing.Enabled},
		}
	}
	if c.CertificateAuthority != "" {
		m["certificateAuthority"] = map[string]string{"data": c.CertificateAuthority}
	}
	if c.ConnectorConfig != nil {
		m["connectorConfig"] = c.ConnectorConfig
	}
}

func clusterNetConfigJSON(cfg *KubernetesNetworkConfig) map[string]any {
	if cfg == nil {
		return nil
	}
	net := map[string]any{}
	if cfg.IPFamily != "" {
		net["ipFamily"] = cfg.IPFamily
	}
	if cfg.ServiceIPv4CIDR != "" {
		net["serviceIpv4Cidr"] = cfg.ServiceIPv4CIDR
	}
	if cfg.ServiceIPv6CIDR != "" {
		net["serviceIpv6Cidr"] = cfg.ServiceIPv6CIDR
	}
	if len(net) == 0 {
		return nil
	}

	return net
}

func clusterLoggingJSON(entries []ClusterLogEntry) []map[string]any {
	out := make([]map[string]any, len(entries))
	for i, e := range entries {
		out[i] = map[string]any{"types": e.Types, keyEnabled: e.Enabled}
	}

	return out
}

func clusterComputeConfigJSON(cc *ComputeConfig) map[string]any {
	m := map[string]any{keyEnabled: cc.Enabled}
	if cc.NodeRoleARN != "" {
		m["nodeRoleArn"] = cc.NodeRoleARN
	}
	if len(cc.NodePools) > 0 {
		m["nodePools"] = cc.NodePools
	}

	return m
}

// clusterTagsMap returns the cluster tags as a plain map, or an empty map if unset.
func clusterTagsMap(c *Cluster) map[string]string {
	if c.Tags == nil {
		return map[string]string{}
	}

	return c.Tags.Clone()
}

type vpcConfigJSON struct {
	SubnetIDs             []string `json:"subnetIds"`
	SecurityGroupIDs      []string `json:"securityGroupIds"`
	PublicAccessCIDRs     []string `json:"publicAccessCidrs"`
	EndpointPrivateAccess bool     `json:"endpointPrivateAccess"`
	EndpointPublicAccess  bool     `json:"endpointPublicAccess"`
}

type kubernetesNetworkConfigJSON struct {
	IPFamily        string `json:"ipFamily"`
	ServiceIPv4CIDR string `json:"serviceIpv4Cidr"`
	ServiceIPv6CIDR string `json:"serviceIpv6Cidr"`
}

type accessConfigJSON struct {
	BootstrapClusterCreatorAdminPermissions *bool  `json:"bootstrapClusterCreatorAdminPermissions,omitempty"`
	AuthenticationMode                      string `json:"authenticationMode"`
}

type computeConfigJSON struct {
	Enabled     *bool    `json:"enabled,omitempty"`
	NodeRoleArn string   `json:"nodeRoleArn,omitempty"`
	NodePools   []string `json:"nodePools,omitempty"`
}

type blockStorageConfigJSON struct {
	Enabled *bool `json:"enabled,omitempty"`
}

type storageConfigJSON struct {
	BlockStorage *blockStorageConfigJSON `json:"blockStorage,omitempty"`
}

type elasticLoadBalancingConfigJSON struct {
	Enabled *bool `json:"enabled,omitempty"`
}

type networkingConfigJSON struct {
	ElasticLoadBalancing *elasticLoadBalancingConfigJSON `json:"elasticLoadBalancing,omitempty"`
}

type createClusterBody struct {
	Tags                    map[string]string            `json:"tags"`
	ResourcesVpcConfig      *vpcConfigJSON               `json:"resourcesVpcConfig"`
	KubernetesNetworkConfig *kubernetesNetworkConfigJSON `json:"kubernetesNetworkConfig"`
	AccessConfig            *accessConfigJSON            `json:"accessConfig"`
	ComputeConfig           *computeConfigJSON           `json:"computeConfig"`
	StorageConfig           *storageConfigJSON           `json:"storageConfig"`
	NetworkingConfig        *networkingConfigJSON        `json:"networkingConfig"`
	Name                    string                       `json:"name"`
	Version                 string                       `json:"version"`
	RoleArn                 string                       `json:"roleArn"`
}

func (h *Handler) handleCreateCluster(c *echo.Context, body []byte) error {
	var in createClusterBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "name is required"))
	}

	if err := validateTagMap(in.Tags, 0); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException",
			"tag key must be 1-128 chars, value 0-256 chars, max 50 tags per resource"))
	}

	var vpcCfg *VpcConfig
	if in.ResourcesVpcConfig != nil {
		vpcCfg = &VpcConfig{
			SubnetIDs:             in.ResourcesVpcConfig.SubnetIDs,
			SecurityGroupIDs:      in.ResourcesVpcConfig.SecurityGroupIDs,
			PublicAccessCIDRs:     in.ResourcesVpcConfig.PublicAccessCIDRs,
			EndpointPrivateAccess: in.ResourcesVpcConfig.EndpointPrivateAccess,
			EndpointPublicAccess:  in.ResourcesVpcConfig.EndpointPublicAccess,
		}
	}

	var netCfg *KubernetesNetworkConfig
	if in.KubernetesNetworkConfig != nil {
		netCfg = &KubernetesNetworkConfig{
			IPFamily:        in.KubernetesNetworkConfig.IPFamily,
			ServiceIPv4CIDR: in.KubernetesNetworkConfig.ServiceIPv4CIDR,
			ServiceIPv6CIDR: in.KubernetesNetworkConfig.ServiceIPv6CIDR,
		}
	}

	cluster, err := h.Backend.CreateCluster(
		in.Name,
		in.Version,
		in.RoleArn,
		vpcCfg,
		netCfg,
		in.Tags,
		buildClusterOptConfig(in),
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCluster: clusterToJSON(cluster),
	})
}

func buildClusterOptConfig(in createClusterBody) ClusterOptionalConfig {
	var opt ClusterOptionalConfig

	if in.AccessConfig != nil {
		ac := &AccessConfig{AuthenticationMode: in.AccessConfig.AuthenticationMode}
		if in.AccessConfig.BootstrapClusterCreatorAdminPermissions != nil {
			ac.BootstrapClusterCreatorAdminPermissions = *in.AccessConfig.BootstrapClusterCreatorAdminPermissions
		}
		opt.AccessConfig = ac
	}

	if in.ComputeConfig != nil {
		cc := &ComputeConfig{NodeRoleARN: in.ComputeConfig.NodeRoleArn, NodePools: in.ComputeConfig.NodePools}
		if in.ComputeConfig.Enabled != nil {
			cc.Enabled = *in.ComputeConfig.Enabled
		}
		opt.ComputeConfig = cc
	}

	if in.StorageConfig != nil && in.StorageConfig.BlockStorage != nil {
		sc := &StorageConfig{BlockStorage: &BlockStorageConfig{}}
		if in.StorageConfig.BlockStorage.Enabled != nil {
			sc.BlockStorage.Enabled = *in.StorageConfig.BlockStorage.Enabled
		}
		opt.StorageConfig = sc
	}

	if in.NetworkingConfig != nil && in.NetworkingConfig.ElasticLoadBalancing != nil {
		nc := &NetworkingConfig{ElasticLoadBalancing: &ElasticLoadBalancingConfig{}}
		if in.NetworkingConfig.ElasticLoadBalancing.Enabled != nil {
			nc.ElasticLoadBalancing.Enabled = *in.NetworkingConfig.ElasticLoadBalancing.Enabled
		}
		opt.NetworkingConfig = nc
	}

	return opt
}

func (h *Handler) handleDescribeCluster(c *echo.Context, name string) error {
	cluster, err := h.Backend.DescribeCluster(name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCluster: clusterToJSON(cluster),
	})
}

func (h *Handler) handleListClusters(c *echo.Context) error {
	names := h.Backend.ListClusters()

	maxResults, nextToken := eksPaginationParams(c)
	p := page.New(names, nextToken, maxResults, eksDefaultPageSize)

	return c.JSON(http.StatusOK, eksPageResponse("clusters", p))
}

func (h *Handler) handleDeleteCluster(c *echo.Context, name string) error {
	cluster, err := h.Backend.DeleteCluster(name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCluster: clusterToJSON(cluster),
	})
}

type connectorConfigJSON struct {
	Provider string `json:"provider"`
	RoleArn  string `json:"roleArn"`
}

type registerClusterBody struct {
	Tags            map[string]string    `json:"tags"`
	ConnectorConfig *connectorConfigJSON `json:"connectorConfig"`
	Name            string               `json:"name"`
}

func (h *Handler) handleRegisterCluster(c *echo.Context, body []byte) error {
	var in registerClusterBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "name is required"))
	}

	var provider, roleArn string
	if in.ConnectorConfig != nil {
		provider = in.ConnectorConfig.Provider
		roleArn = in.ConnectorConfig.RoleArn
	}

	cluster, err := h.Backend.RegisterCluster(in.Name, provider, roleArn, in.Tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"cluster": clusterToJSON(cluster),
	})
}

func (h *Handler) handleDeregisterCluster(c *echo.Context, name string) error {
	cluster, err := h.Backend.DeregisterCluster(name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"cluster": clusterToJSON(cluster),
	})
}

func (h *Handler) handleDescribeClusterVersions(c *echo.Context) error {
	versions := h.Backend.DescribeClusterVersions()

	return c.JSON(http.StatusOK, map[string]any{
		"clusterVersions": versions,
	})
}
