package eks

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// dispatchUpdateOps handles cluster-level config/version update and
// encryption-config association operations.
func (h *Handler) dispatchUpdateOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opAssociateEncryptionConfig:
		return true, h.handleAssociateEncryptionConfig(c, route.clusterName, body)
	case opUpdateClusterConfig:
		return true, h.handleUpdateClusterConfig(c, route.clusterName, body)
	case opUpdateClusterVersion:
		return true, h.handleUpdateClusterVersion(c, route.clusterName, body)
	case opDescribeUpdate:
		return true, h.handleDescribeUpdate(c, route.clusterName, route.nodegroupName)
	case opListUpdates:
		return true, h.handleListUpdates(c, route.clusterName)
	}

	return false, nil
}

// parseUpdatesRoute returns the route for /clusters/{name}/updates[/{id}].
func parseUpdatesRoute(method, clusterName string, parts []string) eksRoute {
	const updatesParts = 2

	// GET lists updates; POST on the same path starts a new
	// UpdateClusterVersion (there is no separate "/update-version" cluster
	// path in the real API) -- verified against the SDK serializer.
	if len(parts) == updatesParts {
		switch method {
		case http.MethodGet:
			return eksRoute{operation: opListUpdates, clusterName: clusterName}
		case http.MethodPost:
			return eksRoute{operation: opUpdateClusterVersion, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	updateID := parts[2]

	if method == http.MethodGet {
		return eksRoute{operation: opDescribeUpdate, clusterName: clusterName, nodegroupName: updateID}
	}

	return eksRoute{operation: opUnknown}
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
		configs[i] = EncryptionConfig(ec)
	}

	result, err := h.Backend.AssociateEncryptionConfig(clusterName, configs)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyUpdate: map[string]any{
			"id":           uuid.NewString()[:8],
			keyStatusField: statusInProgress,
			keyType:        opAssociateEncryptionConfig,
			keyClusterName: clusterName,
			"params": map[string]any{
				"encryptionConfig": result,
			},
		},
	})
}

type updateClusterConfigLogging struct {
	ClusterLogging []struct {
		Types   []string `json:"types"`
		Enabled bool     `json:"enabled"`
	} `json:"clusterLogging"`
}

type updateClusterConfigVpcConfig struct {
	SubnetIDs             []string `json:"subnetIds"`
	EndpointPublicAccess  *bool    `json:"endpointPublicAccess"`
	EndpointPrivateAccess *bool    `json:"endpointPrivateAccess"`
	PublicAccessCidrs     []string `json:"publicAccessCidrs"`
}

type updateClusterConfigBody struct {
	Logging            *updateClusterConfigLogging   `json:"logging"`
	ResourcesVpcConfig *updateClusterConfigVpcConfig `json:"resourcesVpcConfig"`
	AccessConfig       *accessConfigJSON             `json:"accessConfig"`
	ComputeConfig      *computeConfigJSON            `json:"computeConfig"`
	StorageConfig      *storageConfigJSON            `json:"storageConfig"`
}

func (h *Handler) handleUpdateClusterConfig(c *echo.Context, clusterName string, body []byte) error {
	var in updateClusterConfigBody

	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
		}
	}

	cfgUpd := buildClusterConfigUpdate(in)

	update, err := h.Backend.UpdateClusterConfig(clusterName, cfgUpd)
	if err != nil {
		return h.handleError(c, err)
	}

	if vpcErr := h.applyVpcEndpointUpdate(c, clusterName, in.ResourcesVpcConfig, update); vpcErr != nil {
		return vpcErr
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyUpdate: updateToJSON(update),
	})
}

func buildClusterConfigUpdate(in updateClusterConfigBody) ClusterConfigUpdate {
	cfgUpd := ClusterConfigUpdate{}

	if in.Logging != nil {
		cfgUpd.LogEntries = make([]ClusterLogEntry, len(in.Logging.ClusterLogging))
		for i, entry := range in.Logging.ClusterLogging {
			cfgUpd.LogEntries[i] = ClusterLogEntry{Types: entry.Types, Enabled: entry.Enabled}
		}
	}

	if in.ResourcesVpcConfig != nil && len(in.ResourcesVpcConfig.SubnetIDs) > 0 {
		cfgUpd.SubnetIDs = in.ResourcesVpcConfig.SubnetIDs
	}

	if in.AccessConfig != nil {
		ac := &AccessConfig{AuthenticationMode: in.AccessConfig.AuthenticationMode}
		if in.AccessConfig.BootstrapClusterCreatorAdminPermissions != nil {
			ac.BootstrapClusterCreatorAdminPermissions = *in.AccessConfig.BootstrapClusterCreatorAdminPermissions
		}
		cfgUpd.AccessConfig = ac
	}

	if in.ComputeConfig != nil {
		cc := &ComputeConfig{NodeRoleARN: in.ComputeConfig.NodeRoleArn, NodePools: in.ComputeConfig.NodePools}
		if in.ComputeConfig.Enabled != nil {
			cc.Enabled = *in.ComputeConfig.Enabled
		}
		cfgUpd.ComputeConfig = cc
	}

	if in.StorageConfig != nil && in.StorageConfig.BlockStorage != nil {
		sc := &StorageConfig{BlockStorage: &BlockStorageConfig{}}
		if in.StorageConfig.BlockStorage.Enabled != nil {
			sc.BlockStorage.Enabled = *in.StorageConfig.BlockStorage.Enabled
		}
		cfgUpd.StorageConfig = sc
	}

	return cfgUpd
}

func (h *Handler) applyVpcEndpointUpdate(
	c *echo.Context, clusterName string,
	vpcIn *updateClusterConfigVpcConfig, update *Update,
) error {
	if vpcIn == nil {
		return nil
	}
	vpcUpd := VpcEndpointUpdate{
		EndpointPublicAccess:  vpcIn.EndpointPublicAccess,
		EndpointPrivateAccess: vpcIn.EndpointPrivateAccess,
		PublicAccessCIDRs:     vpcIn.PublicAccessCidrs,
	}
	if vpcUpd.EndpointPublicAccess == nil && vpcUpd.EndpointPrivateAccess == nil && vpcUpd.PublicAccessCIDRs == nil {
		return nil
	}
	vpcUpdate, err := h.Backend.UpdateClusterVpcEndpoint(clusterName, vpcUpd)
	if err != nil {
		return h.handleError(c, err)
	}
	update.Params = append(update.Params, vpcUpdate.Params...)

	return nil
}

type updateClusterVersionBody struct {
	Version string `json:"version"`
}

func (h *Handler) handleUpdateClusterVersion(c *echo.Context, clusterName string, body []byte) error {
	var in updateClusterVersionBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
		}
	}

	update, err := h.Backend.UpdateClusterVersion(clusterName, in.Version)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyUpdate: updateToJSON(update),
	})
}

func (h *Handler) handleDescribeUpdate(c *echo.Context, clusterName, updateID string) error {
	update, err := h.Backend.DescribeUpdate(clusterName, updateID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyUpdate: updateToJSON(update),
	})
}

func (h *Handler) handleListUpdates(c *echo.Context, clusterName string) error {
	ids, err := h.Backend.ListUpdates(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"updateIds": ids,
	})
}

func updateToJSON(u *Update) map[string]any {
	m := map[string]any{
		"id":           u.ID,
		keyStatusField: u.Status,
		keyType:        u.Type,
		keyCreatedAt:   float64(u.CreatedAt.Unix()),
		"params":       u.Params,
		"errors":       u.Errors,
	}
	if u.Params == nil {
		m["params"] = []UpdateParam{}
	}
	if u.Errors == nil {
		m["errors"] = []UpdateError{}
	}

	return m
}
