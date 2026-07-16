package eks

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// dispatchIDPOps handles identity provider config association/CRUD operations.
func (h *Handler) dispatchIDPOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opAssociateIdentityProviderConfig:
		return true, h.handleAssociateIdentityProviderConfig(c, route.clusterName, body)
	case opDescribeIdentityProviderConfig:
		return true, h.handleDescribeIdentityProviderConfig(c, route.clusterName, body)
	case opListIdentityProviderConfigs:
		return true, h.handleListIdentityProviderConfigs(c, route.clusterName)
	case opDisassociateIdentityProviderConfig:
		return true, h.handleDisassociateIdentityProviderConfig(c, route.clusterName, body)
	}

	return false, nil
}

// parseIdentityProviderRoute returns routes for /clusters/{name}/identity-provider-configs/...
func parseIdentityProviderRoute(method, clusterName string, parts []string, maxParts int) eksRoute {
	if len(parts) == maxParts && parts[2] == "associate" {
		if method == http.MethodPost {
			return eksRoute{operation: opAssociateIdentityProviderConfig, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	if len(parts) == maxParts && parts[2] == "disassociate" {
		if method == http.MethodPost {
			return eksRoute{operation: opDisassociateIdentityProviderConfig, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	const idpListParts = 2

	if len(parts) == idpListParts {
		if method == http.MethodGet {
			return eksRoute{operation: opListIdentityProviderConfigs, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	if len(parts) == maxParts {
		if method == http.MethodPost {
			return eksRoute{operation: opDescribeIdentityProviderConfig, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	return eksRoute{operation: opUnknown}
}

type oidcConfigJSON struct {
	ClientID                   string `json:"clientId"`
	GroupsClaim                string `json:"groupsClaim,omitempty"`
	GroupsPrefix               string `json:"groupsPrefix,omitempty"`
	IdentityProviderConfigName string `json:"identityProviderConfigName,omitempty"`
	IssuerURL                  string `json:"issuerUrl"`
	UsernameClaim              string `json:"usernameClaim,omitempty"`
	UsernamePrefix             string `json:"usernamePrefix,omitempty"`
}

type associateIdentityProviderConfigBody struct {
	Tags map[string]string `json:"tags"`
	Oidc *oidcConfigJSON   `json:"oidc"`
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

	configName := in.Oidc.IdentityProviderConfigName
	if configName == "" {
		configName = in.Oidc.ClientID
	}

	cfg, err := h.Backend.AssociateIdentityProviderConfig(clusterName, "oidc", configName, params, in.Tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyUpdate: map[string]any{
			"id":           uuid.NewString()[:8],
			keyStatusField: statusInProgress,
			keyType:        opAssociateIdentityProviderConfig,
			keyClusterName: clusterName,
		},
		keyTags: cfg.Tags.Clone(),
	})
}

type describeIDPBody struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type describeIDPRequestBody struct {
	IdentityProviderConfig describeIDPBody `json:"identityProviderConfig"`
}

func (h *Handler) handleDescribeIdentityProviderConfig(c *echo.Context, clusterName string, body []byte) error {
	var in describeIDPRequestBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
		}
	}

	cfg, err := h.Backend.DescribeIdentityProviderConfig(clusterName, in.IdentityProviderConfig.Name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"identityProviderConfig": map[string]any{
			keyClusterName: cfg.ClusterName,
			keyName:        cfg.Name,
			keyType:        cfg.Type,
			keyStatusField: cfg.Status,
			"oidc":         cfg.OIDC,
			keyCreatedAt:   cfg.CreatedAt.Unix(),
		},
	})
}

func (h *Handler) handleListIdentityProviderConfigs(c *echo.Context, clusterName string) error {
	configs, err := h.Backend.ListIdentityProviderConfigs(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"identityProviderConfigs": configs,
	})
}

type disassociateIDPBody struct {
	IdentityProviderConfig describeIDPBody `json:"identityProviderConfig"`
}

func (h *Handler) handleDisassociateIdentityProviderConfig(c *echo.Context, clusterName string, body []byte) error {
	var in disassociateIDPBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
		}
	}

	if err := h.Backend.DisassociateIdentityProviderConfig(clusterName, in.IdentityProviderConfig.Name); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyUpdate: map[string]any{
			"id":           uuid.NewString()[:8],
			keyStatusField: statusInProgress,
			keyType:        "DisassociateIdentityProviderConfig",
			keyClusterName: clusterName,
		},
	})
}
