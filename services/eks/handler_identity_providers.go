package eks

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
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
	RequiredClaims             map[string]string `json:"requiredClaims,omitempty"`
	ClientID                   string            `json:"clientId"`
	GroupsClaim                string            `json:"groupsClaim,omitempty"`
	GroupsPrefix               string            `json:"groupsPrefix,omitempty"`
	IdentityProviderConfigName string            `json:"identityProviderConfigName,omitempty"`
	IssuerURL                  string            `json:"issuerUrl"`
	UsernameClaim              string            `json:"usernameClaim,omitempty"`
	UsernamePrefix             string            `json:"usernamePrefix,omitempty"`
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

	if in.Oidc.IdentityProviderConfigName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterException", "oidc.identityProviderConfigName is required"),
		)
	}

	params := map[string]string{
		"issuerUrl": in.Oidc.IssuerURL,
		"clientId":  in.Oidc.ClientID,
	}
	if in.Oidc.UsernameClaim != "" {
		params["usernameClaim"] = in.Oidc.UsernameClaim
	}
	if in.Oidc.UsernamePrefix != "" {
		params["usernamePrefix"] = in.Oidc.UsernamePrefix
	}
	if in.Oidc.GroupsClaim != "" {
		params["groupsClaim"] = in.Oidc.GroupsClaim
	}
	if in.Oidc.GroupsPrefix != "" {
		params["groupsPrefix"] = in.Oidc.GroupsPrefix
	}

	cfg, err := h.Backend.AssociateIdentityProviderConfig(
		clusterName, "oidc", in.Oidc.IdentityProviderConfigName, params, in.Oidc.RequiredClaims, in.Tags,
	)
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

	// Real shape is {identityProviderConfig: {oidc: {...}}} -- verified
	// against aws-sdk-go-v2/service/eks/types.IdentityProviderConfigResponse,
	// which nests the full OidcIdentityProviderConfig under an "oidc" key
	// rather than returning a flat object.
	oidc := map[string]any{
		"identityProviderConfigName": cfg.Name,
		"identityProviderConfigArn":  cfg.ARN,
		keyClusterName:               cfg.ClusterName,
		keyStatusField:               cfg.Status,
		"issuerUrl":                  cfg.OIDC["issuerUrl"],
		"clientId":                   cfg.OIDC["clientId"],
	}
	if v := cfg.OIDC["usernameClaim"]; v != "" {
		oidc["usernameClaim"] = v
	}
	if v := cfg.OIDC["usernamePrefix"]; v != "" {
		oidc["usernamePrefix"] = v
	}
	if v := cfg.OIDC["groupsClaim"]; v != "" {
		oidc["groupsClaim"] = v
	}
	if v := cfg.OIDC["groupsPrefix"]; v != "" {
		oidc["groupsPrefix"] = v
	}
	if len(cfg.RequiredClaims) > 0 {
		oidc["requiredClaims"] = cfg.RequiredClaims
	}
	if cfg.Tags != nil {
		oidc[keyTags] = cfg.Tags.Clone()
	} else {
		oidc[keyTags] = map[string]string{}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"identityProviderConfig": map[string]any{
			"oidc": oidc,
		},
	})
}

func (h *Handler) handleListIdentityProviderConfigs(c *echo.Context, clusterName string) error {
	configs, err := h.Backend.ListIdentityProviderConfigs(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	maxResults, nextToken := eksPaginationParams(c)
	p := page.New(configs, nextToken, maxResults, eksDefaultPageSize)

	return c.JSON(http.StatusOK, eksPageResponse("identityProviderConfigs", p))
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
