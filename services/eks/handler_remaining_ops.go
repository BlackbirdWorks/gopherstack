package eks

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// dispatchRemainingOps handles the remaining EKS operations (CRUD for addons, fargate, pod identity, etc.).
func (h *Handler) dispatchRemainingOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	if handled, err := h.dispatchAddonOps(c, route, body); handled {
		return true, err
	}

	if handled, err := h.dispatchCapabilityOps(c, route, body); handled {
		return true, err
	}

	if handled, err := h.dispatchSubscriptionOps(c, route, body); handled {
		return true, err
	}

	if handled, err := h.dispatchFargateOps(c, route); handled {
		return true, err
	}

	if handled, err := h.dispatchPodIdentityOps(c, route, body); handled {
		return true, err
	}

	if handled, err := h.dispatchAccessOps(c, route, body); handled {
		return true, err
	}

	if handled, err := h.dispatchIDPOps(c, route, body); handled {
		return true, err
	}

	if handled, err := h.dispatchInsightsOps(c, route, body); handled {
		return true, err
	}

	return h.dispatchClusterUpdateOps(c, route, body)
}

// --- Addon ops ---

func (h *Handler) dispatchAddonOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opDeleteAddon:
		return true, h.handleDeleteAddon(c, route.clusterName, route.nodegroupName)
	case opDescribeAddon:
		return true, h.handleDescribeAddon(c, route.clusterName, route.nodegroupName)
	case opListAddons:
		return true, h.handleListAddons(c, route.clusterName)
	case opUpdateAddon:
		return true, h.handleUpdateAddon(c, route.clusterName, route.nodegroupName, body)
	case opDescribeAddonVersions:
		return true, h.handleDescribeAddonVersions(c)
	case opDescribeAddonConfiguration:
		return true, h.handleDescribeAddonConfiguration(c)
	}

	return false, nil
}

func (h *Handler) handleDeleteAddon(c *echo.Context, clusterName, addonName string) error {
	addon, err := h.Backend.DeleteAddon(clusterName, addonName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"addon": addonToJSON(addon),
	})
}

func (h *Handler) handleDescribeAddon(c *echo.Context, clusterName, addonName string) error {
	addon, err := h.Backend.DescribeAddon(clusterName, addonName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"addon": addonToJSON(addon),
	})
}

func (h *Handler) handleListAddons(c *echo.Context, clusterName string) error {
	names, err := h.Backend.ListAddons(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"addons": names,
	})
}

type updateAddonBody struct {
	AddonVersion          string `json:"addonVersion"`
	ServiceAccountRoleArn string `json:"serviceAccountRoleArn"`
	ConfigurationValues   string `json:"configurationValues"`
	ResolveConflicts      string `json:"resolveConflicts"`
}

func (h *Handler) handleUpdateAddon(c *echo.Context, clusterName, addonName string, body []byte) error {
	var in updateAddonBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
		}
	}

	addon, err := h.Backend.UpdateAddon(
		clusterName, addonName, in.AddonVersion, in.ServiceAccountRoleArn,
		in.ConfigurationValues, in.ResolveConflicts,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyUpdate: map[string]any{
			"id":           uuid.NewString()[:8],
			keyStatusField: statusInProgress,
			keyType:        "AddonUpdate",
			keyClusterName: clusterName,
			"addonName":    addon.AddonName,
			keyCreatedAt:   float64(time.Now().Unix()),
		},
	})
}

func (h *Handler) handleDescribeAddonVersions(c *echo.Context) error {
	versions := h.Backend.DescribeAddonVersions()

	return c.JSON(http.StatusOK, map[string]any{
		"addons": versions,
	})
}

func (h *Handler) handleDescribeAddonConfiguration(c *echo.Context) error {
	addonName := c.Request().URL.Query().Get("addonName")
	addonVersion := c.Request().URL.Query().Get("addonVersion")

	if addonName == "" {
		addonName = addonVPCCNI
	}

	result := h.Backend.DescribeAddonConfiguration(addonName, addonVersion)

	return c.JSON(http.StatusOK, result)
}

func addonToJSON(a *Addon) map[string]any {
	m := map[string]any{
		keyClusterName: a.ClusterName,
		"addonName":    a.AddonName,
		"addonArn":     a.ARN,
		keyStatusField: a.Status,
		keyCreatedAt:   a.CreatedAt.Unix(),
		"addonVersion": a.AddonVersion,
	}

	if a.ServiceAccountRoleARN != "" {
		m["serviceAccountRoleArn"] = a.ServiceAccountRoleARN
	}

	if a.MarketplaceVersion != "" {
		m["marketplaceVersion"] = a.MarketplaceVersion
	}

	if a.Health != nil {
		m["health"] = map[string]any{
			"issues": a.Health.Issues,
		}
	}

	if a.Configuration != "" {
		m["configurationValues"] = a.Configuration
	}

	if a.ResolveConflicts != "" {
		m["resolveConflicts"] = a.ResolveConflicts
	}

	return m
}

// --- Capability ops ---

func (h *Handler) dispatchCapabilityOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opDeleteCapability:
		return true, h.handleDeleteCapability(c, route.clusterName)
	case opDescribeCapability:
		return true, h.handleDescribeCapability(c, route.clusterName)
	case opListCapabilities:
		return true, h.handleListCapabilities(c)
	case opUpdateCapability:
		return true, h.handleUpdateCapability(c, route.clusterName, body)
	}

	return false, nil
}

func (h *Handler) handleDeleteCapability(c *echo.Context, name string) error {
	capa, err := h.Backend.DeleteCapability(name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCapability: map[string]any{
			keyName:        capa.Name,
			keyVersion:     capa.Version,
			keyStatusField: capa.Status,
		},
	})
}

func (h *Handler) handleDescribeCapability(c *echo.Context, name string) error {
	capa, err := h.Backend.DescribeCapability(name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCapability: map[string]any{
			keyName:        capa.Name,
			keyVersion:     capa.Version,
			keyStatusField: capa.Status,
		},
	})
}

func (h *Handler) handleListCapabilities(c *echo.Context) error {
	names := h.Backend.ListCapabilities()

	return c.JSON(http.StatusOK, map[string]any{
		"capabilities": names,
	})
}

type updateCapabilityBody struct {
	Version string `json:"version"`
}

func (h *Handler) handleUpdateCapability(c *echo.Context, name string, body []byte) error {
	var in updateCapabilityBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
		}
	}

	capa, err := h.Backend.UpdateCapability(name, in.Version)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCapability: map[string]any{
			keyName:        capa.Name,
			keyVersion:     capa.Version,
			keyStatusField: capa.Status,
		},
	})
}

// --- Subscription ops ---

func (h *Handler) dispatchSubscriptionOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opDeleteEksAnywhereSubscription:
		return true, h.handleDeleteEksAnywhereSubscription(c, route.clusterName)
	case opDescribeEksAnywhereSubscription:
		return true, h.handleDescribeEksAnywhereSubscription(c, route.clusterName)
	case opListEksAnywhereSubscriptions:
		return true, h.handleListEksAnywhereSubscriptions(c)
	case opUpdateEksAnywhereSubscription:
		return true, h.handleUpdateEksAnywhereSubscription(c, route.clusterName, body)
	}

	return false, nil
}

func (h *Handler) handleDeleteEksAnywhereSubscription(c *echo.Context, id string) error {
	sub, err := h.Backend.DeleteEksAnywhereSubscription(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keySubscription: subscriptionToJSON(sub),
	})
}

func (h *Handler) handleDescribeEksAnywhereSubscription(c *echo.Context, id string) error {
	sub, err := h.Backend.DescribeEksAnywhereSubscription(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keySubscription: subscriptionToJSON(sub),
	})
}

func (h *Handler) handleListEksAnywhereSubscriptions(c *echo.Context) error {
	subs := h.Backend.ListEksAnywhereSubscriptions()

	result := make([]map[string]any, len(subs))
	for i, sub := range subs {
		result[i] = subscriptionToJSON(sub)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"subscriptions": result,
	})
}

type updateSubscriptionBody struct {
	LicenseQuantity *int32 `json:"licenseQuantity,omitempty"`
	LicenseType     string `json:"licenseType"`
}

func (h *Handler) handleUpdateEksAnywhereSubscription(c *echo.Context, id string, body []byte) error {
	var in updateSubscriptionBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
		}
	}

	sub, err := h.Backend.UpdateEksAnywhereSubscription(id, in.LicenseQuantity, in.LicenseType)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keySubscription: subscriptionToJSON(sub),
	})
}

func subscriptionToJSON(sub *AnywhereSubscription) map[string]any {
	return map[string]any{
		"id":              sub.ID,
		"arn":             sub.ARN,
		keyName:           sub.Name,
		keyStatusField:    sub.Status,
		"licenseType":     sub.LicenseType,
		"licenseQuantity": sub.LicenseQuantity,
		keyCreatedAt:      sub.CreatedAt.Unix(),
	}
}

// --- Fargate ops ---

func (h *Handler) dispatchFargateOps(c *echo.Context, route eksRoute) (bool, error) {
	switch route.operation {
	case opDeleteFargateProfile:
		return true, h.handleDeleteFargateProfile(c, route.clusterName, route.nodegroupName)
	case opDescribeFargateProfile:
		return true, h.handleDescribeFargateProfile(c, route.clusterName, route.nodegroupName)
	case opListFargateProfiles:
		return true, h.handleListFargateProfiles(c, route.clusterName)
	}

	return false, nil
}

func (h *Handler) handleDeleteFargateProfile(c *echo.Context, clusterName, profileName string) error {
	profile, err := h.Backend.DeleteFargateProfile(clusterName, profileName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"fargateProfile": fargateProfileToJSON(profile),
	})
}

func (h *Handler) handleDescribeFargateProfile(c *echo.Context, clusterName, profileName string) error {
	profile, err := h.Backend.DescribeFargateProfile(clusterName, profileName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"fargateProfile": fargateProfileToJSON(profile),
	})
}

func (h *Handler) handleListFargateProfiles(c *echo.Context, clusterName string) error {
	names, err := h.Backend.ListFargateProfiles(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"fargateProfileNames": names,
	})
}

func fargateProfileToJSON(p *FargateProfile) map[string]any {
	return map[string]any{
		keyClusterName:        p.ClusterName,
		"fargateProfileName":  p.FargateProfileName,
		"fargateProfileArn":   p.ARN,
		"podExecutionRoleArn": p.PodExecutionRoleARN,
		keyStatusField:        p.Status,
		"selectors":           p.Selectors,
		keyCreatedAt:          p.CreatedAt.Unix(),
	}
}

// --- Pod Identity ops ---

func (h *Handler) dispatchPodIdentityOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opDeletePodIdentityAssociation:
		return true, h.handleDeletePodIdentityAssociation(c, route.clusterName, route.nodegroupName)
	case opDescribePodIdentityAssociation:
		return true, h.handleDescribePodIdentityAssociation(c, route.clusterName, route.nodegroupName)
	case opListPodIdentityAssociations:
		return true, h.handleListPodIdentityAssociations(c, route.clusterName)
	case opUpdatePodIdentityAssociation:
		return true, h.handleUpdatePodIdentityAssociation(c, route.clusterName, route.nodegroupName, body)
	}

	return false, nil
}

func (h *Handler) handleDeletePodIdentityAssociation(c *echo.Context, clusterName, assocID string) error {
	assoc, err := h.Backend.DeletePodIdentityAssociation(clusterName, assocID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyAssociation: podIdentityToJSON(assoc),
	})
}

func (h *Handler) handleDescribePodIdentityAssociation(c *echo.Context, clusterName, assocID string) error {
	assoc, err := h.Backend.DescribePodIdentityAssociation(clusterName, assocID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyAssociation: podIdentityToJSON(assoc),
	})
}

func (h *Handler) handleListPodIdentityAssociations(c *echo.Context, clusterName string) error {
	assocs, err := h.Backend.ListPodIdentityAssociations(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	result := make([]map[string]any, len(assocs))
	for i, a := range assocs {
		result[i] = podIdentityToJSON(a)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"associations": result,
	})
}

type updatePodIdentityBody struct {
	RoleArn string `json:"roleArn"`
}

func (h *Handler) handleUpdatePodIdentityAssociation(c *echo.Context, clusterName, assocID string, body []byte) error {
	var in updatePodIdentityBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
		}
	}

	assoc, err := h.Backend.UpdatePodIdentityAssociation(clusterName, assocID, in.RoleArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyAssociation: podIdentityToJSON(assoc),
	})
}

func podIdentityToJSON(a *PodIdentityAssociation) map[string]any {
	return map[string]any{
		keyClusterName:   a.ClusterName,
		"associationId":  a.AssociationID,
		"associationArn": a.ARN,
		"namespace":      a.Namespace,
		"serviceAccount": a.ServiceAccount,
		"roleArn":        a.RoleARN,
		keyCreatedAt:     a.CreatedAt.Unix(),
	}
}

// --- Access ops ---

func (h *Handler) dispatchAccessOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opDescribeAccessEntry:
		return true, h.handleDescribeAccessEntry(c, route.clusterName, route.principalARN)
	case opListAccessEntries:
		return true, h.handleListAccessEntries(c, route.clusterName)
	case opUpdateAccessEntry:
		return true, h.handleUpdateAccessEntry(c, route.clusterName, route.principalARN, body)
	case opListAccessPolicies:
		return true, h.handleListAccessPolicies(c)
	case opListAssociatedAccessPolicies:
		return true, h.handleListAssociatedAccessPolicies(c, route.clusterName, route.principalARN)
	case opDisassociateAccessPolicy:
		return true, h.handleDisassociateAccessPolicy(c, route.clusterName, route.principalARN, route.resourceARN)
	}

	return false, nil
}

func (h *Handler) handleDescribeAccessEntry(c *echo.Context, clusterName, principalARN string) error {
	entry, err := h.Backend.DescribeAccessEntry(clusterName, principalARN)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"accessEntry": map[string]any{
			keyClusterName:   entry.ClusterName,
			keyPrincipalArn:  entry.PrincipalARN,
			"accessEntryArn": entry.ARN,
			keyType:          entry.Type,
			"username":       entry.Username,
			keyCreatedAt:     entry.CreatedAt.Unix(),
		},
	})
}

func (h *Handler) handleListAccessEntries(c *echo.Context, clusterName string) error {
	arns, err := h.Backend.ListAccessEntries(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"accessEntries": arns,
	})
}

type updateAccessEntryBody struct {
	Username string `json:"username"`
}

func (h *Handler) handleUpdateAccessEntry(c *echo.Context, clusterName, principalARN string, body []byte) error {
	var in updateAccessEntryBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
		}
	}

	entry, err := h.Backend.UpdateAccessEntry(clusterName, principalARN, in.Username)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"accessEntry": map[string]any{
			keyClusterName:   entry.ClusterName,
			keyPrincipalArn:  entry.PrincipalARN,
			"accessEntryArn": entry.ARN,
			keyType:          entry.Type,
			"username":       entry.Username,
			keyCreatedAt:     entry.CreatedAt.Unix(),
		},
	})
}

func (h *Handler) handleListAccessPolicies(c *echo.Context) error {
	policies := h.Backend.ListAccessPolicies()

	return c.JSON(http.StatusOK, map[string]any{
		"accessPolicies": policies,
	})
}

func (h *Handler) handleListAssociatedAccessPolicies(c *echo.Context, clusterName, principalARN string) error {
	policies, err := h.Backend.ListAssociatedAccessPolicies(clusterName, principalARN)
	if err != nil {
		return h.handleError(c, err)
	}

	result := make([]map[string]any, len(policies))
	for i, p := range policies {
		result[i] = map[string]any{
			keyPolicyArn:   p.PolicyARN,
			"accessScope":  p.AccessScope,
			"associatedAt": p.AssociatedAt.Unix(),
		}
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyClusterName:             clusterName,
		keyPrincipalArn:            principalARN,
		"associatedAccessPolicies": result,
	})
}

func (h *Handler) handleDisassociateAccessPolicy(c *echo.Context, clusterName, principalARN, policyARN string) error {
	if err := h.Backend.DisassociateAccessPolicy(clusterName, principalARN, policyARN); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- IDP ops ---

func (h *Handler) dispatchIDPOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opDescribeIdentityProviderConfig:
		return true, h.handleDescribeIdentityProviderConfig(c, route.clusterName, body)
	case opListIdentityProviderConfigs:
		return true, h.handleListIdentityProviderConfigs(c, route.clusterName)
	case opDisassociateIdentityProviderConfig:
		return true, h.handleDisassociateIdentityProviderConfig(c, route.clusterName, body)
	}

	return false, nil
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

// --- Insights ops ---

func (h *Handler) dispatchInsightsOps(c *echo.Context, route eksRoute, _ []byte) (bool, error) {
	switch route.operation {
	case opDescribeInsight:
		return true, h.handleDescribeInsight(c, route.clusterName, route.nodegroupName)
	case opListInsights:
		return true, h.handleListInsights(c, route.clusterName)
	case opStartInsightsRefresh:
		return true, h.handleStartInsightsRefresh(c, route.clusterName)
	case opDescribeInsightsRefresh:
		return true, h.handleDescribeInsightsRefresh(c, route.clusterName, route.nodegroupName)
	}

	return false, nil
}

func (h *Handler) handleDescribeInsight(c *echo.Context, clusterName, insightID string) error {
	insight, err := h.Backend.DescribeInsight(clusterName, insightID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"insight": insightToJSON(insight),
	})
}

func (h *Handler) handleListInsights(c *echo.Context, clusterName string) error {
	insights, err := h.Backend.ListInsights(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	result := make([]map[string]any, len(insights))
	for i, ins := range insights {
		result[i] = insightToJSON(ins)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"insights": result,
	})
}

func (h *Handler) handleStartInsightsRefresh(c *echo.Context, clusterName string) error {
	refresh, err := h.Backend.StartInsightsRefresh(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"insightsRefresh": map[string]any{
			"id":           refresh.ID,
			keyClusterName: refresh.ClusterName,
			keyStatusField: refresh.Status,
			"startedAt":    refresh.StartedAt.Unix(),
		},
	})
}

func (h *Handler) handleDescribeInsightsRefresh(c *echo.Context, clusterName, refreshID string) error {
	refresh, err := h.Backend.DescribeInsightsRefresh(clusterName, refreshID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"insightsRefresh": map[string]any{
			"id":           refresh.ID,
			keyClusterName: refresh.ClusterName,
			keyStatusField: refresh.Status,
			"startedAt":    refresh.StartedAt.Unix(),
		},
	})
}

func insightToJSON(ins *Insight) map[string]any {
	m := map[string]any{
		"id":                 ins.ID,
		keyClusterName:       ins.ClusterName,
		"category":           ins.Category,
		keyStatusField:       ins.Status,
		"lastRefreshTime":    ins.LastRefreshTime.Unix(),
		"lastTransitionTime": ins.LastTransition.Unix(),
	}

	if ins.Description != "" {
		m["description"] = ins.Description
	}

	if ins.Recommendation != "" {
		m["recommendation"] = ins.Recommendation
	}

	return m
}

// --- Cluster update ops ---

func (h *Handler) dispatchClusterUpdateOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opUpdateClusterConfig:
		return true, h.handleUpdateClusterConfig(c, route.clusterName, body)
	case opUpdateClusterVersion:
		return true, h.handleUpdateClusterVersion(c, route.clusterName, body)
	case opUpdateNodegroupVersion:
		return true, h.handleUpdateNodegroupVersion(c, route.clusterName, route.nodegroupName, body)
	case opDescribeUpdate:
		return true, h.handleDescribeUpdate(c, route.clusterName, route.nodegroupName)
	case opListUpdates:
		return true, h.handleListUpdates(c, route.clusterName)
	case opRegisterCluster:
		return true, h.handleRegisterCluster(c, body)
	case opDeregisterCluster:
		return true, h.handleDeregisterCluster(c, route.clusterName)
	case opDescribeClusterVersions:
		return true, h.handleDescribeClusterVersions(c)
	}

	return false, nil
}

type updateClusterConfigLogging struct {
	ClusterLogging []struct {
		Types   []string `json:"types"`
		Enabled bool     `json:"enabled"`
	} `json:"clusterLogging"`
}

type updateClusterConfigBody struct {
	Logging *updateClusterConfigLogging `json:"logging"`
}

func (h *Handler) handleUpdateClusterConfig(c *echo.Context, clusterName string, body []byte) error {
	var in updateClusterConfigBody
	var logEntries []ClusterLogEntry

	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
		}

		if in.Logging != nil {
			logEntries = make([]ClusterLogEntry, len(in.Logging.ClusterLogging))
			for i, entry := range in.Logging.ClusterLogging {
				logEntries[i] = ClusterLogEntry{Types: entry.Types, Enabled: entry.Enabled}
			}
		}
	}

	update, err := h.Backend.UpdateClusterConfig(clusterName, logEntries)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyUpdate: updateToJSON(update),
	})
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

type registerClusterBody struct {
	Tags                    map[string]string `json:"tags"`
	Name                    string            `json:"name"`
	ConnectorConfigProvider string            `json:"connectorConfig.provider"`
	ConnectorConfigRoleArn  string            `json:"connectorConfig.roleArn"`
}

func (h *Handler) handleRegisterCluster(c *echo.Context, body []byte) error {
	var in registerClusterBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "name is required"))
	}

	cluster, err := h.Backend.RegisterCluster(in.Name, in.ConnectorConfigProvider, in.ConnectorConfigRoleArn, in.Tags)
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

func updateToJSON(u *Update) map[string]any {
	return map[string]any{
		"id":           u.ID,
		keyStatusField: u.Status,
		keyType:        u.Type,
		keyCreatedAt:   float64(u.CreatedAt.Unix()),
	}
}

// --- Node group version update routing ---
