package eks

import (
	"encoding/json"
	"net/http"
	"strings"
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
	case "DeleteAddon":
		return true, h.handleDeleteAddon(c, route.clusterName, route.nodegroupName)
	case "DescribeAddon":
		return true, h.handleDescribeAddon(c, route.clusterName, route.nodegroupName)
	case "ListAddons":
		return true, h.handleListAddons(c, route.clusterName)
	case "UpdateAddon":
		return true, h.handleUpdateAddon(c, route.clusterName, route.nodegroupName, body)
	case "DescribeAddonVersions":
		return true, h.handleDescribeAddonVersions(c)
	case "DescribeAddonConfiguration":
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
}

func (h *Handler) handleUpdateAddon(c *echo.Context, clusterName, addonName string, body []byte) error {
	var in updateAddonBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
		}
	}

	addon, err := h.Backend.UpdateAddon(clusterName, addonName, in.AddonVersion, in.ServiceAccountRoleArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"update": map[string]any{
			"id":          uuid.NewString()[:8],
			"status":      "InProgress",
			"type":        "AddonUpdate",
			"clusterName": clusterName,
			"addonName":   addon.AddonName,
			"createdAt":   float64(time.Now().Unix()),
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
		addonName = "vpc-cni"
	}

	result := h.Backend.DescribeAddonConfiguration(addonName, addonVersion)

	return c.JSON(http.StatusOK, result)
}

func addonToJSON(a *Addon) map[string]any {
	m := map[string]any{
		"clusterName":  a.ClusterName,
		"addonName":    a.AddonName,
		"addonArn":     a.ARN,
		"status":       a.Status,
		"createdAt":    a.CreatedAt.Unix(),
		"addonVersion": a.AddonVersion,
	}

	if a.ServiceAccountRoleARN != "" {
		m["serviceAccountRoleArn"] = a.ServiceAccountRoleARN
	}

	return m
}

// --- Capability ops ---

func (h *Handler) dispatchCapabilityOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case "DeleteCapability":
		return true, h.handleDeleteCapability(c, route.clusterName)
	case "DescribeCapability":
		return true, h.handleDescribeCapability(c, route.clusterName)
	case "ListCapabilities":
		return true, h.handleListCapabilities(c)
	case "UpdateCapability":
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
		"capability": map[string]any{
			"name":    capa.Name,
			"version": capa.Version,
			"status":  capa.Status,
		},
	})
}

func (h *Handler) handleDescribeCapability(c *echo.Context, name string) error {
	capa, err := h.Backend.DescribeCapability(name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"capability": map[string]any{
			"name":    capa.Name,
			"version": capa.Version,
			"status":  capa.Status,
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
		"capability": map[string]any{
			"name":    capa.Name,
			"version": capa.Version,
			"status":  capa.Status,
		},
	})
}

// --- Subscription ops ---

func (h *Handler) dispatchSubscriptionOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case "DeleteEksAnywhereSubscription":
		return true, h.handleDeleteEksAnywhereSubscription(c, route.clusterName)
	case "DescribeEksAnywhereSubscription":
		return true, h.handleDescribeEksAnywhereSubscription(c, route.clusterName)
	case "ListEksAnywhereSubscriptions":
		return true, h.handleListEksAnywhereSubscriptions(c)
	case "UpdateEksAnywhereSubscription":
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
		"subscription": subscriptionToJSON(sub),
	})
}

func (h *Handler) handleDescribeEksAnywhereSubscription(c *echo.Context, id string) error {
	sub, err := h.Backend.DescribeEksAnywhereSubscription(id)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"subscription": subscriptionToJSON(sub),
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
	LicenseType     string `json:"licenseType"`
	LicenseQuantity *int32 `json:"licenseQuantity,omitempty"`
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
		"subscription": subscriptionToJSON(sub),
	})
}

func subscriptionToJSON(sub *AnywhereSubscription) map[string]any {
	return map[string]any{
		"id":              sub.ID,
		"arn":             sub.ARN,
		"name":            sub.Name,
		"status":          sub.Status,
		"licenseType":     sub.LicenseType,
		"licenseQuantity": sub.LicenseQuantity,
		"createdAt":       sub.CreatedAt.Unix(),
	}
}

// --- Fargate ops ---

func (h *Handler) dispatchFargateOps(c *echo.Context, route eksRoute) (bool, error) {
	switch route.operation {
	case "DeleteFargateProfile":
		return true, h.handleDeleteFargateProfile(c, route.clusterName, route.nodegroupName)
	case "DescribeFargateProfile":
		return true, h.handleDescribeFargateProfile(c, route.clusterName, route.nodegroupName)
	case "ListFargateProfiles":
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
		"clusterName":         p.ClusterName,
		"fargateProfileName":  p.FargateProfileName,
		"fargateProfileArn":   p.ARN,
		"podExecutionRoleArn": p.PodExecutionRoleARN,
		"status":              p.Status,
		"selectors":           p.Selectors,
		"createdAt":           p.CreatedAt.Unix(),
	}
}

// --- Pod Identity ops ---

func (h *Handler) dispatchPodIdentityOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case "DeletePodIdentityAssociation":
		return true, h.handleDeletePodIdentityAssociation(c, route.clusterName, route.nodegroupName)
	case "DescribePodIdentityAssociation":
		return true, h.handleDescribePodIdentityAssociation(c, route.clusterName, route.nodegroupName)
	case "ListPodIdentityAssociations":
		return true, h.handleListPodIdentityAssociations(c, route.clusterName)
	case "UpdatePodIdentityAssociation":
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
		"association": podIdentityToJSON(assoc),
	})
}

func (h *Handler) handleDescribePodIdentityAssociation(c *echo.Context, clusterName, assocID string) error {
	assoc, err := h.Backend.DescribePodIdentityAssociation(clusterName, assocID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"association": podIdentityToJSON(assoc),
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
		"association": podIdentityToJSON(assoc),
	})
}

func podIdentityToJSON(a *PodIdentityAssociation) map[string]any {
	return map[string]any{
		"clusterName":    a.ClusterName,
		"associationId":  a.AssociationID,
		"associationArn": a.ARN,
		"namespace":      a.Namespace,
		"serviceAccount": a.ServiceAccount,
		"roleArn":        a.RoleARN,
		"createdAt":      a.CreatedAt.Unix(),
	}
}

// --- Access ops ---

func (h *Handler) dispatchAccessOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case "DescribeAccessEntry":
		return true, h.handleDescribeAccessEntry(c, route.clusterName, route.principalARN)
	case "ListAccessEntries":
		return true, h.handleListAccessEntries(c, route.clusterName)
	case "UpdateAccessEntry":
		return true, h.handleUpdateAccessEntry(c, route.clusterName, route.principalARN, body)
	case "ListAccessPolicies":
		return true, h.handleListAccessPolicies(c)
	case "ListAssociatedAccessPolicies":
		return true, h.handleListAssociatedAccessPolicies(c, route.clusterName, route.principalARN)
	case "DisassociateAccessPolicy":
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
			"clusterName":    entry.ClusterName,
			"principalArn":   entry.PrincipalARN,
			"accessEntryArn": entry.ARN,
			"type":           entry.Type,
			"username":       entry.Username,
			"createdAt":      entry.CreatedAt.Unix(),
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
			"clusterName":    entry.ClusterName,
			"principalArn":   entry.PrincipalARN,
			"accessEntryArn": entry.ARN,
			"type":           entry.Type,
			"username":       entry.Username,
			"createdAt":      entry.CreatedAt.Unix(),
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
			"policyArn":    p.PolicyARN,
			"accessScope":  p.AccessScope,
			"associatedAt": p.AssociatedAt.Unix(),
		}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"clusterName":            clusterName,
		"principalArn":           principalARN,
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
	case "DescribeIdentityProviderConfig":
		return true, h.handleDescribeIdentityProviderConfig(c, route.clusterName, body)
	case "ListIdentityProviderConfigs":
		return true, h.handleListIdentityProviderConfigs(c, route.clusterName)
	case "DisassociateIdentityProviderConfig":
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
			"clusterName": cfg.ClusterName,
			"name":        cfg.Name,
			"type":        cfg.Type,
			"status":      cfg.Status,
			"oidc":        cfg.OIDC,
			"createdAt":   cfg.CreatedAt.Unix(),
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
		"update": map[string]any{
			"id":          uuid.NewString()[:8],
			"status":      "InProgress",
			"type":        "DisassociateIdentityProviderConfig",
			"clusterName": clusterName,
		},
	})
}

// --- Insights ops ---

func (h *Handler) dispatchInsightsOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case "DescribeInsight":
		return true, h.handleDescribeInsight(c, route.clusterName, route.nodegroupName)
	case "ListInsights":
		return true, h.handleListInsights(c, route.clusterName)
	case "StartInsightsRefresh":
		return true, h.handleStartInsightsRefresh(c, route.clusterName)
	case "DescribeInsightsRefresh":
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
			"id":          refresh.ID,
			"clusterName": refresh.ClusterName,
			"status":      refresh.Status,
			"startedAt":   refresh.StartedAt.Unix(),
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
			"id":          refresh.ID,
			"clusterName": refresh.ClusterName,
			"status":      refresh.Status,
			"startedAt":   refresh.StartedAt.Unix(),
		},
	})
}

func insightToJSON(ins *Insight) map[string]any {
	m := map[string]any{
		"id":              ins.ID,
		"clusterName":     ins.ClusterName,
		"category":        ins.Category,
		"status":          ins.Status,
		"lastRefreshTime": ins.LastRefreshTime.Unix(),
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
	case "UpdateClusterConfig":
		return true, h.handleUpdateClusterConfig(c, route.clusterName)
	case "UpdateClusterVersion":
		return true, h.handleUpdateClusterVersion(c, route.clusterName, body)
	case "UpdateNodegroupVersion":
		return true, h.handleUpdateNodegroupVersion(c, route.clusterName, route.nodegroupName, body)
	case "DescribeUpdate":
		return true, h.handleDescribeUpdate(c, route.clusterName, route.nodegroupName)
	case "ListUpdates":
		return true, h.handleListUpdates(c, route.clusterName)
	case "RegisterCluster":
		return true, h.handleRegisterCluster(c, body)
	case "DeregisterCluster":
		return true, h.handleDeregisterCluster(c, route.clusterName)
	case "DescribeClusterVersions":
		return true, h.handleDescribeClusterVersions(c)
	}

	return false, nil
}

func (h *Handler) handleUpdateClusterConfig(c *echo.Context, clusterName string) error {
	update, err := h.Backend.UpdateClusterConfig(clusterName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"update": updateToJSON(update),
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
		"update": updateToJSON(update),
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
		"update": updateToJSON(update),
	})
}

func (h *Handler) handleDescribeUpdate(c *echo.Context, clusterName, updateID string) error {
	update, err := h.Backend.DescribeUpdate(clusterName, updateID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"update": updateToJSON(update),
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
	Tags                     map[string]string `json:"tags"`
	Name                     string            `json:"name"`
	ConnectorConfigProvider  string            `json:"connectorConfig.provider"`
	ConnectorConfigRoleArn   string            `json:"connectorConfig.roleArn"`
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
		"id":        u.ID,
		"status":    u.Status,
		"type":      u.Type,
		"createdAt": float64(u.CreatedAt.Unix()),
	}
}

// --- Node group version update routing ---

func parseNodegroupVersionRoute(method, clusterName string, parts []string) eksRoute {
	// /clusters/{name}/node-groups/{ng}/update-version
	ngName := parts[2]

	if strings.HasSuffix(ngName, "/update-version") {
		ngName = strings.TrimSuffix(ngName, "/update-version")
		if method == http.MethodPost {
			return eksRoute{operation: "UpdateNodegroupVersion", clusterName: clusterName, nodegroupName: ngName}
		}

		return eksRoute{operation: "Unknown"}
	}

	return eksRoute{operation: "Unknown"}
}
