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
	keyName           = "name"
	keyStatusField    = "status"
	keyVersion        = "version"
	keyCreatedAt      = "createdAt"
	keyNodegroup      = "nodegroup"
	keyUpdate         = "update"
	keyType           = "type"
	keyPrincipalArn   = "principalArn"
	keyUsername       = "username"
	keyPolicyArn      = "policyArn"
	keySubscription   = "subscription"
	keyFargateProfile = "fargateProfile"
	keyTags           = "tags"
	keyEnabled        = "enabled"
)

const (
	opUnknown         = "Unknown"
	keyAddons         = "addons"
	keyArn            = "arn"
	keyClusterName    = "clusterName"
	keyCluster        = "cluster"
	keyAccessEntry    = "accessEntry"
	keyAccessEntryArn = "accessEntryArn"
	keyAddon          = "addon"
	keyCapability     = "capability"
	keyAssociation    = "association"
)

const (
	opUpdateNodegroupConfig         = "UpdateNodegroupConfig"
	opUpdateNodegroupVersion        = "UpdateNodegroupVersion"
	opTagResource                   = "TagResource"
	opUntagResource                 = "UntagResource"
	opUpdateAccessEntry             = "UpdateAccessEntry"
	opUpdateAddon                   = "UpdateAddon"
	opUpdateCapability              = "UpdateCapability"
	opUpdateEksAnywhereSubscription = "UpdateEksAnywhereSubscription"
	opUpdatePodIdentityAssociation  = "UpdatePodIdentityAssociation"
	opStartInsightsRefresh          = "StartInsightsRefresh"
	opUpdateClusterConfig           = "UpdateClusterConfig"
	opUpdateClusterVersion          = "UpdateClusterVersion"
	opRegisterCluster               = "RegisterCluster"
)

const (
	opListClusters                 = "ListClusters"
	opListNodegroups               = "ListNodegroups"
	opListTagsForResource          = "ListTagsForResource"
	opListAccessEntries            = "ListAccessEntries"
	opListAccessPolicies           = "ListAccessPolicies"
	opListAssociatedAccessPolicies = "ListAssociatedAccessPolicies"
	opListAddons                   = "ListAddons"
	opListCapabilities             = "ListCapabilities"
	opListEksAnywhereSubscriptions = "ListEksAnywhereSubscriptions"
	opListFargateProfiles          = "ListFargateProfiles"
	opListPodIdentityAssociations  = "ListPodIdentityAssociations"
	opListIdentityProviderConfigs  = "ListIdentityProviderConfigs"
	opListInsights                 = "ListInsights"
	opListUpdates                  = "ListUpdates"
)

const (
	opDescribeNodegroup                  = "DescribeNodegroup"
	opDisassociateAccessPolicy           = "DisassociateAccessPolicy"
	opDescribePodIdentityAssociation     = "DescribePodIdentityAssociation"
	opDisassociateIdentityProviderConfig = "DisassociateIdentityProviderConfig"
	opDescribeInsight                    = "DescribeInsight"
	opDescribeInsightsRefresh            = "DescribeInsightsRefresh"
	opDescribeUpdate                     = "DescribeUpdate"
)

const (
	opAssociateAccessPolicy           = "AssociateAccessPolicy"
	opAssociateEncryptionConfig       = "AssociateEncryptionConfig"
	opAssociateIdentityProviderConfig = "AssociateIdentityProviderConfig"
	opCreateAccessEntry               = "CreateAccessEntry"
	opCreateAddon                     = "CreateAddon"
	opCreateCapability                = "CreateCapability"
	opCreateCluster                   = "CreateCluster"
	opCreateEksAnywhereSubscription   = "CreateEksAnywhereSubscription"
	opCreateFargateProfile            = "CreateFargateProfile"
	opCreateNodegroup                 = "CreateNodegroup"
	opCreatePodIdentityAssociation    = "CreatePodIdentityAssociation"
	opDeleteAccessEntry               = "DeleteAccessEntry"
	opDeleteAddon                     = "DeleteAddon"
	opDeleteCapability                = "DeleteCapability"
	opDeleteCluster                   = "DeleteCluster"
	opDeleteEksAnywhereSubscription   = "DeleteEksAnywhereSubscription"
	opDeleteFargateProfile            = "DeleteFargateProfile"
	opDeleteNodegroup                 = "DeleteNodegroup"
	opDeletePodIdentityAssociation    = "DeletePodIdentityAssociation"
	opDeregisterCluster               = "DeregisterCluster"
	opDescribeAccessEntry             = "DescribeAccessEntry"
	opDescribeAddon                   = "DescribeAddon"
	opDescribeAddonConfiguration      = "DescribeAddonConfiguration"
	opDescribeAddonVersions           = "DescribeAddonVersions"
	opDescribeCapability              = "DescribeCapability"
	opDescribeCluster                 = "DescribeCluster"
	opDescribeClusterVersions         = "DescribeClusterVersions"
	opDescribeEksAnywhereSubscription = "DescribeEksAnywhereSubscription"
	opDescribeFargateProfile          = "DescribeFargateProfile"
	opDescribeIdentityProviderConfig  = "DescribeIdentityProviderConfig"
)

const (
	maxTagKeyLen  = 128
	maxTagValLen  = 256
	maxTagsPerRes = 50
)

const (
	eksMatchPriority = service.PriorityPathVersioned

	pathClusters           = "/clusters"
	pathEKSTags            = "/tags/"
	pathCapabilities       = "/capabilities"
	pathSubscriptions      = "/subscriptions"
	pathAccessPolicies     = "/access-policies"
	pathAddonVersions      = "/addon-versions"
	pathAddonConfiguration = "/addon-configuration"
	pathClusterVersions    = "/cluster-versions"
)

// Handler is the Echo HTTP handler for AWS EKS operations (REST-JSON protocol).
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new EKS handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset clears all backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Name returns the service name.
func (h *Handler) Name() string { return "EKS" }

// GetSupportedOperations returns the list of supported EKS operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateCluster,
		opDescribeCluster,
		opListClusters,
		opDeleteCluster,
		opCreateNodegroup,
		opDescribeNodegroup,
		opListNodegroups,
		opDeleteNodegroup,
		opUpdateNodegroupConfig,
		opUpdateNodegroupVersion,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
		opAssociateAccessPolicy,
		opAssociateEncryptionConfig,
		opAssociateIdentityProviderConfig,
		opCreateAccessEntry,
		opDeleteAccessEntry,
		opDescribeAccessEntry,
		opListAccessEntries,
		opUpdateAccessEntry,
		opListAccessPolicies,
		opListAssociatedAccessPolicies,
		opDisassociateAccessPolicy,
		opCreateAddon,
		opDeleteAddon,
		opDescribeAddon,
		opDescribeAddonConfiguration,
		opDescribeAddonVersions,
		opListAddons,
		opUpdateAddon,
		opCreateCapability,
		opDeleteCapability,
		opDescribeCapability,
		opListCapabilities,
		opUpdateCapability,
		opCreateEksAnywhereSubscription,
		opDeleteEksAnywhereSubscription,
		opDescribeEksAnywhereSubscription,
		opListEksAnywhereSubscriptions,
		opUpdateEksAnywhereSubscription,
		opCreateFargateProfile,
		opDeleteFargateProfile,
		opDescribeFargateProfile,
		opListFargateProfiles,
		opCreatePodIdentityAssociation,
		opDeletePodIdentityAssociation,
		opDescribePodIdentityAssociation,
		opListPodIdentityAssociations,
		opUpdatePodIdentityAssociation,
		opDescribeIdentityProviderConfig,
		opListIdentityProviderConfigs,
		opDisassociateIdentityProviderConfig,
		opDescribeInsight,
		opListInsights,
		opStartInsightsRefresh,
		opDescribeInsightsRefresh,
		opUpdateClusterConfig,
		opUpdateClusterVersion,
		opDescribeUpdate,
		opListUpdates,
		opRegisterCluster,
		opDeregisterCluster,
		opDescribeClusterVersions,
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
			strings.HasPrefix(path, pathCapabilities+"/") ||
			path == pathSubscriptions ||
			strings.HasPrefix(path, pathSubscriptions+"/") ||
			path == pathAccessPolicies ||
			path == pathAddonVersions ||
			strings.HasPrefix(path, pathAddonConfiguration) ||
			path == pathClusterVersions
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

	switch method {
	case http.MethodGet:
		return eksRoute{operation: opDescribeNodegroup, clusterName: clusterName, nodegroupName: tail}
	case http.MethodDelete:
		return eksRoute{operation: opDeleteNodegroup, clusterName: clusterName, nodegroupName: tail}
	case http.MethodPost:
		return eksRoute{operation: opUpdateNodegroupConfig, clusterName: clusterName, nodegroupName: tail}
	}

	return eksRoute{operation: opUnknown}
}

// parseAccessEntryRoute returns the route for access entry paths:
// /clusters/{name}/access-entries[/{principalArn}[/access-policies[/{policyArn}]]].
func parseAccessEntryRoute(method, clusterName string, parts []string) eksRoute {
	const accessEntryParts = 2

	if len(parts) == accessEntryParts {
		switch method {
		case http.MethodPost:
			return eksRoute{operation: opCreateAccessEntry, clusterName: clusterName}
		case http.MethodGet:
			return eksRoute{operation: opListAccessEntries, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	// parts[2] may be principalArn, principalArn/access-policies, or principalArn/access-policies/{policyArn}
	tail := parts[2]

	// Check for /access-policies/{policyArn} suffix (DELETE for disassociate)
	if before, after, ok := strings.Cut(tail, "/access-policies/"); ok {
		principalARN := before
		policyARN := after

		if method == http.MethodDelete {
			return eksRoute{
				operation:    opDisassociateAccessPolicy,
				clusterName:  clusterName,
				principalARN: principalARN,
				resourceARN:  policyARN,
			}
		}

		return eksRoute{operation: opUnknown}
	}

	if before, ok := strings.CutSuffix(tail, "/access-policies"); ok {
		principalARN := before

		switch method {
		case http.MethodPost:
			return eksRoute{operation: opAssociateAccessPolicy, clusterName: clusterName, principalARN: principalARN}
		case http.MethodGet:
			return eksRoute{
				operation:    opListAssociatedAccessPolicies,
				clusterName:  clusterName,
				principalARN: principalARN,
			}
		}

		return eksRoute{operation: opUnknown}
	}

	// plain principalArn
	switch method {
	case http.MethodDelete:
		return eksRoute{operation: opDeleteAccessEntry, clusterName: clusterName, principalARN: tail}
	case http.MethodGet:
		return eksRoute{operation: opDescribeAccessEntry, clusterName: clusterName, principalARN: tail}
	case http.MethodPut:
		return eksRoute{operation: opUpdateAccessEntry, clusterName: clusterName, principalARN: tail}
	}

	return eksRoute{operation: opUnknown}
}

// parseAddonRoute returns the route for /clusters/{name}/addons[/{addonName}] paths.
func parseAddonRoute(method, clusterName string, parts []string) eksRoute {
	const addonParts = 2

	if len(parts) == addonParts {
		switch method {
		case http.MethodPost:
			return eksRoute{operation: opCreateAddon, clusterName: clusterName}
		case http.MethodGet:
			return eksRoute{operation: opListAddons, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	addonName := parts[2]

	switch method {
	case http.MethodGet:
		return eksRoute{operation: opDescribeAddon, clusterName: clusterName, nodegroupName: addonName}
	case http.MethodDelete:
		return eksRoute{operation: opDeleteAddon, clusterName: clusterName, nodegroupName: addonName}
	case http.MethodPut:
		return eksRoute{operation: opUpdateAddon, clusterName: clusterName, nodegroupName: addonName}
	}

	return eksRoute{operation: opUnknown}
}

// parseFargateProfileRoute returns the route for /clusters/{name}/fargate-profiles[/{profileName}] paths.
func parseFargateProfileRoute(method, clusterName string, parts []string) eksRoute {
	const fargateProfileParts = 2

	if len(parts) == fargateProfileParts {
		switch method {
		case http.MethodPost:
			return eksRoute{operation: opCreateFargateProfile, clusterName: clusterName}
		case http.MethodGet:
			return eksRoute{operation: opListFargateProfiles, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	profileName := parts[2]

	switch method {
	case http.MethodGet:
		return eksRoute{operation: opDescribeFargateProfile, clusterName: clusterName, nodegroupName: profileName}
	case http.MethodDelete:
		return eksRoute{operation: opDeleteFargateProfile, clusterName: clusterName, nodegroupName: profileName}
	}

	return eksRoute{operation: opUnknown}
}

// parsePodIdentityRoute returns the route for /clusters/{name}/pod-identity-associations[/{id}] paths.
func parsePodIdentityRoute(method, clusterName string, parts []string) eksRoute {
	const podIdentityParts = 2

	if len(parts) == podIdentityParts {
		switch method {
		case http.MethodPost:
			return eksRoute{operation: opCreatePodIdentityAssociation, clusterName: clusterName}
		case http.MethodGet:
			return eksRoute{operation: opListPodIdentityAssociations, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	assocID := parts[2]

	switch method {
	case http.MethodGet:
		return eksRoute{operation: opDescribePodIdentityAssociation, clusterName: clusterName, nodegroupName: assocID}
	case http.MethodDelete:
		return eksRoute{operation: opDeletePodIdentityAssociation, clusterName: clusterName, nodegroupName: assocID}
	case http.MethodPut:
		return eksRoute{operation: opUpdatePodIdentityAssociation, clusterName: clusterName, nodegroupName: assocID}
	}

	return eksRoute{operation: opUnknown}
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

// parseClusterSubPath handles /clusters/{name}/... paths after extracting clusterName.
func parseClusterSubPath(method, clusterName string, parts []string) eksRoute {
	const maxPathParts = 3

	// /clusters/{name}
	if len(parts) == 1 {
		switch method {
		case http.MethodGet:
			return eksRoute{operation: opDescribeCluster, clusterName: clusterName}
		case http.MethodDelete:
			return eksRoute{operation: opDeleteCluster, clusterName: clusterName}
		case http.MethodPut:
			return eksRoute{operation: opUpdateClusterConfig, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	switch parts[1] {
	case "node-groups":
		return parseNodegroupRoute(method, clusterName, parts)
	case "access-entries":
		return parseAccessEntryRoute(method, clusterName, parts)
	case keyAddons:
		return parseAddonRoute(method, clusterName, parts)
	case "fargate-profiles":
		return parseFargateProfileRoute(method, clusterName, parts)
	case "insights":
		return parseInsightsRoute(method, clusterName, parts)
	case "updates":
		return parseUpdatesRoute(method, clusterName, parts)
	case "update-version", "register", "deregister":
		return parseClusterLifecycleRoute(method, clusterName, parts)
	}

	return parseClusterAssocPath(method, clusterName, parts, maxPathParts)
}

// parseInsightsRoute returns the route for /clusters/{name}/insights[/{id}[/refresh[/{refreshId}]]].
func parseInsightsRoute(method, clusterName string, parts []string) eksRoute {
	const insightsParts = 2

	if len(parts) == insightsParts {
		if method == http.MethodGet {
			return eksRoute{operation: opListInsights, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	tail := parts[2]

	if before, ok := strings.CutSuffix(tail, "/refresh"); ok {
		if method == http.MethodPost {
			return eksRoute{operation: opStartInsightsRefresh, clusterName: clusterName, nodegroupName: before}
		}

		return eksRoute{operation: opUnknown}
	}

	if _, after, ok := strings.Cut(tail, "/refresh/"); ok {
		refreshID := after
		if method == http.MethodGet {
			return eksRoute{operation: opDescribeInsightsRefresh, clusterName: clusterName, nodegroupName: refreshID}
		}

		return eksRoute{operation: opUnknown}
	}

	if method == http.MethodGet {
		return eksRoute{operation: opDescribeInsight, clusterName: clusterName, nodegroupName: tail}
	}

	return eksRoute{operation: opUnknown}
}

// parseUpdatesRoute returns the route for /clusters/{name}/updates[/{id}].
func parseUpdatesRoute(method, clusterName string, parts []string) eksRoute {
	const updatesParts = 2

	if len(parts) == updatesParts {
		if method == http.MethodGet {
			return eksRoute{operation: opListUpdates, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	updateID := parts[2]

	if method == http.MethodGet {
		return eksRoute{operation: opDescribeUpdate, clusterName: clusterName, nodegroupName: updateID}
	}

	return eksRoute{operation: opUnknown}
}

// parseClusterAssocPath handles associate paths and pod-identity-associations.
func parseClusterAssocPath(method, clusterName string, parts []string, maxParts int) eksRoute {
	if parts[1] == "encryption-config" && len(parts) == maxParts && parts[2] == "associate" {
		if method == http.MethodPost {
			return eksRoute{operation: opAssociateEncryptionConfig, clusterName: clusterName}
		}

		return eksRoute{operation: opUnknown}
	}

	if parts[1] == "identity-provider-configs" {
		return parseIdentityProviderRoute(method, clusterName, parts, maxParts)
	}

	if parts[1] == "pod-identity-associations" {
		return parsePodIdentityRoute(method, clusterName, parts)
	}

	return eksRoute{operation: opUnknown}
}

// parseEKSPath maps HTTP method + path to an operation name and resource identifiers.
func parseEKSPath(method, rawPath string) eksRoute {
	path, _ := url.PathUnescape(rawPath)

	if r, ok := parseGlobalEKSPath(method, path); ok {
		return r
	}

	// /clusters and /clusters/{name}/...
	if !strings.HasPrefix(path, pathClusters) {
		return eksRoute{operation: opUnknown}
	}

	rest := strings.TrimPrefix(path, pathClusters)

	// /clusters
	if rest == "" {
		if method == http.MethodPost {
			return eksRoute{operation: opCreateCluster}
		}
		if method == http.MethodGet {
			return eksRoute{operation: opListClusters}
		}

		return eksRoute{operation: opUnknown}
	}

	// /clusters/{name}[/...]
	rest = strings.TrimPrefix(rest, "/")

	const maxPathParts = 3

	parts := strings.SplitN(rest, "/", maxPathParts)

	return parseClusterSubPath(method, parts[0], parts)
}

func parseGlobalEKSPath(method, path string) (eksRoute, bool) {
	// /tags/{resourceArn}
	if after, ok := strings.CutPrefix(path, pathEKSTags); ok {
		switch method {
		case http.MethodPost:
			return eksRoute{operation: opTagResource, resourceARN: after}, true
		case http.MethodDelete:
			return eksRoute{operation: opUntagResource, resourceARN: after}, true
		case http.MethodGet:
			return eksRoute{operation: opListTagsForResource, resourceARN: after}, true
		}

		return eksRoute{operation: opUnknown}, true
	}

	if r, ok := parseStaticEKSPath(method, path); ok {
		return r, true
	}

	return parseResourceEKSPath(method, path)
}

func parseStaticEKSPath(method, path string) (eksRoute, bool) {
	switch path {
	case pathAccessPolicies:
		if method == http.MethodGet {
			return eksRoute{operation: opListAccessPolicies}, true
		}

		return eksRoute{operation: opUnknown}, true
	case pathAddonVersions:
		if method == http.MethodGet {
			return eksRoute{operation: opDescribeAddonVersions}, true
		}

		return eksRoute{operation: opUnknown}, true
	case pathClusterVersions:
		if method == http.MethodGet {
			return eksRoute{operation: opDescribeClusterVersions}, true
		}

		return eksRoute{operation: opUnknown}, true
	}

	return eksRoute{}, false
}

func parseResourceEKSPath(method, path string) (eksRoute, bool) {
	if strings.HasPrefix(path, pathAddonConfiguration) {
		if method == http.MethodGet {
			return eksRoute{operation: opDescribeAddonConfiguration}, true
		}

		return eksRoute{operation: opUnknown}, true
	}

	if path == pathCapabilities {
		if method == http.MethodPost {
			return eksRoute{operation: opCreateCapability}, true
		}
		if method == http.MethodGet {
			return eksRoute{operation: opListCapabilities}, true
		}

		return eksRoute{operation: opUnknown}, true
	}

	if after, ok := strings.CutPrefix(path, pathCapabilities+"/"); ok {
		switch method {
		case http.MethodGet:
			return eksRoute{operation: opDescribeCapability, clusterName: after}, true
		case http.MethodDelete:
			return eksRoute{operation: opDeleteCapability, clusterName: after}, true
		case http.MethodPut:
			return eksRoute{operation: opUpdateCapability, clusterName: after}, true
		}

		return eksRoute{operation: opUnknown}, true
	}

	return parseSubscriptionEKSPath(method, path)
}

func parseSubscriptionEKSPath(method, path string) (eksRoute, bool) {
	if path == pathSubscriptions {
		if method == http.MethodPost {
			return eksRoute{operation: opCreateEksAnywhereSubscription}, true
		}
		if method == http.MethodGet {
			return eksRoute{operation: opListEksAnywhereSubscriptions}, true
		}

		return eksRoute{operation: opUnknown}, true
	}

	if after, ok := strings.CutPrefix(path, pathSubscriptions+"/"); ok {
		switch method {
		case http.MethodGet:
			return eksRoute{operation: opDescribeEksAnywhereSubscription, clusterName: after}, true
		case http.MethodDelete:
			return eksRoute{operation: opDeleteEksAnywhereSubscription, clusterName: after}, true
		case http.MethodPut:
			return eksRoute{operation: opUpdateEksAnywhereSubscription, clusterName: after}, true
		}

		return eksRoute{operation: opUnknown}, true
	}

	return eksRoute{}, false
}

func parseClusterLifecycleRoute(method, clusterName string, parts []string) eksRoute {
	switch parts[1] {
	case "update-version":
		if method == http.MethodPost {
			return eksRoute{operation: opUpdateClusterVersion, clusterName: clusterName}
		}
	case "register":
		if method == http.MethodPost {
			return eksRoute{operation: opRegisterCluster}
		}
	case "deregister":
		if method == http.MethodPost {
			return eksRoute{operation: opDeregisterCluster, clusterName: clusterName}
		}
	}

	return eksRoute{operation: opUnknown}
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

		log.Debug("eks request", "operation", route.operation, keyCluster, route.clusterName)

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
	if handled, err := h.dispatchClusterAndTagOps(c, route, body); handled {
		return err
	}

	if handled, err := h.dispatchNodegroupAndEntryOps(c, route, body); handled {
		return err
	}

	if handled, err := h.dispatchNewOps(c, route, body); handled {
		return err
	}

	if handled, err := h.dispatchRemainingOps(c, route, body); handled {
		return err
	}

	return c.JSON(http.StatusNotFound, errResp("ResourceNotFoundException", "unknown operation: "+route.operation))
}

// dispatchClusterAndTagOps handles cluster CRUD and tag operations.
func (h *Handler) dispatchClusterAndTagOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opCreateCluster:
		return true, h.handleCreateCluster(c, body)
	case opDescribeCluster:
		return true, h.handleDescribeCluster(c, route.clusterName)
	case opListClusters:
		return true, h.handleListClusters(c)
	case opDeleteCluster:
		return true, h.handleDeleteCluster(c, route.clusterName)
	case opTagResource:
		return true, h.handleTagResource(c, route.resourceARN, body)
	case opUntagResource:
		return true, h.handleUntagResource(c, route.resourceARN)
	case opListTagsForResource:
		return true, h.handleListTagsForResource(c, route.resourceARN)
	}

	return false, nil
}

// dispatchNodegroupAndEntryOps handles nodegroup and access entry operations.
func (h *Handler) dispatchNodegroupAndEntryOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
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
	case opCreateAccessEntry:
		return true, h.handleCreateAccessEntry(c, route.clusterName, body)
	case opDeleteAccessEntry:
		return true, h.handleDeleteAccessEntry(c, route.clusterName, route.principalARN)
	case opAssociateAccessPolicy:
		return true, h.handleAssociateAccessPolicy(c, route.clusterName, route.principalARN, body)
	}

	return false, nil
}

// dispatchNewOps handles the newer EKS operations added in the initial implementation.
func (h *Handler) dispatchNewOps(c *echo.Context, route eksRoute, body []byte) (bool, error) {
	switch route.operation {
	case opAssociateEncryptionConfig:
		return true, h.handleAssociateEncryptionConfig(c, route.clusterName, body)
	case opAssociateIdentityProviderConfig:
		return true, h.handleAssociateIdentityProviderConfig(c, route.clusterName, body)
	case opCreateAddon:
		return true, h.handleCreateAddon(c, route.clusterName, body)
	case opCreateCapability:
		return true, h.handleCreateCapability(c, body)
	case opCreateEksAnywhereSubscription:
		return true, h.handleCreateEksAnywhereSubscription(c, body)
	case opCreateFargateProfile:
		return true, h.handleCreateFargateProfile(c, route.clusterName, body)
	case opCreatePodIdentityAssociation:
		return true, h.handleCreatePodIdentityAssociation(c, route.clusterName, body)
	}

	return false, nil
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errResp("ResourceNotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errResp("ResourceInUseException", err.Error()))
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterValueException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errResp("InternalFailure", err.Error()))
	}
}

func errResp(code, msg string) map[string]string {
	return map[string]string{"code": code, "message": msg}
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

	return m
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

// nodegroupToJSON converts a Nodegroup to a JSON-serializable map.
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

// --- Cluster handlers ---

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
		keyCluster: clusterToJSON(cluster),
	})
}

// --- Nodegroup handlers ---

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
		keyNodegroup: nodegroupToJSON(ng),
	})
}

// validateTagMap checks AWS EKS tag constraints: key 1-128 chars, value 0-256 chars,
// max 50 tags per resource. existingCount is the number of tags already on the resource.
func validateTagMap(kv map[string]string, existingCount int) error {
	if existingCount+len(kv) > maxTagsPerRes {
		return ErrValidation
	}

	for k, v := range kv {
		if len(k) == 0 || len(k) > maxTagKeyLen {
			return ErrValidation
		}

		if len(v) > maxTagValLen {
			return ErrValidation
		}
	}

	return nil
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

	existing, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return h.handleError(c, err)
	}

	if err := validateTagMap(in.Tags, len(existing)); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException",
			"tag key must be 1-128 chars, value 0-256 chars, max 50 tags per resource"))
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
		keyTags: t,
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

	return c.JSON(http.StatusOK, map[string]any{
		keyUpdate: map[string]any{
			"id":            uuid.NewString()[:8],
			keyStatusField:  statusInProgress,
			keyType:         "ConfigUpdate",
			keyCreatedAt:    float64(time.Now().Unix()),
			keyClusterName:  clusterName,
			"nodegroupName": ng.NodegroupName,
		},
	})
}

// --- New operation handlers ---

type createAccessEntryBody struct {
	Tags             map[string]string `json:"tags"`
	PrincipalArn     string            `json:"principalArn"`
	Type             string            `json:"type"`
	Username         string            `json:"username"`
	KubernetesGroups []string          `json:"kubernetesGroups"`
}

func accessEntryToJSON(entry *AccessEntry) map[string]any {
	m := map[string]any{
		keyClusterName:    entry.ClusterName,
		keyPrincipalArn:   entry.PrincipalARN,
		keyAccessEntryArn: entry.ARN,
		keyType:           entry.Type,
		keyUsername:       entry.Username,
		keyCreatedAt:      entry.CreatedAt.Unix(),
	}

	if len(entry.KubernetesGroups) > 0 {
		m["kubernetesGroups"] = entry.KubernetesGroups
	} else {
		m["kubernetesGroups"] = []string{}
	}

	if entry.Tags != nil {
		m[keyTags] = entry.Tags.Clone()
	} else {
		m[keyTags] = map[string]string{}
	}

	return m
}

func (h *Handler) handleCreateAccessEntry(c *echo.Context, clusterName string, body []byte) error {
	var in createAccessEntryBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.PrincipalArn == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "principalArn is required"))
	}

	entry, err := h.Backend.CreateAccessEntry(
		clusterName,
		in.PrincipalArn,
		in.Type,
		in.Username,
		in.KubernetesGroups,
		in.Tags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyAccessEntry: accessEntryToJSON(entry),
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
			keyClusterName:  assoc.ClusterName,
			keyPrincipalArn: assoc.PrincipalARN,
			keyPolicyArn:    assoc.PolicyARN,
			"associatedAt":  assoc.AssociatedAt.Unix(),
			"accessScope":   assoc.AccessScope,
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

type oidcConfigJSON struct {
	ClientID       string `json:"clientId"`
	GroupsClaim    string `json:"groupsClaim,omitempty"`
	GroupsPrefix   string `json:"groupsPrefix,omitempty"`
	IssuerURL      string `json:"issuerUrl"`
	UsernameClaim  string `json:"usernameClaim,omitempty"`
	UsernamePrefix string `json:"usernamePrefix,omitempty"`
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

	// Use clientId as the config name (unique per cluster).
	configName := in.Oidc.ClientID

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

type createAddonBody struct {
	Tags                  map[string]string `json:"tags"`
	AddonName             string            `json:"addonName"`
	AddonVersion          string            `json:"addonVersion"`
	ServiceAccountRoleArn string            `json:"serviceAccountRoleArn"`
	ConfigurationValues   string            `json:"configurationValues"`
	ResolveConflicts      string            `json:"resolveConflicts"`
}

func (h *Handler) handleCreateAddon(c *echo.Context, clusterName string, body []byte) error {
	var in createAddonBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "invalid request body"))
	}

	if in.AddonName == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", "addonName is required"))
	}

	addon, err := h.Backend.CreateAddon(
		clusterName, in.AddonName, in.AddonVersion, in.ServiceAccountRoleArn,
		in.ConfigurationValues, in.ResolveConflicts,
		in.Tags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyAddon: addonToJSON(addon),
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

	capa, err := h.Backend.CreateCapability(in.Name, in.Version)
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
		keySubscription: map[string]any{
			"id":              sub.ID,
			keyArn:            sub.ARN,
			keyName:           sub.Name,
			keyStatusField:    sub.Status,
			"licenseType":     sub.LicenseType,
			"licenseQuantity": sub.LicenseQuantity,
			keyCreatedAt:      sub.CreatedAt.Unix(),
		},
	})
}

type fargateProfileSelectorJSON struct {
	Labels    map[string]string `json:"labels,omitempty"`
	Namespace string            `json:"namespace"`
}

type createFargateProfileBody struct {
	Tags                map[string]string            `json:"tags"`
	Subnets             []string                     `json:"subnets"`
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
		selectors[i] = FargateProfileSelector(s)
	}

	profile, err := h.Backend.CreateFargateProfile(
		clusterName,
		in.FargateProfileName,
		in.PodExecutionRoleArn,
		selectors,
		in.Subnets,
		in.Tags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyFargateProfile: fargateProfileToJSON(profile),
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

	assoc, err := h.Backend.CreatePodIdentityAssociation(
		clusterName,
		in.Namespace,
		in.ServiceAccount,
		in.RoleArn,
		in.Tags,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyAssociation: podIdentityToJSON(assoc),
	})
}
