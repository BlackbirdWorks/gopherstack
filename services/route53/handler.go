package route53

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	statusInsync = "INSYNC"
)

const (
	opDeactivateKeySigningKey = "DeactivateKeySigningKey"
)

const (
	route53PathPrefix           = "/2013-04-01/"
	route53HostedZone           = "/2013-04-01/hostedzone"
	route53Namespace            = "https://route53.amazonaws.com/doc/2013-04-01/"
	route53RRSetSuffix          = "/rrset"
	route53HZPrefix             = "/2013-04-01/hostedzone/"
	route53TagsPrefix           = "/2013-04-01/tags/"
	route53ChangePrefix         = "/2013-04-01/change/"
	route53HealthCheckRoot      = "/2013-04-01/healthcheck"
	route53HealthCheckPrefix    = "/2013-04-01/healthcheck/"
	route53StatusSuffix         = "/status"
	route53KSKRoot              = "/2013-04-01/keysigningkey"
	route53KSKPrefix            = "/2013-04-01/keysigningkey/"
	route53ActivateSuffix       = "/activate"
	route53AssociateVPCSuffix   = "/associatevpc"
	route53CidrCollectionRoot   = "/2013-04-01/cidrcollection"
	route53CidrCollectionPrefix = "/2013-04-01/cidrcollection/"
	route53QueryLoggingRoot     = "/2013-04-01/queryloggingconfig"
	route53DelegationSetRoot    = "/2013-04-01/delegationset"
	route53TrafficPolicyRoot    = "/2013-04-01/trafficpolicy"
	route53TrafficPolicyPrefix  = "/2013-04-01/trafficpolicy/"
	route53TPInstanceRoot       = "/2013-04-01/trafficpolicyinstance"
	// zoneIDAndRest is the number of parts when splitting a zone path at the first "/".
	zoneIDAndRest = 2
)

const (
	route53DNSSECSuffix          = "/dnssec"
	route53EnableDNSSECSuffix    = "/enable-dnssec"
	route53DisableDNSSECSuffix   = "/disable-dnssec"
	route53DeactivateSuffix      = "/deactivate"
	route53TrafficPoliciesRoot   = "/2013-04-01/trafficpolicies"
	route53TrafficPoliciesPrefix = "/2013-04-01/trafficpolicies/"
	route53TPInstancesRoot       = "/2013-04-01/trafficpolicyinstances"
	route53TPInstanceCount       = "/2013-04-01/trafficpolicyinstancecount"
	route53TPInstancePrefix      = "/2013-04-01/trafficpolicyinstance/"
)

// Handler is the HTTP service handler for Route 53 operations.
type Handler struct {
	Backend StorageBackend
	tags    map[string]*svcTags.Tags
	tagsMu  *lockmetrics.RWMutex
}

// NewHandler creates a new Route 53 Handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend: backend,
		tags:    make(map[string]*svcTags.Tags),
		tagsMu:  lockmetrics.New("route53.tags"),
	}
}

func (h *Handler) deleteTagsForResource(resourceID string) {
	h.tagsMu.Lock("deleteTagsForResource")
	defer h.tagsMu.Unlock()
	delete(h.tags, resourceID)
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = svcTags.New("route53." + resourceID + ".tags")
	}
	h.tags[resourceID].Merge(kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.RLock("removeTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t != nil {
		t.DeleteKeys(keys)
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	h.tagsMu.RLock("getTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t == nil {
		return map[string]string{}
	}

	return t.Clone()
}

// Name returns the service name.
func (h *Handler) Name() string { return "Route53" }

// MatchPriority returns the routing priority for Route 53.
func (h *Handler) MatchPriority() int { return service.PriorityFormStandard }

// RouteMatcher returns a matcher that selects Route 53 requests by path prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, route53PathPrefix)
	}
}

// GetSupportedOperations returns all mocked Route 53 operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"ActivateKeySigningKey",
		"AssociateVPCWithHostedZone",
		"ChangeCidrCollection",
		"ChangeResourceRecordSets",
		"ChangeTagsForResource",
		"CreateCidrCollection",
		"CreateHealthCheck",
		"CreateHostedZone",
		"CreateKeySigningKey",
		"CreateQueryLoggingConfig",
		"CreateReusableDelegationSet",
		"CreateTrafficPolicy",
		"CreateTrafficPolicyInstance",
		"CreateTrafficPolicyVersion",
		opDeactivateKeySigningKey,
		"DeleteCidrCollection",
		"DeleteHealthCheck",
		"DeleteHostedZone",
		"DeleteKeySigningKey",
		"DeleteTrafficPolicy",
		"DeleteTrafficPolicyInstance",
		"DisableHostedZoneDNSSEC",
		"EnableHostedZoneDNSSEC",
		"GetDNSSEC",
		"GetHealthCheck",
		"GetHealthCheckStatus",
		"GetHostedZone",
		"GetTrafficPolicy",
		"GetTrafficPolicyInstance",
		"GetTrafficPolicyInstanceCount",
		"ListCidrCollections",
		"ListHealthChecks",
		"ListHostedZones",
		"ListResourceRecordSets",
		"ListTagsForResource",
		"ListTrafficPolicies",
		"ListTrafficPolicyInstances",
		"ListTrafficPolicyVersions",
		"UpdateHealthCheck",
		// Completeness pass — previously notImplemented
		"CreateVPCAssociationAuthorization",
		"DeleteQueryLoggingConfig",
		"DeleteReusableDelegationSet",
		"DeleteVPCAssociationAuthorization",
		"DisassociateVPCFromHostedZone",
		"GetAccountLimit",
		"GetChange",
		"GetCheckerIpRanges",
		"GetGeoLocation",
		"GetHealthCheckCount",
		"GetHealthCheckLastFailureReason",
		"GetHostedZoneCount",
		"GetHostedZoneLimit",
		"GetQueryLoggingConfig",
		"GetReusableDelegationSet",
		"GetReusableDelegationSetLimit",
		"ListCidrBlocks",
		"ListCidrLocations",
		"ListGeoLocations",
		"ListHostedZonesByName",
		"ListHostedZonesByVPC",
		"ListQueryLoggingConfigs",
		"ListReusableDelegationSets",
		"ListTagsForResources",
		"ListTrafficPolicyInstancesByHostedZone",
		"ListTrafficPolicyInstancesByPolicy",
		"ListVPCAssociationAuthorizations",
		"TestDNSAnswer",
		"UpdateHostedZoneComment",
		"UpdateHostedZoneFeatures",
		"UpdateTrafficPolicyComment",
		"UpdateTrafficPolicyInstance",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "route53" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Route53 instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// extractHealthCheckOperation maps a health-check path+method to an operation name.
// Returns "" when the path does not match any health check route.
func extractHealthCheckOperation(path, method string) string {
	switch {
	case path == route53HealthCheckRoot && method == http.MethodPost:
		return "CreateHealthCheck"
	case path == route53HealthCheckRoot && method == http.MethodGet:
		return "ListHealthChecks"
	}

	if !strings.HasPrefix(path, route53HealthCheckPrefix) {
		return ""
	}

	if method == http.MethodGet && strings.HasSuffix(path, route53StatusSuffix) {
		return "GetHealthCheckStatus"
	}

	// Any non-GET request to the /status sub-path is not a valid health check operation.
	if strings.HasSuffix(path, route53StatusSuffix) {
		return ""
	}

	switch method {
	case http.MethodGet:
		return "GetHealthCheck"
	case http.MethodDelete:
		return "DeleteHealthCheck"
	case http.MethodPost:
		return "UpdateHealthCheck"
	default:
		return ""
	}
}

// ExtractOperation extracts a human-readable operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

	switch {
	case path == route53HostedZone && method == http.MethodPost:
		return "CreateHostedZone"
	case path == route53HostedZone && method == http.MethodGet:
		return "ListHostedZones"
	case strings.HasSuffix(path, route53RRSetSuffix) && method == http.MethodPost:
		return "ChangeResourceRecordSets"
	case strings.HasSuffix(path, route53RRSetSuffix) && method == http.MethodGet:
		return "ListResourceRecordSets"
	case method == http.MethodDelete && strings.HasPrefix(path, route53HZPrefix):
		return "DeleteHostedZone"
	case method == http.MethodGet && strings.HasPrefix(path, route53HZPrefix):
		return "GetHostedZone"
	}

	if op := extractHealthCheckOperation(path, method); op != "" {
		return op
	}

	if op := extractNewOpsOperation(path, method); op != "" {
		return op
	}

	return "Unknown"
}

// extractNewOpsOperation maps the newer Route 53 operation paths to operation names.
func extractNewOpsOperation(path, method string) string {
	switch method {
	case http.MethodPost:
		return extractNewOpsPath(path)
	case http.MethodGet:
		return extractGetOpsPath(path)
	case http.MethodDelete:
		return extractDeleteOpsPath(path)
	default:
		return ""
	}
}

func extractNewOpsPath(path string) string {
	switch {
	case path == route53KSKRoot:
		return "CreateKeySigningKey"
	case strings.HasSuffix(path, route53ActivateSuffix):
		return "ActivateKeySigningKey"
	case strings.HasSuffix(path, route53DeactivateSuffix):
		return opDeactivateKeySigningKey
	case strings.HasSuffix(path, route53EnableDNSSECSuffix):
		return "EnableHostedZoneDNSSEC"
	case strings.HasSuffix(path, route53DisableDNSSECSuffix):
		return "DisableHostedZoneDNSSEC"
	case strings.HasSuffix(path, route53AssociateVPCSuffix):
		return "AssociateVPCWithHostedZone"
	case path == route53CidrCollectionRoot:
		return "CreateCidrCollection"
	case strings.HasPrefix(path, route53CidrCollectionPrefix):
		return "ChangeCidrCollection"
	case path == route53QueryLoggingRoot:
		return "CreateQueryLoggingConfig"
	case path == route53DelegationSetRoot:
		return "CreateReusableDelegationSet"
	case path == route53TrafficPolicyRoot:
		return "CreateTrafficPolicy"
	case strings.HasPrefix(path, route53TrafficPolicyPrefix):
		return "CreateTrafficPolicyVersion"
	case path == route53TPInstanceRoot:
		return "CreateTrafficPolicyInstance"
	}

	return ""
}

func extractGetOpsPath(path string) string {
	switch {
	case strings.HasSuffix(path, route53DNSSECSuffix):
		return "GetDNSSEC"
	case path == route53TrafficPoliciesRoot:
		return "ListTrafficPolicies"
	case strings.HasPrefix(path, route53TrafficPoliciesPrefix):
		return "ListTrafficPolicyVersions"
	case path == route53TPInstancesRoot:
		return "ListTrafficPolicyInstances"
	case path == route53TPInstanceCount:
		return "GetTrafficPolicyInstanceCount"
	case strings.HasPrefix(path, route53TPInstancePrefix):
		return "GetTrafficPolicyInstance"
	case path == route53CidrCollectionRoot:
		return "ListCidrCollections"
	}

	return ""
}

func extractDeleteOpsPath(path string) string {
	switch {
	case strings.HasPrefix(path, route53KSKPrefix):
		if strings.HasSuffix(path, route53DeactivateSuffix) {
			return opDeactivateKeySigningKey
		}

		return "DeleteKeySigningKey"
	case strings.HasPrefix(path, route53CidrCollectionPrefix):
		return "DeleteCidrCollection"
	case strings.HasPrefix(path, route53TPInstancePrefix):
		return "DeleteTrafficPolicyInstance"
	}

	// traffic policy version delete: /2013-04-01/trafficpolicy/{id}/{version}
	if after, ok := strings.CutPrefix(path, route53TrafficPolicyPrefix); ok {
		rest := after
		if strings.Contains(rest, "/") {
			return "DeleteTrafficPolicy"
		}
	}

	return ""
}

// ExtractResource extracts the zone ID from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path
	// /2013-04-01/hostedzone/{Id}  or  /2013-04-01/hostedzone/{Id}/rrset
	parts := strings.Split(strings.TrimPrefix(path, route53HZPrefix), "/")
	if len(parts) > 0 && parts[0] != "" {
		return parts[0]
	}

	return ""
}

// iamActionForHealthCheck maps a health-check path+method to an IAM action string.
// Returns "" when the path does not match any health check route.
func iamActionForHealthCheck(path, method string) string {
	switch {
	case path == route53HealthCheckRoot && method == http.MethodPost:
		return "route53:CreateHealthCheck"
	case path == route53HealthCheckRoot && method == http.MethodGet:
		return "route53:ListHealthChecks"
	}

	if !strings.HasPrefix(path, route53HealthCheckPrefix) {
		return ""
	}

	if method == http.MethodGet && strings.HasSuffix(path, route53StatusSuffix) {
		return "route53:GetHealthCheckStatus"
	}

	// Any non-GET request to the /status sub-path is not a valid IAM-mapped operation.
	if strings.HasSuffix(path, route53StatusSuffix) {
		return ""
	}

	switch method {
	case http.MethodGet:
		return "route53:GetHealthCheck"
	case http.MethodDelete:
		return "route53:DeleteHealthCheck"
	case http.MethodPost:
		return "route53:UpdateHealthCheck"
	default:
		return ""
	}
}

// IAMAction returns the IAM action for a Route53 HTTP request.
// It implements iam.ActionExtractor, providing per-service action extraction
// for Route53 REST API paths that are not covered by the global action mapper.
// iamActionForHostedZone maps hosted zone / rrset paths to IAM action strings.
// Returns "" when the path does not match.
func iamActionForHostedZone(path, method string) string {
	if action := iamActionForHostedZoneRoot(path, method); action != "" {
		return action
	}

	switch {
	case strings.HasPrefix(path, route53TagsPrefix) && method == http.MethodGet:
		return "route53:ListTagsForResource"
	case strings.HasPrefix(path, route53TagsPrefix):
		return "route53:ChangeTagsForResource"
	case method == http.MethodDelete && strings.HasPrefix(path, route53HZPrefix):
		return "route53:DeleteHostedZone"
	case method == http.MethodGet && strings.HasPrefix(path, route53HZPrefix):
		return "route53:GetHostedZone"
	default:
		return ""
	}
}

// iamActionForHostedZoneRoot maps the hosted zone root and rrset paths to IAM action strings.
func iamActionForHostedZoneRoot(path, method string) string {
	switch {
	case path == route53HostedZone && method == http.MethodPost:
		return "route53:CreateHostedZone"
	case path == route53HostedZone && method == http.MethodGet:
		return "route53:ListHostedZones"
	case strings.HasSuffix(path, route53RRSetSuffix) && method == http.MethodPost:
		return "route53:ChangeResourceRecordSets"
	case strings.HasSuffix(path, route53RRSetSuffix) && method == http.MethodGet:
		return "route53:ListResourceRecordSets"
	default:
		return ""
	}
}

func (h *Handler) IAMAction(r *http.Request) string {
	path := r.URL.Path
	if !strings.HasPrefix(path, route53PathPrefix) {
		return ""
	}

	method := r.Method

	if action := iamActionForHostedZone(path, method); action != "" {
		return action
	}

	if action := iamActionForHealthCheck(path, method); action != "" {
		return action
	}

	if action := iamActionForNewOps(path, method); action != "" {
		return action
	}

	return "route53:GetChange"
}

// iamActionForNewOps maps newer Route 53 paths to IAM action strings.
func iamActionForNewOps(path, method string) string {
	switch method {
	case http.MethodPost:
		return iamActionForNewOpsPath(path)
	case http.MethodGet:
		op := extractGetOpsPath(path)
		if op != "" {
			return "route53:" + op
		}

		return ""
	case http.MethodDelete:
		op := extractDeleteOpsPath(path)
		if op != "" {
			return "route53:" + op
		}

		return ""
	default:
		return ""
	}
}

func iamActionForNewOpsPath(path string) string {
	switch {
	case path == route53KSKRoot:
		return "route53:CreateKeySigningKey"
	case strings.HasSuffix(path, route53ActivateSuffix):
		return "route53:ActivateKeySigningKey"
	case strings.HasSuffix(path, route53AssociateVPCSuffix):
		return "route53:AssociateVPCWithHostedZone"
	case path == route53CidrCollectionRoot:
		return "route53:CreateCidrCollection"
	case strings.HasPrefix(path, route53CidrCollectionPrefix):
		return "route53:ChangeCidrCollection"
	case path == route53QueryLoggingRoot:
		return "route53:CreateQueryLoggingConfig"
	case path == route53DelegationSetRoot:
		return "route53:CreateReusableDelegationSet"
	case path == route53TrafficPolicyRoot:
		return "route53:CreateTrafficPolicy"
	case strings.HasPrefix(path, route53TrafficPolicyPrefix):
		return "route53:CreateTrafficPolicyVersion"
	case path == route53TPInstanceRoot:
		return "route53:CreateTrafficPolicyInstance"
	}

	return ""
}

// routeRequest dispatches Route 53 requests to the appropriate handler.
func (h *Handler) routeRequest(c *echo.Context, path, method string) error {
	switch {
	case path == route53HostedZone:
		return h.routeHostedZoneRoot(c, method)
	case strings.HasPrefix(path, route53HZPrefix):
		return h.routeHostedZone(c, path, method)
	case strings.HasPrefix(path, route53TagsPrefix):
		return h.routeTags(c, path, method)
	case strings.HasPrefix(path, route53ChangePrefix):
		return h.routeChange(c, path, method)
	case path == route53HealthCheckRoot:
		return h.routeHealthCheckRoot(c, method)
	case strings.HasPrefix(path, route53HealthCheckPrefix):
		return h.routeHealthCheck(c, path, method)
	default:
		return h.routeNewOps(c, path, method)
	}
}

// routeNewOps dispatches the newly added Route 53 operations.
func (h *Handler) routeNewOps(c *echo.Context, path, method string) error {
	if ok, err := h.routeNewOpsKSKCidr(c, path, method); ok {
		return err
	}

	if ok, err := h.routeNewOpsTP(c, path, method); ok {
		return err
	}

	if ok, err := h.routeCompleteness(c, path, method); ok {
		return err
	}

	switch path {
	case route53QueryLoggingRoot:
		return h.routeQueryLogging(c, method)
	case route53DelegationSetRoot:
		return h.routeDelegationSetRoot(c, method)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			fmt.Sprintf("unknown Route53 endpoint: %s %s", method, path))
	}
}

func (h *Handler) routeNewOpsKSKCidr(c *echo.Context, path, method string) (bool, error) {
	switch {
	case path == route53KSKRoot:
		return true, h.routeKSKRoot(c, method)
	case strings.HasPrefix(path, route53KSKPrefix):
		return true, h.routeKSK(c, path, method)
	case path == route53CidrCollectionRoot:
		return true, h.routeCidrCollectionRoot(c, method)
	case strings.Contains(path, "/cidrblocks") && strings.HasPrefix(path, route53CidrCollectionPrefix):
		return true, h.listCidrBlocks(c, path)
	case strings.Contains(path, "/cidrlocations") && strings.HasPrefix(path, route53CidrCollectionPrefix):
		return true, h.listCidrLocations(c, path)
	case strings.HasPrefix(path, route53CidrCollectionPrefix):
		return true, h.routeCidrCollection(c, path, method)
	default:
		return false, nil
	}
}

func (h *Handler) routeNewOpsTP(c *echo.Context, path, method string) (bool, error) {
	switch {
	case path == route53TrafficPoliciesRoot:
		return true, h.routeTrafficPoliciesRoot(c, method)
	case strings.HasPrefix(path, route53TrafficPoliciesPrefix):
		return true, h.routeTrafficPoliciesVersions(c, path, method)
	case path == route53TrafficPolicyRoot:
		return true, h.routeTrafficPolicyRoot(c, method)
	case strings.HasPrefix(path, route53TrafficPolicyPrefix):
		return true, h.routeTrafficPolicyVersion(c, path, method)
	case path == route53TPInstancesByHZPath:
		return true, h.listTrafficPolicyInstancesByHostedZone(c)
	case path == route53TPInstancesByPolicyPath:
		return true, h.listTrafficPolicyInstancesByPolicy(c)
	case path == route53TPInstancesRoot:
		return true, h.routeTPInstancesRoot(c, method)
	case path == route53TPInstanceCount:
		return true, h.routeTPInstanceCount(c, method)
	case path == route53TPInstanceRoot:
		return true, h.routeTPInstanceRoot(c, method)
	case strings.HasPrefix(path, route53TPInstancePrefix):
		return true, h.routeTPInstance(c, path, method)
	default:
		return false, nil
	}
}

func (h *Handler) routeHostedZoneRoot(c *echo.Context, method string) error {
	switch method {
	case http.MethodPost:
		return h.createHostedZone(c)
	case http.MethodGet:
		return h.listHostedZones(c)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on /hostedzone")
	}
}

// routeHostedZone routes requests for a specific hosted zone by path suffix.
func (h *Handler) routeHostedZone(c *echo.Context, path, method string) error {
	if ok, err := h.routeHostedZoneSuffix(c, path, method); ok {
		return err
	}

	if ok, err := h.routeHostedZoneDNSSEC(c, path, method); ok {
		return err
	}

	switch method {
	case http.MethodDelete:
		return h.deleteHostedZone(c)
	case http.MethodGet:
		return h.getHostedZone(c)
	case http.MethodPost:
		return h.updateHostedZoneComment(c, path)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on hosted zone")
	}
}

// routeHostedZoneSuffix handles suffix-based sub-paths for a hosted zone.
func (h *Handler) routeHostedZoneSuffix(c *echo.Context, path, method string) (bool, error) {
	if strings.HasSuffix(path, route53RRSetSuffix) {
		return true, h.routeRRSet(c, method)
	}

	if strings.HasSuffix(path, route53AssociateVPCSuffix) {
		return true, h.routeAssociateVPC(c, path, method)
	}

	if strings.HasSuffix(path, route53AuthorizeVPCSuffix) {
		return true, h.routeAuthorizeVPC(c, path, method)
	}

	if strings.HasSuffix(path, route53DeauthorizeVPCSuffix) && method == http.MethodPost {
		return true, h.deleteVPCAssociationAuthorization(c, path)
	}

	if strings.HasSuffix(path, route53DisassociateVPCSuffix) && method == http.MethodPost {
		return true, h.disassociateVPCFromHostedZone(c, path)
	}

	if strings.HasSuffix(path, route53FeaturesSuffix) && method == http.MethodPost {
		return true, h.updateHostedZoneFeatures(c, path)
	}

	return false, nil
}

// routeAuthorizeVPC routes VPC association authorization requests.
func (h *Handler) routeAuthorizeVPC(c *echo.Context, path, method string) error {
	switch method {
	case http.MethodPost:
		return h.createVPCAssociationAuthorization(c, path)
	case http.MethodGet:
		return h.listVPCAssociationAuthorizations(c, path)
	case http.MethodDelete:
		return h.deleteVPCAssociationAuthorization(c, path)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on authorizevpcassociation")
	}
}

// routeRRSet routes resource record set requests.
func (h *Handler) routeRRSet(c *echo.Context, method string) error {
	switch method {
	case http.MethodPost:
		return h.changeResourceRecordSets(c)
	case http.MethodGet:
		return h.listResourceRecordSets(c)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on rrset")
	}
}

// routeAssociateVPC routes VPC association requests.
func (h *Handler) routeAssociateVPC(c *echo.Context, path, method string) error {
	switch method {
	case http.MethodPost:
		return h.associateVPCWithHostedZone(c, path)
	case http.MethodGet:
		return h.listVPCAssociationAuthorizations(c, path)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on associatevpc")
	}
}

func (h *Handler) routeHostedZoneDNSSEC(c *echo.Context, path, method string) (bool, error) {
	if strings.HasSuffix(path, route53EnableDNSSECSuffix) {
		if method == http.MethodPost {
			zoneID := strings.TrimSuffix(
				strings.TrimPrefix(path, route53HZPrefix),
				route53EnableDNSSECSuffix,
			)

			return true, h.enableHostedZoneDNSSEC(c, zoneID)
		}

		return true, xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on enable-dnssec")
	}

	if strings.HasSuffix(path, route53DisableDNSSECSuffix) {
		if method == http.MethodPost {
			zoneID := strings.TrimSuffix(
				strings.TrimPrefix(path, route53HZPrefix),
				route53DisableDNSSECSuffix,
			)

			return true, h.disableHostedZoneDNSSEC(c, zoneID)
		}

		return true, xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on disable-dnssec")
	}

	if strings.HasSuffix(path, route53DNSSECSuffix) {
		if method == http.MethodGet {
			zoneID := strings.TrimSuffix(
				strings.TrimPrefix(path, route53HZPrefix),
				route53DNSSECSuffix,
			)

			return true, h.getHostedZoneDNSSEC(c, zoneID)
		}

		return true, xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on dnssec")
	}

	return false, nil
}

func (h *Handler) routeTags(c *echo.Context, path, method string) error {
	// POST /2013-04-01/tags (no resource type suffix) → ListTagsForResources
	if method == http.MethodPost && path == route53TagsPrefix[:len(route53TagsPrefix)-1] {
		return h.listTagsForResources(c)
	}

	switch method {
	case http.MethodGet:
		return h.listTagsForResource(c, path)
	case http.MethodPost:
		return h.changeTagsForResource(c)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on tags")
	}
}

func (h *Handler) routeChange(c *echo.Context, path, method string) error {
	if method == http.MethodGet {
		return h.getChange(c, path)
	}

	return xmlError(c, http.StatusNotFound, "NoSuchOperation",
		"unsupported method on change")
}

func (h *Handler) routeHealthCheckRoot(c *echo.Context, method string) error {
	switch method {
	case http.MethodPost:
		return h.createHealthCheck(c)
	case http.MethodGet:
		return h.listHealthChecks(c)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on /healthcheck")
	}
}

func (h *Handler) routeHealthCheck(c *echo.Context, path, method string) error {
	if strings.HasSuffix(path, route53StatusSuffix) {
		if method == http.MethodGet {
			return h.getHealthCheckStatus(c, path)
		}

		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on health check status")
	}

	switch method {
	case http.MethodGet:
		return h.getHealthCheck(c, path)
	case http.MethodDelete:
		return h.deleteHealthCheck(c, path)
	case http.MethodPost:
		return h.updateHealthCheck(c, path)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on health check")
	}
}

// Handler returns the Echo handler function for Route 53 requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		path := c.Request().URL.Path
		method := c.Request().Method

		log.DebugContext(ctx, "Route53 request", "method", method, "path", path)

		return h.routeRequest(c, path, method)
	}
}

// extractZoneID returns the hosted zone ID from a path like /2013-04-01/hostedzone/{Id}...
func extractZoneID(path string) string {
	rest := strings.TrimPrefix(path, route53HZPrefix)
	// rest is either "{Id}" or "{Id}/rrset"
	parts := strings.SplitN(rest, "/", zoneIDAndRest)
	if len(parts) > 0 {
		return parts[0]
	}

	return ""
}

// ---- XML response types ----

type xmlHostedZoneConfig struct {
	Comment     string `xml:"Comment,omitempty"`
	PrivateZone bool   `xml:"PrivateZone"`
}

type xmlHostedZone struct {
	XMLName                xml.Name            `xml:"HostedZone"`
	ID                     string              `xml:"Id"`
	Name                   string              `xml:"Name"`
	CallerReference        string              `xml:"CallerReference"`
	Config                 xmlHostedZoneConfig `xml:"Config"`
	ResourceRecordSetCount int                 `xml:"ResourceRecordSetCount"`
}

type xmlDelegationSet struct {
	XMLName     xml.Name `xml:"DelegationSet"`
	ID          string   `xml:"Id,omitempty"`
	NameServers []string `xml:"NameServers>NameServer"`
}

type xmlChangeInfo struct {
	XMLName     xml.Name  `xml:"ChangeInfo"`
	SubmittedAt time.Time `xml:"SubmittedAt"`
	ID          string    `xml:"Id"`
	Status      string    `xml:"Status"`
}

type xmlCreateHostedZoneResponse struct {
	ChangeInfo    xmlChangeInfo    `xml:"ChangeInfo"`
	XMLName       xml.Name         `xml:"CreateHostedZoneResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	DelegationSet xmlDelegationSet `xml:"DelegationSet"`
	HostedZone    xmlHostedZone    `xml:"HostedZone"`
}

type xmlGetHostedZoneResponse struct {
	XMLName       xml.Name         `xml:"GetHostedZoneResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	DelegationSet xmlDelegationSet `xml:"DelegationSet"`
	HostedZone    xmlHostedZone    `xml:"HostedZone"`
}

type xmlListHostedZonesResponse struct {
	XMLName     xml.Name        `xml:"ListHostedZonesResponse"`
	Xmlns       string          `xml:"xmlns,attr"`
	MaxItems    string          `xml:"MaxItems"`
	NextMarker  string          `xml:"NextMarker,omitempty"`
	HostedZones []xmlHostedZone `xml:"HostedZones>HostedZone"`
	IsTruncated bool            `xml:"IsTruncated"`
}

type xmlChangeResourceRecordSetsResponse struct {
	XMLName    xml.Name      `xml:"ChangeResourceRecordSetsResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
}

type xmlDeleteHostedZoneResponse struct {
	XMLName    xml.Name      `xml:"DeleteHostedZoneResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
}

type xmlResourceRecord struct {
	Value string `xml:"Value"`
}

type xmlAliasTarget struct {
	HostedZoneID         string `xml:"HostedZoneId"`
	DNSName              string `xml:"DNSName"`
	EvaluateTargetHealth bool   `xml:"EvaluateTargetHealth"`
}

type xmlGeoLocation struct {
	ContinentCode   string `xml:"ContinentCode,omitempty"`
	ContinentName   string `xml:"ContinentName,omitempty"`
	CountryCode     string `xml:"CountryCode,omitempty"`
	CountryName     string `xml:"CountryName,omitempty"`
	SubdivisionCode string `xml:"SubdivisionCode,omitempty"`
	SubdivisionName string `xml:"SubdivisionName,omitempty"`
}

type xmlGeoProximityCoordinates struct {
	Latitude  string `xml:"Latitude,omitempty"`
	Longitude string `xml:"Longitude,omitempty"`
}

type xmlGeoProximityLocation struct {
	Coordinates    *xmlGeoProximityCoordinates `xml:"Coordinates,omitempty"`
	AWSRegion      string                      `xml:"AWSRegion,omitempty"`
	LocalZoneGroup string                      `xml:"LocalZoneGroup,omitempty"`
	Bias           int                         `xml:"Bias,omitempty"`
}

type xmlCidrRoutingConfig struct {
	CollectionID string `xml:"CollectionId,omitempty"`
	LocationName string `xml:"LocationName,omitempty"`
}

type xmlResourceRecordSet struct {
	XMLName              xml.Name                 `xml:"ResourceRecordSet"`
	AliasTarget          *xmlAliasTarget          `xml:"AliasTarget,omitempty"`
	GeoLocation          *xmlGeoLocation          `xml:"GeoLocation,omitempty"`
	GeoProximityLocation *xmlGeoProximityLocation `xml:"GeoProximityLocation,omitempty"`
	CidrRoutingConfig    *xmlCidrRoutingConfig    `xml:"CidrRoutingConfig,omitempty"`
	Name                 string                   `xml:"Name"`
	Type                 string                   `xml:"Type"`
	SetIdentifier        string                   `xml:"SetIdentifier,omitempty"`
	Failover             string                   `xml:"Failover,omitempty"`
	Region               string                   `xml:"Region,omitempty"`
	HealthCheckID        string                   `xml:"HealthCheckId,omitempty"`
	ResourceRecords      []xmlResourceRecord      `xml:"ResourceRecords>ResourceRecord,omitempty"`
	TTL                  int64                    `xml:"TTL,omitempty"`
	Weight               int64                    `xml:"Weight,omitempty"`
	MultiValueAnswer     bool                     `xml:"MultiValueAnswer,omitempty"`
}

type xmlListResourceRecordSetsResponse struct {
	XMLName              xml.Name               `xml:"ListResourceRecordSetsResponse"`
	Xmlns                string                 `xml:"xmlns,attr"`
	NextRecordName       string                 `xml:"NextRecordName,omitempty"`
	NextRecordType       string                 `xml:"NextRecordType,omitempty"`
	NextRecordIdentifier string                 `xml:"NextRecordIdentifier,omitempty"`
	MaxItems             string                 `xml:"MaxItems"`
	ResourceRecordSets   []xmlResourceRecordSet `xml:"ResourceRecordSets>ResourceRecordSet"`
	IsTruncated          bool                   `xml:"IsTruncated"`
}

// ---- XML request types ----

type xmlCreateHostedZoneRequest struct {
	XMLName          xml.Name            `xml:"CreateHostedZoneRequest"`
	Name             string              `xml:"Name"`
	CallerReference  string              `xml:"CallerReference"`
	HostedZoneConfig xmlHostedZoneConfig `xml:"HostedZoneConfig"`
}

// xmlResourceRecordSetChange is the ResourceRecordSet element within a change batch entry.
type xmlResourceRecordSetChange struct {
	AliasTarget          *xmlAliasTarget          `xml:"AliasTarget"`
	GeoLocation          *xmlGeoLocation          `xml:"GeoLocation"`
	GeoProximityLocation *xmlGeoProximityLocation `xml:"GeoProximityLocation"`
	CidrRoutingConfig    *xmlCidrRoutingConfig    `xml:"CidrRoutingConfig"`
	Name                 string                   `xml:"Name"`
	Type                 string                   `xml:"Type"`
	SetIdentifier        string                   `xml:"SetIdentifier"`
	Failover             string                   `xml:"Failover"`
	Region               string                   `xml:"Region"`
	HealthCheckID        string                   `xml:"HealthCheckId"`
	ResourceRecords      []xmlResourceRecord      `xml:"ResourceRecords>ResourceRecord"`
	TTL                  int64                    `xml:"TTL"`
	Weight               int64                    `xml:"Weight"`
	MultiValueAnswer     bool                     `xml:"MultiValueAnswer"`
}

// xmlChange is a single change entry within a ChangeBatch.
type xmlChange struct {
	Action            string                     `xml:"Action"`
	ResourceRecordSet xmlResourceRecordSetChange `xml:"ResourceRecordSet"`
}

type xmlChangeBatch struct {
	XMLName xml.Name    `xml:"ChangeBatch"`
	Changes []xmlChange `xml:"Changes>Change"`
}

type xmlChangeResourceRecordSetsRequest struct {
	XMLName     xml.Name       `xml:"ChangeResourceRecordSetsRequest"`
	ChangeBatch xmlChangeBatch `xml:"ChangeBatch"`
}

// ---- Handlers ----

func (h *Handler) createHostedZone(c *echo.Context) error {
	ctx := c.Request().Context()

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlCreateHostedZoneRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	hz, err := h.Backend.CreateHostedZone(
		req.Name, req.CallerReference,
		req.HostedZoneConfig.Comment, req.HostedZoneConfig.PrivateZone,
	)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 CreateHostedZone", "id", hz.ID, "name", hz.Name)

	resp := xmlCreateHostedZoneResponse{
		Xmlns:      route53Namespace,
		HostedZone: toXMLHostedZone(hz),
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/C" + hz.ID,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
		DelegationSet: xmlDelegationSet{
			NameServers: []string{dnsNS1Default, dnsNS2Default},
		},
	}

	c.Response().Header().Set("Location", "/2013-04-01/hostedzone/"+hz.ID)

	return writeXML(c, http.StatusCreated, resp)
}

func (h *Handler) getHostedZone(c *echo.Context) error {
	ctx := c.Request().Context()
	zoneID := extractZoneID(c.Request().URL.Path)

	hz, err := h.Backend.GetHostedZone(zoneID)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 GetHostedZone", "id", hz.ID)

	resp := xmlGetHostedZoneResponse{
		Xmlns:      route53Namespace,
		HostedZone: toXMLHostedZone(hz),
		DelegationSet: xmlDelegationSet{
			NameServers: []string{dnsNS1Default, dnsNS2Default},
		},
	}

	return writeXML(c, http.StatusOK, resp)
}

func (h *Handler) deleteHostedZone(c *echo.Context) error {
	ctx := c.Request().Context()
	zoneID := extractZoneID(c.Request().URL.Path)

	if err := h.Backend.DeleteHostedZone(zoneID); err != nil {
		return handleBackendError(c, err)
	}

	h.deleteTagsForResource(zoneID)

	logger.Load(ctx).DebugContext(ctx, "Route53 DeleteHostedZone", "id", zoneID)

	resp := xmlDeleteHostedZoneResponse{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			SubmittedAt: time.Now(),
			ID:          "/change/C" + zoneID,
			Status:      statusInsync,
		},
	}

	return writeXML(c, http.StatusOK, resp)
}

func (h *Handler) listHostedZones(c *echo.Context) error {
	q := c.Request().URL.Query()
	marker := q.Get("marker")
	maxItems := 0
	if v := q.Get("maxitems"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxItems = n
		}
	}

	p, err := h.Backend.ListHostedZones(marker, maxItems)
	if err != nil {
		return handleBackendError(c, err)
	}

	xmlZones := make([]xmlHostedZone, len(p.Data))
	for i := range p.Data {
		xmlZones[i] = toXMLHostedZone(&p.Data[i])
	}

	resp := xmlListHostedZonesResponse{
		Xmlns:       route53Namespace,
		HostedZones: xmlZones,
		IsTruncated: p.Next != "",
		NextMarker:  p.Next,
		MaxItems:    strconv.Itoa(maxItems),
	}

	return writeXML(c, http.StatusOK, resp)
}

func (h *Handler) changeResourceRecordSets(c *echo.Context) error {
	ctx := c.Request().Context()
	zoneID := extractZoneID(c.Request().URL.Path)

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlChangeResourceRecordSetsRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	changes := make([]Change, 0, len(req.ChangeBatch.Changes))
	for _, ch := range req.ChangeBatch.Changes {
		records := make([]ResourceRecord, len(ch.ResourceRecordSet.ResourceRecords))
		for i, rr := range ch.ResourceRecordSet.ResourceRecords {
			records[i] = ResourceRecord(rr)
		}

		rrs := ResourceRecordSet{
			Name:             ch.ResourceRecordSet.Name,
			Type:             ch.ResourceRecordSet.Type,
			TTL:              ch.ResourceRecordSet.TTL,
			Records:          records,
			SetIdentifier:    ch.ResourceRecordSet.SetIdentifier,
			Failover:         FailoverPolicy(ch.ResourceRecordSet.Failover),
			Region:           ch.ResourceRecordSet.Region,
			HealthCheckID:    ch.ResourceRecordSet.HealthCheckID,
			Weight:           ch.ResourceRecordSet.Weight,
			MultiValueAnswer: ch.ResourceRecordSet.MultiValueAnswer,
		}

		if ch.ResourceRecordSet.AliasTarget != nil {
			at := ch.ResourceRecordSet.AliasTarget
			rrs.AliasTarget = &AliasTarget{
				HostedZoneID:         at.HostedZoneID,
				DNSName:              at.DNSName,
				EvaluateTargetHealth: at.EvaluateTargetHealth,
			}
		}

		if ch.ResourceRecordSet.GeoLocation != nil {
			gl := ch.ResourceRecordSet.GeoLocation
			rrs.GeoLocation = &GeoLocation{
				ContinentCode:   gl.ContinentCode,
				CountryCode:     gl.CountryCode,
				SubdivisionCode: gl.SubdivisionCode,
			}
		}

		if ch.ResourceRecordSet.GeoProximityLocation != nil {
			gpl := ch.ResourceRecordSet.GeoProximityLocation
			rrs.GeoProximityLocation = &GeoProximityLocation{
				AWSRegion:      gpl.AWSRegion,
				LocalZoneGroup: gpl.LocalZoneGroup,
				Bias:           gpl.Bias,
			}
			if gpl.Coordinates != nil {
				rrs.GeoProximityLocation.Coordinates = &GeoProximityCoordinates{
					Latitude:  gpl.Coordinates.Latitude,
					Longitude: gpl.Coordinates.Longitude,
				}
			}
		}

		if ch.ResourceRecordSet.CidrRoutingConfig != nil {
			crc := ch.ResourceRecordSet.CidrRoutingConfig
			rrs.CidrRoutingConfig = &CidrRoutingConfig{
				CollectionID: crc.CollectionID,
				LocationName: crc.LocationName,
			}
		}

		changes = append(changes, Change{
			Action:            ChangeAction(strings.ToUpper(ch.Action)),
			ResourceRecordSet: rrs,
		})
	}

	changeID, err := h.Backend.ChangeResourceRecordSets(zoneID, changes)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 ChangeResourceRecordSets", "zoneID", zoneID, "changes", len(changes))

	resp := xmlChangeResourceRecordSetsResponse{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          changeID,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
	}

	return writeXML(c, http.StatusOK, resp)
}

func (h *Handler) listResourceRecordSets(c *echo.Context) error {
	ctx := c.Request().Context()
	zoneID := extractZoneID(c.Request().URL.Path)
	q := c.Request().URL.Query()

	startName := q.Get("name")
	startType := q.Get("type")
	startIdentifier := q.Get("identifier")
	maxItems := 0

	if m := q.Get("maxitems"); m != "" {
		fmt.Sscanf(m, "%d", &maxItems) //nolint:errcheck // 0 is a safe default
	}

	pg, err := h.Backend.ListResourceRecordSets(
		zoneID,
		startName,
		startType,
		startIdentifier,
		maxItems,
	)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 ListResourceRecordSets", "zoneID", zoneID, "count", len(pg.Records))

	xmlRecords := make([]xmlResourceRecordSet, len(pg.Records))
	for i, rrs := range pg.Records {
		xmlRecs := make([]xmlResourceRecord, len(rrs.Records))
		for j, rr := range rrs.Records {
			xmlRecs[j] = xmlResourceRecord(rr)
		}

		xrrs := xmlResourceRecordSet{
			Name:             rrs.Name,
			Type:             rrs.Type,
			TTL:              rrs.TTL,
			ResourceRecords:  xmlRecs,
			SetIdentifier:    rrs.SetIdentifier,
			Failover:         string(rrs.Failover),
			Region:           rrs.Region,
			HealthCheckID:    rrs.HealthCheckID,
			Weight:           rrs.Weight,
			MultiValueAnswer: rrs.MultiValueAnswer,
		}

		if rrs.AliasTarget != nil {
			xrrs.AliasTarget = &xmlAliasTarget{
				HostedZoneID:         rrs.AliasTarget.HostedZoneID,
				DNSName:              rrs.AliasTarget.DNSName,
				EvaluateTargetHealth: rrs.AliasTarget.EvaluateTargetHealth,
			}
		}

		if rrs.GeoLocation != nil {
			xrrs.GeoLocation = &xmlGeoLocation{
				ContinentCode:   rrs.GeoLocation.ContinentCode,
				CountryCode:     rrs.GeoLocation.CountryCode,
				SubdivisionCode: rrs.GeoLocation.SubdivisionCode,
			}
		}

		if rrs.GeoProximityLocation != nil {
			xrrs.GeoProximityLocation = &xmlGeoProximityLocation{
				AWSRegion:      rrs.GeoProximityLocation.AWSRegion,
				LocalZoneGroup: rrs.GeoProximityLocation.LocalZoneGroup,
				Bias:           rrs.GeoProximityLocation.Bias,
			}
			if rrs.GeoProximityLocation.Coordinates != nil {
				xrrs.GeoProximityLocation.Coordinates = &xmlGeoProximityCoordinates{
					Latitude:  rrs.GeoProximityLocation.Coordinates.Latitude,
					Longitude: rrs.GeoProximityLocation.Coordinates.Longitude,
				}
			}
		}

		if rrs.CidrRoutingConfig != nil {
			xrrs.CidrRoutingConfig = &xmlCidrRoutingConfig{
				CollectionID: rrs.CidrRoutingConfig.CollectionID,
				LocationName: rrs.CidrRoutingConfig.LocationName,
			}
		}

		xmlRecords[i] = xrrs
	}

	resp := xmlListResourceRecordSetsResponse{
		Xmlns:              route53Namespace,
		ResourceRecordSets: xmlRecords,
		IsTruncated:        pg.IsTruncated,
		MaxItems:           strconv.Itoa(maxItems),
	}

	if pg.IsTruncated {
		resp.NextRecordName = pg.NextName
		resp.NextRecordType = pg.NextType
		resp.NextRecordIdentifier = pg.NextIdentifier
	}

	return writeXML(c, http.StatusOK, resp)
}

// ---- Helpers ----

func toXMLHostedZone(hz *HostedZone) xmlHostedZone {
	return xmlHostedZone{
		ID:              "/hostedzone/" + hz.ID,
		Name:            hz.Name,
		CallerReference: hz.CallerReference,
		Config: xmlHostedZoneConfig{
			Comment:     hz.Comment,
			PrivateZone: hz.PrivateZone,
		},
		ResourceRecordSetCount: hz.ResourceRecordSetCount,
	}
}

func (h *Handler) listTagsForResource(c *echo.Context, path string) error {
	rest := strings.TrimPrefix(path, route53TagsPrefix)
	parts := strings.SplitN(rest, "/", 2) //nolint:mnd // split into type + id
	resourceType := ""
	resourceID := ""

	if len(parts) >= 1 {
		resourceType = parts[0]
	}

	if len(parts) >= 2 { //nolint:mnd // path has two segments: type + id
		resourceID = parts[1]
	}

	tags := h.getTags(resourceID)
	type r53Tag struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	tagList := make([]r53Tag, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, r53Tag{Key: k, Value: v})
	}

	type resourceTagSet struct {
		ResourceType string   `xml:"ResourceType"`
		ResourceID   string   `xml:"ResourceId"`
		Tags         []r53Tag `xml:"Tags>Tag"`
	}
	type tagsResp struct {
		XMLName        xml.Name       `xml:"ListTagsForResourceResponse"`
		Xmlns          string         `xml:"xmlns,attr"`
		ResourceTagSet resourceTagSet `xml:"ResourceTagSet"`
	}

	resp := tagsResp{Xmlns: route53Namespace}
	resp.ResourceTagSet.ResourceType = resourceType
	resp.ResourceTagSet.ResourceID = resourceID
	resp.ResourceTagSet.Tags = tagList

	return writeXML(c, http.StatusOK, resp)
}

// getChange returns the status of a Route 53 change batch.
func (h *Handler) getChange(c *echo.Context, path string) error {
	type getChangeResp struct {
		XMLName    xml.Name      `xml:"GetChangeResponse"`
		Xmlns      string        `xml:"xmlns,attr"`
		ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
	}

	// Extract change ID from path /2013-04-01/change/{id}.
	changeID := strings.TrimPrefix(path, route53ChangePrefix)

	ci, err := h.Backend.GetChange(changeID)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, http.StatusOK, getChangeResp{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          ci.ID,
			Status:      ci.Status,
			SubmittedAt: ci.SubmittedAt,
		},
	})
}

func (h *Handler) changeTagsForResource(c *echo.Context) error {
	path := c.Request().URL.Path
	rest := strings.TrimPrefix(path, route53TagsPrefix)
	parts := strings.SplitN(rest, "/", 2) //nolint:mnd // path has two segments: type + id
	resourceID := ""

	if len(parts) >= 2 { //nolint:mnd // path has two segments: type + id
		resourceID = parts[1]
	}

	if err := h.applyTagChanges(resourceID, c.Request()); err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", err.Error())
	}

	type changeTagsResp struct {
		XMLName xml.Name `xml:"ChangeTagsForResourceResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
	}

	return writeXML(c, http.StatusOK, changeTagsResp{Xmlns: route53Namespace})
}

type applyTagChangesInput struct {
	AddTags       []svcTags.KV `xml:"AddTags>Tag"`
	RemoveTagKeys []string     `xml:"RemoveTagKeys>Key"`
}

// applyTagChanges reads a ChangeTagsForResource XML body and applies the add/remove operations.
// It returns an error if the body cannot be read or parsed.
func (h *Handler) applyTagChanges(resourceID string, r *http.Request) error {
	body, err := httputils.ReadBody(r)
	if err != nil {
		return fmt.Errorf("failed to read request body: %w", err)
	}

	if len(body) == 0 {
		return nil
	}

	var req applyTagChangesInput

	if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
		return fmt.Errorf("failed to parse XML: %w", xmlErr)
	}

	if len(req.AddTags) > 0 {
		kv := make(map[string]string, len(req.AddTags))
		for _, t := range req.AddTags {
			kv[t.Key] = t.Value
		}

		h.setTags(resourceID, kv)
	}

	if len(req.RemoveTagKeys) > 0 {
		h.removeTags(resourceID, req.RemoveTagKeys)
	}

	return nil
}

// writeXML marshals v to XML and writes it to the response.
func writeXML(c *echo.Context, statusCode int, v any) error {
	data, err := xml.MarshalIndent(v, "", "  ")
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	c.Response().Header().Set("Content-Type", "application/xml")
	c.Response().WriteHeader(statusCode)

	header := xml.Header
	_, _ = io.WriteString(c.Response(), header)
	_, _ = c.Response().Write(data)

	return nil
}

// xmlErrDetail is the nested error detail element in a Route53 error response.
type xmlErrDetail struct {
	Type    string `xml:"Type"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// xmlErrResp is the XML error response body for Route53.
type xmlErrResp struct {
	XMLName xml.Name     `xml:"ErrorResponse"`
	Xmlns   string       `xml:"xmlns,attr"`
	Error   xmlErrDetail `xml:"Error"`
}

// xmlError writes a Route 53-style XML error response.
func xmlError(c *echo.Context, statusCode int, code, message string) error {
	resp := xmlErrResp{Xmlns: route53Namespace}
	resp.Error.Type = "Sender"
	resp.Error.Code = code
	resp.Error.Message = message

	data, _ := xml.MarshalIndent(resp, "", "  ")

	c.Response().Header().Set("Content-Type", "application/xml")
	c.Response().WriteHeader(statusCode)
	_, _ = io.WriteString(c.Response(), xml.Header)
	_, _ = c.Response().Write(data)

	return nil
}

// handleBackendError maps backend errors to HTTP responses.
func handleBackendError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrHostedZoneNotFound):
		return xmlError(c, http.StatusNotFound, "NoSuchHostedZone", err.Error())
	case errors.Is(err, ErrHealthCheckNotFound):
		return xmlError(c, http.StatusNotFound, "NoSuchHealthCheck", err.Error())
	case errors.Is(err, ErrKeySigningKeyNotFound):
		return xmlError(c, http.StatusNotFound, "NoSuchKeySigningKey", err.Error())
	case errors.Is(err, ErrCidrCollectionNotFound):
		return xmlError(c, http.StatusNotFound, "NoSuchCidrCollection", err.Error())
	case errors.Is(err, ErrQueryLoggingConfigNotFound):
		return xmlError(c, http.StatusNotFound, "NoSuchQueryLoggingConfig", err.Error())
	case errors.Is(err, ErrDelegationSetNotFound):
		return xmlError(c, http.StatusNotFound, "NoSuchDelegationSet", err.Error())
	case errors.Is(err, ErrTrafficPolicyNotFound):
		return xmlError(c, http.StatusNotFound, "NoSuchTrafficPolicy", err.Error())
	case errors.Is(err, ErrTrafficPolicyInstNotFound):
		return xmlError(c, http.StatusNotFound, "NoSuchTrafficPolicyInstance", err.Error())
	case errors.Is(err, ErrInvalidInput):
		return xmlError(c, http.StatusBadRequest, "InvalidInput", err.Error())
	case errors.Is(err, ErrInvalidAction):
		return xmlError(c, http.StatusBadRequest, "InvalidChangeBatch", err.Error())
	case errors.Is(err, ErrChangeNotFound):
		return xmlError(c, http.StatusNotFound, "NoSuchChange", err.Error())
	case errors.Is(err, ErrNoSuchGeoLocation):
		return xmlError(c, http.StatusNotFound, "NoSuchGeoLocation", err.Error())
	case errors.Is(err, ErrQueryLoggingConfigAlreadyExists):
		return xmlError(c, http.StatusBadRequest, "QueryLoggingConfigAlreadyExists", err.Error())
	case errors.Is(err, ErrPublicZoneVPCAssociation):
		return xmlError(c, http.StatusBadRequest, "PublicZoneVPCAssociation", err.Error())
	case errors.Is(err, ErrVPCAssociationAuthorizationNF):
		return xmlError(c, http.StatusNotFound, "VPCAssociationAuthorizationNotFound", err.Error())
	case errors.Is(err, ErrKeySigningKeyWithActiveStatusNF):
		return xmlError(c, http.StatusBadRequest, "KeySigningKeyWithActiveStatusNotFound", err.Error())
	default:
		return xmlError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}
}

// ---- Health check XML types ----

type xmlAlarmIdentifier struct {
	Name   string `xml:"Name"`
	Region string `xml:"Region"`
}

type xmlHealthCheckConfig struct {
	AlarmIdentifier              *xmlAlarmIdentifier `xml:"AlarmIdentifier,omitempty"`
	IPAddress                    string              `xml:"IPAddress,omitempty"`
	FullyQualifiedDomainName     string              `xml:"FullyQualifiedDomainName,omitempty"`
	ResourcePath                 string              `xml:"ResourcePath,omitempty"`
	SearchString                 string              `xml:"SearchString,omitempty"`
	InsufficientDataHealthStatus string              `xml:"InsufficientDataHealthStatus,omitempty"`
	RoutingControlArn            string              `xml:"RoutingControlArn,omitempty"`
	Type                         string              `xml:"Type"`
	Regions                      []string            `xml:"Regions>Region,omitempty"`
	ChildHealthChecks            []string            `xml:"ChildHealthChecks>ChildHealthCheck,omitempty"`
	Port                         int                 `xml:"Port,omitempty"`
	RequestInterval              int                 `xml:"RequestInterval,omitempty"`
	FailureThreshold             int                 `xml:"FailureThreshold,omitempty"`
	HealthThreshold              int                 `xml:"HealthThreshold,omitempty"`
	EnableSNI                    bool                `xml:"EnableSNI,omitempty"`
	MeasureLatency               bool                `xml:"MeasureLatency,omitempty"`
	Disabled                     bool                `xml:"Disabled,omitempty"`
	Inverted                     bool                `xml:"Inverted,omitempty"`
}

type xmlHealthCheck struct {
	XMLName         xml.Name             `xml:"HealthCheck"`
	ID              string               `xml:"Id"`
	CallerReference string               `xml:"CallerReference"`
	Config          xmlHealthCheckConfig `xml:"HealthCheckConfig"`
}

type xmlCreateHealthCheckRequest struct {
	XMLName         xml.Name             `xml:"CreateHealthCheckRequest"`
	CallerReference string               `xml:"CallerReference"`
	Config          xmlHealthCheckConfig `xml:"HealthCheckConfig"`
}

type xmlUpdateHealthCheckRequest struct {
	AlarmIdentifier              *xmlAlarmIdentifier `xml:"AlarmIdentifier"`
	Inverted                     *bool               `xml:"Inverted"`
	XMLName                      xml.Name            `xml:"UpdateHealthCheckRequest"`
	IPAddress                    string              `xml:"IPAddress,omitempty"`
	FullyQualifiedDomainName     string              `xml:"FullyQualifiedDomainName,omitempty"`
	ResourcePath                 string              `xml:"ResourcePath,omitempty"`
	SearchString                 string              `xml:"SearchString,omitempty"`
	InsufficientDataHealthStatus string              `xml:"InsufficientDataHealthStatus,omitempty"`
	RoutingControlArn            string              `xml:"RoutingControlArn,omitempty"`
	Regions                      []string            `xml:"Regions>Region,omitempty"`
	ChildHealthChecks            []string            `xml:"ChildHealthChecks>ChildHealthCheck,omitempty"`
	Port                         int                 `xml:"Port,omitempty"`
	RequestInterval              int                 `xml:"RequestInterval,omitempty"`
	FailureThreshold             int                 `xml:"FailureThreshold,omitempty"`
	HealthThreshold              int                 `xml:"HealthThreshold,omitempty"`
	EnableSNI                    bool                `xml:"EnableSNI,omitempty"`
	MeasureLatency               bool                `xml:"MeasureLatency,omitempty"`
	Disabled                     bool                `xml:"Disabled,omitempty"`
}

type xmlCreateHealthCheckResponse struct {
	XMLName     xml.Name       `xml:"CreateHealthCheckResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	HealthCheck xmlHealthCheck `xml:"HealthCheck"`
}

type xmlGetHealthCheckResponse struct {
	XMLName     xml.Name       `xml:"GetHealthCheckResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	HealthCheck xmlHealthCheck `xml:"HealthCheck"`
}

type xmlListHealthChecksResponse struct {
	XMLName      xml.Name         `xml:"ListHealthChecksResponse"`
	Xmlns        string           `xml:"xmlns,attr"`
	MaxItems     string           `xml:"MaxItems"`
	NextMarker   string           `xml:"NextMarker,omitempty"`
	HealthChecks []xmlHealthCheck `xml:"HealthChecks>HealthCheck"`
	IsTruncated  bool             `xml:"IsTruncated"`
}

type xmlDeleteHealthCheckResponse struct {
	XMLName xml.Name `xml:"DeleteHealthCheckResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type xmlUpdateHealthCheckResponse struct {
	XMLName     xml.Name       `xml:"UpdateHealthCheckResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	HealthCheck xmlHealthCheck `xml:"HealthCheck"`
}

type xmlHealthCheckObservation struct {
	StatusReport struct {
		CheckedTime time.Time `xml:"CheckedTime"`
		Status      string    `xml:"Status"`
	} `xml:"StatusReport"`
	Region    string `xml:"Region"`
	IPAddress string `xml:"IPAddress"`
}

type xmlGetHealthCheckStatusResponse struct {
	XMLName                 xml.Name                    `xml:"GetHealthCheckStatusResponse"`
	Xmlns                   string                      `xml:"xmlns,attr"`
	HealthCheckObservations []xmlHealthCheckObservation `xml:"HealthCheckObservations>HealthCheckObservation"`
}

// toXMLHealthCheck converts a HealthCheck to its XML representation.
func toXMLHealthCheck(hc *HealthCheck) xmlHealthCheck {
	cfg := xmlHealthCheckConfig{
		IPAddress:                    hc.Config.IPAddress,
		FullyQualifiedDomainName:     hc.Config.FullyQualifiedDomainName,
		ResourcePath:                 hc.Config.ResourcePath,
		SearchString:                 hc.Config.SearchString,
		InsufficientDataHealthStatus: hc.Config.InsufficientDataHealthStatus,
		RoutingControlArn:            hc.Config.RoutingControlArn,
		Type:                         string(hc.Config.Type),
		Port:                         hc.Config.Port,
		RequestInterval:              hc.Config.RequestInterval,
		FailureThreshold:             hc.Config.FailureThreshold,
		HealthThreshold:              hc.Config.HealthThreshold,
		EnableSNI:                    hc.Config.EnableSNI,
		MeasureLatency:               hc.Config.MeasureLatency,
		Disabled:                     hc.Config.Disabled,
		Inverted:                     hc.Config.Inverted,
		ChildHealthChecks:            hc.Config.ChildHealthChecks,
		Regions:                      hc.Config.Regions,
	}

	if hc.Config.AlarmIdentifier != nil {
		cfg.AlarmIdentifier = &xmlAlarmIdentifier{
			Name:   hc.Config.AlarmIdentifier.Name,
			Region: hc.Config.AlarmIdentifier.Region,
		}
	}

	return xmlHealthCheck{
		ID:              hc.ID,
		CallerReference: hc.CallerReference,
		Config:          cfg,
	}
}

// extractHealthCheckID returns the health check ID from a path like /2013-04-01/healthcheck/{Id}...
func extractHealthCheckID(path string) string {
	rest := strings.TrimPrefix(path, route53HealthCheckPrefix)
	parts := strings.SplitN(rest, "/", zoneIDAndRest)
	if len(parts) > 0 {
		return parts[0]
	}

	return ""
}

// ---- Health check handlers ----

func (h *Handler) createHealthCheck(c *echo.Context) error {
	ctx := c.Request().Context()

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlCreateHealthCheckRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	cfg := HealthCheckConfig{
		IPAddress:                req.Config.IPAddress,
		FullyQualifiedDomainName: req.Config.FullyQualifiedDomainName,
		ResourcePath:             req.Config.ResourcePath,
		Type:                     HealthCheckType(req.Config.Type),
		Port:                     req.Config.Port,
		RequestInterval:          req.Config.RequestInterval,
		FailureThreshold:         req.Config.FailureThreshold,
		HealthThreshold:          req.Config.HealthThreshold,
		Inverted:                 req.Config.Inverted,
		ChildHealthChecks:        req.Config.ChildHealthChecks,
	}

	hc, err := h.Backend.CreateHealthCheck(req.CallerReference, cfg)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 CreateHealthCheck", "id", hc.ID)

	resp := xmlCreateHealthCheckResponse{
		Xmlns:       route53Namespace,
		HealthCheck: toXMLHealthCheck(hc),
	}

	c.Response().Header().Set("Location", "/2013-04-01/healthcheck/"+hc.ID)

	return writeXML(c, http.StatusCreated, resp)
}

func (h *Handler) getHealthCheck(c *echo.Context, path string) error {
	ctx := c.Request().Context()
	id := extractHealthCheckID(path)

	hc, err := h.Backend.GetHealthCheck(id)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 GetHealthCheck", "id", id)

	return writeXML(c, http.StatusOK, xmlGetHealthCheckResponse{
		Xmlns:       route53Namespace,
		HealthCheck: toXMLHealthCheck(hc),
	})
}

func (h *Handler) listHealthChecks(c *echo.Context) error {
	q := c.Request().URL.Query()
	marker := q.Get("marker")
	maxItems := 0

	if v := q.Get("maxitems"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxItems = n
		}
	}

	p, err := h.Backend.ListHealthChecks(marker, maxItems)
	if err != nil {
		return handleBackendError(c, err)
	}

	xmlHCs := make([]xmlHealthCheck, len(p.Data))
	for i := range p.Data {
		xmlHCs[i] = toXMLHealthCheck(&p.Data[i])
	}

	return writeXML(c, http.StatusOK, xmlListHealthChecksResponse{
		Xmlns:        route53Namespace,
		HealthChecks: xmlHCs,
		IsTruncated:  p.Next != "",
		NextMarker:   p.Next,
		MaxItems:     strconv.Itoa(maxItems),
	})
}

func (h *Handler) deleteHealthCheck(c *echo.Context, path string) error {
	ctx := c.Request().Context()
	id := extractHealthCheckID(path)

	if err := h.Backend.DeleteHealthCheck(id); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 DeleteHealthCheck", "id", id)

	return writeXML(c, http.StatusOK, xmlDeleteHealthCheckResponse{Xmlns: route53Namespace})
}

func (h *Handler) updateHealthCheck(c *echo.Context, path string) error {
	ctx := c.Request().Context()
	id := extractHealthCheckID(path)

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlUpdateHealthCheckRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	existing, err := h.Backend.GetHealthCheck(id)
	if err != nil {
		return handleBackendError(c, err)
	}

	// Merge non-zero fields from the request into the existing config.
	cfg := existing.Config
	if req.IPAddress != "" {
		cfg.IPAddress = req.IPAddress
	}

	if req.FullyQualifiedDomainName != "" {
		cfg.FullyQualifiedDomainName = req.FullyQualifiedDomainName
	}

	if req.ResourcePath != "" {
		cfg.ResourcePath = req.ResourcePath
	}

	if req.Port != 0 {
		cfg.Port = req.Port
	}

	if req.RequestInterval != 0 {
		cfg.RequestInterval = req.RequestInterval
	}

	if req.FailureThreshold != 0 {
		cfg.FailureThreshold = req.FailureThreshold
	}

	if req.HealthThreshold != 0 {
		cfg.HealthThreshold = req.HealthThreshold
	}

	if req.Inverted != nil {
		cfg.Inverted = *req.Inverted
	}

	if req.SearchString != "" {
		cfg.SearchString = req.SearchString
	}

	if req.InsufficientDataHealthStatus != "" {
		cfg.InsufficientDataHealthStatus = req.InsufficientDataHealthStatus
	}

	if req.RoutingControlArn != "" {
		cfg.RoutingControlArn = req.RoutingControlArn
	}

	if req.EnableSNI {
		cfg.EnableSNI = true
	}

	if req.MeasureLatency {
		cfg.MeasureLatency = true
	}

	if req.Disabled {
		cfg.Disabled = true
	}

	if len(req.Regions) > 0 {
		cfg.Regions = req.Regions
	}

	if len(req.ChildHealthChecks) > 0 {
		cfg.ChildHealthChecks = req.ChildHealthChecks
	}

	if req.AlarmIdentifier != nil {
		cfg.AlarmIdentifier = &AlarmIdentifier{
			Name:   req.AlarmIdentifier.Name,
			Region: req.AlarmIdentifier.Region,
		}
	}

	hc, err := h.Backend.UpdateHealthCheck(id, cfg)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 UpdateHealthCheck", "id", id)

	return writeXML(c, http.StatusOK, xmlUpdateHealthCheckResponse{
		Xmlns:       route53Namespace,
		HealthCheck: toXMLHealthCheck(hc),
	})
}

func (h *Handler) getHealthCheckStatus(c *echo.Context, path string) error {
	ctx := c.Request().Context()
	// path is /2013-04-01/healthcheck/{id}/status — strip the /status suffix first.
	withoutStatus := strings.TrimSuffix(path, route53StatusSuffix)
	id := extractHealthCheckID(withoutStatus)

	status, err := h.Backend.GetHealthCheckStatus(id)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 GetHealthCheckStatus", "id", id, "status", status)

	hc, hcErr := h.Backend.GetHealthCheck(id)
	observerRegions := []string{"us-east-1", "us-west-2", "eu-west-1"}
	if hcErr == nil && len(hc.Config.Regions) > 0 {
		observerRegions = hc.Config.Regions
	}

	checkedAt := time.Now()
	observations := make([]xmlHealthCheckObservation, 0, len(observerRegions))

	for _, region := range observerRegions {
		obsStatus := status
		if hcErr == nil && hc.Config.Inverted {
			if obsStatus == "Healthy" {
				obsStatus = "Unhealthy"
			} else {
				obsStatus = "Healthy"
			}
		}

		obs := xmlHealthCheckObservation{
			Region:    region,
			IPAddress: "0.0.0.0",
		}
		obs.StatusReport.Status = obsStatus
		obs.StatusReport.CheckedTime = checkedAt
		observations = append(observations, obs)
	}

	return writeXML(c, http.StatusOK, xmlGetHealthCheckStatusResponse{
		Xmlns:                   route53Namespace,
		HealthCheckObservations: observations,
	})
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	h.Backend.Reset()
	h.tagsMu.Lock("Reset")
	h.tags = make(map[string]*svcTags.Tags)
	h.tagsMu.Unlock()
}

// ---- New operations: XML types ----

type xmlVPC struct {
	VPCRegion string `xml:"VPCRegion,omitempty"`
	VPCID     string `xml:"VPCId"`
}

type xmlAssociateVPCRequest struct {
	XMLName xml.Name `xml:"AssociateVPCWithHostedZoneRequest"`
	VPC     xmlVPC   `xml:"VPC"`
	Comment string   `xml:"Comment,omitempty"`
}

type xmlAssociateVPCResponse struct {
	XMLName    xml.Name      `xml:"AssociateVPCWithHostedZoneResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
}

type xmlKSK struct {
	XMLName                  xml.Name `xml:"KeySigningKey"`
	Name                     string   `xml:"Name"`
	KMSArn                   string   `xml:"KmsArn,omitempty"`
	Status                   string   `xml:"Status"`
	SigningAlgorithmMnemonic string   `xml:"SigningAlgorithmMnemonic,omitempty"`
	DigestAlgorithmMnemonic  string   `xml:"DigestAlgorithmMnemonic,omitempty"`
	PublicKey                string   `xml:"PublicKey,omitempty"`
	DSRecord                 string   `xml:"DSRecord,omitempty"`
	DigestValue              string   `xml:"DigestValue,omitempty"`
	Flag                     int      `xml:"Flag,omitempty"`
	SigningAlgorithmType     int      `xml:"SigningAlgorithmType,omitempty"`
	DigestAlgorithmType      int      `xml:"DigestAlgorithmType,omitempty"`
	KeyTag                   int      `xml:"KeyTag,omitempty"`
}

type xmlCreateKSKRequest struct {
	XMLName                 xml.Name `xml:"CreateKeySigningKeyRequest"`
	HostedZoneID            string   `xml:"HostedZoneId"`
	CallerReference         string   `xml:"CallerReference"`
	Name                    string   `xml:"Name"`
	KeyManagementServiceArn string   `xml:"KeyManagementServiceArn,omitempty"`
	Status                  string   `xml:"Status,omitempty"`
}

type xmlCreateKSKResponse struct {
	XMLName       xml.Name      `xml:"CreateKeySigningKeyResponse"`
	Xmlns         string        `xml:"xmlns,attr"`
	ChangeInfo    xmlChangeInfo `xml:"ChangeInfo"`
	KeySigningKey xmlKSK        `xml:"KeySigningKey"`
}

type xmlActivateKSKResponse struct {
	XMLName       xml.Name      `xml:"ActivateKeySigningKeyResponse"`
	Xmlns         string        `xml:"xmlns,attr"`
	ChangeInfo    xmlChangeInfo `xml:"ChangeInfo"`
	KeySigningKey xmlKSK        `xml:"KeySigningKey"`
}

type xmlCidrCollection struct {
	ID      string `xml:"Id"`
	Name    string `xml:"Name"`
	ARN     string `xml:"Arn,omitempty"`
	Version int64  `xml:"Version"`
}

type xmlCreateCidrCollectionRequest struct {
	XMLName         xml.Name `xml:"CreateCidrCollectionRequest"`
	Name            string   `xml:"Name"`
	CallerReference string   `xml:"CallerReference,omitempty"`
}

type xmlCreateCidrCollectionResponse struct {
	XMLName    xml.Name          `xml:"CreateCidrCollectionResponse"`
	Xmlns      string            `xml:"xmlns,attr"`
	Collection xmlCidrCollection `xml:"Collection"`
}

type xmlChangeCidrCollectionResponse struct {
	XMLName xml.Name `xml:"ChangeCidrCollectionResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	ID      string   `xml:"Id"`
	Version int64    `xml:"Version"`
}

type xmlCidrChangeEntry struct {
	LocationName string   `xml:"LocationName"`
	Action       string   `xml:"Action"`
	CidrList     []string `xml:"CidrList>Cidr"`
}

type xmlChangeCidrCollectionRequest struct {
	XMLName xml.Name             `xml:"ChangeCidrCollectionRequest"`
	Changes []xmlCidrChangeEntry `xml:"Changes>Change"`
}

type xmlQueryLoggingConfig struct {
	XMLName                   xml.Name `xml:"QueryLoggingConfig"`
	ID                        string   `xml:"Id"`
	HostedZoneID              string   `xml:"HostedZoneId"`
	CloudWatchLogsLogGroupArn string   `xml:"CloudWatchLogsLogGroupArn"`
}

type xmlCreateQueryLoggingConfigRequest struct {
	XMLName                   xml.Name `xml:"CreateQueryLoggingConfigRequest"`
	HostedZoneID              string   `xml:"HostedZoneId"`
	CloudWatchLogsLogGroupArn string   `xml:"CloudWatchLogsLogGroupArn"`
}

type xmlCreateQueryLoggingConfigResponse struct {
	XMLName            xml.Name              `xml:"CreateQueryLoggingConfigResponse"`
	Xmlns              string                `xml:"xmlns,attr"`
	QueryLoggingConfig xmlQueryLoggingConfig `xml:"QueryLoggingConfig"`
}

type xmlDelegationSetCreate struct {
	XMLName         xml.Name `xml:"CreateReusableDelegationSetRequest"`
	CallerReference string   `xml:"CallerReference"`
	HostedZoneID    string   `xml:"HostedZoneId,omitempty"`
}

type xmlReusableDelegationSetResponse struct {
	XMLName       xml.Name         `xml:"CreateReusableDelegationSetResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	DelegationSet xmlDelegationSet `xml:"DelegationSet"`
}

type xmlTrafficPolicy struct {
	XMLName  xml.Name `xml:"TrafficPolicy"`
	ID       string   `xml:"Id"`
	Name     string   `xml:"Name"`
	Document string   `xml:"Document,omitempty"`
	Comment  string   `xml:"Comment,omitempty"`
	Type     string   `xml:"Type"`
	Version  int32    `xml:"Version"`
}

type xmlCreateTrafficPolicyRequest struct {
	XMLName  xml.Name `xml:"CreateTrafficPolicyRequest"`
	Name     string   `xml:"Name"`
	Document string   `xml:"Document"`
	Comment  string   `xml:"Comment,omitempty"`
}

type xmlCreateTrafficPolicyResponse struct {
	XMLName       xml.Name         `xml:"CreateTrafficPolicyResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	TrafficPolicy xmlTrafficPolicy `xml:"TrafficPolicy"`
}

type xmlCreateTrafficPolicyVersionRequest struct {
	XMLName  xml.Name `xml:"CreateTrafficPolicyVersionRequest"`
	Document string   `xml:"Document"`
	Comment  string   `xml:"Comment,omitempty"`
}

type xmlCreateTrafficPolicyVersionResponse struct {
	XMLName       xml.Name         `xml:"CreateTrafficPolicyVersionResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	TrafficPolicy xmlTrafficPolicy `xml:"TrafficPolicy"`
}

type xmlTrafficPolicyInstance struct {
	XMLName              xml.Name `xml:"TrafficPolicyInstance"`
	ID                   string   `xml:"Id"`
	HostedZoneID         string   `xml:"HostedZoneId"`
	Name                 string   `xml:"Name"`
	TrafficPolicyID      string   `xml:"TrafficPolicyId"`
	TrafficPolicyType    string   `xml:"TrafficPolicyType"`
	State                string   `xml:"State"`
	TTL                  int64    `xml:"TTL"`
	TrafficPolicyVersion int32    `xml:"TrafficPolicyVersion"`
}

type xmlCreateTrafficPolicyInstanceRequest struct {
	XMLName              xml.Name `xml:"CreateTrafficPolicyInstanceRequest"`
	HostedZoneID         string   `xml:"HostedZoneId"`
	Name                 string   `xml:"Name"`
	TrafficPolicyID      string   `xml:"TrafficPolicyId"`
	TrafficPolicyVersion int32    `xml:"TrafficPolicyVersion"`
	TTL                  int64    `xml:"TTL"`
}

type xmlCreateTrafficPolicyInstanceResponse struct {
	XMLName               xml.Name                 `xml:"CreateTrafficPolicyInstanceResponse"`
	Xmlns                 string                   `xml:"xmlns,attr"`
	TrafficPolicyInstance xmlTrafficPolicyInstance `xml:"TrafficPolicyInstance"`
}

// ---- New operations: route helpers ----

// DNSSEC XML types

type xmlGetDNSSECResponse struct {
	XMLName        xml.Name        `xml:"GetDNSSECResponse"`
	Xmlns          string          `xml:"xmlns,attr"`
	Status         xmlDNSSECStatus `xml:"Status"`
	KeySigningKeys []xmlKSK        `xml:"KeySigningKeys>member"`
}

type xmlDNSSECStatus struct {
	ServeSignature string `xml:"ServeSignature"`
	StatusMessage  string `xml:"StatusMessage,omitempty"`
}

// Traffic Policy list XML types

type xmlListTrafficPoliciesResponse struct {
	XMLName         xml.Name                  `xml:"ListTrafficPoliciesResponse"`
	Xmlns           string                    `xml:"xmlns,attr"`
	MaxItems        string                    `xml:"MaxItems"`
	TrafficPolicies []xmlTrafficPolicySummary `xml:"TrafficPolicySummaries>TrafficPolicySummary"`
	IsTruncated     bool                      `xml:"IsTruncated"`
}

type xmlTrafficPolicySummary struct {
	ID                 string `xml:"Id"`
	Name               string `xml:"Name"`
	Type               string `xml:"Type"`
	LatestVersion      int32  `xml:"LatestVersion"`
	TrafficPolicyCount int32  `xml:"TrafficPolicyCount"`
}

type xmlListTrafficPolicyVersionsResponse struct {
	XMLName         xml.Name           `xml:"ListTrafficPolicyVersionsResponse"`
	Xmlns           string             `xml:"xmlns,attr"`
	MaxItems        string             `xml:"MaxItems"`
	TrafficPolicies []xmlTrafficPolicy `xml:"TrafficPolicies>TrafficPolicy"`
	IsTruncated     bool               `xml:"IsTruncated"`
}

type xmlListTrafficPolicyInstancesResponse struct {
	XMLName                xml.Name                   `xml:"ListTrafficPolicyInstancesResponse"`
	Xmlns                  string                     `xml:"xmlns,attr"`
	MaxItems               string                     `xml:"MaxItems"`
	TrafficPolicyInstances []xmlTrafficPolicyInstance `xml:"TrafficPolicyInstances>TrafficPolicyInstance"`
	IsTruncated            bool                       `xml:"IsTruncated"`
}

type xmlGetTPInstanceCountResponse struct {
	XMLName                    xml.Name `xml:"GetTrafficPolicyInstanceCountResponse"`
	Xmlns                      string   `xml:"xmlns,attr"`
	TrafficPolicyInstanceCount int32    `xml:"TrafficPolicyInstanceCount"`
}

// CIDR collections list XML types

type xmlListCidrCollectionsResponse struct {
	XMLName         xml.Name                   `xml:"ListCidrCollectionsResponse"`
	Xmlns           string                     `xml:"xmlns,attr"`
	NextToken       string                     `xml:"NextToken,omitempty"`
	CidrCollections []xmlCidrCollectionSummary `xml:"CidrCollections>member"`
	IsTruncated     bool                       `xml:"IsTruncated"`
}

type xmlCidrCollectionSummary struct {
	ARN     string `xml:"Arn"`
	ID      string `xml:"Id"`
	Name    string `xml:"Name"`
	Version int64  `xml:"Version"`
}

func (h *Handler) routeKSKRoot(c *echo.Context, method string) error {
	if method == http.MethodPost {
		return h.createKeySigningKey(c)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported method on /keysigningkey",
	)
}

func (h *Handler) routeKSK(c *echo.Context, path, method string) error {
	if strings.HasSuffix(path, route53ActivateSuffix) && method == http.MethodPost {
		return h.activateKeySigningKey(c, path)
	}

	if strings.HasSuffix(path, route53DeactivateSuffix) && method == http.MethodPost {
		return h.deactivateKeySigningKey(c, path)
	}

	if method == http.MethodDelete {
		return h.deleteKeySigningKey(c, path)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported operation on key signing key",
	)
}

func (h *Handler) routeCidrCollectionRoot(c *echo.Context, method string) error {
	switch method {
	case http.MethodPost:
		return h.createCidrCollection(c)
	case http.MethodGet:
		return h.listCidrCollections(c)
	default:
		return xmlError(
			c,
			http.StatusNotFound,
			"NoSuchOperation",
			"unsupported method on /cidrcollection",
		)
	}
}

func (h *Handler) routeCidrCollection(c *echo.Context, path, method string) error {
	switch method {
	case http.MethodPost:
		return h.changeCidrCollection(c, path)
	case http.MethodDelete:
		return h.deleteCidrCollection(c, path)
	case http.MethodGet:
		// ListCidrLocations → GET /cidrcollection/{Id}
		// ListCidrBlocks → GET /cidrcollection/{Id}/cidrblocks
		if strings.HasSuffix(path, "/cidrblocks") {
			return h.listCidrBlocks(c, path)
		}

		return h.listCidrLocations(c, path)
	default:
		return xmlError(
			c,
			http.StatusNotFound,
			"NoSuchOperation",
			"unsupported method on cidrcollection",
		)
	}
}

func (h *Handler) routeQueryLogging(c *echo.Context, method string) error {
	switch method {
	case http.MethodPost:
		return h.createQueryLoggingConfig(c)
	case http.MethodGet:
		return h.listQueryLoggingConfigs(c)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation", "unsupported method on /queryloggingconfig")
	}
}

func (h *Handler) routeDelegationSetRoot(c *echo.Context, method string) error {
	switch method {
	case http.MethodPost:
		return h.createReusableDelegationSet(c)
	case http.MethodGet:
		return h.listReusableDelegationSets(c)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation", "unsupported method on /delegationset")
	}
}

func (h *Handler) routeTrafficPolicyRoot(c *echo.Context, method string) error {
	if method == http.MethodPost {
		return h.createTrafficPolicy(c)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported method on /trafficpolicy",
	)
}

func (h *Handler) routeTrafficPolicyVersion(c *echo.Context, path, method string) error {
	rest := strings.TrimPrefix(path, route53TrafficPolicyPrefix)
	// If rest contains a "/" it's /{id}/{version}
	if strings.Contains(rest, "/") {
		parts := strings.SplitN(rest, "/", 2) //nolint:mnd // split id and version
		id := parts[0]
		versionStr := parts[1]

		version64, err := strconv.ParseInt(versionStr, 10, 32)
		if err != nil {
			return xmlError(c, http.StatusBadRequest, "InvalidInput", "invalid version number")
		}

		version := int32(version64)

		switch method {
		case http.MethodGet:
			return h.getTrafficPolicy(c, id, version)
		case http.MethodDelete:
			return h.deleteTrafficPolicy(c, id, version)
		case http.MethodPost:
			return h.updateTrafficPolicyComment(c, path)
		default:
			return xmlError(c, http.StatusNotFound, "NoSuchOperation",
				"unsupported method on traffic policy version")
		}
	}

	if method == http.MethodPost {
		return h.createTrafficPolicyVersion(c, path)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported method on traffic policy version",
	)
}

func (h *Handler) routeTPInstanceRoot(c *echo.Context, method string) error {
	if method == http.MethodPost {
		return h.createTrafficPolicyInstance(c)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported method on /trafficpolicyinstance",
	)
}

func (h *Handler) routeTrafficPoliciesRoot(c *echo.Context, method string) error {
	if method == http.MethodGet {
		return h.listTrafficPolicies(c)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported method on /trafficpolicies",
	)
}

func (h *Handler) routeTrafficPoliciesVersions(c *echo.Context, path, method string) error {
	if method == http.MethodGet {
		// path is /2013-04-01/trafficpolicies/{Id}/versions
		id := strings.TrimPrefix(path, route53TrafficPoliciesPrefix)
		id = strings.TrimSuffix(id, "/versions")

		return h.listTrafficPolicyVersions(c, id)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported method on traffic policy versions",
	)
}

func (h *Handler) routeTPInstancesRoot(c *echo.Context, method string) error {
	if method == http.MethodGet {
		return h.listTrafficPolicyInstances(c)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported method on /trafficpolicyinstances",
	)
}

func (h *Handler) routeTPInstanceCount(c *echo.Context, method string) error {
	if method == http.MethodGet {
		return h.getTrafficPolicyInstanceCount(c)
	}

	return xmlError(
		c,
		http.StatusNotFound,
		"NoSuchOperation",
		"unsupported method on /trafficpolicyinstancecount",
	)
}

func (h *Handler) routeTPInstance(c *echo.Context, path, method string) error {
	id := strings.TrimPrefix(path, route53TPInstancePrefix)

	switch method {
	case http.MethodGet:
		return h.getTrafficPolicyInstance(c, id)
	case http.MethodDelete:
		return h.deleteTrafficPolicyInstance(c, id)
	case http.MethodPost:
		return h.updateTrafficPolicyInstance(c, path)
	default:
		return xmlError(
			c,
			http.StatusNotFound,
			"NoSuchOperation",
			"unsupported method on traffic policy instance",
		)
	}
}

// ---- New operations: handler functions ----

func toXMLKSK(ksk *KeySigningKey) xmlKSK {
	return xmlKSK{
		Name:                     ksk.Name,
		KMSArn:                   ksk.KeyManagementServiceArn,
		Status:                   ksk.Status,
		Flag:                     ksk.Flag,
		SigningAlgorithmMnemonic: ksk.SigningAlgorithmMnemonic,
		SigningAlgorithmType:     ksk.SigningAlgorithmType,
		DigestAlgorithmMnemonic:  ksk.DigestAlgorithmMnemonic,
		DigestAlgorithmType:      ksk.DigestAlgorithmType,
		KeyTag:                   ksk.KeyTag,
		PublicKey:                ksk.PublicKey,
		DigestValue:              ksk.DigestValue,
		DSRecord:                 ksk.DSRecord,
	}
}

func toXMLTrafficPolicy(tp *TrafficPolicy) xmlTrafficPolicy {
	return xmlTrafficPolicy{
		ID:       tp.ID,
		Name:     tp.Name,
		Document: tp.Document,
		Comment:  tp.Comment,
		Type:     tp.Type,
		Version:  tp.Version,
	}
}

func toXMLTPInstance(inst *TrafficPolicyInstance) xmlTrafficPolicyInstance {
	return xmlTrafficPolicyInstance{
		ID:                   inst.ID,
		HostedZoneID:         inst.HostedZoneID,
		Name:                 inst.Name,
		TrafficPolicyID:      inst.TrafficPolicyID,
		TrafficPolicyVersion: inst.TrafficPolicyVersion,
		TrafficPolicyType:    inst.TrafficPolicyType,
		TTL:                  inst.TTL,
		State:                inst.State,
	}
}

func (h *Handler) createKeySigningKey(c *echo.Context) error {
	ctx := c.Request().Context()

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlCreateKSKRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	ksk, err := h.Backend.CreateKeySigningKey(
		req.HostedZoneID,
		req.CallerReference,
		req.Name,
		req.KeyManagementServiceArn,
		req.Status,
	)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 CreateKeySigningKey", "name", ksk.Name)

	resp := xmlCreateKSKResponse{
		Xmlns:         route53Namespace,
		KeySigningKey: toXMLKSK(ksk),
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/C" + ksk.HostedZoneID,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
	}

	c.Response().
		Header().
		Set("Location", "/2013-04-01/keysigningkey/"+ksk.HostedZoneID+"/"+ksk.Name)

	return writeXML(c, http.StatusCreated, resp)
}

func (h *Handler) activateKeySigningKey(c *echo.Context, path string) error {
	ctx := c.Request().Context()
	// path is /2013-04-01/keysigningkey/{HostedZoneId}/{Name}/activate
	withoutSuffix := strings.TrimSuffix(path, route53ActivateSuffix)
	rest := strings.TrimPrefix(withoutSuffix, route53KSKPrefix)
	parts := strings.SplitN(rest, "/", zoneIDAndRest)

	if len(parts) < zoneIDAndRest {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "invalid key signing key path")
	}

	hostedZoneID := parts[0]
	name := parts[1]

	ksk, err := h.Backend.ActivateKeySigningKey(hostedZoneID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 ActivateKeySigningKey", "name", name, "zoneID", hostedZoneID)

	return writeXML(c, http.StatusOK, xmlActivateKSKResponse{
		Xmlns:         route53Namespace,
		KeySigningKey: toXMLKSK(ksk),
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/C" + hostedZoneID,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
	})
}

func (h *Handler) associateVPCWithHostedZone(c *echo.Context, path string) error {
	ctx := c.Request().Context()
	// path is /2013-04-01/hostedzone/{Id}/associatevpc
	withoutSuffix := strings.TrimSuffix(path, route53AssociateVPCSuffix)
	zoneID := extractZoneID(withoutSuffix)

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlAssociateVPCRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	if err = h.Backend.AssociateVPCWithHostedZone(zoneID, req.VPC.VPCID, req.VPC.VPCRegion); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 AssociateVPCWithHostedZone", "zoneID", zoneID, "vpcID", req.VPC.VPCID)

	return writeXML(c, http.StatusOK, xmlAssociateVPCResponse{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/C" + zoneID,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
	})
}

func (h *Handler) createCidrCollection(c *echo.Context) error {
	ctx := c.Request().Context()

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlCreateCidrCollectionRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	col, err := h.Backend.CreateCidrCollection(req.Name, req.CallerReference)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 CreateCidrCollection", "id", col.ID, "name", col.Name)

	resp := xmlCreateCidrCollectionResponse{
		Xmlns: route53Namespace,
		Collection: xmlCidrCollection{
			ID:      col.ID,
			Name:    col.Name,
			Version: col.Version,
			ARN:     col.ARN,
		},
	}

	c.Response().Header().Set("Location", "/2013-04-01/cidrcollection/"+col.ID)

	return writeXML(c, http.StatusCreated, resp)
}

func (h *Handler) changeCidrCollection(c *echo.Context, path string) error {
	ctx := c.Request().Context()
	collectionID := strings.TrimPrefix(path, route53CidrCollectionPrefix)

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlChangeCidrCollectionRequest
	if len(body) > 0 {
		if err = xml.Unmarshal(body, &req); err != nil {
			return xmlError(
				c,
				http.StatusBadRequest,
				"InvalidInput",
				"failed to parse XML: "+err.Error(),
			)
		}
	}

	changes := make([]CidrCollectionChange, 0, len(req.Changes))
	for _, ch := range req.Changes {
		changes = append(changes, CidrCollectionChange(ch))
	}

	col, err := h.Backend.ChangeCidrCollection(collectionID, changes)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 ChangeCidrCollection", "id", collectionID)

	return writeXML(c, http.StatusOK, xmlChangeCidrCollectionResponse{
		Xmlns:   route53Namespace,
		ID:      col.ID,
		Version: col.Version,
	})
}

func (h *Handler) createQueryLoggingConfig(c *echo.Context) error {
	ctx := c.Request().Context()

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlCreateQueryLoggingConfigRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	cfg, err := h.Backend.CreateQueryLoggingConfig(req.HostedZoneID, req.CloudWatchLogsLogGroupArn)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 CreateQueryLoggingConfig", "id", cfg.ID)

	resp := xmlCreateQueryLoggingConfigResponse{
		Xmlns: route53Namespace,
		QueryLoggingConfig: xmlQueryLoggingConfig{
			ID:                        cfg.ID,
			HostedZoneID:              cfg.HostedZoneID,
			CloudWatchLogsLogGroupArn: cfg.CloudWatchLogsLogGroupArn,
		},
	}

	c.Response().Header().Set("Location", "/2013-04-01/queryloggingconfig/"+cfg.ID)

	return writeXML(c, http.StatusCreated, resp)
}

func (h *Handler) createReusableDelegationSet(c *echo.Context) error {
	ctx := c.Request().Context()

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlDelegationSetCreate
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	ds, err := h.Backend.CreateReusableDelegationSet(req.CallerReference, req.HostedZoneID)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 CreateReusableDelegationSet", "id", ds.ID)

	resp := xmlReusableDelegationSetResponse{
		Xmlns: route53Namespace,
		DelegationSet: xmlDelegationSet{
			ID:          ds.ID,
			NameServers: ds.NameServers,
		},
	}

	c.Response().Header().Set("Location", "/2013-04-01"+ds.ID)

	return writeXML(c, http.StatusCreated, resp)
}

func (h *Handler) createTrafficPolicy(c *echo.Context) error {
	ctx := c.Request().Context()

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlCreateTrafficPolicyRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	tp, err := h.Backend.CreateTrafficPolicy(req.Name, req.Document, req.Comment)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 CreateTrafficPolicy", "id", tp.ID)

	resp := xmlCreateTrafficPolicyResponse{
		Xmlns:         route53Namespace,
		TrafficPolicy: toXMLTrafficPolicy(tp),
	}

	c.Response().Header().Set("Location", "/2013-04-01/trafficpolicy/"+tp.ID+"/1")

	return writeXML(c, http.StatusCreated, resp)
}

func (h *Handler) createTrafficPolicyVersion(c *echo.Context, path string) error {
	ctx := c.Request().Context()
	id := strings.TrimPrefix(path, route53TrafficPolicyPrefix)

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlCreateTrafficPolicyVersionRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	tp, err := h.Backend.CreateTrafficPolicyVersion(id, req.Document, req.Comment)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 CreateTrafficPolicyVersion", "id", id, "version", tp.Version)

	return writeXML(c, http.StatusCreated, xmlCreateTrafficPolicyVersionResponse{
		Xmlns:         route53Namespace,
		TrafficPolicy: toXMLTrafficPolicy(tp),
	})
}

func (h *Handler) createTrafficPolicyInstance(c *echo.Context) error {
	ctx := c.Request().Context()

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlCreateTrafficPolicyInstanceRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	inst, err := h.Backend.CreateTrafficPolicyInstance(
		req.HostedZoneID,
		req.Name,
		req.TrafficPolicyID,
		req.TrafficPolicyVersion,
		req.TTL,
	)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 CreateTrafficPolicyInstance", "id", inst.ID)

	resp := xmlCreateTrafficPolicyInstanceResponse{
		Xmlns:                 route53Namespace,
		TrafficPolicyInstance: toXMLTPInstance(inst),
	}

	c.Response().Header().Set("Location", "/2013-04-01/trafficpolicyinstance/"+inst.ID)

	return writeXML(c, http.StatusCreated, resp)
}

// ---- DNSSEC handlers ----

func (h *Handler) enableHostedZoneDNSSEC(c *echo.Context, zoneID string) error {
	ctx := c.Request().Context()

	if err := h.Backend.EnableHostedZoneDNSSEC(zoneID); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 EnableHostedZoneDNSSEC", "zoneID", zoneID)

	resp := struct {
		XMLName    xml.Name      `xml:"EnableHostedZoneDNSSECResponse"`
		Xmlns      string        `xml:"xmlns,attr"`
		ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
	}{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/enable-dnssec-" + zoneID,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
	}

	return writeXML(c, http.StatusOK, resp)
}

func (h *Handler) disableHostedZoneDNSSEC(c *echo.Context, zoneID string) error {
	ctx := c.Request().Context()

	if err := h.Backend.DisableHostedZoneDNSSEC(zoneID); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 DisableHostedZoneDNSSEC", "zoneID", zoneID)

	resp := struct {
		XMLName    xml.Name      `xml:"DisableHostedZoneDNSSECResponse"`
		Xmlns      string        `xml:"xmlns,attr"`
		ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
	}{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/disable-dnssec-" + zoneID,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
	}

	return writeXML(c, http.StatusOK, resp)
}

func (h *Handler) getHostedZoneDNSSEC(c *echo.Context, zoneID string) error {
	ctx := c.Request().Context()

	enabled, ksks, err := h.Backend.GetDNSSEC(zoneID)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 GetDNSSEC", "zoneID", zoneID, "enabled", enabled)

	serveSignature := "NOT_SIGNING"
	if enabled {
		serveSignature = "SIGNING"
	}

	xmlKSKs := make([]xmlKSK, 0, len(ksks))
	for i := range ksks {
		xmlKSKs = append(xmlKSKs, toXMLKSK(&ksks[i]))
	}

	resp := xmlGetDNSSECResponse{
		Xmlns: route53Namespace,
		Status: xmlDNSSECStatus{
			ServeSignature: serveSignature,
		},
		KeySigningKeys: xmlKSKs,
	}

	return writeXML(c, http.StatusOK, resp)
}

// ---- KSK deactivate/delete handlers ----

func (h *Handler) deactivateKeySigningKey(c *echo.Context, path string) error {
	ctx := c.Request().Context()

	// path: /2013-04-01/keysigningkey/{zoneId}/{name}/deactivate
	rest := strings.TrimPrefix(path, route53KSKPrefix)
	rest = strings.TrimSuffix(rest, route53DeactivateSuffix)
	parts := strings.SplitN(rest, "/", zoneIDAndRest)

	if len(parts) != zoneIDAndRest {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "invalid KSK deactivate path")
	}

	zoneID, name := parts[0], parts[1]

	if _, err := h.Backend.DeactivateKeySigningKey(zoneID, name); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 DeactivateKeySigningKey", "zoneID", zoneID, "name", name)

	resp := struct {
		XMLName    xml.Name      `xml:"DeactivateKeySigningKeyResponse"`
		Xmlns      string        `xml:"xmlns,attr"`
		ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
	}{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/deactivate-ksk-" + zoneID + "-" + name,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
	}

	return writeXML(c, http.StatusOK, resp)
}

func (h *Handler) deleteKeySigningKey(c *echo.Context, path string) error {
	ctx := c.Request().Context()

	// path: /2013-04-01/keysigningkey/{zoneId}/{name}
	rest := strings.TrimPrefix(path, route53KSKPrefix)
	parts := strings.SplitN(rest, "/", zoneIDAndRest)

	if len(parts) != zoneIDAndRest {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "invalid KSK delete path")
	}

	zoneID, name := parts[0], parts[1]

	if err := h.Backend.DeleteKeySigningKey(zoneID, name); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 DeleteKeySigningKey", "zoneID", zoneID, "name", name)

	resp := struct {
		XMLName    xml.Name      `xml:"DeleteKeySigningKeyResponse"`
		Xmlns      string        `xml:"xmlns,attr"`
		ChangeInfo xmlChangeInfo `xml:"ChangeInfo"`
	}{
		Xmlns: route53Namespace,
		ChangeInfo: xmlChangeInfo{
			ID:          "/change/delete-ksk-" + zoneID + "-" + name,
			Status:      statusInsync,
			SubmittedAt: time.Now(),
		},
	}

	return writeXML(c, http.StatusOK, resp)
}

// ---- Traffic Policy get/delete handlers ----

func (h *Handler) getTrafficPolicy(c *echo.Context, id string, version int32) error {
	ctx := c.Request().Context()

	tp, err := h.Backend.GetTrafficPolicy(id, version)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 GetTrafficPolicy", "id", id, "version", version)

	return writeXML(c, http.StatusOK, xmlCreateTrafficPolicyResponse{
		Xmlns:         route53Namespace,
		TrafficPolicy: toXMLTrafficPolicy(tp),
	})
}

func (h *Handler) deleteTrafficPolicy(c *echo.Context, id string, version int32) error {
	ctx := c.Request().Context()

	if err := h.Backend.DeleteTrafficPolicy(id, version); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 DeleteTrafficPolicy", "id", id, "version", version)

	return writeXML(c, http.StatusOK, struct {
		XMLName xml.Name `xml:"DeleteTrafficPolicyResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
	}{Xmlns: route53Namespace})
}

func (h *Handler) listTrafficPolicies(c *echo.Context) error {
	ctx := c.Request().Context()

	policies, err := h.Backend.ListTrafficPolicies()
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 ListTrafficPolicies", "count", len(policies))

	summaries := make([]xmlTrafficPolicySummary, 0, len(policies))
	for _, p := range policies {
		summaries = append(summaries, xmlTrafficPolicySummary{
			ID:                 p.ID,
			Name:               p.Name,
			Type:               p.Type,
			LatestVersion:      p.Version,
			TrafficPolicyCount: 1,
		})
	}

	return writeXML(c, http.StatusOK, xmlListTrafficPoliciesResponse{
		Xmlns:           route53Namespace,
		TrafficPolicies: summaries,
		IsTruncated:     false,
		MaxItems:        "100",
	})
}

func (h *Handler) listTrafficPolicyVersions(c *echo.Context, id string) error {
	ctx := c.Request().Context()

	versions, err := h.Backend.ListTrafficPolicyVersions(id)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 ListTrafficPolicyVersions", "id", id, "count", len(versions))

	xmlPolicies := make([]xmlTrafficPolicy, 0, len(versions))
	for _, v := range versions {
		xmlPolicies = append(xmlPolicies, toXMLTrafficPolicy(v))
	}

	return writeXML(c, http.StatusOK, xmlListTrafficPolicyVersionsResponse{
		Xmlns:           route53Namespace,
		TrafficPolicies: xmlPolicies,
		IsTruncated:     false,
		MaxItems:        "100",
	})
}

// ---- Traffic Policy Instance get/delete/list handlers ----

func (h *Handler) getTrafficPolicyInstance(c *echo.Context, id string) error {
	ctx := c.Request().Context()

	inst, err := h.Backend.GetTrafficPolicyInstance(id)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 GetTrafficPolicyInstance", "id", id)

	return writeXML(c, http.StatusOK, xmlCreateTrafficPolicyInstanceResponse{
		Xmlns:                 route53Namespace,
		TrafficPolicyInstance: toXMLTPInstance(inst),
	})
}

func (h *Handler) deleteTrafficPolicyInstance(c *echo.Context, id string) error {
	ctx := c.Request().Context()

	if err := h.Backend.DeleteTrafficPolicyInstance(id); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 DeleteTrafficPolicyInstance", "id", id)

	return writeXML(c, http.StatusOK, struct {
		XMLName xml.Name `xml:"DeleteTrafficPolicyInstanceResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
	}{Xmlns: route53Namespace})
}

func (h *Handler) listTrafficPolicyInstances(c *echo.Context) error {
	ctx := c.Request().Context()

	instances, err := h.Backend.ListTrafficPolicyInstances()
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 ListTrafficPolicyInstances", "count", len(instances))

	xmlInstances := make([]xmlTrafficPolicyInstance, 0, len(instances))
	for _, inst := range instances {
		xmlInstances = append(xmlInstances, toXMLTPInstance(inst))
	}

	return writeXML(c, http.StatusOK, xmlListTrafficPolicyInstancesResponse{
		Xmlns:                  route53Namespace,
		TrafficPolicyInstances: xmlInstances,
		IsTruncated:            false,
		MaxItems:               "100",
	})
}

func (h *Handler) getTrafficPolicyInstanceCount(c *echo.Context) error {
	ctx := c.Request().Context()

	instances, err := h.Backend.ListTrafficPolicyInstances()
	if err != nil {
		return handleBackendError(c, err)
	}

	count := int32(len(instances)) //nolint:gosec // instance count fits in int32

	logger.Load(ctx).DebugContext(ctx, "Route53 GetTrafficPolicyInstanceCount", "count", count)

	return writeXML(c, http.StatusOK, xmlGetTPInstanceCountResponse{
		Xmlns:                      route53Namespace,
		TrafficPolicyInstanceCount: count,
	})
}

// ---- CIDR Collection list/delete handlers ----

func (h *Handler) listCidrCollections(c *echo.Context) error {
	ctx := c.Request().Context()

	collections, err := h.Backend.ListCidrCollections()
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 ListCidrCollections", "count", len(collections))

	summaries := make([]xmlCidrCollectionSummary, 0, len(collections))
	for _, col := range collections {
		summaries = append(summaries, xmlCidrCollectionSummary{
			ARN:     col.ARN,
			ID:      col.ID,
			Name:    col.Name,
			Version: col.Version,
		})
	}

	return writeXML(c, http.StatusOK, xmlListCidrCollectionsResponse{
		Xmlns:           route53Namespace,
		CidrCollections: summaries,
		IsTruncated:     false,
	})
}

func (h *Handler) deleteCidrCollection(c *echo.Context, path string) error {
	ctx := c.Request().Context()

	id := strings.TrimPrefix(path, route53CidrCollectionPrefix)

	if err := h.Backend.DeleteCidrCollection(id); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 DeleteCidrCollection", "id", id)

	return writeXML(c, http.StatusOK, struct {
		XMLName xml.Name `xml:"DeleteCidrCollectionResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
	}{Xmlns: route53Namespace})
}

