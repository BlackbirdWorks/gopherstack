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
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	statusInsync = "INSYNC"
)

const (
	opDeactivateKeySigningKey     = "DeactivateKeySigningKey"
	opAssociateVPCWithHostedZone  = "AssociateVPCWithHostedZone"
	opCreateQueryLoggingConfig    = "CreateQueryLoggingConfig"
	opCreateReusableDelegationSet = "CreateReusableDelegationSet"
	opDisableHostedZoneDNSSEC     = "DisableHostedZoneDNSSEC"
	opEnableHostedZoneDNSSEC      = "EnableHostedZoneDNSSEC"
	opGetDNSSEC                   = "GetDNSSEC"
	opUnknown                     = "Unknown"
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
}

// NewHandler creates a new Route 53 Handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend: backend,
	}
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
		opAssociateVPCWithHostedZone,
		"ChangeCidrCollection",
		"ChangeResourceRecordSets",
		"ChangeTagsForResource",
		"CreateCidrCollection",
		"CreateHealthCheck",
		"CreateHostedZone",
		"CreateKeySigningKey",
		opCreateQueryLoggingConfig,
		opCreateReusableDelegationSet,
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
		opDisableHostedZoneDNSSEC,
		opEnableHostedZoneDNSSEC,
		opGetDNSSEC,
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

// ExtractOperation extracts a human-readable operation name from the request.
// It mirrors routeRequest's real dispatch tree op-for-op (gopherstack-l5ir) so
// TestExtractOperation_SDKRouteTable in handler_paths_sdk_diff_test.go can
// exercise it directly against every real op's authoritative method+path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

	if path == route53HostedZone {
		return extractHostedZoneRootOp(method)
	}

	if strings.HasPrefix(path, route53HZPrefix) {
		return extractHostedZoneOp(path, method)
	}

	if strings.HasPrefix(path, route53TagsPrefix) {
		return extractTagsOperation(path, method)
	}

	if strings.HasPrefix(path, route53ChangePrefix) {
		if method == http.MethodGet {
			return "GetChange"
		}

		return opUnknown
	}

	if op := extractHealthCheckOperation(path, method); op != "" {
		return op
	}

	if op := extractNewOpsOperation(path, method); op != "" {
		return op
	}

	if op := extractCompletenessOperation(c, path, method); op != "" {
		return op
	}

	switch path {
	case route53QueryLoggingRoot:
		return extractQueryLoggingRootOp(method)
	case route53DelegationSetRoot:
		return extractDelegationSetRootOp(method)
	}

	return opUnknown
}

// extractHostedZoneRootOp handles POST/GET on the bare /hostedzone root.
func extractHostedZoneRootOp(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateHostedZone"
	case http.MethodGet:
		return "ListHostedZones"
	}

	return opUnknown
}

// extractHostedZoneOp mirrors routeHostedZone: suffix-based sub-routes first
// (rrset, VPC association, DNSSEC), then the generic Delete/Get/UpdateComment
// fallback on the bare /hostedzone/{Id} path.
func extractHostedZoneOp(path, method string) string {
	if op := extractHostedZoneSuffixOp(path, method); op != "" {
		return op
	}

	if op := extractHostedZoneDNSSECOp(path, method); op != "" {
		return op
	}

	switch method {
	case http.MethodDelete:
		return "DeleteHostedZone"
	case http.MethodGet:
		return "GetHostedZone"
	case http.MethodPost:
		return "UpdateHostedZoneComment"
	}

	return opUnknown
}

// extractHostedZoneSuffixOp mirrors routeHostedZoneSuffix.
func extractHostedZoneSuffixOp(path, method string) string {
	switch {
	case strings.HasSuffix(path, route53RRSetSuffix):
		if method == http.MethodPost {
			return "ChangeResourceRecordSets"
		}

		return "ListResourceRecordSets"
	case strings.HasSuffix(path, route53AssociateVPCSuffix):
		if method == http.MethodPost {
			return opAssociateVPCWithHostedZone
		}
	case strings.HasSuffix(path, route53AuthorizeVPCSuffix):
		if method == http.MethodGet {
			return "ListVPCAssociationAuthorizations"
		}

		if method == http.MethodPost {
			return "CreateVPCAssociationAuthorization"
		}
	case strings.HasSuffix(path, route53DeauthorizeVPCSuffix) && method == http.MethodPost:
		return "DeleteVPCAssociationAuthorization"
	case strings.HasSuffix(path, route53DisassociateVPCSuffix) && method == http.MethodPost:
		return "DisassociateVPCFromHostedZone"
	case strings.HasSuffix(path, route53FeaturesSuffix) && method == http.MethodPost:
		return "UpdateHostedZoneFeatures"
	}

	return ""
}

// extractHostedZoneDNSSECOp mirrors routeHostedZoneDNSSEC.
func extractHostedZoneDNSSECOp(path, method string) string {
	switch {
	case strings.HasSuffix(path, route53EnableDNSSECSuffix) && method == http.MethodPost:
		return opEnableHostedZoneDNSSEC
	case strings.HasSuffix(path, route53DisableDNSSECSuffix) && method == http.MethodPost:
		return opDisableHostedZoneDNSSEC
	case strings.HasSuffix(path, route53DNSSECSuffix) && method == http.MethodGet:
		return opGetDNSSEC
	}

	return ""
}

// extractTagsOperation mirrors routeTags.
func extractTagsOperation(path, method string) string {
	rest := strings.TrimPrefix(path, route53TagsPrefix)
	if method == http.MethodPost && !strings.Contains(rest, "/") {
		return "ListTagsForResources"
	}

	switch method {
	case http.MethodGet:
		return "ListTagsForResource"
	case http.MethodPost:
		return "ChangeTagsForResource"
	}

	return opUnknown
}

// extractQueryLoggingRootOp handles POST/GET on the bare /queryloggingconfig root.
func extractQueryLoggingRootOp(method string) string {
	switch method {
	case http.MethodPost:
		return opCreateQueryLoggingConfig
	case http.MethodGet:
		return "ListQueryLoggingConfigs"
	}

	return opUnknown
}

// extractDelegationSetRootOp handles POST/GET on the bare /delegationset root.
func extractDelegationSetRootOp(method string) string {
	switch method {
	case http.MethodPost:
		return opCreateReusableDelegationSet
	case http.MethodGet:
		return "ListReusableDelegationSets"
	}

	return opUnknown
}

// extractCompletenessOperation covers the GET-only info/limit endpoints and
// the DelegationSet/QueryLoggingConfig by-ID routes -- the only branches of
// routeCompleteness that are actually reachable (its VPC-association and
// traffic-policy-comment cases are shadowed by routeHostedZone/routeNewOpsTP,
// which always intercept those paths first in routeRequest's top-level
// switch -- see routeCompletenessLimits' doc comment for the same trap on
// GetHealthCheckLastFailureReason).
func extractCompletenessOperation(c *echo.Context, path, method string) string {
	if op := extractCompletenessInfoOp(c, path, method); op != "" {
		return op
	}

	if op := extractCompletenessLimitsOp(path, method); op != "" {
		return op
	}

	if op := extractCompletenessDelegationSetOp(path, method); op != "" {
		return op
	}

	return extractCompletenessQueryLoggingOp(path, method)
}

// geoLocationQueryParamsSet reports whether any of GetGeoLocation's filter
// query params are present -- the same signal routeCompletenessInfo uses to
// disambiguate GetGeoLocation from ListGeoLocations when both share this
// switch case (they resolve to different real paths, /geolocation vs
// /geolocations, but the handler combines them into one case for brevity).
func geoLocationQueryParamsSet(c *echo.Context) bool {
	q := c.Request().URL.Query()

	return q.Get("continentcode") != "" || q.Get("countrycode") != "" || q.Get("subdivisioncode") != ""
}

func extractCompletenessInfoOp(c *echo.Context, path, method string) string {
	if method != http.MethodGet {
		return ""
	}

	switch path {
	case route53TestDNSAnswerPath:
		return "TestDNSAnswer"
	case route53CheckerIPRangesPath:
		return "GetCheckerIpRanges"
	case route53GeoLocationPath, route53GeoLocationsPath:
		if geoLocationQueryParamsSet(c) {
			return "GetGeoLocation"
		}

		return "ListGeoLocations"
	case route53HealthCheckCountPath:
		return "GetHealthCheckCount"
	case route53HostedZoneCountPath:
		return "GetHostedZoneCount"
	case route53HostedZonesByNamePath:
		return "ListHostedZonesByName"
	case route53HostedZonesByVPCPath:
		return "ListHostedZonesByVPC"
	case route53TPInstancesByHZPath:
		return "ListTrafficPolicyInstancesByHostedZone"
	case route53TPInstancesByPolicyPath:
		return "ListTrafficPolicyInstancesByPolicy"
	}

	return ""
}

func extractCompletenessLimitsOp(path, method string) string {
	if method != http.MethodGet {
		return ""
	}

	switch {
	case strings.HasPrefix(path, route53AccountLimitPrefix):
		return "GetAccountLimit"
	case strings.HasPrefix(path, route53HostedZoneLimitPrefix):
		return "GetHostedZoneLimit"
	case strings.HasPrefix(path, route53ReusableDSLimitPrefix):
		return "GetReusableDelegationSetLimit"
	}

	return ""
}

func extractCompletenessDelegationSetOp(path, method string) string {
	if !strings.HasPrefix(path, route53DelegationSetRoot+"/") {
		return ""
	}

	switch method {
	case http.MethodGet:
		return "GetReusableDelegationSet"
	case http.MethodDelete:
		return "DeleteReusableDelegationSet"
	}

	return ""
}

func extractCompletenessQueryLoggingOp(path, method string) string {
	if !strings.HasPrefix(path, route53QueryLoggingRoot+"/") {
		return ""
	}

	switch method {
	case http.MethodGet:
		return "GetQueryLoggingConfig"
	case http.MethodDelete:
		return "DeleteQueryLoggingConfig"
	}

	return ""
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
	if op := extractNewOpsPathKSKDNSSECVPC(path); op != "" {
		return op
	}

	if op := extractNewOpsPathCidrQueryLoggingDelegationSet(path); op != "" {
		return op
	}

	return extractNewOpsPathTrafficPolicy(path)
}

func extractNewOpsPathKSKDNSSECVPC(path string) string {
	switch {
	case path == route53KSKRoot:
		return "CreateKeySigningKey"
	case strings.HasSuffix(path, route53ActivateSuffix):
		return "ActivateKeySigningKey"
	case strings.HasSuffix(path, route53DeactivateSuffix):
		return opDeactivateKeySigningKey
	case strings.HasSuffix(path, route53EnableDNSSECSuffix):
		return opEnableHostedZoneDNSSEC
	case strings.HasSuffix(path, route53DisableDNSSECSuffix):
		return opDisableHostedZoneDNSSEC
	case strings.HasSuffix(path, route53AssociateVPCSuffix):
		return opAssociateVPCWithHostedZone
	}

	return ""
}

func extractNewOpsPathCidrQueryLoggingDelegationSet(path string) string {
	switch {
	case path == route53CidrCollectionRoot:
		return "CreateCidrCollection"
	case strings.HasPrefix(path, route53CidrCollectionPrefix):
		return "ChangeCidrCollection"
	case path == route53QueryLoggingRoot:
		return opCreateQueryLoggingConfig
	case path == route53DelegationSetRoot:
		return opCreateReusableDelegationSet
	}

	return ""
}

func extractNewOpsPathTrafficPolicy(path string) string {
	switch {
	case path == route53TrafficPolicyRoot:
		return "CreateTrafficPolicy"
	case strings.HasPrefix(path, route53TrafficPolicyPrefix):
		// CreateTrafficPolicyVersion is POST /{Id} (no version segment);
		// UpdateTrafficPolicyComment is POST /{Id}/{Version} -- mirrors
		// routeTrafficPolicyVersion's strings.Contains(rest, "/") check.
		if strings.Contains(strings.TrimPrefix(path, route53TrafficPolicyPrefix), "/") {
			return "UpdateTrafficPolicyComment"
		}

		return "CreateTrafficPolicyVersion"
	case path == route53TPInstanceRoot:
		return "CreateTrafficPolicyInstance"
	case strings.HasPrefix(path, route53TPInstancePrefix):
		return "UpdateTrafficPolicyInstance"
	}

	return ""
}

func extractGetOpsPath(path string) string {
	switch {
	case strings.HasSuffix(path, route53DNSSECSuffix):
		return opGetDNSSEC
	case path == route53TrafficPoliciesRoot:
		return "ListTrafficPolicies"
	case strings.HasPrefix(path, route53TrafficPoliciesPrefix):
		return "ListTrafficPolicyVersions"
	case strings.HasPrefix(path, route53TrafficPolicyPrefix):
		// GetTrafficPolicy is GET /{Id}/{Version} -- only reachable with a
		// version segment (a bare /{Id} GET has no real op and 404s).
		if strings.Contains(strings.TrimPrefix(path, route53TrafficPolicyPrefix), "/") {
			return "GetTrafficPolicy"
		}
	case path == route53TPInstancesRoot:
		return "ListTrafficPolicyInstances"
	case path == route53TPInstanceCount:
		return "GetTrafficPolicyInstanceCount"
	case strings.HasPrefix(path, route53TPInstancePrefix):
		return "GetTrafficPolicyInstance"
	case path == route53CidrCollectionRoot:
		return "ListCidrCollections"
	case strings.HasSuffix(path, "/cidrblocks") && strings.HasPrefix(path, route53CidrCollectionPrefix):
		return "ListCidrBlocks"
	case strings.HasPrefix(path, route53CidrCollectionPrefix):
		return "ListCidrLocations"
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

// normaliseDelegationSetID adds the "/delegationset/" prefix used as the
// store key if the caller supplied a bare ID (e.g. "N1PA6795SAMPLE"). Real
// AWS clients (and this codebase's own delegation-set routes, see
// handler_reusable_delegation_sets.go) commonly pass the bare form even though the Id
// field on the wire is fully qualified, so both are accepted. Empty input
// is passed through unchanged (means "no reusable delegation set").
func normaliseDelegationSetID(id string) string {
	if id == "" || strings.HasPrefix(id, "/delegationset/") {
		return id
	}

	return "/delegationset/" + id
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

type xmlChangeInfo struct {
	XMLName     xml.Name  `xml:"ChangeInfo"`
	SubmittedAt time.Time `xml:"SubmittedAt"`
	ID          string    `xml:"Id"`
	Status      string    `xml:"Status"`
}

// parseMaxItemsResult holds the parsed and effective maxItems values.
type parseMaxItemsResult struct {
	requested int
	effective int
}

// parseMaxItems parses the "maxitems" query parameter.
// Returns the requested value (0 if absent) and the effective value after applying the backend
// default/clamp. Returns an error response if the parameter is present but invalid.
func parseMaxItems(c *echo.Context, paramVal string) (parseMaxItemsResult, error) {
	if paramVal == "" {
		return parseMaxItemsResult{0, route53DefaultMaxItems}, nil
	}

	n, parseErr := strconv.Atoi(paramVal)
	if parseErr != nil || n < 0 {
		return parseMaxItemsResult{}, xmlError(c, http.StatusBadRequest, "InvalidInput", "invalid maxitems parameter")
	}

	if n == 0 || n > route53DefaultMaxItems {
		return parseMaxItemsResult{n, route53DefaultMaxItems}, nil
	}

	return parseMaxItemsResult{n, n}, nil
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

// backendErrorMapping is one entry in backendErrorTable: a sentinel error
// mapped to the AWS wire error code and HTTP status handleBackendError
// should emit for it.
type backendErrorMapping struct {
	sentinel error
	code     string
	status   int
}

// backendErrorTable is the sentinel-error -> (wire code, HTTP status) lookup
// handleBackendError walks. Using a table instead of a long switch keeps
// handleBackendError's cyclomatic complexity flat as new backend errors are
// added — every entry is an independent, order-irrelevant mapping (the
// errors are pairwise disjoint, so match order doesn't matter). Comments
// call out entries whose HTTP status is a real AWS quirk rather than the
// "obvious" choice.
//
//nolint:gochecknoglobals // read-only dispatch table built once at package init
var backendErrorTable = []backendErrorMapping{
	{ErrHostedZoneNotFound, "NoSuchHostedZone", http.StatusNotFound},
	{ErrHealthCheckNotFound, "NoSuchHealthCheck", http.StatusNotFound},
	{ErrKeySigningKeyNotFound, "NoSuchKeySigningKey", http.StatusNotFound},
	{ErrCidrCollectionNotFound, "NoSuchCidrCollectionException", http.StatusNotFound},
	{ErrQueryLoggingConfigNotFound, "NoSuchQueryLoggingConfig", http.StatusNotFound},
	// AWS: NoSuchDelegationSet has httpStatusCode 400, unlike the other
	// NoSuch* Route53 errors which are 404.
	{ErrDelegationSetNotFound, "NoSuchDelegationSet", http.StatusBadRequest},
	{ErrTrafficPolicyNotFound, "NoSuchTrafficPolicy", http.StatusNotFound},
	{ErrTrafficPolicyInstNotFound, "NoSuchTrafficPolicyInstance", http.StatusNotFound},
	{ErrInvalidInput, "InvalidInput", http.StatusBadRequest},
	{ErrInvalidAction, "InvalidChangeBatch", http.StatusBadRequest},
	{ErrChangeNotFound, "NoSuchChange", http.StatusNotFound},
	{ErrNoSuchGeoLocation, "NoSuchGeoLocation", http.StatusNotFound},
	// AWS: QueryLoggingConfigAlreadyExists has httpStatusCode 409.
	{ErrQueryLoggingConfigAlreadyExists, "QueryLoggingConfigAlreadyExists", http.StatusConflict},
	{ErrPublicZoneVPCAssociation, "PublicZoneVPCAssociation", http.StatusBadRequest},
	{ErrVPCAssociationAuthorizationNF, "VPCAssociationAuthorizationNotFound", http.StatusNotFound},
	{ErrVPCAssociationNotFound, "VPCAssociationNotFound", http.StatusNotFound},
	{ErrKeySigningKeyWithActiveStatusNF, "KeySigningKeyWithActiveStatusNotFound", http.StatusBadRequest},
	{ErrTrafficPolicyInUse, "TrafficPolicyInUse", http.StatusBadRequest},
	{ErrInvalidKeySigningKeyStatus, "InvalidKeySigningKeyStatus", http.StatusBadRequest},
	// AWS: TrafficPolicyAlreadyExists has httpStatusCode 409.
	{ErrTrafficPolicyAlreadyExists, "TrafficPolicyAlreadyExists", http.StatusConflict},
	{ErrHostedZoneNotEmpty, "HostedZoneNotEmpty", http.StatusBadRequest},
	{ErrLastVPCAssociation, "LastVPCAssociation", http.StatusBadRequest},
	{ErrHostedZoneAlreadyExists, "HostedZoneAlreadyExists", http.StatusConflict},
	{ErrHealthCheckAlreadyExists, "HealthCheckAlreadyExists", http.StatusConflict},
	{ErrKeySigningKeyAlreadyExists, "KeySigningKeyAlreadyExists", http.StatusConflict},
	{ErrTrafficPolicyInstanceAlreadyExists, "TrafficPolicyInstanceAlreadyExists", http.StatusConflict},
	// AWS: CidrCollectionAlreadyExistsException has httpStatusCode 400
	// (unlike most other *AlreadyExists Route53 errors, which are 409).
	{ErrCidrCollectionAlreadyExists, "CidrCollectionAlreadyExistsException", http.StatusBadRequest},
	// AWS: HealthCheckVersionMismatch has httpStatusCode 409.
	{ErrHealthCheckVersionMismatch, "HealthCheckVersionMismatch", http.StatusConflict},
	// AWS: CidrCollectionVersionMismatchException has httpStatusCode 409.
	{ErrCidrCollectionVersionMismatch, "CidrCollectionVersionMismatchException", http.StatusConflict},
	// AWS: CidrCollectionInUseException has httpStatusCode 400.
	{ErrCidrCollectionInUse, "CidrCollectionInUseException", http.StatusBadRequest},
	// AWS: DelegationSetInUse has httpStatusCode 400.
	{ErrDelegationSetInUse, "DelegationSetInUse", http.StatusBadRequest},
	// AWS: HostedZoneNotFound (CreateReusableDelegationSet's HostedZoneId
	// validation) has httpStatusCode 400 — distinct from NoSuchHostedZone,
	// which every other hosted-zone-lookup op returns.
	{ErrHostedZoneNotFoundForDelegationSet, "HostedZoneNotFound", http.StatusBadRequest},
	{ErrDelegationSetAlreadyReusable, "DelegationSetAlreadyReusable", http.StatusBadRequest},
	{ErrDelegationSetAlreadyCreated, "DelegationSetAlreadyCreated", http.StatusBadRequest},
	{ErrInvalidKMSArn, "InvalidKMSArn", http.StatusBadRequest},
}

// handleBackendError maps a backend sentinel error to its AWS wire error
// code and HTTP status via backendErrorTable, falling back to a generic
// InternalError (500) for anything unrecognized.
func handleBackendError(c *echo.Context, err error) error {
	for _, m := range backendErrorTable {
		if errors.Is(err, m.sentinel) {
			return xmlError(c, m.status, m.code, err.Error())
		}
	}

	return xmlError(c, http.StatusInternalServerError, "InternalError", err.Error())
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Route53 limit/count constants for completeness operations.
const (
	maxHealthChecks    = 1000
	maxHostedZoneCount = 10000
	maxHZByName        = 300
	defaultLimitValue  = 500
	defaultDSLimit     = 100
)

// Route53 path constants for completeness operations.
const (
	route53TestDNSAnswerPath       = "/2013-04-01/testdnsanswer"
	route53CheckerIPRangesPath     = "/2013-04-01/checkeripranges"
	route53GeoLocationPath         = "/2013-04-01/geolocation"
	route53GeoLocationsPath        = "/2013-04-01/geolocations"
	route53HealthCheckCountPath    = "/2013-04-01/healthcheckcount"
	route53HostedZoneCountPath     = "/2013-04-01/hostedzonecount"
	route53HostedZonesByNamePath   = "/2013-04-01/hostedzonesbyname"
	route53HostedZonesByVPCPath    = "/2013-04-01/hostedzonesbyvpc"
	route53AccountLimitPrefix      = "/2013-04-01/accountlimit/"
	route53HostedZoneLimitPrefix   = "/2013-04-01/hostedzonelimit/"
	route53ReusableDSLimitPrefix   = "/2013-04-01/reusabledelegationsetlimit/"
	route53AuthorizeVPCSuffix      = "/authorizevpcassociation"
	route53DeauthorizeVPCSuffix    = "/deauthorizevpcassociation"
	route53DisassociateVPCSuffix   = "/disassociatevpc"
	route53FeaturesSuffix          = "/features"
	route53TPInstancesByHZPath     = "/2013-04-01/trafficpolicyinstances/hostedzone"
	route53TPInstancesByPolicyPath = "/2013-04-01/trafficpolicyinstances/trafficpolicy"
	route53LastFailureReasonSuffix = "/lastfailurereason"
)

// Route53 limit type identifiers (AWS LimitName values).
const (
	route53LimitMaxHostedZonesByOwner            = "MAX_HOSTED_ZONES_BY_OWNER"
	route53LimitMaxHealthChecksByOwner           = "MAX_HEALTH_CHECKS_BY_OWNER"
	route53LimitMaxReusableDelegationSetsByOwner = "MAX_REUSABLE_DELEGATION_SETS_BY_OWNER"
	route53LimitMaxTrafficPoliciesByOwner        = "MAX_TRAFFIC_POLICIES_BY_OWNER"
	route53LimitMaxTrafficPolicyInstancesByOwner = "MAX_TRAFFIC_POLICY_INSTANCES_BY_OWNER"
	route53LimitMaxVPCsAssociatedByZone          = "MAX_VPCS_ASSOCIATED_BY_ZONE"
	route53LimitMaxZonesByReusableDelegationSet  = "MAX_ZONES_BY_REUSABLE_DELEGATION_SET"
)

// routeCompleteness handles previously-notImplemented Route53 paths.
// Returns (true, err) if the path was handled, (false, nil) if not.
func (h *Handler) routeCompleteness(c *echo.Context, path, method string) (bool, error) {
	if ok, err := h.routeCompletenessCore(c, path, method); ok {
		return true, err
	}

	if ok, err := h.routeCompletenessVPC(c, path, method); ok {
		return true, err
	}

	if ok, err := h.routeCompletenessTP(c, path, method); ok {
		return true, err
	}

	if ok, err := h.routeCompletenessDelegationSet(c, path, method); ok {
		return true, err
	}

	if ok, err := h.routeCompletenessQueryLogging(c, path, method); ok {
		return true, err
	}

	return false, nil
}

// routeCompletenessCore handles basic info and limit endpoints.
func (h *Handler) routeCompletenessCore(c *echo.Context, path, method string) (bool, error) {
	if ok, err := h.routeCompletenessInfo(c, path, method); ok {
		return true, err
	}

	return h.routeCompletenessLimits(c, path, method)
}

// routeCompletenessInfo handles DNS answer, geo, health-check, and hosted-zone info endpoints.
func (h *Handler) routeCompletenessInfo(c *echo.Context, path, method string) (bool, error) {
	if method != http.MethodGet {
		return false, nil
	}

	switch path {
	case route53TestDNSAnswerPath:
		return true, h.testDNSAnswer(c)
	case route53CheckerIPRangesPath:
		return true, h.getCheckerIPRanges(c)
	case route53GeoLocationPath, route53GeoLocationsPath:
		q := c.Request().URL.Query()
		if q.Get("continentcode") != "" || q.Get("countrycode") != "" || q.Get("subdivisioncode") != "" {
			return true, h.getGeoLocation(c)
		}

		return true, h.listGeoLocations(c)
	case route53HealthCheckCountPath:
		return true, h.getHealthCheckCount(c)
	case route53HostedZoneCountPath:
		return true, h.getHostedZoneCount(c)
	case route53HostedZonesByNamePath:
		return true, h.listHostedZonesByName(c)
	case route53HostedZonesByVPCPath:
		return true, h.listHostedZonesByVPC(c)
	case route53TPInstancesByHZPath:
		return true, h.listTrafficPolicyInstancesByHostedZone(c)
	case route53TPInstancesByPolicyPath:
		return true, h.listTrafficPolicyInstancesByPolicy(c)
	}

	return false, nil
}

// routeCompletenessLimits handles limit endpoints. GetHealthCheckLastFailureReason
// is NOT here despite matching route53LastFailureReasonSuffix: its real path
// starts with route53HealthCheckPrefix, so routeRequest's top-level switch
// always routes it to routeHealthCheck before this function is ever reached
// -- see routeHealthCheck for the real dispatch (gopherstack-l5ir).
func (h *Handler) routeCompletenessLimits(c *echo.Context, path, method string) (bool, error) {
	switch {
	case strings.HasPrefix(path, route53AccountLimitPrefix) && method == http.MethodGet:
		return true, h.getAccountLimit(c, path)
	case strings.HasPrefix(path, route53HostedZoneLimitPrefix) && method == http.MethodGet:
		return true, h.getHostedZoneLimit(c, path)
	case strings.HasPrefix(path, route53ReusableDSLimitPrefix) && method == http.MethodGet:
		return true, h.getReusableDelegationSetLimit(c, path)
	}

	return false, nil
}

// routeCompletenessVPC handles VPC association and disassociation endpoints.
func (h *Handler) routeCompletenessVPC(c *echo.Context, path, method string) (bool, error) {
	switch {
	case strings.HasSuffix(path, route53AuthorizeVPCSuffix) && method == http.MethodGet:
		return true, h.listVPCAssociationAuthorizations(c, path)
	case strings.HasSuffix(path, route53AuthorizeVPCSuffix) && method == http.MethodPost:
		return true, h.createVPCAssociationAuthorization(c, path)
	case strings.HasSuffix(path, route53DeauthorizeVPCSuffix) && method == http.MethodPost:
		return true, h.deleteVPCAssociationAuthorization(c, path)
	case strings.HasSuffix(path, route53DisassociateVPCSuffix) && method == http.MethodPost:
		return true, h.disassociateVPCFromHostedZone(c, path)
	case strings.HasSuffix(path, route53FeaturesSuffix) && method == http.MethodPost:
		return true, h.updateHostedZoneFeatures(c, path)
	}

	return false, nil
}

// routeCompletenessTP handles traffic policy completeness routes.
func (h *Handler) routeCompletenessTP(c *echo.Context, path, method string) (bool, error) {
	switch {
	case strings.HasPrefix(path, route53TrafficPolicyPrefix) && method == http.MethodPost:
		// UpdateTrafficPolicyComment — POST /2013-04-01/trafficpolicy/{Id}/{Version}
		return true, h.updateTrafficPolicyComment(c, path)
	case strings.HasPrefix(path, route53TPInstancePrefix) && method == http.MethodPost:
		// UpdateTrafficPolicyInstance — POST /2013-04-01/trafficpolicyinstance/{Id}
		return true, h.updateTrafficPolicyInstance(c, path)
	}

	// UpdateHostedZoneComment — POST /2013-04-01/hostedzone/{Id} (no suffix)
	if method == http.MethodPost && strings.HasPrefix(path, route53HZPrefix) {
		tail := strings.TrimPrefix(path, route53HZPrefix)
		if !strings.Contains(tail, "/") {
			return true, h.updateHostedZoneComment(c, path)
		}
	}

	return false, nil
}

func (h *Handler) routeCompletenessDelegationSet(c *echo.Context, path, method string) (bool, error) {
	if !strings.HasPrefix(path, "/2013-04-01/delegationset/") {
		return false, nil
	}

	switch method {
	case http.MethodGet:
		return true, h.getReusableDelegationSet(c, path)
	case http.MethodDelete:
		return true, h.deleteReusableDelegationSet(c, path)
	}

	return false, nil
}

func (h *Handler) routeCompletenessQueryLogging(c *echo.Context, path, method string) (bool, error) {
	if !strings.HasPrefix(path, route53QueryLoggingRoot+"/") {
		return false, nil
	}

	switch method {
	case http.MethodGet:
		return true, h.getQueryLoggingConfig(c, path)
	case http.MethodDelete:
		return true, h.deleteQueryLoggingConfig(c, path)
	}

	return false, nil
}

type checkerIPRangesResponse struct {
	XMLName    xml.Name `xml:"GetCheckerIpRangesResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	CheckerIPs []string `xml:"CheckerIpRanges>member"`
}

func (h *Handler) getCheckerIPRanges(c *echo.Context) error {
	return writeXML(c, http.StatusOK, checkerIPRangesResponse{
		Xmlns:      route53Namespace,
		CheckerIPs: []string{"15.177.0.0/18"},
	})
}

type accountLimitResponse struct {
	XMLName xml.Name `xml:"GetAccountLimitResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Limit   xmlLimit `xml:"Limit"`
	Count   int      `xml:"Count"`
}

type xmlLimit struct {
	Type  string `xml:"Type"`
	Value int    `xml:"Value"`
}

func (h *Handler) getAccountLimit(c *echo.Context, path string) error {
	limitType := strings.TrimPrefix(path, route53AccountLimitPrefix)

	count := 0

	switch limitType {
	case route53LimitMaxHostedZonesByOwner:
		count = h.Backend.GetHostedZoneCount()
	case route53LimitMaxHealthChecksByOwner:
		count = h.Backend.GetHealthCheckCount()
	case route53LimitMaxReusableDelegationSetsByOwner:
		if sets, err := h.Backend.ListReusableDelegationSets(); err == nil {
			count = len(sets)
		}
	case route53LimitMaxTrafficPoliciesByOwner:
		if policies, err := h.Backend.ListTrafficPolicies(); err == nil {
			count = len(policies)
		}
	case route53LimitMaxTrafficPolicyInstancesByOwner:
		if instances, err := h.Backend.ListTrafficPolicyInstances(); err == nil {
			count = len(instances)
		}
	}

	return writeXML(c, http.StatusOK, accountLimitResponse{
		Xmlns: route53Namespace,
		Limit: xmlLimit{Type: limitType, Value: defaultLimitValue},
		Count: count,
	})
}
