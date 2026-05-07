package cloudfront

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	opTestFunction                = "TestFunction"
	opUntagResource               = "UntagResource"
	opUpdateCachePolicy           = "UpdateCachePolicy"
	opUpdateDistribution          = "UpdateDistribution"
	opUpdateFunction              = "UpdateFunction"
	opUpdateOriginAccessControl   = "UpdateOriginAccessControl"
	opUpdateOriginRequestPolicy   = "UpdateOriginRequestPolicy"
	opUpdateResponseHeadersPolicy = "UpdateResponseHeadersPolicy"
)

// Stub operation constants for not-yet-implemented CloudFront operations.
const (
	opCreateDistributionTenant              = "CreateDistributionTenant"
	opCreateDistributionWithTags            = "CreateDistributionWithTags"
	opCreateFieldLevelEncryptionConfig      = "CreateFieldLevelEncryptionConfig"
	opCreateFieldLevelEncryptionProfile     = "CreateFieldLevelEncryptionProfile"
	opCreateInvalidationForDistTenant       = "CreateInvalidationForDistributionTenant"
	opCreateKeyGroup                        = "CreateKeyGroup"
	opCreateKeyValueStore                   = "CreateKeyValueStore"
	opCreateMonitoringSubscription          = "CreateMonitoringSubscription"
	opCreatePublicKey                       = "CreatePublicKey"
	opCreateRealtimeLogConfig               = "CreateRealtimeLogConfig"
	opCreateStreamingDistribution           = "CreateStreamingDistribution"
	opCreateStreamingDistributionWithTags   = "CreateStreamingDistributionWithTags"
	opCreateTrustStore                      = "CreateTrustStore"
	opCreateVpcOrigin                       = "CreateVpcOrigin"
	opDeleteAnycastIPList                   = "DeleteAnycastIpList"
	opDeleteConnectionFunction              = "DeleteConnectionFunction"
	opDeleteConnectionGroup                 = "DeleteConnectionGroup"
	opDeleteContinuousDeploymentPolicy      = "DeleteContinuousDeploymentPolicy"
	opDeleteDistributionTenant              = "DeleteDistributionTenant"
	opDeleteFieldLevelEncryptionConfig      = "DeleteFieldLevelEncryptionConfig"
	opDeleteFieldLevelEncryptionProfile     = "DeleteFieldLevelEncryptionProfile"
	opDeleteKeyGroup                        = "DeleteKeyGroup"
	opDeleteKeyValueStore                   = "DeleteKeyValueStore"
	opDeleteMonitoringSubscription          = "DeleteMonitoringSubscription"
	opDeletePublicKey                       = "DeletePublicKey"
	opDeleteRealtimeLogConfig               = "DeleteRealtimeLogConfig"
	opDeleteResourcePolicy                  = "DeleteResourcePolicy"
	opDeleteStreamingDistribution           = "DeleteStreamingDistribution"
	opDeleteTrustStore                      = "DeleteTrustStore"
	opDeleteVpcOrigin                       = "DeleteVpcOrigin"
	opDescribeConnectionFunction            = "DescribeConnectionFunction"
	opDescribeKeyValueStore                 = "DescribeKeyValueStore"
	opDisassociateDistributionTenantWebACL  = "DisassociateDistributionTenantWebACL"
	opDisassociateDistributionWebACL        = "DisassociateDistributionWebACL"
	opGetAnycastIPList                      = "GetAnycastIpList"
	opGetConnectionFunction                 = "GetConnectionFunction"
	opGetConnectionGroup                    = "GetConnectionGroup"
	opGetConnectionGroupByRoutingEndpoint   = "GetConnectionGroupByRoutingEndpoint"
	opGetContinuousDeploymentPolicy         = "GetContinuousDeploymentPolicy"
	opGetContinuousDeploymentPolicyConfig   = "GetContinuousDeploymentPolicyConfig"
	opGetDistributionTenant                 = "GetDistributionTenant"
	opGetDistributionTenantByDomain         = "GetDistributionTenantByDomain"
	opGetFieldLevelEncryption               = "GetFieldLevelEncryption"
	opGetFieldLevelEncryptionConfig         = "GetFieldLevelEncryptionConfig"
	opGetFieldLevelEncryptionProfile        = "GetFieldLevelEncryptionProfile"
	opGetFieldLevelEncryptionProfileConfig  = "GetFieldLevelEncryptionProfileConfig"
	opGetInvalidationForDistTenant          = "GetInvalidationForDistributionTenant"
	opGetKeyGroup                           = "GetKeyGroup"
	opGetKeyGroupConfig                     = "GetKeyGroupConfig"
	opGetManagedCertificateDetails          = "GetManagedCertificateDetails"
	opGetMonitoringSubscription             = "GetMonitoringSubscription"
	opGetPublicKey                          = "GetPublicKey"
	opGetPublicKeyConfig                    = "GetPublicKeyConfig"
	opGetRealtimeLogConfig                  = "GetRealtimeLogConfig"
	opGetResourcePolicy                     = "GetResourcePolicy"
	opGetStreamingDistribution              = "GetStreamingDistribution"
	opGetStreamingDistributionConfig        = "GetStreamingDistributionConfig"
	opGetTrustStore                         = "GetTrustStore"
	opGetVpcOrigin                          = "GetVpcOrigin"
	opListAnycastIPLists                    = "ListAnycastIpLists"
	opListConflictingAliases                = "ListConflictingAliases"
	opListConnectionFunctions               = "ListConnectionFunctions"
	opListConnectionGroups                  = "ListConnectionGroups"
	opListContinuousDeploymentPolicies      = "ListContinuousDeploymentPolicies"
	opListDistributionTenants               = "ListDistributionTenants"
	opListDistributionTenantsByCustom       = "ListDistributionTenantsByCustomization"
	opListDistributionsByAnycastIPListID    = "ListDistributionsByAnycastIpListId"
	opListDistributionsByCachePolicyID      = "ListDistributionsByCachePolicyId"
	opListDistributionsByConnectionFunction = "ListDistributionsByConnectionFunction"
	opListDistributionsByConnectionMode     = "ListDistributionsByConnectionMode"
	opListDistributionsByKeyGroup           = "ListDistributionsByKeyGroup"
	opListDistributionsByOriginRequestPol   = "ListDistributionsByOriginRequestPolicyId"
	opListDistributionsByOwnedResource      = "ListDistributionsByOwnedResource"
	opListDistributionsByRealtimeLogConfig  = "ListDistributionsByRealtimeLogConfig"
	opListDistributionsByResponseHeadersPol = "ListDistributionsByResponseHeadersPolicyId"
	opListDistributionsByTrustStore         = "ListDistributionsByTrustStore"
	opListDistributionsByVpcOriginID        = "ListDistributionsByVpcOriginId"
	opListDistributionsByWebACLID           = "ListDistributionsByWebACLId"
	opListDomainConflicts                   = "ListDomainConflicts"
	opListFieldLevelEncryptionConfigs       = "ListFieldLevelEncryptionConfigs"
	opListFieldLevelEncryptionProfiles      = "ListFieldLevelEncryptionProfiles"
	opListInvalidationsForDistTenant        = "ListInvalidationsForDistributionTenant"
	opListKeyGroups                         = "ListKeyGroups"
	opListKeyValueStores                    = "ListKeyValueStores"
	opListPublicKeys                        = "ListPublicKeys"
	opListRealtimeLogConfigs                = "ListRealtimeLogConfigs"
	opListStreamingDistributions            = "ListStreamingDistributions"
	opListTrustStores                       = "ListTrustStores"
	opListVpcOrigins                        = "ListVpcOrigins"
	opPublishConnectionFunction             = "PublishConnectionFunction"
	opPutResourcePolicy                     = "PutResourcePolicy"
	opTestConnectionFunction                = "TestConnectionFunction"
	opUpdateAnycastIPList                   = "UpdateAnycastIpList"
	opUpdateConnectionFunction              = "UpdateConnectionFunction"
	opUpdateConnectionGroup                 = "UpdateConnectionGroup"
	opUpdateContinuousDeploymentPolicy      = "UpdateContinuousDeploymentPolicy"
	opUpdateDistributionTenant              = "UpdateDistributionTenant"
	opUpdateDistributionWithStagingConfig   = "UpdateDistributionWithStagingConfig"
	opUpdateDomainAssociation               = "UpdateDomainAssociation"
	opUpdateFieldLevelEncryptionConfig      = "UpdateFieldLevelEncryptionConfig"
	opUpdateFieldLevelEncryptionProfile     = "UpdateFieldLevelEncryptionProfile"
	opUpdateKeyGroup                        = "UpdateKeyGroup"
	opUpdateKeyValueStore                   = "UpdateKeyValueStore"
	opUpdatePublicKey                       = "UpdatePublicKey"
	opUpdateRealtimeLogConfig               = "UpdateRealtimeLogConfig"
	opUpdateStreamingDistribution           = "UpdateStreamingDistribution"
	opUpdateTrustStore                      = "UpdateTrustStore"
	opUpdateVpcOrigin                       = "UpdateVpcOrigin"
	opVerifyDNSConfiguration                = "VerifyDnsConfiguration"
)

const (
	opAssociateAlias                          = "AssociateAlias"
	opAssociateDistributionTenantWebACL       = "AssociateDistributionTenantWebACL"
	opAssociateDistributionWebACL             = "AssociateDistributionWebACL"
	opCopyDistribution                        = "CopyDistribution"
	opCreateAnycastIPList                     = "CreateAnycastIpList"
	opCreateCachePolicy                       = "CreateCachePolicy"
	opCreateCloudFrontOriginAccessIdentity    = "CreateCloudFrontOriginAccessIdentity"
	opCreateConnectionFunction                = "CreateConnectionFunction"
	opCreateConnectionGroup                   = "CreateConnectionGroup"
	opCreateContinuousDeploymentPolicy        = "CreateContinuousDeploymentPolicy"
	opCreateDistribution                      = "CreateDistribution"
	opCreateFunction                          = "CreateFunction"
	opCreateInvalidation                      = "CreateInvalidation"
	opCreateOriginAccessControl               = "CreateOriginAccessControl"
	opCreateOriginRequestPolicy               = "CreateOriginRequestPolicy"
	opCreateResponseHeadersPolicy             = "CreateResponseHeadersPolicy"
	opDeleteCachePolicy                       = "DeleteCachePolicy"
	opDeleteCloudFrontOriginAccessIdentity    = "DeleteCloudFrontOriginAccessIdentity"
	opDeleteDistribution                      = "DeleteDistribution"
	opDeleteFunction                          = "DeleteFunction"
	opDeleteOriginAccessControl               = "DeleteOriginAccessControl"
	opDeleteOriginRequestPolicy               = "DeleteOriginRequestPolicy"
	opDeleteResponseHeadersPolicy             = "DeleteResponseHeadersPolicy"
	opDescribeFunction                        = "DescribeFunction"
	opGetCachePolicy                          = "GetCachePolicy"
	opGetCachePolicyConfig                    = "GetCachePolicyConfig"
	opGetCloudFrontOriginAccessIdentity       = "GetCloudFrontOriginAccessIdentity"
	opGetCloudFrontOriginAccessIdentityConfig = "GetCloudFrontOriginAccessIdentityConfig"
	opGetDistribution                         = "GetDistribution"
	opGetDistributionConfig                   = "GetDistributionConfig"
	opGetFunction                             = "GetFunction"
	opGetInvalidation                         = "GetInvalidation"
	opGetOriginAccessControl                  = "GetOriginAccessControl"
	opGetOriginAccessControlConfig            = "GetOriginAccessControlConfig"
	opGetOriginRequestPolicy                  = "GetOriginRequestPolicy"
	opGetOriginRequestPolicyConfig            = "GetOriginRequestPolicyConfig"
	opGetResponseHeadersPolicy                = "GetResponseHeadersPolicy"
	opGetResponseHeadersPolicyConfig          = "GetResponseHeadersPolicyConfig"
	opListCachePolicies                       = "ListCachePolicies"
	opListCloudFrontOriginAccessIdentities    = "ListCloudFrontOriginAccessIdentities"
	opListDistributions                       = "ListDistributions"
	opListFunctions                           = "ListFunctions"
	opListInvalidations                       = "ListInvalidations"
	opListOriginAccessControls                = "ListOriginAccessControls"
	opListOriginRequestPolicies               = "ListOriginRequestPolicies"
	opListResponseHeadersPolicies             = "ListResponseHeadersPolicies"
	opListTagsForResource                     = "ListTagsForResource"
	opPublishFunction                         = "PublishFunction"
	opTagResource                             = "TagResource"
)

const (
	cfNS         = "http://cloudfront.amazonaws.com/doc/2020-05-31/"
	cfPathPrefix = "/2020-05-31/"
	maxItems     = 100

	opUpdateCloudFrontOAI = "UpdateCloudFrontOriginAccessIdentity"

	// Path segment constants used in parseCFPath.
	sfxDistribution   = "distribution"
	sfxResourcePolicy = "resource-policy"
)

// Handler is the Echo HTTP handler for AWS CloudFront operations (REST-XML protocol).
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new CloudFront handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset clears all backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Name returns the service name.
func (h *Handler) Name() string { return "CloudFront" }

// GetSupportedOperations returns the list of supported CloudFront operations.
//
//nolint:funlen // extended list for stub operations is inherently long
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opAssociateAlias,
		opAssociateDistributionTenantWebACL,
		opAssociateDistributionWebACL,
		opCopyDistribution,
		opCreateAnycastIPList,
		opCreateCachePolicy,
		opCreateCloudFrontOriginAccessIdentity,
		opCreateConnectionFunction,
		opCreateConnectionGroup,
		opCreateContinuousDeploymentPolicy,
		opCreateDistribution,
		opCreateFunction,
		opCreateInvalidation,
		opCreateOriginAccessControl,
		opCreateOriginRequestPolicy,
		opCreateResponseHeadersPolicy,
		opDeleteCachePolicy,
		opDeleteCloudFrontOriginAccessIdentity,
		opDeleteDistribution,
		opDeleteFunction,
		opDeleteOriginAccessControl,
		opDeleteOriginRequestPolicy,
		opDeleteResponseHeadersPolicy,
		opDescribeFunction,
		opGetCachePolicy,
		opGetCachePolicyConfig,
		opGetCloudFrontOriginAccessIdentity,
		opGetCloudFrontOriginAccessIdentityConfig,
		opGetDistribution,
		opGetDistributionConfig,
		opGetFunction,
		opGetInvalidation,
		opGetOriginAccessControl,
		opGetOriginAccessControlConfig,
		opGetOriginRequestPolicy,
		opGetOriginRequestPolicyConfig,
		opGetResponseHeadersPolicy,
		opGetResponseHeadersPolicyConfig,
		opListCachePolicies,
		opListCloudFrontOriginAccessIdentities,
		opListDistributions,
		opListFunctions,
		opListInvalidations,
		opListOriginAccessControls,
		opListOriginRequestPolicies,
		opListResponseHeadersPolicies,
		opListTagsForResource,
		opPublishFunction,
		opTagResource,
		opTestFunction,
		opUntagResource,
		opUpdateCachePolicy,
		opUpdateCloudFrontOAI,
		opUpdateDistribution,
		opUpdateFunction,
		opUpdateOriginAccessControl,
		opUpdateOriginRequestPolicy,
		opUpdateResponseHeadersPolicy,
		// Stub operations.
		opCreateDistributionTenant,
		opCreateDistributionWithTags,
		opCreateFieldLevelEncryptionConfig,
		opCreateFieldLevelEncryptionProfile,
		opCreateInvalidationForDistTenant,
		opCreateKeyGroup,
		opCreateKeyValueStore,
		opCreateMonitoringSubscription,
		opCreatePublicKey,
		opCreateRealtimeLogConfig,
		opCreateStreamingDistribution,
		opCreateStreamingDistributionWithTags,
		opCreateTrustStore,
		opCreateVpcOrigin,
		opDeleteAnycastIPList,
		opDeleteConnectionFunction,
		opDeleteConnectionGroup,
		opDeleteContinuousDeploymentPolicy,
		opDeleteDistributionTenant,
		opDeleteFieldLevelEncryptionConfig,
		opDeleteFieldLevelEncryptionProfile,
		opDeleteKeyGroup,
		opDeleteKeyValueStore,
		opDeleteMonitoringSubscription,
		opDeletePublicKey,
		opDeleteRealtimeLogConfig,
		opDeleteResourcePolicy,
		opDeleteStreamingDistribution,
		opDeleteTrustStore,
		opDeleteVpcOrigin,
		opDescribeConnectionFunction,
		opDescribeKeyValueStore,
		opDisassociateDistributionTenantWebACL,
		opDisassociateDistributionWebACL,
		opGetAnycastIPList,
		opGetConnectionFunction,
		opGetConnectionGroup,
		opGetConnectionGroupByRoutingEndpoint,
		opGetContinuousDeploymentPolicy,
		opGetContinuousDeploymentPolicyConfig,
		opGetDistributionTenant,
		opGetDistributionTenantByDomain,
		opGetFieldLevelEncryption,
		opGetFieldLevelEncryptionConfig,
		opGetFieldLevelEncryptionProfile,
		opGetFieldLevelEncryptionProfileConfig,
		opGetInvalidationForDistTenant,
		opGetKeyGroup,
		opGetKeyGroupConfig,
		opGetManagedCertificateDetails,
		opGetMonitoringSubscription,
		opGetPublicKey,
		opGetPublicKeyConfig,
		opGetRealtimeLogConfig,
		opGetResourcePolicy,
		opGetStreamingDistribution,
		opGetStreamingDistributionConfig,
		opGetTrustStore,
		opGetVpcOrigin,
		opListAnycastIPLists,
		opListConflictingAliases,
		opListConnectionFunctions,
		opListConnectionGroups,
		opListContinuousDeploymentPolicies,
		opListDistributionTenants,
		opListDistributionTenantsByCustom,
		opListDistributionsByAnycastIPListID,
		opListDistributionsByCachePolicyID,
		opListDistributionsByConnectionFunction,
		opListDistributionsByConnectionMode,
		opListDistributionsByKeyGroup,
		opListDistributionsByOriginRequestPol,
		opListDistributionsByOwnedResource,
		opListDistributionsByRealtimeLogConfig,
		opListDistributionsByResponseHeadersPol,
		opListDistributionsByTrustStore,
		opListDistributionsByVpcOriginID,
		opListDistributionsByWebACLID,
		opListDomainConflicts,
		opListFieldLevelEncryptionConfigs,
		opListFieldLevelEncryptionProfiles,
		opListInvalidationsForDistTenant,
		opListKeyGroups,
		opListKeyValueStores,
		opListPublicKeys,
		opListRealtimeLogConfigs,
		opListStreamingDistributions,
		opListTrustStores,
		opListVpcOrigins,
		opPublishConnectionFunction,
		opPutResourcePolicy,
		opTestConnectionFunction,
		opUpdateAnycastIPList,
		opUpdateConnectionFunction,
		opUpdateConnectionGroup,
		opUpdateContinuousDeploymentPolicy,
		opUpdateDistributionTenant,
		opUpdateDistributionWithStagingConfig,
		opUpdateDomainAssociation,
		opUpdateFieldLevelEncryptionConfig,
		opUpdateFieldLevelEncryptionProfile,
		opUpdateKeyGroup,
		opUpdateKeyValueStore,
		opUpdatePublicKey,
		opUpdateRealtimeLogConfig,
		opUpdateStreamingDistribution,
		opUpdateTrustStore,
		opUpdateVpcOrigin,
		opVerifyDNSConfiguration,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "cloudfront" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this CloudFront instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches AWS CloudFront REST requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, cfPathPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the CloudFront operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := parseCFPath(
		c.Request().Method,
		c.Request().URL.Path,
		c.Request().URL.Query().Get("Resource"),
	)

	return op
}

// ExtractResource extracts the primary resource identifier from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, res := parseCFPath(
		c.Request().Method,
		c.Request().URL.Path,
		c.Request().URL.Query().Get("Resource"),
	)

	return res
}

// cfErrorXML returns an XML error response string.
func cfErrorXML(code, message string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<ErrorResponse xmlns="%s"><Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message></Error></ErrorResponse>`,
		cfNS, code, message)
}

// xmlResp writes an XML response with the given status code.
func xmlResp(c *echo.Context, status int, body string) error {
	c.Response().Header().Set("Content-Type", "text/xml")

	return c.XMLBlob(status, []byte(body))
}

// parseCFPath maps HTTP method + path to (operationName, resourceID).
//
//nolint:cyclop,funlen,gocognit,gocyclo,lll,nlreturn // dispatch table for many REST operations is inherently wide
func parseCFPath(method, path, resourceParam string) (string, string) {
	suffix := strings.TrimPrefix(path, cfPathPrefix)

	switch {
	case suffix == sfxDistribution && method == http.MethodPost:
		return opCreateDistribution, ""
	case suffix == sfxDistribution && method == http.MethodGet:
		return opListDistributions, ""
	case strings.HasPrefix(suffix, "distribution/") && strings.HasSuffix(suffix, "/config"):
		id := strings.TrimPrefix(suffix, "distribution/")
		id = strings.TrimSuffix(id, "/config")
		switch method {
		case http.MethodGet:
			return opGetDistributionConfig, id
		case http.MethodPut:
			return opUpdateDistribution, id
		}
	case strings.HasPrefix(suffix, "distribution/") && strings.HasSuffix(suffix, "/invalidation"):
		id := strings.TrimPrefix(suffix, "distribution/")
		id = strings.TrimSuffix(id, "/invalidation")
		switch method {
		case http.MethodPost:
			return opCreateInvalidation, id
		case http.MethodGet:
			return opListInvalidations, id
		}
	case strings.HasPrefix(suffix, "distribution/") && strings.Contains(suffix, "/invalidation/"):
		// distribution/{distID}/invalidation/{invID}
		inner := strings.TrimPrefix(suffix, "distribution/")
		before, _, ok := strings.Cut(inner, "/invalidation/")
		if ok && method == http.MethodGet {
			return opGetInvalidation, before
		}
	case strings.HasPrefix(suffix, "distribution/") && strings.HasSuffix(suffix, "/associate-alias"):
		id := strings.TrimPrefix(suffix, "distribution/")
		id = strings.TrimSuffix(id, "/associate-alias")
		if method == http.MethodPut {
			return opAssociateAlias, id
		}
	case strings.HasPrefix(suffix, "distribution/") && strings.HasSuffix(suffix, "/associate-web-acl"):
		id := strings.TrimPrefix(suffix, "distribution/")
		id = strings.TrimSuffix(id, "/associate-web-acl")
		if method == http.MethodPut {
			return opAssociateDistributionWebACL, id
		}
	case strings.HasPrefix(suffix, "distribution/") && strings.HasSuffix(suffix, "/copy"):
		id := strings.TrimPrefix(suffix, "distribution/")
		id = strings.TrimSuffix(id, "/copy")
		if method == http.MethodPost {
			return opCopyDistribution, id
		}
	case strings.HasPrefix(suffix, "distribution/") &&
		!strings.Contains(strings.TrimPrefix(suffix, "distribution/"), "/"):
		id := strings.TrimPrefix(suffix, "distribution/")
		switch method {
		case http.MethodGet:
			return opGetDistribution, id
		case http.MethodDelete:
			return opDeleteDistribution, id
		}
	case strings.HasPrefix(suffix, "distribution-tenant/") && strings.HasSuffix(suffix, "/associate-web-acl"):
		id := strings.TrimPrefix(suffix, "distribution-tenant/")
		id = strings.TrimSuffix(id, "/associate-web-acl")
		if method == http.MethodPut {
			return opAssociateDistributionTenantWebACL, id
		}
	case suffix == "origin-access-identity/cloudfront" && method == http.MethodPost:
		return opCreateCloudFrontOriginAccessIdentity, ""
	case suffix == "origin-access-identity/cloudfront" && method == http.MethodGet:
		return opListCloudFrontOriginAccessIdentities, ""
	case strings.HasPrefix(suffix, "origin-access-identity/cloudfront/") && strings.HasSuffix(suffix, "/config"):
		id := strings.TrimPrefix(suffix, "origin-access-identity/cloudfront/")
		id = strings.TrimSuffix(id, "/config")
		if method == http.MethodGet {
			return opGetCloudFrontOriginAccessIdentityConfig, id
		}
		if method == http.MethodPut {
			return opUpdateCloudFrontOAI, id
		}
	case strings.HasPrefix(suffix, "origin-access-identity/cloudfront/") &&
		!strings.Contains(strings.TrimPrefix(suffix, "origin-access-identity/cloudfront/"), "/"):
		id := strings.TrimPrefix(suffix, "origin-access-identity/cloudfront/")
		switch method {
		case http.MethodGet:
			return opGetCloudFrontOriginAccessIdentity, id
		case http.MethodPut:
			return opUpdateCloudFrontOAI, id
		case http.MethodDelete:
			return opDeleteCloudFrontOriginAccessIdentity, id
		}
	case suffix == "anycast-ip-list" && method == http.MethodPost:
		return opCreateAnycastIPList, ""
	// --- Cache Policy ---
	case suffix == "cache-policy" && method == http.MethodPost:
		return opCreateCachePolicy, ""
	case suffix == "cache-policy" && method == http.MethodGet:
		return opListCachePolicies, ""
	case strings.HasPrefix(suffix, "cache-policy/") && strings.HasSuffix(suffix, "/config"):
		id := strings.TrimPrefix(suffix, "cache-policy/")
		id = strings.TrimSuffix(id, "/config")
		if method == http.MethodGet {
			return opGetCachePolicyConfig, id
		}
	case strings.HasPrefix(suffix, "cache-policy/") &&
		!strings.Contains(strings.TrimPrefix(suffix, "cache-policy/"), "/"):
		id := strings.TrimPrefix(suffix, "cache-policy/")
		switch method {
		case http.MethodGet:
			return opGetCachePolicy, id
		case http.MethodPut:
			return opUpdateCachePolicy, id
		case http.MethodDelete:
			return opDeleteCachePolicy, id
		}
	// --- Origin Access Control ---
	case suffix == "origin-access-control" && method == http.MethodPost:
		return opCreateOriginAccessControl, ""
	case suffix == "origin-access-control" && method == http.MethodGet:
		return opListOriginAccessControls, ""
	case strings.HasPrefix(suffix, "origin-access-control/") && strings.HasSuffix(suffix, "/config"):
		id := strings.TrimPrefix(suffix, "origin-access-control/")
		id = strings.TrimSuffix(id, "/config")
		switch method {
		case http.MethodGet:
			return opGetOriginAccessControlConfig, id
		case http.MethodPut:
			return opUpdateOriginAccessControl, id
		}
	case strings.HasPrefix(suffix, "origin-access-control/") &&
		!strings.Contains(strings.TrimPrefix(suffix, "origin-access-control/"), "/"):
		id := strings.TrimPrefix(suffix, "origin-access-control/")
		switch method {
		case http.MethodGet:
			return opGetOriginAccessControl, id
		case http.MethodDelete:
			return opDeleteOriginAccessControl, id
		}
	// --- Response Headers Policy ---
	case suffix == "response-headers-policy" && method == http.MethodPost:
		return opCreateResponseHeadersPolicy, ""
	case suffix == "response-headers-policy" && method == http.MethodGet:
		return opListResponseHeadersPolicies, ""
	case strings.HasPrefix(suffix, "response-headers-policy/") && strings.HasSuffix(suffix, "/config"):
		id := strings.TrimPrefix(suffix, "response-headers-policy/")
		id = strings.TrimSuffix(id, "/config")
		switch method {
		case http.MethodGet:
			return opGetResponseHeadersPolicyConfig, id
		case http.MethodPut:
			return opUpdateResponseHeadersPolicy, id
		}
	case strings.HasPrefix(suffix, "response-headers-policy/") &&
		!strings.Contains(strings.TrimPrefix(suffix, "response-headers-policy/"), "/"):
		id := strings.TrimPrefix(suffix, "response-headers-policy/")
		switch method {
		case http.MethodGet:
			return opGetResponseHeadersPolicy, id
		case http.MethodDelete:
			return opDeleteResponseHeadersPolicy, id
		}
	// --- CloudFront Function ---
	case suffix == "function" && method == http.MethodPost:
		return opCreateFunction, ""
	case suffix == "function" && method == http.MethodGet:
		return opListFunctions, ""
	case strings.HasPrefix(suffix, "function/") && strings.HasSuffix(suffix, "/publish"):
		name := strings.TrimPrefix(suffix, "function/")
		name = strings.TrimSuffix(name, "/publish")
		if method == http.MethodPost {
			return opPublishFunction, name
		}
	case strings.HasPrefix(suffix, "function/") && strings.HasSuffix(suffix, "/describe"):
		name := strings.TrimPrefix(suffix, "function/")
		name = strings.TrimSuffix(name, "/describe")
		if method == http.MethodGet {
			return opDescribeFunction, name
		}
	case strings.HasPrefix(suffix, "function/") && strings.HasSuffix(suffix, "/test"):
		name := strings.TrimPrefix(suffix, "function/")
		name = strings.TrimSuffix(name, "/test")
		if method == http.MethodPost {
			return opTestFunction, name
		}
	case strings.HasPrefix(suffix, "function/") && !strings.Contains(strings.TrimPrefix(suffix, "function/"), "/"):
		name := strings.TrimPrefix(suffix, "function/")
		switch method {
		case http.MethodGet:
			return opGetFunction, name
		case http.MethodPut:
			return opUpdateFunction, name
		case http.MethodDelete:
			return opDeleteFunction, name
		}
	// --- Origin Request Policy ---
	case suffix == "origin-request-policy" && method == http.MethodPost:
		return opCreateOriginRequestPolicy, ""
	case suffix == "origin-request-policy" && method == http.MethodGet:
		return opListOriginRequestPolicies, ""
	case strings.HasPrefix(suffix, "origin-request-policy/") && strings.HasSuffix(suffix, "/config"):
		id := strings.TrimPrefix(suffix, "origin-request-policy/")
		id = strings.TrimSuffix(id, "/config")
		switch method {
		case http.MethodGet:
			return opGetOriginRequestPolicyConfig, id
		case http.MethodPut:
			return opUpdateOriginRequestPolicy, id
		}
	case strings.HasPrefix(suffix, "origin-request-policy/") &&
		!strings.Contains(strings.TrimPrefix(suffix, "origin-request-policy/"), "/"):
		id := strings.TrimPrefix(suffix, "origin-request-policy/")
		switch method {
		case http.MethodGet:
			return opGetOriginRequestPolicy, id
		case http.MethodDelete:
			return opDeleteOriginRequestPolicy, id
		}
	// --- Connection / ContinuousDeployment / Tags ---
	case suffix == "connection-function" && method == http.MethodPost:
		return opCreateConnectionFunction, ""
	case suffix == "connection-group" && method == http.MethodPost:
		return opCreateConnectionGroup, ""
	case suffix == "continuous-deployment-policy" && method == http.MethodPost:
		return opCreateContinuousDeploymentPolicy, ""
	case suffix == "tagging":
		switch method {
		case http.MethodGet:
			return opListTagsForResource, resourceParam
		case http.MethodPost:
			return opTagResource, resourceParam
		case http.MethodDelete:
			return opUntagResource, resourceParam
		}
	// --- Stub operation paths ---
	case suffix == sfxDistribution && method == http.MethodPost && resourceParam == "WithTags":
		return opCreateDistributionWithTags, ""
	case suffix == "distribution-tenant" && method == http.MethodPost:
		return opCreateDistributionTenant, ""
	case suffix == "distribution-tenant" && method == http.MethodGet:
		return opListDistributionTenants, ""
	case strings.HasPrefix(suffix, "distribution-tenant/") && !strings.Contains(strings.TrimPrefix(suffix, "distribution-tenant/"), "/"):
		id := strings.TrimPrefix(suffix, "distribution-tenant/")
		switch method {
		case http.MethodGet:
			return opGetDistributionTenant, id
		case http.MethodPut:
			return opUpdateDistributionTenant, id
		case http.MethodDelete:
			return opDeleteDistributionTenant, id
		}
	case suffix == "field-level-encryption" && method == http.MethodPost:
		return opCreateFieldLevelEncryptionConfig, ""
	case suffix == "field-level-encryption" && method == http.MethodGet:
		return opListFieldLevelEncryptionConfigs, ""
	case strings.HasPrefix(suffix, "field-level-encryption/") && !strings.Contains(strings.TrimPrefix(suffix, "field-level-encryption/"), "/"):
		id := strings.TrimPrefix(suffix, "field-level-encryption/")
		switch method {
		case http.MethodGet:
			return opGetFieldLevelEncryption, id
		case http.MethodPut:
			return opUpdateFieldLevelEncryptionConfig, id
		case http.MethodDelete:
			return opDeleteFieldLevelEncryptionConfig, id
		}
	case strings.HasPrefix(suffix, "field-level-encryption/") && strings.HasSuffix(suffix, "/config"):
		id := strings.TrimPrefix(suffix, "field-level-encryption/")
		id = strings.TrimSuffix(id, "/config")
		if method == http.MethodGet {
			return opGetFieldLevelEncryptionConfig, id
		}
	case suffix == "field-level-encryption-profile" && method == http.MethodPost:
		return opCreateFieldLevelEncryptionProfile, ""
	case suffix == "field-level-encryption-profile" && method == http.MethodGet:
		return opListFieldLevelEncryptionProfiles, ""
	case strings.HasPrefix(suffix, "field-level-encryption-profile/") && !strings.Contains(strings.TrimPrefix(suffix, "field-level-encryption-profile/"), "/"):
		id := strings.TrimPrefix(suffix, "field-level-encryption-profile/")
		switch method {
		case http.MethodGet:
			return opGetFieldLevelEncryptionProfile, id
		case http.MethodPut:
			return opUpdateFieldLevelEncryptionProfile, id
		case http.MethodDelete:
			return opDeleteFieldLevelEncryptionProfile, id
		}
	case strings.HasPrefix(suffix, "field-level-encryption-profile/") && strings.HasSuffix(suffix, "/config"):
		id := strings.TrimPrefix(suffix, "field-level-encryption-profile/")
		id = strings.TrimSuffix(id, "/config")
		if method == http.MethodGet {
			return opGetFieldLevelEncryptionProfileConfig, id
		}
	case suffix == "key-group" && method == http.MethodPost:
		return opCreateKeyGroup, ""
	case suffix == "key-group" && method == http.MethodGet:
		return opListKeyGroups, ""
	case strings.HasPrefix(suffix, "key-group/") && !strings.Contains(strings.TrimPrefix(suffix, "key-group/"), "/"):
		id := strings.TrimPrefix(suffix, "key-group/")
		switch method {
		case http.MethodGet:
			return opGetKeyGroup, id
		case http.MethodPut:
			return opUpdateKeyGroup, id
		case http.MethodDelete:
			return opDeleteKeyGroup, id
		}
	case strings.HasPrefix(suffix, "key-group/") && strings.HasSuffix(suffix, "/config"):
		id := strings.TrimPrefix(suffix, "key-group/")
		id = strings.TrimSuffix(id, "/config")
		if method == http.MethodGet {
			return opGetKeyGroupConfig, id
		}
	case suffix == "key-value-store" && method == http.MethodPost:
		return opCreateKeyValueStore, ""
	case suffix == "key-value-store" && method == http.MethodGet:
		return opListKeyValueStores, ""
	case strings.HasPrefix(suffix, "key-value-store/") && !strings.Contains(strings.TrimPrefix(suffix, "key-value-store/"), "/"):
		id := strings.TrimPrefix(suffix, "key-value-store/")
		switch method {
		case http.MethodGet:
			return opDescribeKeyValueStore, id
		case http.MethodPut:
			return opUpdateKeyValueStore, id
		case http.MethodDelete:
			return opDeleteKeyValueStore, id
		}
	case suffix == "public-key" && method == http.MethodPost:
		return opCreatePublicKey, ""
	case suffix == "public-key" && method == http.MethodGet:
		return opListPublicKeys, ""
	case strings.HasPrefix(suffix, "public-key/") && !strings.Contains(strings.TrimPrefix(suffix, "public-key/"), "/"):
		id := strings.TrimPrefix(suffix, "public-key/")
		switch method {
		case http.MethodGet:
			return opGetPublicKey, id
		case http.MethodPut:
			return opUpdatePublicKey, id
		case http.MethodDelete:
			return opDeletePublicKey, id
		}
	case strings.HasPrefix(suffix, "public-key/") && strings.HasSuffix(suffix, "/config"):
		id := strings.TrimPrefix(suffix, "public-key/")
		id = strings.TrimSuffix(id, "/config")
		if method == http.MethodGet {
			return opGetPublicKeyConfig, id
		}
	case suffix == "realtime-log-config" && method == http.MethodPost:
		return opCreateRealtimeLogConfig, ""
	case suffix == "realtime-log-config" && method == http.MethodGet:
		return opListRealtimeLogConfigs, ""
	case strings.HasPrefix(suffix, "realtime-log-config/") && !strings.Contains(strings.TrimPrefix(suffix, "realtime-log-config/"), "/"):
		id := strings.TrimPrefix(suffix, "realtime-log-config/")
		switch method {
		case http.MethodGet:
			return opGetRealtimeLogConfig, id
		case http.MethodPut:
			return opUpdateRealtimeLogConfig, id
		case http.MethodDelete:
			return opDeleteRealtimeLogConfig, id
		}
	case suffix == "streaming-distribution" && method == http.MethodPost:
		return opCreateStreamingDistribution, ""
	case suffix == "streaming-distribution" && method == http.MethodGet:
		return opListStreamingDistributions, ""
	case strings.HasPrefix(suffix, "streaming-distribution/") && !strings.Contains(strings.TrimPrefix(suffix, "streaming-distribution/"), "/"):
		id := strings.TrimPrefix(suffix, "streaming-distribution/")
		switch method {
		case http.MethodGet:
			return opGetStreamingDistribution, id
		case http.MethodPut:
			return opUpdateStreamingDistribution, id
		case http.MethodDelete:
			return opDeleteStreamingDistribution, id
		}
	case strings.HasPrefix(suffix, "streaming-distribution/") && strings.HasSuffix(suffix, "/config"):
		id := strings.TrimPrefix(suffix, "streaming-distribution/")
		id = strings.TrimSuffix(id, "/config")
		if method == http.MethodGet {
			return opGetStreamingDistributionConfig, id
		}
	case suffix == "trust-store" && method == http.MethodPost:
		return opCreateTrustStore, ""
	case suffix == "trust-store" && method == http.MethodGet:
		return opListTrustStores, ""
	case strings.HasPrefix(suffix, "trust-store/") && !strings.Contains(strings.TrimPrefix(suffix, "trust-store/"), "/"):
		id := strings.TrimPrefix(suffix, "trust-store/")
		switch method {
		case http.MethodGet:
			return opGetTrustStore, id
		case http.MethodPut:
			return opUpdateTrustStore, id
		case http.MethodDelete:
			return opDeleteTrustStore, id
		}
	case suffix == "vpc-origin" && method == http.MethodPost:
		return opCreateVpcOrigin, ""
	case suffix == "vpc-origin" && method == http.MethodGet:
		return opListVpcOrigins, ""
	case strings.HasPrefix(suffix, "vpc-origin/") && !strings.Contains(strings.TrimPrefix(suffix, "vpc-origin/"), "/"):
		id := strings.TrimPrefix(suffix, "vpc-origin/")
		switch method {
		case http.MethodGet:
			return opGetVpcOrigin, id
		case http.MethodPut:
			return opUpdateVpcOrigin, id
		case http.MethodDelete:
			return opDeleteVpcOrigin, id
		}
	case strings.HasPrefix(suffix, "anycast-ip-list/") && !strings.Contains(strings.TrimPrefix(suffix, "anycast-ip-list/"), "/"):
		id := strings.TrimPrefix(suffix, "anycast-ip-list/")
		switch method {
		case http.MethodGet:
			return opGetAnycastIPList, id
		case http.MethodPut:
			return opUpdateAnycastIPList, id
		case http.MethodDelete:
			return opDeleteAnycastIPList, id
		}
	case suffix == "anycast-ip-list" && method == http.MethodGet:
		return opListAnycastIPLists, ""
	case strings.HasPrefix(suffix, "connection-function/") && strings.HasSuffix(suffix, "/describe"):
		id := strings.TrimPrefix(suffix, "connection-function/")
		id = strings.TrimSuffix(id, "/describe")
		return opDescribeConnectionFunction, id
	case strings.HasPrefix(suffix, "connection-function/") && strings.HasSuffix(suffix, "/publish"):
		id := strings.TrimPrefix(suffix, "connection-function/")
		id = strings.TrimSuffix(id, "/publish")
		return opPublishConnectionFunction, id
	case strings.HasPrefix(suffix, "connection-function/") && strings.HasSuffix(suffix, "/test"):
		id := strings.TrimPrefix(suffix, "connection-function/")
		id = strings.TrimSuffix(id, "/test")
		return opTestConnectionFunction, id
	case strings.HasPrefix(suffix, "connection-function/") && !strings.Contains(strings.TrimPrefix(suffix, "connection-function/"), "/"):
		id := strings.TrimPrefix(suffix, "connection-function/")
		switch method {
		case http.MethodGet:
			return opGetConnectionFunction, id
		case http.MethodPut:
			return opUpdateConnectionFunction, id
		case http.MethodDelete:
			return opDeleteConnectionFunction, id
		}
	case suffix == "connection-function" && method == http.MethodGet:
		return opListConnectionFunctions, ""
	case suffix == "connection-group" && method == http.MethodGet:
		return opListConnectionGroups, ""
	case strings.HasPrefix(suffix, "connection-group/") && !strings.Contains(strings.TrimPrefix(suffix, "connection-group/"), "/"):
		id := strings.TrimPrefix(suffix, "connection-group/")
		switch method {
		case http.MethodGet:
			return opGetConnectionGroup, id
		case http.MethodPut:
			return opUpdateConnectionGroup, id
		case http.MethodDelete:
			return opDeleteConnectionGroup, id
		}
	case suffix == "continuous-deployment-policy" && method == http.MethodGet:
		return opListContinuousDeploymentPolicies, ""
	case strings.HasPrefix(suffix, "continuous-deployment-policy/") && strings.HasSuffix(suffix, "/config"):
		id := strings.TrimPrefix(suffix, "continuous-deployment-policy/")
		id = strings.TrimSuffix(id, "/config")
		if method == http.MethodGet {
			return opGetContinuousDeploymentPolicyConfig, id
		}
	case strings.HasPrefix(suffix, "continuous-deployment-policy/") && !strings.Contains(strings.TrimPrefix(suffix, "continuous-deployment-policy/"), "/"):
		id := strings.TrimPrefix(suffix, "continuous-deployment-policy/")
		switch method {
		case http.MethodGet:
			return opGetContinuousDeploymentPolicy, id
		case http.MethodPut:
			return opUpdateContinuousDeploymentPolicy, id
		case http.MethodDelete:
			return opDeleteContinuousDeploymentPolicy, id
		}
	case suffix == sfxResourcePolicy && method == http.MethodGet:
		return opGetResourcePolicy, resourceParam
	case suffix == sfxResourcePolicy && method == http.MethodPost:
		return opPutResourcePolicy, resourceParam
	case suffix == sfxResourcePolicy && method == http.MethodDelete:
		return opDeleteResourcePolicy, resourceParam
	case strings.HasPrefix(suffix, "distribution/") && strings.HasSuffix(suffix, "/monitoring-subscription"):
		id := strings.TrimPrefix(suffix, "distribution/")
		id = strings.TrimSuffix(id, "/monitoring-subscription")
		switch method {
		case http.MethodPost:
			return opCreateMonitoringSubscription, id
		case http.MethodGet:
			return opGetMonitoringSubscription, id
		case http.MethodDelete:
			return opDeleteMonitoringSubscription, id
		}
	case strings.HasPrefix(suffix, "distribution/") && strings.HasSuffix(suffix, "/staging"):
		id := strings.TrimPrefix(suffix, "distribution/")
		id = strings.TrimSuffix(id, "/staging")
		if method == http.MethodPut {
			return opUpdateDistributionWithStagingConfig, id
		}
	case strings.HasPrefix(suffix, "distribution/") && strings.HasSuffix(suffix, "/disassociate-web-acl"):
		id := strings.TrimPrefix(suffix, "distribution/")
		id = strings.TrimSuffix(id, "/disassociate-web-acl")
		if method == http.MethodPut {
			return opDisassociateDistributionWebACL, id
		}
	case strings.HasPrefix(suffix, "distribution-tenant/") && strings.HasSuffix(suffix, "/disassociate-web-acl"):
		id := strings.TrimPrefix(suffix, "distribution-tenant/")
		id = strings.TrimSuffix(id, "/disassociate-web-acl")
		if method == http.MethodPut {
			return opDisassociateDistributionTenantWebACL, id
		}
	case strings.HasPrefix(suffix, "distribution-tenant/") && strings.Contains(suffix, "/invalidation"):
		inner := strings.TrimPrefix(suffix, "distribution-tenant/")
		if strings.HasSuffix(suffix, "/invalidation") && method == http.MethodPost {
			id := strings.TrimSuffix(inner, "/invalidation")
			return opCreateInvalidationForDistTenant, id
		}
		if strings.HasSuffix(suffix, "/invalidation") && method == http.MethodGet {
			id := strings.TrimSuffix(inner, "/invalidation")
			return opListInvalidationsForDistTenant, id
		}
		if before, after, ok := strings.Cut(inner, "/invalidation/"); ok &&
			method == http.MethodGet {
			_ = after
			return opGetInvalidationForDistTenant, before
		}
	case strings.HasPrefix(suffix, "distribution/") && strings.Contains(suffix, "/list-by-"):
		if method == http.MethodGet {
			return opListDistributionsByOwnedResource, ""
		}
	case suffix == "conflicting-alias" && method == http.MethodGet:
		return opListConflictingAliases, ""
	case suffix == "domain-conflict" && method == http.MethodPost:
		return opListDomainConflicts, ""
	case suffix == "domain-association" && method == http.MethodPost:
		return opUpdateDomainAssociation, ""
	case suffix == "verify-dns-configuration" && method == http.MethodPost:
		return opVerifyDNSConfiguration, ""
	case strings.HasPrefix(suffix, "distributions/by-cache-policy-id/") && method == http.MethodGet:
		id := strings.TrimPrefix(suffix, "distributions/by-cache-policy-id/")
		return opListDistributionsByCachePolicyID, id
	case strings.HasPrefix(suffix, "distributions/by-origin-request-policy-id/") && method == http.MethodGet:
		id := strings.TrimPrefix(suffix, "distributions/by-origin-request-policy-id/")
		return opListDistributionsByOriginRequestPol, id
	case strings.HasPrefix(suffix, "distributions/by-response-headers-policy-id/") && method == http.MethodGet:
		id := strings.TrimPrefix(suffix, "distributions/by-response-headers-policy-id/")
		return opListDistributionsByResponseHeadersPol, id
	case strings.HasPrefix(suffix, "distributions/by-web-acl-id/") && method == http.MethodGet:
		id := strings.TrimPrefix(suffix, "distributions/by-web-acl-id/")
		return opListDistributionsByWebACLID, id
	case strings.HasPrefix(suffix, "distributions/by-key-group/") && method == http.MethodGet:
		id := strings.TrimPrefix(suffix, "distributions/by-key-group/")
		return opListDistributionsByKeyGroup, id
	case strings.HasPrefix(suffix, "distributions/by-realtime-log-config") && method == http.MethodGet:
		return opListDistributionsByRealtimeLogConfig, ""
	case strings.HasPrefix(suffix, "distributions/by-vpc-origin-id/") && method == http.MethodGet:
		id := strings.TrimPrefix(suffix, "distributions/by-vpc-origin-id/")
		return opListDistributionsByVpcOriginID, id
	case strings.HasPrefix(suffix, "distributions/by-anycast-ip-list-id/") && method == http.MethodGet:
		id := strings.TrimPrefix(suffix, "distributions/by-anycast-ip-list-id/")
		return opListDistributionsByAnycastIPListID, id
	case strings.HasPrefix(suffix, "distributions/by-connection-function/") && method == http.MethodGet:
		id := strings.TrimPrefix(suffix, "distributions/by-connection-function/")
		return opListDistributionsByConnectionFunction, id
	case suffix == "distributions/by-connection-mode" && method == http.MethodGet:
		return opListDistributionsByConnectionMode, ""
	case suffix == "distribution-tenants/by-customization" && method == http.MethodGet:
		return opListDistributionTenantsByCustom, ""
	case strings.HasPrefix(suffix, "distribution-tenant/") && strings.HasSuffix(suffix, "/managed-certificate-details"):
		id := strings.TrimPrefix(suffix, "distribution-tenant/")
		id = strings.TrimSuffix(id, "/managed-certificate-details")
		return opGetManagedCertificateDetails, id
	case suffix == "connection-group-by-routing-endpoint" && method == http.MethodGet:
		return opGetConnectionGroupByRoutingEndpoint, ""
	case suffix == "distribution-tenant-by-domain" && method == http.MethodGet:
		return opGetDistributionTenantByDomain, ""
	case strings.HasPrefix(suffix, "trust-store/") && strings.Contains(suffix, "/by-trust-store/"):
		return opListDistributionsByTrustStore, ""
	}

	return "Unknown", ""
}

// --- Incoming XML structs ---

type distributionConfigMinimal struct {
	CallerReference string `xml:"CallerReference"`
	Comment         string `xml:"Comment"`
	Enabled         bool   `xml:"Enabled"`
}

type oaiConfigXML struct {
	XMLName         xml.Name `xml:"CloudFrontOriginAccessIdentityConfig"`
	CallerReference string   `xml:"CallerReference"`
	Comment         string   `xml:"Comment"`
}

type oaiResponseXML struct {
	XMLName           xml.Name     `xml:"CloudFrontOriginAccessIdentity"`
	XMLNS             string       `xml:"xmlns,attr"`
	ID                string       `xml:"Id"`
	S3CanonicalUserID string       `xml:"S3CanonicalUserId"`
	Config            oaiConfigXML `xml:"CloudFrontOriginAccessIdentityConfig"`
}

type oaiSummary struct {
	XMLName           xml.Name `xml:"CloudFrontOriginAccessIdentitySummary"`
	ID                string   `xml:"Id"`
	S3CanonicalUserID string   `xml:"S3CanonicalUserId"`
	Comment           string   `xml:"Comment"`
}

type oaiList struct {
	XMLName     xml.Name     `xml:"CloudFrontOriginAccessIdentityList"`
	XMLNS       string       `xml:"xmlns,attr"`
	Items       []oaiSummary `xml:"Items>CloudFrontOriginAccessIdentitySummary"`
	MaxItems    int          `xml:"MaxItems"`
	Quantity    int          `xml:"Quantity"`
	IsTruncated bool         `xml:"IsTruncated"`
}

type tagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type tagsXML struct {
	XMLName xml.Name `xml:"Tags"`
	XMLNS   string   `xml:"xmlns,attr,omitempty"`
	Items   []tagXML `xml:"Items>Tag"`
}

type untagBody struct {
	Keys []string `xml:"Items>Key"`
}

type distributionSummaryXML struct {
	XMLName xml.Name `xml:"DistributionSummary"`
	Origins struct {
		Inner    string `xml:",innerxml"`
		Quantity int    `xml:"Quantity"`
	} `xml:"Origins"`
	DefaultCacheBehavior struct {
		Inner string `xml:",innerxml"`
	} `xml:"DefaultCacheBehavior"`
	Status       string `xml:"Status"`
	DomainName   string `xml:"DomainName"`
	Comment      string `xml:"Comment"`
	ARN          string `xml:"ARN"`
	ID           string `xml:"Id"`
	PriceClass   string `xml:"PriceClass"`
	HTTPVersion  string `xml:"HttpVersion"`
	Restrictions struct {
		GeoRestriction struct {
			RestrictionType string `xml:"RestrictionType"`
			Quantity        int    `xml:"Quantity"`
		} `xml:"GeoRestriction"`
	} `xml:"Restrictions"`
	Aliases struct {
		Quantity int `xml:"Quantity"`
	} `xml:"Aliases"`
	Enabled           bool `xml:"Enabled"`
	ViewerCertificate struct {
		CloudFrontDefaultCertificate bool `xml:"CloudFrontDefaultCertificate"`
	} `xml:"ViewerCertificate"`
	IsIPV6Enabled bool `xml:"IsIPV6Enabled"`
}

// distributionResponseXML builds the full Distribution XML response.
func distributionResponseXML(d *Distribution) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<Distribution xmlns="%s">`+
		`<Id>%s</Id>`+
		`<ARN>%s</ARN>`+
		`<Status>%s</Status>`+
		`<DomainName>%s</DomainName>`+
		`<InProgressInvalidationBatches>0</InProgressInvalidationBatches>`+
		`%s`+
		`</Distribution>`,
		cfNS, d.ID, d.ARN, d.Status, d.DomainName, string(d.RawConfig))
}

// Handler returns the Echo handler function for CloudFront requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		log := logger.Load(c.Request().Context())
		operation, resource := parseCFPath(
			c.Request().Method,
			c.Request().URL.Path,
			c.Request().URL.Query().Get("Resource"),
		)

		log.Debug("cloudfront request", "operation", operation, "resource", resource)

		return h.dispatch(c, operation, resource)
	}
}

func (h *Handler) dispatch(c *echo.Context, operation, resource string) error {
	if err := h.dispatchCreate(c, operation); !errors.Is(err, errNotDispatched) {
		return err
	}

	if err := h.dispatchList(c, operation, resource); !errors.Is(err, errNotDispatched) {
		return err
	}

	if err := h.dispatchGetOrMutate(c, operation, resource); !errors.Is(err, errNotDispatched) {
		return err
	}

	if err := h.dispatchMisc(c, operation, resource); !errors.Is(err, errNotDispatched) {
		return err
	}

	return h.dispatchStubs(c, operation)
}

// errNotDispatched is a sentinel returned by sub-dispatchers when an operation
// did not match, so the caller can try the next sub-dispatcher.
var errNotDispatched = errors.New("not dispatched")

func (h *Handler) dispatchCreate(c *echo.Context, operation string) error {
	switch operation {
	case opCreateAnycastIPList:
		return h.handleCreateAnycastIPList(c)
	case opCreateCachePolicy:
		return h.handleCreateCachePolicy(c)
	case opCreateCloudFrontOriginAccessIdentity:
		return h.handleCreateOAI(c)
	case opCreateConnectionFunction:
		return h.handleCreateConnectionFunction(c)
	case opCreateConnectionGroup:
		return h.handleCreateConnectionGroup(c)
	case opCreateContinuousDeploymentPolicy:
		return h.handleCreateContinuousDeploymentPolicy(c)
	case opCreateDistribution:
		return h.handleCreateDistribution(c)
	case opCreateFunction:
		return h.handleCreateFunction(c)
	case opCreateOriginAccessControl:
		return h.handleCreateOriginAccessControl(c)
	case opCreateOriginRequestPolicy:
		return h.handleCreateOriginRequestPolicy(c)
	case opCreateResponseHeadersPolicy:
		return h.handleCreateResponseHeadersPolicy(c)
	default:
		return errNotDispatched
	}
}

// dispatchGetOrMutate handles all GET, PUT, and DELETE operations.
//
//nolint:cyclop,funlen // combined GET/mutate dispatch table is inherently wide
func (h *Handler) dispatchGetOrMutate(c *echo.Context, operation, resource string) error {
	switch operation {
	case opGetCachePolicy:
		return h.handleGetCachePolicy(c, resource)
	case opGetCachePolicyConfig:
		return h.handleGetCachePolicyConfig(c, resource)
	case opGetDistribution:
		return h.handleGetDistribution(c, resource)
	case opGetDistributionConfig:
		return h.handleGetDistributionConfig(c, resource)
	case opGetFunction:
		return h.handleGetFunction(c, resource)
	case opGetInvalidation:
		return h.handleGetInvalidation(c, resource)
	case opGetOriginAccessControl:
		return h.handleGetOriginAccessControl(c, resource)
	case opGetOriginAccessControlConfig:
		return h.handleGetOriginAccessControlConfig(c, resource)
	case opGetCloudFrontOriginAccessIdentity:
		return h.handleGetOAI(c, resource)
	case opGetCloudFrontOriginAccessIdentityConfig:
		return h.handleGetOAIConfig(c, resource)
	case opGetOriginRequestPolicy:
		return h.handleGetOriginRequestPolicy(c, resource)
	case opGetOriginRequestPolicyConfig:
		return h.handleGetOriginRequestPolicyConfig(c, resource)
	case opGetResponseHeadersPolicy:
		return h.handleGetResponseHeadersPolicy(c, resource)
	case opGetResponseHeadersPolicyConfig:
		return h.handleGetResponseHeadersPolicyConfig(c, resource)
	case opDeleteCachePolicy:
		return h.handleDeleteCachePolicy(c, resource)
	case opDeleteDistribution:
		return h.handleDeleteDistribution(c, resource)
	case opDeleteFunction:
		return h.handleDeleteFunction(c, resource)
	case opDeleteOriginAccessControl:
		return h.handleDeleteOriginAccessControl(c, resource)
	case opDeleteCloudFrontOriginAccessIdentity:
		return h.handleDeleteOAI(c, resource)
	case opDeleteOriginRequestPolicy:
		return h.handleDeleteOriginRequestPolicy(c, resource)
	case opDeleteResponseHeadersPolicy:
		return h.handleDeleteResponseHeadersPolicy(c, resource)
	case opUpdateCloudFrontOAI:
		return h.handleUpdateOAI(c, resource)
	case opUpdateCachePolicy:
		return h.handleUpdateCachePolicy(c, resource)
	case opUpdateDistribution:
		return h.handleUpdateDistribution(c, resource)
	case opUpdateFunction:
		return h.handleUpdateFunction(c, resource)
	case opUpdateOriginAccessControl:
		return h.handleUpdateOriginAccessControl(c, resource)
	case opUpdateOriginRequestPolicy:
		return h.handleUpdateOriginRequestPolicy(c, resource)
	case opUpdateResponseHeadersPolicy:
		return h.handleUpdateResponseHeadersPolicy(c, resource)
	default:
		return errNotDispatched
	}
}

func (h *Handler) dispatchList(c *echo.Context, operation, resource string) error {
	switch operation {
	case opListCachePolicies:
		return h.handleListCachePolicies(c)
	case opListDistributions:
		return h.handleListDistributions(c)
	case opListFunctions:
		return h.handleListFunctions(c)
	case opListInvalidations:
		return h.handleListInvalidations(c, resource)
	case opListOriginAccessControls:
		return h.handleListOriginAccessControls(c)
	case opListCloudFrontOriginAccessIdentities:
		return h.handleListOAIs(c)
	case opListOriginRequestPolicies:
		return h.handleListOriginRequestPolicies(c)
	case opListResponseHeadersPolicies:
		return h.handleListResponseHeadersPolicies(c)
	case opListTagsForResource:
		return h.handleListTagsForResource(c)
	default:
		return errNotDispatched
	}
}

func (h *Handler) dispatchMisc(c *echo.Context, operation, resource string) error {
	switch operation {
	case opAssociateAlias:
		return h.handleAssociateAlias(c, resource)
	case opAssociateDistributionTenantWebACL:
		return h.handleAssociateDistributionTenantWebACL(c, resource)
	case opAssociateDistributionWebACL:
		return h.handleAssociateDistributionWebACL(c, resource)
	case opCopyDistribution:
		return h.handleCopyDistribution(c, resource)
	case opCreateInvalidation:
		return h.handleCreateInvalidation(c, resource)
	case opDescribeFunction:
		return h.handleDescribeFunction(c, resource)
	case opPublishFunction:
		return h.handlePublishFunction(c, resource)
	case opTagResource:
		return h.handleTagResource(c)
	case opTestFunction:
		return h.handleTestFunction(c, resource)
	case opUntagResource:
		return h.handleUntagResource(c)
	default:
		return errNotDispatched
	}
}

// cfStubXMLList returns an empty CloudFront list XML response for stub operations.
func cfStubXMLList(ns, listTag string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<%s xmlns="%s"><Items/><MaxItems>0</MaxItems><Quantity>0</Quantity><IsTruncated>false</IsTruncated></%s>`,
		listTag, ns, listTag)
}

// dispatchStubs handles all stub CloudFront operations with minimal valid responses.
//
//nolint:funlen,cyclop,gocyclo,nlreturn // large dispatch table for stub operations
func (h *Handler) dispatchStubs(c *echo.Context, operation string) error {
	emptyItems := `<?xml version="1.0" encoding="UTF-8"?><Items xmlns="` + cfNS + `"/>`
	noContent := func() error { return c.NoContent(http.StatusNoContent) }
	emptyList := func(tag string) error { return xmlResp(c, http.StatusOK, cfStubXMLList(cfNS, tag)) }
	created := func(tag, id string) error {
		return xmlResp(c, http.StatusCreated, fmt.Sprintf(
			`<?xml version="1.0" encoding="UTF-8"?><%s xmlns="%s"><Id>%s</Id></%s>`,
			tag, cfNS, id, tag))
	}
	getStub := func(tag, id string) error {
		return xmlResp(c, http.StatusOK, fmt.Sprintf(
			`<?xml version="1.0" encoding="UTF-8"?><%s xmlns="%s"><Id>%s</Id></%s>`,
			tag, cfNS, id, tag))
	}
	_ = emptyItems

	switch operation {
	// Distribution variants.
	case opCreateDistributionTenant:
		return created("DistributionTenant", "tenant-stub")
	case opCreateDistributionWithTags:
		return created("Distribution", "dist-stub")
	case opUpdateDistributionTenant:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><DistributionTenant xmlns="`+cfNS+`"/>`,
		)
	case opDeleteDistributionTenant:
		return noContent()
	case opGetDistributionTenant, opGetDistributionTenantByDomain:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><DistributionTenant xmlns="`+cfNS+`"/>`,
		)
	case opListDistributionTenants, opListDistributionTenantsByCustom:
		return emptyList("DistributionTenantList")
	case opUpdateDistributionWithStagingConfig:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><Distribution xmlns="`+cfNS+`"/>`,
		)
	case opUpdateDomainAssociation:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><DomainAssociation xmlns="`+cfNS+`"/>`,
		)
	case opVerifyDNSConfiguration:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><VerifyDnsConfigurationResponse xmlns="`+cfNS+`"/>`,
		)

	// Monitoring subscription.
	case opCreateMonitoringSubscription:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><MonitoringSubscription xmlns="`+cfNS+`"/>`,
		)
	case opGetMonitoringSubscription:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><MonitoringSubscription xmlns="`+cfNS+`"/>`,
		)
	case opDeleteMonitoringSubscription:
		return noContent()

	// Disassociate WebACL.
	case opDisassociateDistributionWebACL, opDisassociateDistributionTenantWebACL:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><Distribution xmlns="`+cfNS+`"/>`,
		)

	// Field level encryption.
	case opCreateFieldLevelEncryptionConfig:
		return created("FieldLevelEncryption", "fle-stub")
	case opGetFieldLevelEncryption, opGetFieldLevelEncryptionConfig:
		return getStub("FieldLevelEncryption", "fle-stub")
	case opUpdateFieldLevelEncryptionConfig:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><FieldLevelEncryption xmlns="`+cfNS+`"/>`,
		)
	case opDeleteFieldLevelEncryptionConfig:
		return noContent()
	case opListFieldLevelEncryptionConfigs:
		return emptyList("FieldLevelEncryptionList")
	case opCreateFieldLevelEncryptionProfile:
		return created("FieldLevelEncryptionProfile", "flep-stub")
	case opGetFieldLevelEncryptionProfile, opGetFieldLevelEncryptionProfileConfig:
		return getStub("FieldLevelEncryptionProfile", "flep-stub")
	case opUpdateFieldLevelEncryptionProfile:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><FieldLevelEncryptionProfile xmlns="`+cfNS+`"/>`,
		)
	case opDeleteFieldLevelEncryptionProfile:
		return noContent()
	case opListFieldLevelEncryptionProfiles:
		return emptyList("FieldLevelEncryptionProfileList")

	// Key group.
	case opCreateKeyGroup:
		return created("KeyGroup", "kg-stub")
	case opGetKeyGroup, opGetKeyGroupConfig:
		return getStub("KeyGroup", "kg-stub")
	case opUpdateKeyGroup:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><KeyGroup xmlns="`+cfNS+`"/>`,
		)
	case opDeleteKeyGroup:
		return noContent()
	case opListKeyGroups:
		return emptyList("KeyGroupList")

	// Key value store.
	case opCreateKeyValueStore:
		return created("KeyValueStore", "kvs-stub")
	case opDescribeKeyValueStore:
		return getStub("KeyValueStore", "kvs-stub")
	case opUpdateKeyValueStore:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><KeyValueStore xmlns="`+cfNS+`"/>`,
		)
	case opDeleteKeyValueStore:
		return noContent()
	case opListKeyValueStores:
		return emptyList("KeyValueStoreList")

	// Public key.
	case opCreatePublicKey:
		return created("PublicKey", "pk-stub")
	case opGetPublicKey, opGetPublicKeyConfig:
		return getStub("PublicKey", "pk-stub")
	case opUpdatePublicKey:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><PublicKey xmlns="`+cfNS+`"/>`,
		)
	case opDeletePublicKey:
		return noContent()
	case opListPublicKeys:
		return emptyList("PublicKeyList")

	// Realtime log config.
	case opCreateRealtimeLogConfig:
		rlcARN := `arn:aws:cloudfront::000000000000:realtime-log-config/stub`
		rlcXML := `<?xml version="1.0" encoding="UTF-8"?><RealtimeLogConfig xmlns="` +
			cfNS + `"><ARN>` + rlcARN + `</ARN></RealtimeLogConfig>`
		return xmlResp(c, http.StatusCreated, rlcXML)
	case opGetRealtimeLogConfig:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><RealtimeLogConfig xmlns="`+cfNS+`"/>`,
		)
	case opUpdateRealtimeLogConfig:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><RealtimeLogConfig xmlns="`+cfNS+`"/>`,
		)
	case opDeleteRealtimeLogConfig:
		return noContent()
	case opListRealtimeLogConfigs:
		return emptyList("RealtimeLogConfigs")

	// Streaming distribution.
	case opCreateStreamingDistribution, opCreateStreamingDistributionWithTags:
		return created("StreamingDistribution", "sdist-stub")
	case opGetStreamingDistribution, opGetStreamingDistributionConfig:
		return getStub("StreamingDistribution", "sdist-stub")
	case opUpdateStreamingDistribution:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><StreamingDistribution xmlns="`+cfNS+`"/>`,
		)
	case opDeleteStreamingDistribution:
		return noContent()
	case opListStreamingDistributions:
		return emptyList("StreamingDistributionList")

	// Trust store.
	case opCreateTrustStore:
		return created("TrustStore", "ts-stub")
	case opGetTrustStore:
		return getStub("TrustStore", "ts-stub")
	case opUpdateTrustStore:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><TrustStore xmlns="`+cfNS+`"/>`,
		)
	case opDeleteTrustStore:
		return noContent()
	case opListTrustStores:
		return emptyList("TrustStoreList")

	// VPC origin.
	case opCreateVpcOrigin:
		return created("VpcOrigin", "vpc-origin-stub")
	case opGetVpcOrigin:
		return getStub("VpcOrigin", "vpc-origin-stub")
	case opUpdateVpcOrigin:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><VpcOrigin xmlns="`+cfNS+`"/>`,
		)
	case opDeleteVpcOrigin:
		return noContent()
	case opListVpcOrigins:
		return emptyList("VpcOriginList")

	// Anycast IP list.
	case opGetAnycastIPList:
		return getStub("AnycastIpList", "anycast-stub")
	case opUpdateAnycastIPList:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><AnycastIpList xmlns="`+cfNS+`"/>`,
		)
	case opDeleteAnycastIPList:
		return noContent()
	case opListAnycastIPLists:
		return emptyList("AnycastIpLists")

	// Connection function.
	case opDescribeConnectionFunction:
		return getStub("ConnectionFunction", "cf-stub")
	case opGetConnectionFunction:
		return getStub("ConnectionFunction", "cf-stub")
	case opUpdateConnectionFunction:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><ConnectionFunction xmlns="`+cfNS+`"/>`,
		)
	case opDeleteConnectionFunction:
		return noContent()
	case opListConnectionFunctions:
		return emptyList("ConnectionFunctionList")
	case opPublishConnectionFunction:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><ConnectionFunction xmlns="`+cfNS+`"/>`,
		)
	case opTestConnectionFunction:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><TestResult xmlns="`+cfNS+`"/>`,
		)

	// Connection group.
	case opGetConnectionGroup, opGetConnectionGroupByRoutingEndpoint:
		return getStub("ConnectionGroup", "cg-stub")
	case opUpdateConnectionGroup:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><ConnectionGroup xmlns="`+cfNS+`"/>`,
		)
	case opDeleteConnectionGroup:
		return noContent()
	case opListConnectionGroups:
		return emptyList("ConnectionGroupList")

	// Continuous deployment policy.
	case opGetContinuousDeploymentPolicy, opGetContinuousDeploymentPolicyConfig:
		return getStub("ContinuousDeploymentPolicy", "cdp-stub")
	case opUpdateContinuousDeploymentPolicy:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><ContinuousDeploymentPolicy xmlns="`+cfNS+`"/>`,
		)
	case opDeleteContinuousDeploymentPolicy:
		return noContent()
	case opListContinuousDeploymentPolicies:
		return emptyList("ContinuousDeploymentPolicyList")

	// Resource policy.
	case opGetResourcePolicy:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><ResourcePolicy xmlns="`+cfNS+`"><Policy>{}</Policy></ResourcePolicy>`,
		)
	case opPutResourcePolicy:
		return noContent()
	case opDeleteResourcePolicy:
		return noContent()

	// List distributions by X.
	case opListDistributionsByCachePolicyID, opListDistributionsByOriginRequestPol,
		opListDistributionsByResponseHeadersPol, opListDistributionsByWebACLID,
		opListDistributionsByKeyGroup, opListDistributionsByRealtimeLogConfig,
		opListDistributionsByVpcOriginID, opListDistributionsByAnycastIPListID,
		opListDistributionsByConnectionFunction, opListDistributionsByConnectionMode,
		opListDistributionsByTrustStore, opListDistributionsByOwnedResource:
		return emptyList("DistributionList")

	// Misc list operations.
	case opListConflictingAliases:
		return emptyList("ConflictingAliasesList")
	case opListDomainConflicts:
		return emptyList("DomainConflictList")

	// Invalidation for distribution tenant.
	case opCreateInvalidationForDistTenant:
		return created("Invalidation", "inv-stub")
	case opGetInvalidationForDistTenant:
		return getStub("Invalidation", "inv-stub")
	case opListInvalidationsForDistTenant:
		return emptyList("InvalidationList")

	// Managed certificate details.
	case opGetManagedCertificateDetails:
		return xmlResp(
			c,
			http.StatusOK,
			`<?xml version="1.0" encoding="UTF-8"?><ManagedCertificateDetails xmlns="`+cfNS+`"/>`,
		)

	default:
		return xmlResp(
			c,
			http.StatusNotFound,
			cfErrorXML("NoSuchOperation", "unknown operation: "+operation),
		)
	}
}

// notFoundCode returns the CloudFront error code for well-known not-found errors.
// The second return value is false when err is not a known not-found error.
func notFoundCode(err error) (string, bool) {
	switch {
	case errors.Is(err, ErrNotFound):
		return "NoSuchDistribution", true
	case errors.Is(err, ErrOAINotFound):
		return "NoSuchCloudFrontOriginAccessIdentity", true
	case errors.Is(err, ErrCachePolicyNotFound):
		return "NoSuchCachePolicy", true
	case errors.Is(err, ErrAnycastIPListNotFound):
		return "NoSuchAnycastIPList", true
	case errors.Is(err, ErrConnectionFunctionNotFound):
		return "NoSuchConnectionFunction", true
	case errors.Is(err, ErrConnectionGroupNotFound):
		return "NoSuchConnectionGroup", true
	case errors.Is(err, ErrContinuousDeploymentPolicyNotFound):
		return "NoSuchContinuousDeploymentPolicy", true
	case errors.Is(err, ErrInvalidationNotFound):
		return "NoSuchInvalidation", true
	case errors.Is(err, ErrOACNotFound):
		return "NoSuchOriginAccessControl", true
	case errors.Is(err, ErrResponseHeadersPolicyNotFound):
		return "NoSuchResponseHeadersPolicy", true
	case errors.Is(err, ErrFunctionNotFound):
		return "NoSuchFunctionExists", true
	case errors.Is(err, ErrOriginRequestPolicyNotFound):
		return "NoSuchOriginRequestPolicy", true
	}

	return "", false
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	if code, ok := notFoundCode(err); ok {
		return xmlResp(c, http.StatusNotFound, cfErrorXML(code, err.Error()))
	}

	switch {
	case errors.Is(err, ErrAlreadyExists):
		return xmlResp(c, http.StatusConflict, cfErrorXML("DistributionAlreadyExists", err.Error()))
	case errors.Is(err, ErrValidation):
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("InvalidArgument", err.Error()))
	default:
		return xmlResp(
			c,
			http.StatusInternalServerError,
			cfErrorXML("InternalFailure", err.Error()),
		)
	}
}

// --- Distribution handlers ---

func (h *Handler) handleCreateDistribution(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var cfg distributionConfigMinimal
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		return xmlResp(
			c,
			http.StatusBadRequest,
			cfErrorXML("MalformedXML", "invalid DistributionConfig XML"),
		)
	}

	d, createErr := h.Backend.CreateDistribution(
		cfg.CallerReference,
		cfg.Comment,
		cfg.Enabled,
		body,
	)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("Location", cfPathPrefix+"distribution/"+d.ID)
	c.Response().Header().Set("ETag", d.ETag)

	return xmlResp(c, http.StatusCreated, distributionResponseXML(d))
}

func (h *Handler) handleGetDistribution(c *echo.Context, id string) error {
	d, err := h.Backend.GetDistribution(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", d.ETag)

	return xmlResp(c, http.StatusOK, distributionResponseXML(d))
}

func (h *Handler) handleGetDistributionConfig(c *echo.Context, id string) error {
	d, err := h.Backend.GetDistribution(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", d.ETag)

	return xmlResp(c, http.StatusOK, string(d.RawConfig))
}

func (h *Handler) handleUpdateDistribution(c *echo.Context, id string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var cfg distributionConfigMinimal
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		return xmlResp(
			c,
			http.StatusBadRequest,
			cfErrorXML("MalformedXML", "invalid DistributionConfig XML"),
		)
	}

	current, getErr := h.Backend.GetDistribution(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current distribution config ETag",
			),
		)
	}

	d, updateErr := h.Backend.UpdateDistribution(id, cfg.Comment, cfg.Enabled, body)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", d.ETag)

	return xmlResp(c, http.StatusOK, distributionResponseXML(d))
}

func (h *Handler) handleDeleteDistribution(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetDistribution(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current distribution ETag",
			),
		)
	}

	if err := h.Backend.DeleteDistribution(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListDistributions(c *echo.Context) error {
	dists := h.Backend.ListDistributions()

	summaries := make([]distributionSummaryXML, 0, len(dists))
	for _, d := range dists {
		aliases := h.Backend.ListAliases(d.ID)
		s := distributionSummaryXML{
			ID:         d.ID,
			ARN:        d.ARN,
			Status:     d.Status,
			DomainName: d.DomainName,
			Comment:    d.Comment,
			Enabled:    d.Enabled,
		}
		s.Aliases.Quantity = len(aliases)
		s.ViewerCertificate.CloudFrontDefaultCertificate = true
		s.Restrictions.GeoRestriction.RestrictionType = "none"
		s.PriceClass = "PriceClass_All"
		s.HTTPVersion = "http2"
		summaries = append(summaries, s)
	}

	type distListXML struct {
		XMLName     xml.Name                 `xml:"DistributionList"`
		XMLNS       string                   `xml:"xmlns,attr"`
		Items       []distributionSummaryXML `xml:"Items>DistributionSummary"`
		MaxItems    int                      `xml:"MaxItems"`
		Quantity    int                      `xml:"Quantity"`
		IsTruncated bool                     `xml:"IsTruncated"`
	}

	list := distListXML{
		XMLNS:    cfNS,
		MaxItems: maxItems,
		Quantity: len(summaries),
		Items:    summaries,
	}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return xmlResp(
			c,
			http.StatusInternalServerError,
			cfErrorXML("InternalFailure", xmlErr.Error()),
		)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

// --- New operation handlers ---

type webACLAssociationXML struct {
	XMLName  xml.Name `xml:"WebACLAssociation"`
	WebACLID string   `xml:"WebACLId"`
}

type copyDistributionRequestXML struct {
	XMLName         xml.Name `xml:"CopyDistributionRequest"`
	CallerReference string   `xml:"CallerReference"`
}

type anycastIPListRequestXML struct {
	XMLName xml.Name `xml:"AnycastIPListRequest"`
	Name    string   `xml:"Name"`
	IPCount int32    `xml:"IPCount"`
}

type cachePolicyConfigXML struct {
	XMLName    xml.Name `xml:"CachePolicyConfig"`
	Name       string   `xml:"Name"`
	Comment    string   `xml:"Comment"`
	DefaultTTL int64    `xml:"DefaultTTL"`
	MaxTTL     int64    `xml:"MaxTTL"`
	MinTTL     int64    `xml:"MinTTL"`
}

type connectionFunctionRequestXML struct {
	XMLName xml.Name `xml:"CreateConnectionFunctionRequest"`
	Name    string   `xml:"Name"`
	Comment string   `xml:"Comment"`
}

type connectionGroupRequestXML struct {
	XMLName xml.Name `xml:"CreateConnectionGroupRequest"`
	Name    string   `xml:"Name"`
	Comment string   `xml:"Comment"`
}

type continuousDeploymentPolicyConfigXML struct {
	XMLName xml.Name `xml:"ContinuousDeploymentPolicyConfig"`
	Enabled bool     `xml:"Enabled"`
}

func (h *Handler) handleAssociateAlias(c *echo.Context, distributionID string) error {
	alias := c.Request().URL.Query().Get("Alias")

	if err := h.Backend.AssociateAlias(distributionID, alias); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleAssociateDistributionWebACL(c *echo.Context, distributionID string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req webACLAssociationXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid WebACLAssociation XML"),
			)
		}
	}

	if assocErr := h.Backend.AssociateDistributionWebACL(distributionID, req.WebACLID); assocErr != nil {
		return h.handleError(c, assocErr)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleAssociateDistributionTenantWebACL(c *echo.Context, tenantID string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req webACLAssociationXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid WebACLAssociation XML"),
			)
		}
	}

	if assocErr := h.Backend.AssociateDistributionTenantWebACL(tenantID, req.WebACLID); assocErr != nil {
		return h.handleError(c, assocErr)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleCopyDistribution(c *echo.Context, primaryDistID string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req copyDistributionRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid CopyDistributionRequest XML"),
			)
		}
	}

	d, copyErr := h.Backend.CopyDistribution(primaryDistID, req.CallerReference)
	if copyErr != nil {
		return h.handleError(c, copyErr)
	}

	c.Response().Header().Set("Location", cfPathPrefix+"distribution/"+d.ID)
	c.Response().Header().Set("ETag", d.ETag)

	return xmlResp(c, http.StatusCreated, distributionResponseXML(d))
}

func (h *Handler) handleCreateAnycastIPList(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req anycastIPListRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid AnycastIPListRequest XML"),
			)
		}
	}

	list, createErr := h.Backend.CreateAnycastIPList(req.Name, req.IPCount)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<AnycastIPList xmlns="%s">`+
		`<Id>%s</Id>`+
		`<ARN>%s</ARN>`+
		`<Name>%s</Name>`+
		`<Status>%s</Status>`+
		`<IPCount>%d</IPCount>`+
		`</AnycastIPList>`,
		cfNS, list.ID, list.ARN, list.Name, list.Status, list.IPCount)

	c.Response().Header().Set("Location", cfPathPrefix+"anycast-ip-list/"+list.ID)

	return xmlResp(c, http.StatusCreated, resp)
}

func (h *Handler) handleCreateCachePolicy(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req cachePolicyConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid CachePolicyConfig XML"),
			)
		}
	}

	policy, createErr := h.Backend.CreateCachePolicy(
		req.Name,
		req.Comment,
		req.DefaultTTL,
		req.MaxTTL,
		req.MinTTL,
	)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	resp := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<CachePolicy xmlns="%s">`+
			`<Id>%s</Id>`+
			`<CachePolicyConfig>`+
			`<Name>%s</Name>`+
			`<Comment>%s</Comment>`+
			`<DefaultTTL>%d</DefaultTTL>`+
			`<MaxTTL>%d</MaxTTL>`+
			`<MinTTL>%d</MinTTL>`+
			`</CachePolicyConfig>`+
			`</CachePolicy>`,
		cfNS,
		policy.ID,
		policy.Name,
		policy.Comment,
		policy.DefaultTTL,
		policy.MaxTTL,
		policy.MinTTL,
	)

	c.Response().Header().Set("ETag", policy.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"cache-policy/"+policy.ID)

	return xmlResp(c, http.StatusCreated, resp)
}

func (h *Handler) handleCreateConnectionFunction(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req connectionFunctionRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid CreateConnectionFunctionRequest XML"),
			)
		}
	}

	fn, createErr := h.Backend.CreateConnectionFunction(req.Name, req.Comment)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<ConnectionFunction xmlns="%s">`+
		`<ARN>%s</ARN>`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`</ConnectionFunction>`,
		cfNS, fn.ARN, fn.Name, fn.Comment)

	c.Response().Header().Set("Location", cfPathPrefix+"connection-function/"+fn.Name)

	return xmlResp(c, http.StatusCreated, resp)
}

func (h *Handler) handleCreateConnectionGroup(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req connectionGroupRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid CreateConnectionGroupRequest XML"),
			)
		}
	}

	group, createErr := h.Backend.CreateConnectionGroup(req.Name, req.Comment)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<ConnectionGroup xmlns="%s">`+
		`<Id>%s</Id>`+
		`<ARN>%s</ARN>`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`</ConnectionGroup>`,
		cfNS, group.ID, group.ARN, group.Name, group.Comment)

	c.Response().Header().Set("Location", cfPathPrefix+"connection-group/"+group.ID)

	return xmlResp(c, http.StatusCreated, resp)
}

func (h *Handler) handleCreateContinuousDeploymentPolicy(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req continuousDeploymentPolicyConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid ContinuousDeploymentPolicyConfig XML"))
		}
	}

	policy, createErr := h.Backend.CreateContinuousDeploymentPolicy(req.Enabled)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<ContinuousDeploymentPolicy xmlns="%s">`+
		`<Id>%s</Id>`+
		`<ContinuousDeploymentPolicyConfig>`+
		`<Enabled>%v</Enabled>`+
		`</ContinuousDeploymentPolicyConfig>`+
		`</ContinuousDeploymentPolicy>`,
		cfNS, policy.ID, policy.Enabled)

	c.Response().Header().Set("Location", cfPathPrefix+"continuous-deployment-policy/"+policy.ID)

	return xmlResp(c, http.StatusCreated, resp)
}

// --- OAI handlers ---

func (h *Handler) handleCreateOAI(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var cfg oaiConfigXML
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		return xmlResp(
			c,
			http.StatusBadRequest,
			cfErrorXML("MalformedXML", "invalid OAI config XML"),
		)
	}

	oai, createErr := h.Backend.CreateOAI(cfg.CallerReference, cfg.Comment)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("Location", cfPathPrefix+"origin-access-identity/cloudfront/"+oai.ID)
	c.Response().Header().Set("ETag", oai.ETag)

	resp := oaiResponseXML{
		XMLNS:             cfNS,
		ID:                oai.ID,
		S3CanonicalUserID: oai.S3CanonicalUserID,
		Config: oaiConfigXML{
			CallerReference: oai.CallerReference,
			Comment:         oai.Comment,
		},
	}

	out, xmlErr := xml.Marshal(resp)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusCreated, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleGetOAI(c *echo.Context, id string) error {
	oai, err := h.Backend.GetOAI(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", oai.ETag)

	resp := oaiResponseXML{
		XMLNS:             cfNS,
		ID:                oai.ID,
		S3CanonicalUserID: oai.S3CanonicalUserID,
		Config: oaiConfigXML{
			CallerReference: oai.CallerReference,
			Comment:         oai.Comment,
		},
	}

	out, xmlErr := xml.Marshal(resp)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleListOAIs(c *echo.Context) error {
	oais := h.Backend.ListOAIs()

	summaries := make([]oaiSummary, 0, len(oais))
	for _, oai := range oais {
		summaries = append(summaries, oaiSummary{
			ID:                oai.ID,
			S3CanonicalUserID: oai.S3CanonicalUserID,
			Comment:           oai.Comment,
		})
	}

	list := oaiList{
		XMLNS:    cfNS,
		MaxItems: maxItems,
		Quantity: len(summaries),
		Items:    summaries,
	}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleDeleteOAI(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetOAI(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current OAI ETag"))
	}

	if err := h.Backend.DeleteOAI(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetOAIConfig(c *echo.Context, id string) error {
	oai, err := h.Backend.GetOAI(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", oai.ETag)

	resp := oaiConfigXML{
		CallerReference: oai.CallerReference,
		Comment:         oai.Comment,
	}

	out, xmlErr := xml.Marshal(resp)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleUpdateOAI(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetOAI(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current OAI ETag"))
	}

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req oaiConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid CloudFrontOriginAccessIdentityConfig XML"),
			)
		}
	}

	oai, updateErr := h.Backend.UpdateOAI(id, req.Comment)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", oai.ETag)

	resp := oaiResponseXML{
		XMLNS:             cfNS,
		ID:                oai.ID,
		S3CanonicalUserID: oai.S3CanonicalUserID,
		Config: oaiConfigXML{
			CallerReference: oai.CallerReference,
			Comment:         oai.Comment,
		},
	}

	out, xmlErr := xml.Marshal(resp)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

// --- Tagging handlers ---

func (h *Handler) handleTagResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("Resource")

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var tags tagsXML
	if xmlErr := xml.Unmarshal(body, &tags); xmlErr != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "invalid Tags XML"))
	}

	kv := make(map[string]string, len(tags.Items))
	for _, t := range tags.Items {
		kv[t.Key] = t.Value
	}

	if tagErr := h.Backend.TagResource(resourceARN, kv); tagErr != nil {
		return h.handleError(c, tagErr)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("Resource")

	var keys []string

	// Keys may be in query params or body.
	keys = append(keys, c.Request().URL.Query()["TagKeys.Key"]...)

	if len(keys) == 0 {
		body, err := readBody(c)
		if err == nil && len(body) > 0 {
			var ub untagBody
			if xmlErr := xml.Unmarshal(body, &ub); xmlErr == nil {
				keys = ub.Keys
			}
		}
	}

	if untagErr := h.Backend.UntagResource(resourceARN, keys); untagErr != nil {
		return h.handleError(c, untagErr)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("Resource")

	kv, err := h.Backend.ListTags(resourceARN)
	if err != nil {
		return h.handleError(c, err)
	}

	// Sort tags by key for deterministic output.
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	items := make([]tagXML, 0, len(kv))
	for _, k := range keys {
		items = append(items, tagXML{Key: k, Value: kv[k]})
	}

	tags := tagsXML{XMLNS: cfNS, Items: items}

	type listTagsResp struct {
		XMLName xml.Name `xml:"ListTagsForResourceResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Tags    tagsXML  `xml:"Tags"`
	}

	resp := listTagsResp{XMLNS: cfNS, Tags: tags}

	out, xmlErr := xml.Marshal(resp)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

// --- Invalidation handlers ---

type invalidationBatchXML struct {
	XMLName         xml.Name        `xml:"InvalidationBatch"`
	CallerReference string          `xml:"CallerReference"`
	Paths           invalidPathsXML `xml:"Paths"`
}

type invalidPathsXML struct {
	Items    []string `xml:"Items>Path"`
	Quantity int      `xml:"Quantity"`
}

func (h *Handler) handleCreateInvalidation(c *echo.Context, distID string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(
			c,
			http.StatusInternalServerError,
			cfErrorXML("InternalFailure", err.Error()),
		)
	}

	var batch invalidationBatchXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &batch); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", xmlErr.Error()))
		}
	}

	inv, backendErr := h.Backend.CreateInvalidation(
		distID,
		batch.CallerReference,
		batch.Paths.Items,
	)
	if backendErr != nil {
		return h.handleError(c, backendErr)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<Invalidation xmlns="%s">`+
		`<Id>%s</Id>`+
		`<Status>%s</Status>`+
		`<CreateTime>%s</CreateTime>`+
		`</Invalidation>`,
		cfNS, inv.ID, inv.Status, inv.CreateTime.Format(time.RFC3339))

	c.Response().
		Header().
		Set("Location", cfPathPrefix+"distribution/"+distID+"/invalidation/"+inv.ID)

	return xmlResp(c, http.StatusCreated, resp)
}

func (h *Handler) handleListInvalidations(c *echo.Context, distID string) error {
	invs, err := h.Backend.ListInvalidations(distID)
	if err != nil {
		return h.handleError(c, err)
	}

	quantity := len(invs)
	var sb strings.Builder

	for _, inv := range invs {
		fmt.Fprintf(&sb,
			`<InvalidationSummary>`+
				`<Id>%s</Id>`+
				`<Status>%s</Status>`+
				`<CreateTime>%s</CreateTime>`+
				`</InvalidationSummary>`,
			inv.ID, inv.Status, inv.CreateTime.Format(time.RFC3339),
		)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<InvalidationList xmlns="%s">`+
		`<IsTruncated>false</IsTruncated>`+
		`<MaxItems>%d</MaxItems>`+
		`<Quantity>%d</Quantity>`+
		`<Items>%s</Items>`+
		`</InvalidationList>`,
		cfNS, maxItems, quantity, sb.String())

	return xmlResp(c, http.StatusOK, resp)
}

// handleGetInvalidation returns a specific CloudFront invalidation by ID.
func (h *Handler) handleGetInvalidation(c *echo.Context, distID string) error {
	// Extract invalidation ID from the URL path after /invalidation/.
	path := c.Request().URL.Path
	invID := ""

	if _, after, ok := strings.Cut(path, "/invalidation/"); ok {
		invID = after
		// Trim any trailing slashes or sub-paths.
		if slash := strings.Index(invID, "/"); slash >= 0 {
			invID = invID[:slash]
		}
	}

	if invID == "" {
		return xmlResp(c, http.StatusBadRequest,
			cfErrorXML("InvalidArgument", "invalidation ID is required"))
	}

	inv, err := h.Backend.GetInvalidation(distID, invID)
	if err != nil {
		return h.handleError(c, err)
	}

	var pathsSB strings.Builder

	for _, p := range inv.Paths {
		pathsSB.WriteString(`<Path>` + p + `</Path>`)
	}

	resp := fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<Invalidation xmlns="%s">`+
			`<Id>%s</Id>`+
			`<Status>%s</Status>`+
			`<CreateTime>%s</CreateTime>`+
			`<InvalidationBatch>`+
			`<CallerReference>%s</CallerReference>`+
			`<Paths>`+
			`<Quantity>%d</Quantity>`+
			`<Items>%s</Items>`+
			`</Paths>`+
			`</InvalidationBatch>`+
			`</Invalidation>`,
		cfNS,
		inv.ID,
		inv.Status,
		inv.CreateTime.Format(time.RFC3339),
		inv.CallerRef,
		len(inv.Paths),
		pathsSB.String(),
	)

	return xmlResp(c, http.StatusOK, resp)
}

// readBody reads the entire request body.
func readBody(c *echo.Context) ([]byte, error) {
	if c.Request().Body == nil {
		return []byte{}, nil
	}

	return io.ReadAll(c.Request().Body)
}

// --- Cache Policy additional handlers ---

func (h *Handler) handleGetCachePolicy(c *echo.Context, id string) error {
	p, err := h.Backend.GetCachePolicy(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", p.ETag)

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<CachePolicy xmlns="%s">`+
		`<Id>%s</Id>`+
		`<CachePolicyConfig>`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`<DefaultTTL>%d</DefaultTTL>`+
		`<MaxTTL>%d</MaxTTL>`+
		`<MinTTL>%d</MinTTL>`+
		`</CachePolicyConfig>`+
		`</CachePolicy>`,
		cfNS, p.ID, p.Name, p.Comment, p.DefaultTTL, p.MaxTTL, p.MinTTL)

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handleGetCachePolicyConfig(c *echo.Context, id string) error {
	p, err := h.Backend.GetCachePolicy(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", p.ETag)

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<CachePolicyConfig xmlns="%s">`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`<DefaultTTL>%d</DefaultTTL>`+
		`<MaxTTL>%d</MaxTTL>`+
		`<MinTTL>%d</MinTTL>`+
		`</CachePolicyConfig>`,
		cfNS, p.Name, p.Comment, p.DefaultTTL, p.MaxTTL, p.MinTTL)

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handleListCachePolicies(c *echo.Context) error {
	policies := h.Backend.ListCachePolicies()

	var sb strings.Builder

	for _, p := range policies {
		fmt.Fprintf(&sb,
			`<CachePolicySummary>`+
				`<CachePolicy>`+
				`<Id>%s</Id>`+
				`<CachePolicyConfig>`+
				`<Name>%s</Name>`+
				`<Comment>%s</Comment>`+
				`<DefaultTTL>%d</DefaultTTL>`+
				`<MaxTTL>%d</MaxTTL>`+
				`<MinTTL>%d</MinTTL>`+
				`</CachePolicyConfig>`+
				`</CachePolicy>`+
				`</CachePolicySummary>`,
			p.ID, p.Name, p.Comment, p.DefaultTTL, p.MaxTTL, p.MinTTL)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<CachePolicyList xmlns="%s">`+
		`<MaxItems>%d</MaxItems>`+
		`<Quantity>%d</Quantity>`+
		`<Items>%s</Items>`+
		`</CachePolicyList>`,
		cfNS, maxItems, len(policies), sb.String())

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handleUpdateCachePolicy(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetCachePolicy(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current cache policy ETag",
			),
		)
	}

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req cachePolicyConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid CachePolicyConfig XML"),
			)
		}
	}

	p, updateErr := h.Backend.UpdateCachePolicy(
		id,
		req.Name,
		req.Comment,
		req.DefaultTTL,
		req.MaxTTL,
		req.MinTTL,
	)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<CachePolicy xmlns="%s">`+
		`<Id>%s</Id>`+
		`<CachePolicyConfig>`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`<DefaultTTL>%d</DefaultTTL>`+
		`<MaxTTL>%d</MaxTTL>`+
		`<MinTTL>%d</MinTTL>`+
		`</CachePolicyConfig>`+
		`</CachePolicy>`,
		cfNS, p.ID, p.Name, p.Comment, p.DefaultTTL, p.MaxTTL, p.MinTTL)

	c.Response().Header().Set("ETag", p.ETag)

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handleDeleteCachePolicy(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetCachePolicy(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current cache policy ETag",
			),
		)
	}

	if err := h.Backend.DeleteCachePolicy(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Origin Access Control handlers ---

type oacConfigXML struct {
	XMLName         xml.Name `xml:"OriginAccessControlConfig"`
	Name            string   `xml:"Name"`
	Description     string   `xml:"Description"`
	OriginType      string   `xml:"OriginAccessControlOriginType"`
	SigningBehavior string   `xml:"SigningBehavior"`
	SigningProtocol string   `xml:"SigningProtocol"`
}

func (h *Handler) handleCreateOriginAccessControl(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req oacConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			const oacMsg = "invalid OriginAccessControlConfig XML"

			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", oacMsg))
		}
	}

	oac, createErr := h.Backend.CreateOriginAccessControl(
		req.Name, req.Description, req.OriginType, req.SigningBehavior, req.SigningProtocol,
	)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", oac.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"origin-access-control/"+oac.ID)

	return xmlResp(c, http.StatusCreated, oacResponseXML(oac))
}

func (h *Handler) handleGetOriginAccessControl(c *echo.Context, id string) error {
	oac, err := h.Backend.GetOriginAccessControl(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", oac.ETag)

	return xmlResp(c, http.StatusOK, oacResponseXML(oac))
}

func (h *Handler) handleGetOriginAccessControlConfig(c *echo.Context, id string) error {
	oac, err := h.Backend.GetOriginAccessControl(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", oac.ETag)

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<OriginAccessControlConfig xmlns="%s">`+
		`<Name>%s</Name>`+
		`<Description>%s</Description>`+
		`<OriginAccessControlOriginType>%s</OriginAccessControlOriginType>`+
		`<SigningBehavior>%s</SigningBehavior>`+
		`<SigningProtocol>%s</SigningProtocol>`+
		`</OriginAccessControlConfig>`,
		cfNS, oac.Name, oac.Description, oac.OriginType, oac.SigningBehavior, oac.SigningProtocol)

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handleListOriginAccessControls(c *echo.Context) error {
	oacs := h.Backend.ListOriginAccessControls()

	var sb strings.Builder

	for _, oac := range oacs {
		fmt.Fprintf(
			&sb,
			`<OriginAccessControlSummary>`+
				`<Id>%s</Id>`+
				`<Name>%s</Name>`+
				`<Description>%s</Description>`+
				`<OriginAccessControlOriginType>%s</OriginAccessControlOriginType>`+
				`<SigningBehavior>%s</SigningBehavior>`+
				`<SigningProtocol>%s</SigningProtocol>`+
				`</OriginAccessControlSummary>`,
			oac.ID,
			oac.Name,
			oac.Description,
			oac.OriginType,
			oac.SigningBehavior,
			oac.SigningProtocol,
		)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<OriginAccessControlList xmlns="%s">`+
		`<MaxItems>%d</MaxItems>`+
		`<Quantity>%d</Quantity>`+
		`<Items>%s</Items>`+
		`</OriginAccessControlList>`,
		cfNS, maxItems, len(oacs), sb.String())

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handleUpdateOriginAccessControl(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetOriginAccessControl(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current OAC ETag"))
	}

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req oacConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			const oacMsg = "invalid OriginAccessControlConfig XML"

			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", oacMsg))
		}
	}

	oac, updateErr := h.Backend.UpdateOriginAccessControl(
		id, req.Name, req.Description, req.OriginType, req.SigningBehavior, req.SigningProtocol,
	)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", oac.ETag)

	return xmlResp(c, http.StatusOK, oacResponseXML(oac))
}

func (h *Handler) handleDeleteOriginAccessControl(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetOriginAccessControl(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current OAC ETag"))
	}

	if err := h.Backend.DeleteOriginAccessControl(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func oacResponseXML(oac *OriginAccessControl) string {
	return fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<OriginAccessControl xmlns="%s">`+
			`<Id>%s</Id>`+
			`<OriginAccessControlConfig>`+
			`<Name>%s</Name>`+
			`<Description>%s</Description>`+
			`<OriginAccessControlOriginType>%s</OriginAccessControlOriginType>`+
			`<SigningBehavior>%s</SigningBehavior>`+
			`<SigningProtocol>%s</SigningProtocol>`+
			`</OriginAccessControlConfig>`+
			`</OriginAccessControl>`,
		cfNS,
		oac.ID,
		oac.Name,
		oac.Description,
		oac.OriginType,
		oac.SigningBehavior,
		oac.SigningProtocol,
	)
}

// --- Response Headers Policy handlers ---

type rhpConfigXML struct {
	XMLName xml.Name `xml:"ResponseHeadersPolicyConfig"`
	Name    string   `xml:"Name"`
	Comment string   `xml:"Comment"`
}

func (h *Handler) handleCreateResponseHeadersPolicy(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req rhpConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			const rhpMsg = "invalid ResponseHeadersPolicyConfig XML"

			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", rhpMsg))
		}
	}

	if req.Name == "" {
		return xmlResp(c, http.StatusBadRequest,
			cfErrorXML("InvalidArgument", "ResponseHeadersPolicyConfig Name is required"))
	}

	p, createErr := h.Backend.CreateResponseHeadersPolicy(req.Name, req.Comment)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", p.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"response-headers-policy/"+p.ID)

	return xmlResp(c, http.StatusCreated, rhpResponseXML(p))
}

func (h *Handler) handleGetResponseHeadersPolicy(c *echo.Context, id string) error {
	p, err := h.Backend.GetResponseHeadersPolicy(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", p.ETag)

	return xmlResp(c, http.StatusOK, rhpResponseXML(p))
}

func (h *Handler) handleGetResponseHeadersPolicyConfig(c *echo.Context, id string) error {
	p, err := h.Backend.GetResponseHeadersPolicy(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", p.ETag)

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<ResponseHeadersPolicyConfig xmlns="%s">`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`</ResponseHeadersPolicyConfig>`,
		cfNS, p.Name, p.Comment)

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handleListResponseHeadersPolicies(c *echo.Context) error {
	policies := h.Backend.ListResponseHeadersPolicies()

	var sb strings.Builder

	for _, p := range policies {
		fmt.Fprintf(&sb,
			`<ResponseHeadersPolicySummary>`+
				`<ResponseHeadersPolicy>`+
				`<Id>%s</Id>`+
				`<ResponseHeadersPolicyConfig>`+
				`<Name>%s</Name>`+
				`<Comment>%s</Comment>`+
				`</ResponseHeadersPolicyConfig>`+
				`</ResponseHeadersPolicy>`+
				`</ResponseHeadersPolicySummary>`,
			p.ID, p.Name, p.Comment)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<ResponseHeadersPolicyList xmlns="%s">`+
		`<MaxItems>%d</MaxItems>`+
		`<Quantity>%d</Quantity>`+
		`<Items>%s</Items>`+
		`</ResponseHeadersPolicyList>`,
		cfNS, maxItems, len(policies), sb.String())

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handleUpdateResponseHeadersPolicy(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetResponseHeadersPolicy(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current response headers policy ETag",
			),
		)
	}

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req rhpConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			const rhpMsg = "invalid ResponseHeadersPolicyConfig XML"

			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", rhpMsg))
		}
	}

	if req.Name == "" {
		req.Name = current.Name
	}

	p, updateErr := h.Backend.UpdateResponseHeadersPolicy(id, req.Name, req.Comment)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", p.ETag)

	return xmlResp(c, http.StatusOK, rhpResponseXML(p))
}

func (h *Handler) handleDeleteResponseHeadersPolicy(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetResponseHeadersPolicy(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current response headers policy ETag",
			),
		)
	}

	if err := h.Backend.DeleteResponseHeadersPolicy(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func rhpResponseXML(p *ResponseHeadersPolicy) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<ResponseHeadersPolicy xmlns="%s">`+
		`<Id>%s</Id>`+
		`<ResponseHeadersPolicyConfig>`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`</ResponseHeadersPolicyConfig>`+
		`</ResponseHeadersPolicy>`,
		cfNS, p.ID, p.Name, p.Comment)
}

// --- CloudFront Function handlers ---

type functionConfigXML struct {
	XMLName      xml.Name `xml:"FunctionConfig"`
	Comment      string   `xml:"Comment"`
	Runtime      string   `xml:"Runtime"`
	FunctionCode string   `xml:"FunctionCode"`
}

type createFunctionRequestXML struct {
	XMLName        xml.Name          `xml:"CreateFunctionRequest"`
	Name           string            `xml:"Name"`
	FunctionCode   string            `xml:"FunctionCode"`
	FunctionConfig functionConfigXML `xml:"FunctionConfig"`
}

func (h *Handler) handleCreateFunction(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req createFunctionRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid CreateFunctionRequest XML"),
			)
		}
	}

	code := req.FunctionCode
	if code == "" {
		code = req.FunctionConfig.FunctionCode
	}

	fn, createErr := h.Backend.CreateFunction(
		req.Name,
		req.FunctionConfig.Comment,
		req.FunctionConfig.Runtime,
		code,
	)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", fn.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"function/"+fn.Name)

	return xmlResp(c, http.StatusCreated, functionResponseXML(fn))
}

func (h *Handler) handleGetFunction(c *echo.Context, name string) error {
	fn, err := h.Backend.GetFunction(name)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", fn.ETag)

	return xmlResp(c, http.StatusOK, functionResponseXML(fn))
}

func (h *Handler) handleDescribeFunction(c *echo.Context, name string) error {
	fn, err := h.Backend.GetFunction(name)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", fn.ETag)

	return xmlResp(c, http.StatusOK, functionResponseXML(fn))
}

func (h *Handler) handleListFunctions(c *echo.Context) error {
	fns := h.Backend.ListFunctions()

	var sb strings.Builder

	for _, fn := range fns {
		fmt.Fprintf(&sb,
			`<FunctionSummary>`+
				`<Name>%s</Name>`+
				`<Status>%s</Status>`+
				`<FunctionConfig>`+
				`<Comment>%s</Comment>`+
				`<Runtime>%s</Runtime>`+
				`</FunctionConfig>`+
				`<FunctionMetadata>`+
				`<Stage>%s</Stage>`+
				`</FunctionMetadata>`+
				`</FunctionSummary>`,
			fn.Name, fn.Status, fn.Comment, fn.Runtime, fn.Status)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<FunctionList xmlns="%s">`+
		`<MaxItems>%d</MaxItems>`+
		`<Quantity>%d</Quantity>`+
		`<Items>%s</Items>`+
		`</FunctionList>`,
		cfNS, maxItems, len(fns), sb.String())

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handlePublishFunction(c *echo.Context, name string) error {
	current, getErr := h.Backend.GetFunction(name)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current function ETag",
			),
		)
	}

	fn, err := h.Backend.PublishFunction(name)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", fn.ETag)

	return xmlResp(c, http.StatusCreated, functionResponseXML(fn))
}

func (h *Handler) handleUpdateFunction(c *echo.Context, name string) error {
	current, getErr := h.Backend.GetFunction(name)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current function ETag",
			),
		)
	}

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req createFunctionRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid UpdateFunctionRequest XML"),
			)
		}
	}

	code := req.FunctionCode
	if code == "" {
		code = req.FunctionConfig.FunctionCode
	}

	fn, updateErr := h.Backend.UpdateFunction(
		name,
		req.FunctionConfig.Comment,
		req.FunctionConfig.Runtime,
		code,
	)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", fn.ETag)

	return xmlResp(c, http.StatusOK, functionResponseXML(fn))
}

func (h *Handler) handleDeleteFunction(c *echo.Context, name string) error {
	current, getErr := h.Backend.GetFunction(name)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current function ETag",
			),
		)
	}

	if err := h.Backend.DeleteFunction(name); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleTestFunction(c *echo.Context, name string) error {
	// TestFunction validates function logic; in-memory mock simply confirms it exists.
	_, err := h.Backend.GetFunction(name)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<TestResult xmlns="%s">`+
		`<FunctionSummary>`+
		`<Name>%s</Name>`+
		`</FunctionSummary>`+
		`<FunctionExecutionLogs></FunctionExecutionLogs>`+
		`<FunctionErrorMessage></FunctionErrorMessage>`+
		`<FunctionOutput></FunctionOutput>`+
		`</TestResult>`,
		cfNS, name)

	return xmlResp(c, http.StatusOK, resp)
}

func functionResponseXML(fn *Function) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<FunctionSummary xmlns="%s">`+
		`<Name>%s</Name>`+
		`<Status>%s</Status>`+
		`<FunctionConfig>`+
		`<Comment>%s</Comment>`+
		`<Runtime>%s</Runtime>`+
		`</FunctionConfig>`+
		`<FunctionMetadata>`+
		`<Stage>%s</Stage>`+
		`</FunctionMetadata>`+
		`</FunctionSummary>`,
		cfNS, fn.Name, fn.Status, fn.Comment, fn.Runtime, fn.Status)
}

// --- Origin Request Policy handlers ---

type orpConfigXML struct {
	XMLName xml.Name `xml:"OriginRequestPolicyConfig"`
	Name    string   `xml:"Name"`
	Comment string   `xml:"Comment"`
}

func (h *Handler) handleCreateOriginRequestPolicy(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if len(body) == 0 {
		return xmlResp(c, http.StatusBadRequest,
			cfErrorXML("MalformedXML", "OriginRequestPolicyConfig body is required"))
	}

	var req orpConfigXML
	if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
		const orpMsg = "invalid OriginRequestPolicyConfig XML"

		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", orpMsg))
	}

	if req.Name == "" {
		return xmlResp(c, http.StatusBadRequest,
			cfErrorXML("InvalidArgument", "OriginRequestPolicyConfig Name is required"))
	}

	p, createErr := h.Backend.CreateOriginRequestPolicy(req.Name, req.Comment)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", p.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"origin-request-policy/"+p.ID)

	return xmlResp(c, http.StatusCreated, orpResponseXML(p))
}

func (h *Handler) handleGetOriginRequestPolicy(c *echo.Context, id string) error {
	p, err := h.Backend.GetOriginRequestPolicy(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", p.ETag)

	return xmlResp(c, http.StatusOK, orpResponseXML(p))
}

func (h *Handler) handleGetOriginRequestPolicyConfig(c *echo.Context, id string) error {
	p, err := h.Backend.GetOriginRequestPolicy(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", p.ETag)

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<OriginRequestPolicyConfig xmlns="%s">`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`</OriginRequestPolicyConfig>`,
		cfNS, p.Name, p.Comment)

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handleListOriginRequestPolicies(c *echo.Context) error {
	policies := h.Backend.ListOriginRequestPolicies()

	var sb strings.Builder

	for _, p := range policies {
		fmt.Fprintf(&sb,
			`<OriginRequestPolicySummary>`+
				`<OriginRequestPolicy>`+
				`<Id>%s</Id>`+
				`<OriginRequestPolicyConfig>`+
				`<Name>%s</Name>`+
				`<Comment>%s</Comment>`+
				`</OriginRequestPolicyConfig>`+
				`</OriginRequestPolicy>`+
				`</OriginRequestPolicySummary>`,
			p.ID, p.Name, p.Comment)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<OriginRequestPolicyList xmlns="%s">`+
		`<MaxItems>%d</MaxItems>`+
		`<Quantity>%d</Quantity>`+
		`<Items>%s</Items>`+
		`</OriginRequestPolicyList>`,
		cfNS, maxItems, len(policies), sb.String())

	return xmlResp(c, http.StatusOK, resp)
}

func (h *Handler) handleUpdateOriginRequestPolicy(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetOriginRequestPolicy(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current origin request policy ETag",
			),
		)
	}

	var req orpConfigXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			const orpMsg = "invalid OriginRequestPolicyConfig XML"

			return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", orpMsg))
		}
	}

	if req.Name == "" {
		req.Name = current.Name
	}

	p, updateErr := h.Backend.UpdateOriginRequestPolicy(id, req.Name, req.Comment)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", p.ETag)

	return xmlResp(c, http.StatusOK, orpResponseXML(p))
}

func (h *Handler) handleDeleteOriginRequestPolicy(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetOriginRequestPolicy(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(
			c,
			http.StatusPreconditionFailed,
			cfErrorXML(
				"PreconditionFailed",
				"If-Match ETag did not match the current origin request policy ETag",
			),
		)
	}

	if err := h.Backend.DeleteOriginRequestPolicy(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func orpResponseXML(p *OriginRequestPolicy) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<OriginRequestPolicy xmlns="%s">`+
		`<Id>%s</Id>`+
		`<OriginRequestPolicyConfig>`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`</OriginRequestPolicyConfig>`+
		`</OriginRequestPolicy>`,
		cfNS, p.ID, p.Name, p.Comment)
}
