package cloudfront

import (
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
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
	opGetKVSKey                             = "GetKey"
	opPutKVSKey                             = "PutKey"
	opDeleteKVSKey                          = "DeleteKey"
	opListKVSKeys                           = "ListKeys"
	opUpdateKVSKeys                         = "UpdateKeys"
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
	opGetFunctionAssociations = "GetFunctionAssociations"
	opSetFunctionAssociations = "SetFunctionAssociations"
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

	// resourceParamWithTags is the Resource query-param value marking the *WithTags create variant.
	resourceParamWithTags = "WithTags"
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
		opGetFunctionAssociations,
		opSetFunctionAssociations,
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
		opDeleteKVSKey,
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
		opGetKVSKey,
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
		opListKVSKeys,
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
		opUpdateKVSKeys,
		opPutKVSKey,
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
	c.Response().Header().Set("X-Amz-Cf-Id", generateID())

	return c.XMLBlob(status, []byte(body))
}

// parseCFPath maps HTTP method + path to (operationName, resourceID).
//
// parseCFPath maps an HTTP method + URL path to a CloudFront operation name and resource identifier.
func parseCFPath(method, path, resourceParam string) (string, string) {
	suffix := strings.TrimPrefix(path, cfPathPrefix)

	if op, id := parseCFDistributionPath(method, suffix, resourceParam); op != "" {
		return op, id
	}

	if op, id := parseCFPolicyPath(method, suffix); op != "" {
		return op, id
	}

	if op, id := parseCFEncryptionKeyPath(method, suffix, resourceParam); op != "" {
		return op, id
	}

	if op, id := parseCFConnectionPath(method, suffix, resourceParam); op != "" {
		return op, id
	}

	if op, id := parseCFDistributionsByPath(method, suffix); op != "" {
		return op, id
	}

	return parseCFPathCore(method, suffix, resourceParam)
}

// parseCFDistributionPath routes distribution and distribution-tenant paths.
func parseCFDistributionPath(method, suffix, resourceParam string) (string, string) {
	if op, id := parseCFDistributionCorePath(method, suffix, resourceParam); op != "" {
		return op, id
	}

	return parseCFOAIPath(method, suffix)
}

// parseCFDistributionCorePath routes core distribution paths.
// parseCFDistributionCorePath routes core distribution paths.
func parseCFDistributionCorePath(method, suffix, resourceParam string) (string, string) {
	if !strings.HasPrefix(suffix, "distribution") {
		return "", ""
	}

	if op, id := parseCFDistributionRoot(method, suffix, resourceParam); op != "" {
		return op, id
	}

	if rest, ok := strings.CutPrefix(suffix, "distribution-tenant/"); ok {
		id := strings.TrimSuffix(rest, "/associate-web-acl")
		if strings.HasSuffix(suffix, "/associate-web-acl") && method == http.MethodPut {
			return opAssociateDistributionTenantWebACL, id
		}

		return "", ""
	}

	return parseCFDistributionSubPath(method, suffix)
}

// parseCFDistributionRoot handles the distribution collection and simple CRUD operations.
func parseCFDistributionRoot(method, suffix, resourceParam string) (string, string) {
	switch {
	case suffix == sfxDistribution && method == http.MethodPost && resourceParam != resourceParamWithTags:
		return opCreateDistribution, ""
	case suffix == sfxDistribution && method == http.MethodGet:
		return opListDistributions, ""
	case strings.HasPrefix(suffix, "distribution/") && !strings.Contains(strings.TrimPrefix(suffix, "distribution/"), "/"):
		id := strings.TrimPrefix(suffix, "distribution/")
		switch method {
		case http.MethodGet:
			return opGetDistribution, id
		case http.MethodDelete:
			return opDeleteDistribution, id
		}
	}

	return "", ""
}

// parseCFDistributionSubPath handles distribution sub-path operations (config, invalidation, etc.).
func parseCFDistributionSubPath(method, suffix string) (string, string) {
	if !strings.HasPrefix(suffix, "distribution/") {
		return "", ""
	}

	inner := strings.TrimPrefix(suffix, "distribution/")

	if op, id := parseCFDistributionConfigAndInvalidation(method, inner); op != "" {
		return op, id
	}

	return parseCFDistributionAssocAndAction(method, inner)
}

// parseCFDistributionConfigAndInvalidation handles config and invalidation sub-paths.
func parseCFDistributionConfigAndInvalidation(method, inner string) (string, string) {
	switch {
	case strings.HasSuffix(inner, "/config"):
		id := strings.TrimSuffix(inner, "/config")
		switch method {
		case http.MethodGet:
			return opGetDistributionConfig, id
		case http.MethodPut:
			return opUpdateDistribution, id
		}
	case strings.HasSuffix(inner, "/invalidation"):
		id := strings.TrimSuffix(inner, "/invalidation")
		switch method {
		case http.MethodPost:
			return opCreateInvalidation, id
		case http.MethodGet:
			return opListInvalidations, id
		}
	case strings.Contains(inner, "/invalidation/"):
		before, _, ok := strings.Cut(inner, "/invalidation/")
		if ok && method == http.MethodGet {
			return opGetInvalidation, before
		}
	}

	return "", ""
}

// parseCFDistributionAssocAndAction handles associate, copy, and other sub-paths.
func parseCFDistributionAssocAndAction(method, inner string) (string, string) {
	switch {
	case strings.HasSuffix(inner, "/associate-alias"):
		if method == http.MethodPut {
			return opAssociateAlias, strings.TrimSuffix(inner, "/associate-alias")
		}
	case strings.HasSuffix(inner, "/associate-web-acl"):
		if method == http.MethodPut {
			return opAssociateDistributionWebACL, strings.TrimSuffix(inner, "/associate-web-acl")
		}
	case strings.HasSuffix(inner, "/copy"):
		if method == http.MethodPost {
			return opCopyDistribution, strings.TrimSuffix(inner, "/copy")
		}
	}

	return "", ""
}

// parseCFOAIPath routes CloudFront Origin Access Identity paths.
func parseCFOAIPath(method, suffix string) (string, string) {
	const oaiPrefix = "origin-access-identity/cloudfront/"
	const oaiRoot = "origin-access-identity/cloudfront"

	switch {
	case suffix == oaiRoot && method == http.MethodPost:
		return opCreateCloudFrontOriginAccessIdentity, ""
	case suffix == oaiRoot && method == http.MethodGet:
		return opListCloudFrontOriginAccessIdentities, ""
	case strings.HasPrefix(suffix, oaiPrefix) && strings.HasSuffix(suffix, "/config"):
		id := strings.TrimSuffix(strings.TrimPrefix(suffix, oaiPrefix), "/config")
		switch method {
		case http.MethodGet:
			return opGetCloudFrontOriginAccessIdentityConfig, id
		case http.MethodPut:
			return opUpdateCloudFrontOAI, id
		}
	case strings.HasPrefix(suffix, oaiPrefix) && !strings.Contains(strings.TrimPrefix(suffix, oaiPrefix), "/"):
		id := strings.TrimPrefix(suffix, oaiPrefix)
		switch method {
		case http.MethodGet:
			return opGetCloudFrontOriginAccessIdentity, id
		case http.MethodPut:
			return opUpdateCloudFrontOAI, id
		case http.MethodDelete:
			return opDeleteCloudFrontOriginAccessIdentity, id
		}
	}

	return "", ""
}

// parseCFPolicyPath routes cache policy, OAC, response headers policy, function, and origin request policy paths.
func parseCFPolicyPath(method, suffix string) (string, string) {
	if op, id := parseCFResourcePath(method, suffix, "cache-policy",
		opCreateCachePolicy, opListCachePolicies, opGetCachePolicy, opUpdateCachePolicy, opDeleteCachePolicy,
		opGetCachePolicyConfig, ""); op != "" {
		return op, id
	}

	if op, id := parseCFOriginAccessControlPath(method, suffix); op != "" {
		return op, id
	}

	if op, id := parseCFResourcePath(
		method, suffix, "response-headers-policy",
		opCreateResponseHeadersPolicy, opListResponseHeadersPolicies,
		opGetResponseHeadersPolicy, opUpdateResponseHeadersPolicy,
		opDeleteResponseHeadersPolicy, opGetResponseHeadersPolicyConfig,
		opUpdateResponseHeadersPolicy,
	); op != "" {
		return op, id
	}

	if op, id := parseCFOriginRequestPolicyPath(method, suffix); op != "" {
		return op, id
	}

	return parseCFFunctionPath(method, suffix)
}

// parseCFOriginAccessControlPath routes OAC paths.
func parseCFOriginAccessControlPath(method, suffix string) (string, string) {
	const prefix = "origin-access-control/"
	const root = "origin-access-control"

	switch {
	case suffix == root && method == http.MethodPost:
		return opCreateOriginAccessControl, ""
	case suffix == root && method == http.MethodGet:
		return opListOriginAccessControls, ""
	case strings.HasPrefix(suffix, prefix) && strings.HasSuffix(suffix, "/config"):
		id := strings.TrimSuffix(strings.TrimPrefix(suffix, prefix), "/config")
		switch method {
		case http.MethodGet:
			return opGetOriginAccessControlConfig, id
		case http.MethodPut:
			return opUpdateOriginAccessControl, id
		}
	case strings.HasPrefix(suffix, prefix) && !strings.Contains(strings.TrimPrefix(suffix, prefix), "/"):
		id := strings.TrimPrefix(suffix, prefix)
		switch method {
		case http.MethodGet:
			return opGetOriginAccessControl, id
		case http.MethodDelete:
			return opDeleteOriginAccessControl, id
		}
	}

	return "", ""
}

// parseCFOriginRequestPolicyPath routes origin request policy paths.
func parseCFOriginRequestPolicyPath(method, suffix string) (string, string) {
	const prefix = "origin-request-policy/"
	const root = "origin-request-policy"

	switch {
	case suffix == root && method == http.MethodPost:
		return opCreateOriginRequestPolicy, ""
	case suffix == root && method == http.MethodGet:
		return opListOriginRequestPolicies, ""
	case strings.HasPrefix(suffix, prefix) && strings.HasSuffix(suffix, "/config"):
		id := strings.TrimSuffix(strings.TrimPrefix(suffix, prefix), "/config")
		switch method {
		case http.MethodGet:
			return opGetOriginRequestPolicyConfig, id
		case http.MethodPut:
			return opUpdateOriginRequestPolicy, id
		}
	case strings.HasPrefix(suffix, prefix) && !strings.Contains(strings.TrimPrefix(suffix, prefix), "/"):
		id := strings.TrimPrefix(suffix, prefix)
		switch method {
		case http.MethodGet:
			return opGetOriginRequestPolicy, id
		case http.MethodDelete:
			return opDeleteOriginRequestPolicy, id
		}
	}

	return "", ""
}

// parseCFFunctionPath routes CloudFront function paths.
func parseCFFunctionPath(method, suffix string) (string, string) {
	const prefix = "function/"
	const root = "function"

	if suffix == root {
		switch method {
		case http.MethodPost:
			return opCreateFunction, ""
		case http.MethodGet:
			return opListFunctions, ""
		}

		return "", ""
	}

	if !strings.HasPrefix(suffix, prefix) {
		return "", ""
	}

	inner := strings.TrimPrefix(suffix, prefix)
	switch {
	case strings.HasSuffix(inner, "/publish"):
		name := strings.TrimSuffix(inner, "/publish")
		if method == http.MethodPost {
			return opPublishFunction, name
		}
	case strings.HasSuffix(inner, "/describe"):
		name := strings.TrimSuffix(inner, "/describe")
		if method == http.MethodGet {
			return opDescribeFunction, name
		}
	case strings.HasSuffix(inner, "/test"):
		name := strings.TrimSuffix(inner, "/test")
		if method == http.MethodPost {
			return opTestFunction, name
		}
	case !strings.Contains(inner, "/"):
		switch method {
		case http.MethodGet:
			return opGetFunction, inner
		case http.MethodPut:
			return opUpdateFunction, inner
		case http.MethodDelete:
			return opDeleteFunction, inner
		}
	}

	return "", ""
}

// parseCFEncryptionKeyPath routes field-level encryption, key group, key value store, public key,
// realtime log config, streaming distribution, trust store, vpc origin, and anycast paths.
func parseCFEncryptionKeyPath(method, suffix, resourceParam string) (string, string) {
	if op, id := parseCFFieldLevelEncryptionPath(method, suffix); op != "" {
		return op, id
	}

	if op, id := parseCFKeyAndLogPath(method, suffix); op != "" {
		return op, id
	}

	return parseCFStreamingTrustVPCPath(method, suffix, resourceParam)
}

// parseCFFieldLevelEncryptionPath routes field-level encryption and profile paths.
func parseCFFieldLevelEncryptionPath(method, suffix string) (string, string) {
	if op, id := parseCFResourcePath(method, suffix, "field-level-encryption",
		opCreateFieldLevelEncryptionConfig, opListFieldLevelEncryptionConfigs,
		opGetFieldLevelEncryption, opUpdateFieldLevelEncryptionConfig, opDeleteFieldLevelEncryptionConfig,
		opGetFieldLevelEncryptionConfig, ""); op != "" {
		return op, id
	}

	return parseCFResourcePath(method, suffix, "field-level-encryption-profile",
		opCreateFieldLevelEncryptionProfile, opListFieldLevelEncryptionProfiles,
		opGetFieldLevelEncryptionProfile, opUpdateFieldLevelEncryptionProfile, opDeleteFieldLevelEncryptionProfile,
		opGetFieldLevelEncryptionProfileConfig, "")
}

// parseCFResourcePath is a generic helper for simple resource CRUD + optional config.
func parseCFResourcePath(method, suffix, resourceType,
	createOp, listOp, getOp, updateOp, deleteOp, getConfigOp, updateConfigOp string,
) (string, string) {
	prefix := resourceType + "/"

	if suffix == resourceType {
		switch method {
		case http.MethodPost:
			return createOp, ""
		case http.MethodGet:
			return listOp, ""
		}

		return "", ""
	}

	if !strings.HasPrefix(suffix, prefix) {
		return "", ""
	}

	inner := strings.TrimPrefix(suffix, prefix)
	if id, ok := strings.CutSuffix(inner, "/config"); ok {
		if method == http.MethodGet {
			return getConfigOp, id
		}

		if method == http.MethodPut && updateConfigOp != "" {
			return updateConfigOp, id
		}

		return "", ""
	}

	if !strings.Contains(inner, "/") {
		switch method {
		case http.MethodGet:
			return getOp, inner
		case http.MethodPut:
			return updateOp, inner
		case http.MethodDelete:
			return deleteOp, inner
		}
	}

	return "", ""
}

// parseCFKeyAndLogPath routes key group, key value store, public key, and realtime log config paths.
func parseCFKeyAndLogPath(method, suffix string) (string, string) {
	if op, id := parseCFResourcePath(method, suffix, "key-group",
		opCreateKeyGroup, opListKeyGroups, opGetKeyGroup, opUpdateKeyGroup, opDeleteKeyGroup,
		opGetKeyGroupConfig, ""); op != "" {
		return op, id
	}

	if op, id := parseCFResourcePath(
		method, suffix, "key-value-store",
		opCreateKeyValueStore, opListKeyValueStores,
		opDescribeKeyValueStore, opUpdateKeyValueStore,
		opDeleteKeyValueStore, "", "",
	); op != "" {
		return op, id
	}

	if op, kvsID, key := parseCFKVSDataPlanePath(method, suffix); op != "" {
		if key != "" {
			return op, kvsID + "/" + key
		}

		return op, kvsID
	}

	return parseCFPublicKeyRealtimePath(method, suffix)
}

// parseCFKVSDataPlanePath routes KVS data-plane key operations.
// Returns (op, kvsID, key) — key is empty for list/batch ops.
func parseCFKVSDataPlanePath(method, suffix string) (string, string, string) {
	const kvsPrefix = "key-value-store/"
	if !strings.HasPrefix(suffix, kvsPrefix) {
		return "", "", ""
	}

	rest := strings.TrimPrefix(suffix, kvsPrefix)
	kvsID, after, ok := strings.Cut(rest, "/keys")
	if !ok {
		return "", "", ""
	}

	if after == "" || after == "/" {
		switch method {
		case http.MethodGet:
			return opListKVSKeys, kvsID, ""
		case http.MethodPost:
			return opUpdateKVSKeys, kvsID, ""
		}

		return "", "", ""
	}

	if key, hasSlash := strings.CutPrefix(after, "/"); hasSlash {
		if key != "" && !strings.Contains(key, "/") {
			switch method {
			case http.MethodGet:
				return opGetKVSKey, kvsID, key
			case http.MethodPut:
				return opPutKVSKey, kvsID, key
			case http.MethodDelete:
				return opDeleteKVSKey, kvsID, key
			}
		}
	}

	return "", "", ""
}

// parseCFPublicKeyRealtimePath routes public key and realtime log config paths.
func parseCFPublicKeyRealtimePath(method, suffix string) (string, string) {
	if op, id := parseCFResourcePath(method, suffix, "public-key",
		opCreatePublicKey, opListPublicKeys, opGetPublicKey, opUpdatePublicKey, opDeletePublicKey,
		opGetPublicKeyConfig, ""); op != "" {
		return op, id
	}

	return parseCFResourcePath(
		method,
		suffix,
		"realtime-log-config",
		opCreateRealtimeLogConfig,
		opListRealtimeLogConfigs,
		opGetRealtimeLogConfig,
		opUpdateRealtimeLogConfig,
		opDeleteRealtimeLogConfig,
		"",
		"",
	)
}

// parseCFStreamingTrustVPCPath routes streaming distribution, trust store, vpc origin, and anycast paths.
func parseCFStreamingTrustVPCPath(method, suffix, resourceParam string) (string, string) {
	if op, id := parseCFStreamingDistributionPath(method, suffix, resourceParam); op != "" {
		return op, id
	}

	// The real SDK sends ListTrustStores as POST /trust-stores (plural resource, POST method),
	// distinct from the singular /trust-store resource used by the other trust store operations.
	if suffix == "trust-stores" && method == http.MethodPost {
		return opListTrustStores, ""
	}

	if op, id := parseCFResourcePath(method, suffix, "trust-store",
		opCreateTrustStore, opListTrustStores, opGetTrustStore, opUpdateTrustStore, opDeleteTrustStore,
		"", ""); op != "" {
		return op, id
	}

	if op, id := parseCFResourcePath(method, suffix, "vpc-origin",
		opCreateVpcOrigin, opListVpcOrigins, opGetVpcOrigin, opUpdateVpcOrigin, opDeleteVpcOrigin,
		"", ""); op != "" {
		return op, id
	}

	if op, id := parseCFResourcePath(method, suffix, "anycast-ip-list",
		"", opListAnycastIPLists, opGetAnycastIPList, opUpdateAnycastIPList, opDeleteAnycastIPList,
		"", ""); op != "" {
		return op, id
	}

	return "", ""
}

// parseCFStreamingDistributionPath routes streaming distribution paths, including the
// CreateStreamingDistributionWithTags variant (POST .../streaming-distribution?Resource=WithTags).
func parseCFStreamingDistributionPath(method, suffix, resourceParam string) (string, string) {
	const streamingDistributionResource = "streaming-distribution"

	if suffix == streamingDistributionResource && method == http.MethodPost {
		if resourceParam == resourceParamWithTags {
			return opCreateStreamingDistributionWithTags, ""
		}

		return opCreateStreamingDistribution, ""
	}

	return parseCFResourcePath(method, suffix, streamingDistributionResource,
		"", opListStreamingDistributions,
		opGetStreamingDistribution, opUpdateStreamingDistribution, opDeleteStreamingDistribution,
		opGetStreamingDistributionConfig, opUpdateStreamingDistribution)
}

// parseCFConnectionPath routes connection function, group, and continuous deployment policy paths.
func parseCFConnectionPath(method, suffix, resourceParam string) (string, string) {
	if op, id := parseCFConnectionFunctionPath(method, suffix); op != "" {
		return op, id
	}

	if op, id := parseCFConnectionGroupPath(method, suffix); op != "" {
		return op, id
	}

	return parseCFContinuousDeploymentPath(method, suffix, resourceParam)
}

// parseCFConnectionFunctionPath routes connection function paths.
func parseCFConnectionFunctionPath(method, suffix string) (string, string) {
	const prefix = "connection-function/"
	const root = "connection-function"

	switch {
	case suffix == root && method == http.MethodGet:
		return opListConnectionFunctions, ""
	case strings.HasPrefix(suffix, prefix) && strings.HasSuffix(suffix, "/describe"):
		id := strings.TrimSuffix(strings.TrimPrefix(suffix, prefix), "/describe")

		return opDescribeConnectionFunction, id
	case strings.HasPrefix(suffix, prefix) && strings.HasSuffix(suffix, "/publish"):
		id := strings.TrimSuffix(strings.TrimPrefix(suffix, prefix), "/publish")

		return opPublishConnectionFunction, id
	case strings.HasPrefix(suffix, prefix) && strings.HasSuffix(suffix, "/test"):
		id := strings.TrimSuffix(strings.TrimPrefix(suffix, prefix), "/test")

		return opTestConnectionFunction, id
	case strings.HasPrefix(suffix, prefix) && !strings.Contains(strings.TrimPrefix(suffix, prefix), "/"):
		id := strings.TrimPrefix(suffix, prefix)
		switch method {
		case http.MethodGet:
			return opGetConnectionFunction, id
		case http.MethodPut:
			return opUpdateConnectionFunction, id
		case http.MethodDelete:
			return opDeleteConnectionFunction, id
		}
	}

	return "", ""
}

// parseCFConnectionGroupPath routes connection group paths.
func parseCFConnectionGroupPath(method, suffix string) (string, string) {
	const prefix = "connection-group/"
	const root = "connection-group"

	switch {
	case suffix == root && method == http.MethodGet:
		return opListConnectionGroups, ""
	case suffix == "connection-group-by-routing-endpoint" && method == http.MethodGet:
		return opGetConnectionGroupByRoutingEndpoint, ""
	case strings.HasPrefix(suffix, prefix) && !strings.Contains(strings.TrimPrefix(suffix, prefix), "/"):
		id := strings.TrimPrefix(suffix, prefix)
		switch method {
		case http.MethodGet:
			return opGetConnectionGroup, id
		case http.MethodPut:
			return opUpdateConnectionGroup, id
		case http.MethodDelete:
			return opDeleteConnectionGroup, id
		}
	}

	return "", ""
}

// parseCFContinuousDeploymentPath routes continuous deployment policy and distribution-tenant extended paths.
func parseCFContinuousDeploymentPath(method, suffix, resourceParam string) (string, string) {
	const prefix = "continuous-deployment-policy/"
	const root = "continuous-deployment-policy"

	switch {
	case suffix == root && method == http.MethodGet:
		return opListContinuousDeploymentPolicies, ""
	case strings.HasPrefix(suffix, prefix) && strings.HasSuffix(suffix, "/config"):
		id := strings.TrimSuffix(strings.TrimPrefix(suffix, prefix), "/config")
		if method == http.MethodGet {
			return opGetContinuousDeploymentPolicyConfig, id
		}
	case strings.HasPrefix(suffix, prefix) && !strings.Contains(strings.TrimPrefix(suffix, prefix), "/"):
		id := strings.TrimPrefix(suffix, prefix)
		switch method {
		case http.MethodGet:
			return opGetContinuousDeploymentPolicy, id
		case http.MethodPut:
			return opUpdateContinuousDeploymentPolicy, id
		case http.MethodDelete:
			return opDeleteContinuousDeploymentPolicy, id
		}
	case suffix == "distribution-tenant-by-domain" && method == http.MethodGet:
		return opGetDistributionTenantByDomain, ""
	}

	_ = resourceParam

	return "", ""
}

// parseCFDistributionsByPath routes "distributions/by-*" paths.
func parseCFDistributionsByPath(method, suffix string) (string, string) {
	if method != http.MethodGet {
		return "", ""
	}

	switch {
	case strings.HasPrefix(suffix, "distributions/by-cache-policy-id/"):
		return opListDistributionsByCachePolicyID, strings.TrimPrefix(suffix, "distributions/by-cache-policy-id/")
	case strings.HasPrefix(suffix, "distributions/by-origin-request-policy-id/"):
		return opListDistributionsByOriginRequestPol, strings.TrimPrefix(
			suffix,
			"distributions/by-origin-request-policy-id/",
		)
	case strings.HasPrefix(suffix, "distributions/by-response-headers-policy-id/"):
		return opListDistributionsByResponseHeadersPol, strings.TrimPrefix(
			suffix,
			"distributions/by-response-headers-policy-id/",
		)
	case strings.HasPrefix(suffix, "distributions/by-web-acl-id/"):
		return opListDistributionsByWebACLID, strings.TrimPrefix(suffix, "distributions/by-web-acl-id/")
	case strings.HasPrefix(suffix, "distributions/by-key-group/"):
		return opListDistributionsByKeyGroup, strings.TrimPrefix(suffix, "distributions/by-key-group/")
	case strings.HasPrefix(suffix, "distributions/by-realtime-log-config"):
		return opListDistributionsByRealtimeLogConfig, ""
	case strings.HasPrefix(suffix, "distributions/by-vpc-origin-id/"):
		return opListDistributionsByVpcOriginID, strings.TrimPrefix(suffix, "distributions/by-vpc-origin-id/")
	case strings.HasPrefix(suffix, "distributions/by-anycast-ip-list-id/"):
		return opListDistributionsByAnycastIPListID, strings.TrimPrefix(suffix, "distributions/by-anycast-ip-list-id/")
	case strings.HasPrefix(suffix, "distributions/by-connection-function/"):
		return opListDistributionsByConnectionFunction, strings.TrimPrefix(
			suffix,
			"distributions/by-connection-function/",
		)
	case suffix == "distributions/by-connection-mode":
		return opListDistributionsByConnectionMode, ""
	case suffix == "distribution-tenants/by-customization":
		return opListDistributionTenantsByCustom, ""
	case strings.HasPrefix(suffix, "distributions/by-trust-store-id/"):
		return opListDistributionsByTrustStore, strings.TrimPrefix(suffix, "distributions/by-trust-store-id/")
	}

	return "", ""
}

// parseCFPathCore handles remaining distribution-tenant, create ops, tags, and resource policy paths.
func parseCFPathCore(method, suffix, resourceParam string) (string, string) {
	if op, id := parseCFCreateAndTagOps(method, suffix, resourceParam); op != "" {
		return op, id
	}

	if op, id := parseCFDistributionTenantOps(method, suffix); op != "" {
		return op, id
	}

	return parseCFDistributionExtPath(method, suffix)
}

// parseCFCreateAndTagOps handles create operations and tagging.
func parseCFCreateAndTagOps(method, suffix, resourceParam string) (string, string) {
	if op, id := parseCFCreateAndTagCoreOps(method, suffix, resourceParam); op != "" {
		return op, id
	}

	if suffix == sfxResourcePolicy {
		switch method {
		case http.MethodGet:
			return opGetResourcePolicy, resourceParam
		case http.MethodPost:
			return opPutResourcePolicy, resourceParam
		case http.MethodDelete:
			return opDeleteResourcePolicy, resourceParam
		}
	}

	return "", ""
}

// parseCFCreateAndTagCoreOps handles create ops and tagging (without resource policy).
// parseCFCreateAndTagCoreOps handles create ops and tagging (without resource policy).
func parseCFCreateAndTagCoreOps(method, suffix, resourceParam string) (string, string) {
	if op, id := parseCFTaggingOps(method, suffix, resourceParam); op != "" {
		return op, id
	}

	return parseCFCreateOps(method, suffix, resourceParam)
}

// parseCFTaggingOps handles tagging and distribution-with-tags creation.
func parseCFTaggingOps(method, suffix, resourceParam string) (string, string) {
	if suffix == "tagging" {
		switch method {
		case http.MethodGet:
			return opListTagsForResource, resourceParam
		case http.MethodPost:
			return opTagResource, resourceParam
		case http.MethodDelete:
			return opUntagResource, resourceParam
		}
	}

	if suffix == sfxDistribution && method == http.MethodPost && resourceParam == resourceParamWithTags {
		return opCreateDistributionWithTags, ""
	}

	return "", ""
}

// parseCFCreateOps handles simple POST create operations.
func parseCFCreateOps(method, suffix, _ string) (string, string) {
	if method != http.MethodPost {
		return "", ""
	}

	switch suffix {
	case "anycast-ip-list":
		return opCreateAnycastIPList, ""
	case "connection-function":
		return opCreateConnectionFunction, ""
	case "connection-group":
		return opCreateConnectionGroup, ""
	case "continuous-deployment-policy":
		return opCreateContinuousDeploymentPolicy, ""
	}

	return "", ""
}

// parseCFDistributionTenantOps handles distribution-tenant CRUD operations.
func parseCFDistributionTenantOps(method, suffix string) (string, string) {
	// The real SDK sends ListDistributionTenants as POST /distribution-tenants (plural
	// resource, POST method), distinct from the singular /distribution-tenant resource used
	// by Create/Get/Update/Delete.
	if suffix == "distribution-tenants" && method == http.MethodPost {
		return opListDistributionTenants, ""
	}

	if suffix == "distribution-tenant" {
		switch method {
		case http.MethodPost:
			return opCreateDistributionTenant, ""
		case http.MethodGet:
			return opListDistributionTenants, ""
		}
	}

	if strings.HasPrefix(suffix, "distribution-tenant/") &&
		!strings.Contains(strings.TrimPrefix(suffix, "distribution-tenant/"), "/") {
		id := strings.TrimPrefix(suffix, "distribution-tenant/")
		switch method {
		case http.MethodGet:
			return opGetDistributionTenant, id
		case http.MethodPut:
			return opUpdateDistributionTenant, id
		case http.MethodDelete:
			return opDeleteDistributionTenant, id
		}
	}

	return "", ""
}

// parseCFDistributionExtPath handles extended distribution paths (monitoring, staging, tenant invalidations, etc.).
func parseCFDistributionExtPath(method, suffix string) (string, string) {
	if strings.HasPrefix(suffix, "distribution/") {
		if op, id := parseCFDistributionMonitoringOps(method, suffix); op != "" {
			return op, id
		}
	}

	if strings.HasPrefix(suffix, "distribution-tenant/") {
		if op, id := parseCFDistributionTenantExtOps(method, suffix); op != "" {
			return op, id
		}
	}

	return parseCFMiscPath(method, suffix)
}

// parseCFDistributionMonitoringOps handles distribution monitoring, staging, and disassociate paths.
func parseCFDistributionMonitoringOps(method, suffix string) (string, string) {
	inner := strings.TrimPrefix(suffix, "distribution/")

	switch {
	case strings.HasSuffix(inner, "/monitoring-subscription"):
		id := strings.TrimSuffix(inner, "/monitoring-subscription")
		switch method {
		case http.MethodPost:
			return opCreateMonitoringSubscription, id
		case http.MethodGet:
			return opGetMonitoringSubscription, id
		case http.MethodDelete:
			return opDeleteMonitoringSubscription, id
		}
	case strings.HasSuffix(inner, "/staging") && method == http.MethodPut:
		return opUpdateDistributionWithStagingConfig, strings.TrimSuffix(inner, "/staging")
	case strings.HasSuffix(inner, "/disassociate-web-acl") && method == http.MethodPut:
		return opDisassociateDistributionWebACL, strings.TrimSuffix(inner, "/disassociate-web-acl")
	case strings.Contains(inner, "/list-by-") && method == http.MethodGet:
		return opListDistributionsByOwnedResource, ""
	}

	return "", ""
}

// parseCFDistributionTenantExtOps handles distribution-tenant extended paths.
func parseCFDistributionTenantExtOps(method, suffix string) (string, string) {
	if strings.Contains(suffix, "/invalidation") {
		return parseCFDistributionTenantInvalidation(method, suffix)
	}

	if strings.HasSuffix(suffix, "/managed-certificate-details") {
		id := strings.TrimSuffix(strings.TrimPrefix(suffix, "distribution-tenant/"), "/managed-certificate-details")

		return opGetManagedCertificateDetails, id
	}

	return "", ""
}

// parseCFDistributionTenantInvalidation handles distribution-tenant invalidation paths.
func parseCFDistributionTenantInvalidation(method, suffix string) (string, string) {
	inner := strings.TrimPrefix(suffix, "distribution-tenant/")
	switch {
	case strings.HasSuffix(suffix, "/invalidation") && method == http.MethodPost:
		return opCreateInvalidationForDistTenant, strings.TrimSuffix(inner, "/invalidation")
	case strings.HasSuffix(suffix, "/invalidation") && method == http.MethodGet:
		return opListInvalidationsForDistTenant, strings.TrimSuffix(inner, "/invalidation")
	}

	if before, _, ok := strings.Cut(inner, "/invalidation/"); ok && method == http.MethodGet {
		return opGetInvalidationForDistTenant, before
	}

	return "", ""
}

// parseCFMiscPath handles miscellaneous CloudFront paths.
func parseCFMiscPath(method, suffix string) (string, string) {
	if op := parseCFMiscPathSimple(method, suffix); op != "" {
		return op, ""
	}

	return parseCFMiscPathByDistribution(method, suffix)
}

// parseCFMiscPathSimple handles simple exact-match miscellaneous paths.
func parseCFMiscPathSimple(method, suffix string) string {
	type exactMatch struct {
		suffix string
		method string
		op     string
	}

	exact := []exactMatch{
		{"conflicting-alias", http.MethodGet, opListConflictingAliases},
		{"domain-conflict", http.MethodPost, opListDomainConflicts},
		{"domain-association", http.MethodPost, opUpdateDomainAssociation},
		{"verify-dns-configuration", http.MethodPost, opVerifyDNSConfiguration},
		{"distributions/by-connection-mode", http.MethodGet, opListDistributionsByConnectionMode},
		{"distribution-tenants/by-customization", http.MethodGet, opListDistributionTenantsByCustom},
		{"connection-group-by-routing-endpoint", http.MethodGet, opGetConnectionGroupByRoutingEndpoint},
		{"distribution-tenant-by-domain", http.MethodGet, opGetDistributionTenantByDomain},
	}

	for _, m := range exact {
		if suffix == m.suffix && method == m.method {
			return m.op
		}
	}

	if strings.HasPrefix(suffix, "distributions/by-trust-store-id/") {
		return opListDistributionsByTrustStore
	}

	if strings.HasPrefix(suffix, "distributions/by-realtime-log-config") && method == http.MethodGet {
		return opListDistributionsByRealtimeLogConfig
	}

	return ""
}

// parseCFMiscPathByDistribution handles prefix-based distribution listing paths.
func parseCFMiscPathByDistribution(method, suffix string) (string, string) {
	type prefixOp struct {
		prefix string
		op     string
	}

	prefixes := []prefixOp{
		{"distributions/by-cache-policy-id/", opListDistributionsByCachePolicyID},
		{"distributions/by-origin-request-policy-id/", opListDistributionsByOriginRequestPol},
		{"distributions/by-response-headers-policy-id/", opListDistributionsByResponseHeadersPol},
		{"distributions/by-web-acl-id/", opListDistributionsByWebACLID},
		{"distributions/by-key-group/", opListDistributionsByKeyGroup},
		{"distributions/by-vpc-origin-id/", opListDistributionsByVpcOriginID},
		{"distributions/by-anycast-ip-list-id/", opListDistributionsByAnycastIPListID},
		{"distributions/by-connection-function/", opListDistributionsByConnectionFunction},
	}

	if method == http.MethodGet {
		for _, p := range prefixes {
			if after, ok := strings.CutPrefix(suffix, p.prefix); ok {
				return p.op, after
			}
		}
	}

	if strings.HasPrefix(suffix, "distribution-tenant/") && strings.HasSuffix(suffix, "/managed-certificate-details") {
		id := strings.TrimPrefix(suffix, "distribution-tenant/")
		id = strings.TrimSuffix(id, "/managed-certificate-details")

		return opGetManagedCertificateDetails, id
	}

	if strings.HasPrefix(suffix, "distribution/") && strings.HasSuffix(suffix, "/function-associations") {
		id := strings.TrimPrefix(suffix, "distribution/")
		id = strings.TrimSuffix(id, "/function-associations")

		switch method {
		case http.MethodGet:
			return opGetFunctionAssociations, id
		case http.MethodPut:
			return opSetFunctionAssociations, id
		}
	}

	return "Unknown", ""
}

// --- Incoming XML structs ---

type distributionConfigMinimal struct {
	CallerReference string `xml:"CallerReference"`
	Comment         string `xml:"Comment"`
	PriceClass      string `xml:"PriceClass"`
	HTTPVersion     string `xml:"HttpVersion"`
	IsIPV6Enabled   bool   `xml:"IsIPV6Enabled"`
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
	Status           string `xml:"Status"`
	LastModifiedTime string `xml:"LastModifiedTime"`
	DomainName       string `xml:"DomainName"`
	Comment          string `xml:"Comment"`
	ARN              string `xml:"ARN"`
	ID               string `xml:"Id"`
	PriceClass       string `xml:"PriceClass"`
	HTTPVersion      string `xml:"HttpVersion"`
	Restrictions     struct {
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
func distributionResponseXML(d *Distribution, inProgressCount int) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<Distribution xmlns="%s">`+
		`<Id>%s</Id>`+
		`<ARN>%s</ARN>`+
		`<Status>%s</Status>`+
		`<LastModifiedTime>%s</LastModifiedTime>`+
		`<DomainName>%s</DomainName>`+
		`<InProgressInvalidationBatches>%d</InProgressInvalidationBatches>`+
		`%s`+
		`</Distribution>`,
		cfNS, d.ID, d.ARN, d.Status, d.LastModifiedTime, d.DomainName, inProgressCount, string(d.RawConfig))
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
	if err := h.dispatchCreateCore(c, operation); !errors.Is(err, errNotDispatched) {
		return err
	}

	return h.dispatchCreateExtended(c, operation)
}

func (h *Handler) dispatchCreateCore(c *echo.Context, operation string) error {
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

func (h *Handler) dispatchCreateExtended(c *echo.Context, operation string) error {
	switch operation {
	case opCreateFieldLevelEncryptionConfig:
		return h.handleCreateFieldLevelEncryption(c)
	case opCreateFieldLevelEncryptionProfile:
		return h.handleCreateFieldLevelEncryptionProfile(c)
	case opCreatePublicKey:
		return h.handleCreatePublicKey(c)
	case opCreateKeyGroup:
		return h.handleCreateKeyGroup(c)
	case opCreateRealtimeLogConfig:
		return h.handleCreateRealtimeLogConfig(c)
	case opCreateKeyValueStore:
		return h.handleCreateKeyValueStore(c)
	case opCreateVpcOrigin:
		return h.handleCreateVpcOrigin(c)
	case opCreateStreamingDistribution:
		return h.handleCreateStreamingDistribution(c)
	case opCreateStreamingDistributionWithTags:
		return h.handleCreateStreamingDistributionWithTags(c)
	default:

		return errNotDispatched
	}
}

// dispatchGetOrMutate handles all GET, PUT, and DELETE operations.
//

func (h *Handler) dispatchGetOrMutate(c *echo.Context, operation, resource string) error {
	if err := h.dispatchGetOrMutateCoreOps(c, operation, resource); !errors.Is(err, errNotDispatched) {
		return err
	}

	if err := h.dispatchGetOrMutateEncryptionOps(c, operation, resource); !errors.Is(err, errNotDispatched) {
		return err
	}

	return h.dispatchGetOrMutateExtOps(c, operation, resource)
}

// dispatchGetOrMutateCoreOps handles core GET, DELETE, and UPDATE operations.
func (h *Handler) dispatchGetOrMutateCoreOps(c *echo.Context, operation, resource string) error {
	if err := h.dispatchGetDistributionAndCachePolicyOps(c, operation, resource); !errors.Is(err, errNotDispatched) {
		return err
	}

	return h.dispatchGetIdentityAndPolicyDeleteOps(c, operation, resource)
}

// dispatchGetDistributionAndCachePolicyOps handles get ops for distributions, cache policy, functions, OAI, and OAC.
func (h *Handler) dispatchGetDistributionAndCachePolicyOps(
	c *echo.Context, operation, resource string,
) error {
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
	}

	return errNotDispatched
}

// dispatchGetIdentityAndPolicyDeleteOps handles get ops for OAI config / policies and all core delete ops.
func (h *Handler) dispatchGetIdentityAndPolicyDeleteOps(
	c *echo.Context, operation, resource string,
) error {
	switch operation {
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
	}

	return errNotDispatched
}

// dispatchGetOrMutateEncryptionOps handles OAI, policy, and encryption operations.
func (h *Handler) dispatchGetOrMutateEncryptionOps(c *echo.Context, operation, resource string) error {
	if err := h.dispatchUpdateDeletePolicyAndOAIOps(c, operation, resource); !errors.Is(err, errNotDispatched) {
		return err
	}

	return h.dispatchFieldLevelEncryptionOps(c, operation, resource)
}

// dispatchUpdateDeletePolicyAndOAIOps handles update/delete operations for OAI, policies, distribution, and function.
func (h *Handler) dispatchUpdateDeletePolicyAndOAIOps(
	c *echo.Context, operation, resource string,
) error {
	switch operation {
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
	}

	return errNotDispatched
}

// dispatchFieldLevelEncryptionOps handles field-level encryption config and profile operations.
func (h *Handler) dispatchFieldLevelEncryptionOps(
	c *echo.Context, operation, resource string,
) error {
	switch operation {
	case opGetFieldLevelEncryption:
		return h.handleGetFieldLevelEncryption(c, resource)
	case opGetFieldLevelEncryptionConfig:
		return h.handleGetFieldLevelEncryptionConfig(c, resource)
	case opUpdateFieldLevelEncryptionConfig:
		return h.handleUpdateFieldLevelEncryption(c, resource)
	case opDeleteFieldLevelEncryptionConfig:
		return h.handleDeleteFieldLevelEncryption(c, resource)
	case opGetFieldLevelEncryptionProfile:
		return h.handleGetFieldLevelEncryptionProfile(c, resource)
	case opGetFieldLevelEncryptionProfileConfig:
		return h.handleGetFieldLevelEncryptionProfileConfig(c, resource)
	case opUpdateFieldLevelEncryptionProfile:
		return h.handleUpdateFieldLevelEncryptionProfile(c, resource)
	case opDeleteFieldLevelEncryptionProfile:
		return h.handleDeleteFieldLevelEncryptionProfile(c, resource)
	}

	return errNotDispatched
}

// dispatchGetOrMutateExtOps handles public key, key group, log config, key value store, VPC origin,
// and streaming distribution operations.
func (h *Handler) dispatchGetOrMutateExtOps(c *echo.Context, operation, resource string) error {
	if err := h.dispatchPublicKeyAndGroupOps(c, operation, resource); !errors.Is(err, errNotDispatched) {
		return err
	}

	if err := h.dispatchLogStoreVPCOps(c, operation, resource); !errors.Is(err, errNotDispatched) {
		return err
	}

	return h.dispatchStreamingDistributionOps(c, operation, resource)
}

// dispatchStreamingDistributionOps handles get, update, and delete operations for streaming distributions.
func (h *Handler) dispatchStreamingDistributionOps(c *echo.Context, operation, resource string) error {
	switch operation {
	case opGetStreamingDistribution:
		return h.handleGetStreamingDistribution(c, resource)
	case opGetStreamingDistributionConfig:
		return h.handleGetStreamingDistributionConfig(c, resource)
	case opUpdateStreamingDistribution:
		return h.handleUpdateStreamingDistribution(c, resource)
	case opDeleteStreamingDistribution:
		return h.handleDeleteStreamingDistribution(c, resource)
	default:

		return errNotDispatched
	}
}

// dispatchPublicKeyAndGroupOps handles public key and key group operations.
func (h *Handler) dispatchPublicKeyAndGroupOps(c *echo.Context, operation, resource string) error {
	switch operation {
	case opGetPublicKey:
		return h.handleGetPublicKey(c, resource)
	case opGetPublicKeyConfig:
		return h.handleGetPublicKeyConfig(c, resource)
	case opUpdatePublicKey:
		return h.handleUpdatePublicKey(c, resource)
	case opDeletePublicKey:
		return h.handleDeletePublicKey(c, resource)
	case opGetKeyGroup:
		return h.handleGetKeyGroup(c, resource)
	case opGetKeyGroupConfig:
		return h.handleGetKeyGroupConfig(c, resource)
	case opUpdateKeyGroup:
		return h.handleUpdateKeyGroup(c, resource)
	case opDeleteKeyGroup:
		return h.handleDeleteKeyGroup(c, resource)
	}

	return errNotDispatched
}

// dispatchLogStoreVPCOps handles realtime log config, key value store, and VPC origin operations.
func (h *Handler) dispatchLogStoreVPCOps(c *echo.Context, operation, resource string) error {
	if err := h.dispatchKVSOps(c, operation, resource); !errors.Is(err, errNotDispatched) {
		return err
	}

	switch operation {
	case opGetRealtimeLogConfig:
		return h.handleGetRealtimeLogConfig(c, resource)
	case opUpdateRealtimeLogConfig:
		return h.handleUpdateRealtimeLogConfig(c, resource)
	case opDeleteRealtimeLogConfig:
		return h.handleDeleteRealtimeLogConfig(c, resource)
	case opGetVpcOrigin:
		return h.handleGetVpcOrigin(c, resource)
	case opUpdateVpcOrigin:
		return h.handleUpdateVpcOrigin(c, resource)
	case opDeleteVpcOrigin:
		return h.handleDeleteVpcOrigin(c, resource)
	case opGetContinuousDeploymentPolicy, opGetContinuousDeploymentPolicyConfig:
		return h.handleGetContinuousDeploymentPolicy(c, resource)
	case opUpdateContinuousDeploymentPolicy:
		return h.handleUpdateContinuousDeploymentPolicy(c, resource)
	case opDeleteContinuousDeploymentPolicy:
		return h.handleDeleteContinuousDeploymentPolicy(c, resource)
	default:

		return errNotDispatched
	}
}

// dispatchKVSOps handles KVS control-plane and data-plane operations.
func (h *Handler) dispatchKVSOps(c *echo.Context, operation, resource string) error {
	switch operation {
	case opDescribeKeyValueStore:
		return h.handleGetKeyValueStore(c, resource)
	case opUpdateKeyValueStore:
		return h.handleUpdateKeyValueStore(c, resource)
	case opDeleteKeyValueStore:
		return h.handleDeleteKeyValueStore(c, resource)
	case opGetKVSKey:
		kvsID, key, _ := strings.Cut(resource, "/")

		return h.handleGetKVSKey(c, kvsID, key)
	case opPutKVSKey:
		kvsID, key, _ := strings.Cut(resource, "/")

		return h.handlePutKVSKey(c, kvsID, key)
	case opDeleteKVSKey:
		kvsID, key, _ := strings.Cut(resource, "/")

		return h.handleDeleteKVSKey(c, kvsID, key)
	default:

		return errNotDispatched
	}
}

func (h *Handler) dispatchList(c *echo.Context, operation, resource string) error {
	if err := h.dispatchListCore(c, operation, resource); !errors.Is(err, errNotDispatched) {
		return err
	}

	return h.dispatchListExtended(c, operation)
}

func (h *Handler) dispatchListCore(c *echo.Context, operation, resource string) error {
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
	case opListKVSKeys:
		return h.handleListKVSKeys(c, resource)
	default:

		return errNotDispatched
	}
}

func (h *Handler) dispatchListExtended(c *echo.Context, operation string) error {
	switch operation {
	case opListFieldLevelEncryptionConfigs:
		return h.handleListFieldLevelEncryptions(c)
	case opListFieldLevelEncryptionProfiles:
		return h.handleListFieldLevelEncryptionProfiles(c)
	case opListPublicKeys:
		return h.handleListPublicKeys(c)
	case opListKeyGroups:
		return h.handleListKeyGroups(c)
	case opListRealtimeLogConfigs:
		return h.handleListRealtimeLogConfigs(c)
	case opListKeyValueStores:
		return h.handleListKeyValueStores(c)
	case opListVpcOrigins:
		return h.handleListVpcOrigins(c)
	case opListContinuousDeploymentPolicies:
		return h.handleListContinuousDeploymentPolicies(c)
	case opListStreamingDistributions:
		return h.handleListStreamingDistributions(c)
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
	case opGetFunctionAssociations:
		return h.handleGetFunctionAssociations(c, resource)
	case opSetFunctionAssociations:
		return h.handleSetFunctionAssociations(c, resource)
	case opUpdateKVSKeys:
		return h.handleUpdateKVSKeys(c, resource)
	default:

		return errNotDispatched
	}
}

// dispatchStubs is the dispatch table for CloudFront operations that don't have a
// dedicated route match earlier in the handler chain. Despite the legacy name, every
// operation handled here is backed by real InMemoryBackend state — there are no
// remaining hardcoded/empty stub responses. Unknown operations fall through to a
// real NoSuchOperation error at the end of the chain.
func (h *Handler) dispatchStubs(c *echo.Context, operation string) error {
	if err := h.dispatchStubsDistributions(c, operation); !errors.Is(err, errNotDispatched) {
		return err
	}

	if err := h.dispatchStubsTrustAndMisc(c, operation); !errors.Is(err, errNotDispatched) {
		return err
	}

	return h.dispatchStubsConnectionAndPolicy(c, operation)
}

// dispatchStubsDistributions handles distribution tenant and monitoring responses.
func (h *Handler) dispatchStubsDistributions(c *echo.Context, operation string) error {
	if err := h.dispatchStubsDistributionTenant(c, operation); !errors.Is(err, errNotDispatched) {
		return err
	}

	return h.dispatchStubsMonitoringAndStreaming(c, operation)
}

// dispatchStubsDistributionTenant handles distribution tenant and web ACL operations.
func (h *Handler) dispatchStubsDistributionTenant(c *echo.Context, operation string) error {
	path := c.Request().URL.Path

	switch operation {
	case opCreateDistributionTenant:
		return h.handleCreateDistributionTenant(c)
	case opCreateDistributionWithTags:
		return h.handleCreateDistributionWithTags(c)
	case opUpdateDistributionTenant:
		return h.handleUpdateDistributionTenant(c, extractResourceID(path, "distribution-tenant/"))
	case opDeleteDistributionTenant:
		return h.handleDeleteDistributionTenant(c, extractResourceID(path, "distribution-tenant/"))
	case opGetDistributionTenant:
		return h.handleGetDistributionTenant(c, extractResourceID(path, "distribution-tenant/"))
	case opGetDistributionTenantByDomain:
		return h.handleGetDistributionTenantByDomain(c)
	case opListDistributionTenants:
		return h.handleListDistributionTenants(c)
	case opListDistributionTenantsByCustom:
		return h.handleListDistributionTenantsByCustomization(c)
	case opUpdateDistributionWithStagingConfig:
		return h.handleUpdateDistributionWithStagingConfig(c, extractResourceID(path, "distribution/"))
	case opUpdateDomainAssociation:
		return h.handleUpdateDomainAssociation(c)
	case opVerifyDNSConfiguration:
		return h.handleVerifyDNSConfiguration(c)
	}

	return errNotDispatched
}

// dispatchStubsMonitoringAndStreaming handles monitoring subscription and web ACL disassociation stubs.
func (h *Handler) dispatchStubsMonitoringAndStreaming(c *echo.Context, operation string) error {
	switch operation {
	case opCreateMonitoringSubscription:
		distID := extractMonitoringDistID(c.Request().URL.Path)

		return h.handleCreateMonitoringSubscription(c, distID)
	case opGetMonitoringSubscription:
		distID := extractMonitoringDistID(c.Request().URL.Path)

		return h.handleGetMonitoringSubscription(c, distID)
	case opDeleteMonitoringSubscription:
		distID := extractMonitoringDistID(c.Request().URL.Path)

		return h.handleDeleteMonitoringSubscription(c, distID)
	case opDisassociateDistributionWebACL:
		distID := strings.TrimSuffix(
			strings.TrimPrefix(c.Request().URL.Path, cfPathPrefix+"distribution/"),
			"/disassociate-web-acl",
		)

		return h.handleDisassociateDistributionWebACL(c, distID)
	case opDisassociateDistributionTenantWebACL:
		tenantID := strings.TrimSuffix(
			strings.TrimPrefix(c.Request().URL.Path, cfPathPrefix+"distribution-tenant/"),
			"/disassociate-web-acl",
		)

		return h.handleDisassociateDistributionTenantWebACL(c, tenantID)
	}

	return errNotDispatched
}

// dispatchStubsTrustAndMisc handles trust store, anycast, and connection function stubs.
func (h *Handler) dispatchStubsTrustAndMisc(c *echo.Context, operation string) error {
	if err := h.dispatchStubsTrustAnycast(c, operation); !errors.Is(err, errNotDispatched) {
		return err
	}

	return h.dispatchStubsConnectionFunction(c, operation)
}

// dispatchStubsTrustAnycast handles trust store and anycast IP list stubs.
func (h *Handler) dispatchStubsTrustAnycast(c *echo.Context, operation string) error {
	path := c.Request().URL.Path
	switch operation {
	case opCreateTrustStore:
		return h.handleCreateTrustStore(c)
	case opGetTrustStore:
		return h.handleGetTrustStore(c, extractResourceID(path, "trust-store/"))
	case opUpdateTrustStore:
		return h.handleUpdateTrustStore(c, extractResourceID(path, "trust-store/"))
	case opDeleteTrustStore:
		return h.handleDeleteTrustStore(c, extractResourceID(path, "trust-store/"))
	case opListTrustStores:
		return h.handleListTrustStores(c)
	case opGetAnycastIPList:
		return h.handleGetAnycastIPList(c, extractResourceID(path, "anycast-ip-list/"))
	case opUpdateAnycastIPList:
		return h.handleUpdateAnycastIPList(c, extractResourceID(path, "anycast-ip-list/"))
	case opDeleteAnycastIPList:
		return h.handleDeleteAnycastIPList(c, extractResourceID(path, "anycast-ip-list/"))
	case opListAnycastIPLists:
		return h.handleListAnycastIPLists(c)
	}

	return errNotDispatched
}

// dispatchStubsConnectionFunction handles connection function and connection group stubs.
func (h *Handler) dispatchStubsConnectionFunction(c *echo.Context, operation string) error {
	path := c.Request().URL.Path
	switch operation {
	case opDescribeConnectionFunction:
		return h.handleDescribeConnectionFunction(c, extractResourceID(path, "connection-function/"))
	case opGetConnectionFunction:
		return h.handleGetConnectionFunction(c, extractResourceID(path, "connection-function/"))
	case opUpdateConnectionFunction:
		return h.handleUpdateConnectionFunction(c, extractResourceID(path, "connection-function/"))
	case opDeleteConnectionFunction:
		return h.handleDeleteConnectionFunction(c, extractResourceID(path, "connection-function/"))
	case opListConnectionFunctions:
		return h.handleListConnectionFunctions(c)
	case opPublishConnectionFunction:
		return h.handlePublishConnectionFunction(c, extractResourceID(path, "connection-function/"))
	case opTestConnectionFunction:
		return h.handleTestConnectionFunction(c, extractResourceID(path, "connection-function/"))
	case opGetConnectionGroup:
		return h.handleGetConnectionGroup(c, extractResourceID(path, "connection-group/"))
	case opGetConnectionGroupByRoutingEndpoint:
		return h.handleGetConnectionGroupByRoutingEndpoint(
			c,
			c.Request().URL.Query().Get("RoutingEndpoint"),
		)
	}

	return errNotDispatched
}

// dispatchStubsConnectionAndPolicy handles connection group, continuous deployment, resource policy, and misc stubs.
func (h *Handler) dispatchStubsConnectionAndPolicy(c *echo.Context, operation string) error {
	if err := h.dispatchStubsConnectionGroupAndCDP(c, operation); !errors.Is(err, errNotDispatched) {
		return err
	}

	return h.dispatchStubsResourcePolicyAndMisc(c, operation)
}

// dispatchStubsConnectionGroupAndCDP handles connection group and continuous deployment policy stubs.
func (h *Handler) dispatchStubsConnectionGroupAndCDP(
	c *echo.Context,
	operation string,
) error {
	path := c.Request().URL.Path

	switch operation {
	case opUpdateConnectionGroup:
		return h.handleUpdateConnectionGroup(c, extractResourceID(path, "connection-group/"))
	case opDeleteConnectionGroup:
		return h.handleDeleteConnectionGroup(c, extractResourceID(path, "connection-group/"))
	case opListConnectionGroups:
		return h.handleListConnectionGroups(c)

	// Continuous deployment policy — promoted to real handlers.

	// Resource policy.
	case opGetResourcePolicy:
		return h.handleGetResourcePolicy(c)
	case opPutResourcePolicy:
		return h.handlePutResourcePolicy(c)
	case opDeleteResourcePolicy:
		return h.handleDeleteResourcePolicy(c)
	}

	return errNotDispatched
}

// dispatchStubsResourcePolicyAndMisc handles distribution list, invalidation, and
// managed-certificate-details routes (the latter now backed by real state, not a stub).
func (h *Handler) dispatchStubsResourcePolicyAndMisc(c *echo.Context, operation string) error {
	if err := h.dispatchStubsDistributionListBy(c, operation); !errors.Is(err, errNotDispatched) {
		return err
	}

	return h.dispatchStubsTenantAndCerts(c, operation)
}

// dispatchStubsDistributionListBy handles the ListDistributionsBy-* operations.
func (h *Handler) dispatchStubsDistributionListBy(c *echo.Context, operation string) error {
	path := c.Request().URL.Path

	switch operation {
	case opListDistributionsByCachePolicyID:
		return h.handleListDistributionsByCachePolicyID(c, extractResourceID(path, "distributions/by-cache-policy-id/"))
	case opListDistributionsByOriginRequestPol:
		return h.handleListDistributionsByOriginRequestPolicyID(
			c,
			extractResourceID(path, "distributions/by-origin-request-policy-id/"),
		)
	case opListDistributionsByResponseHeadersPol:
		return h.handleListDistributionsByResponseHeadersPolicyID(
			c,
			extractResourceID(path, "distributions/by-response-headers-policy-id/"),
		)
	case opListDistributionsByWebACLID:
		return h.handleListDistributionsByWebACLID(c, extractResourceID(path, "distributions/by-web-acl-id/"))
	case opListDistributionsByRealtimeLogConfig:
		return h.handleListDistributionsByRealtimeLogConfig(
			c,
			c.Request().URL.Query().Get("RealtimeLogConfigArn"),
		)
	case opListDistributionsByKeyGroup:
		return h.handleListDistributionsByKeyGroup(c, extractResourceID(path, "distributions/by-key-group/"))
	case opListDistributionsByVpcOriginID:
		return h.handleListDistributionsByVpcOriginID(c, extractResourceID(path, "distributions/by-vpc-origin-id/"))
	case opListDistributionsByAnycastIPListID:
		return h.handleListDistributionsByAnycastIPListID(
			c,
			extractResourceID(path, "distributions/by-anycast-ip-list-id/"),
		)
	case opListDistributionsByConnectionFunction:
		return h.handleListDistributionsByConnectionFunction(
			c,
			extractResourceID(path, "distributions/by-connection-function/"),
		)
	case opListDistributionsByConnectionMode:
		return h.handleListDistributionsByConnectionMode(c, c.Request().URL.Query().Get("ConnectionMode"))
	case opListDistributionsByTrustStore:
		return h.handleListDistributionsByTrustStore(c, extractResourceID(path, "distributions/by-trust-store-id/"))
	case opListDistributionsByOwnedResource:
		return h.handleListDistributionsByOwnedResource(c, c.Request().URL.Query().Get("ResourceArn"))
	case opListConflictingAliases:
		return h.handleListConflictingAliases(c)
	case opListDomainConflicts:
		return h.handleListDomainConflicts(c)
	}

	return errNotDispatched
}

// dispatchStubsTenantAndCerts handles tenant invalidation routes and
// GetManagedCertificateDetails (now backed by real per-tenant state, not a stub).
func (h *Handler) dispatchStubsTenantAndCerts(c *echo.Context, operation string) error {
	path := c.Request().URL.Path

	switch operation {
	case opCreateInvalidationForDistTenant:
		return h.handleCreateInvalidationForTenant(c, extractResourceID(path, "distribution-tenant/"))
	case opGetInvalidationForDistTenant:
		return h.handleGetInvalidationForTenant(c, extractResourceID(path, "distribution-tenant/"))
	case opListInvalidationsForDistTenant:
		return h.handleListInvalidationsForTenant(c, extractResourceID(path, "distribution-tenant/"))
	case opGetManagedCertificateDetails:
		return h.handleGetManagedCertificateDetails(c, extractResourceID(path, "distribution-tenant/"))
	default:

		return xmlResp(c, http.StatusNotFound, cfErrorXML("NoSuchOperation", "unknown operation: "+operation))
	}
}

// notFoundCode returns the CloudFront error code for well-known not-found errors.
// The second return value is false when err is not a known not-found error.
func notFoundCode(err error) (string, bool) {
	if code, ok := notFoundCodeCore(err); ok {
		return code, true
	}

	return notFoundCodeExtended(err)
}

// notFoundCodeCore checks core distribution, OAI, and policy not-found errors.
func notFoundCodeCore(err error) (string, bool) {
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

// notFoundCodeExtended checks FLE, public key, key group, realtime log, KVS, and VPC origin errors.
func notFoundCodeExtended(err error) (string, bool) {
	switch {
	case errors.Is(err, ErrFLENotFound):
		return "NoSuchFieldLevelEncryptionConfig", true
	case errors.Is(err, ErrFLEProfileNotFound):
		return "NoSuchFieldLevelEncryptionProfile", true
	case errors.Is(err, ErrPublicKeyNotFound):
		return "NoSuchPublicKey", true
	case errors.Is(err, ErrKeyGroupNotFound):
		return "NoSuchResource", true
	case errors.Is(err, ErrRealtimeLogConfigNotFound):
		return "NoSuchRealtimeLogConfig", true
	case errors.Is(err, ErrKeyValueStoreNotFound):
		return "EntityNotFound", true
	case errors.Is(err, ErrVpcOriginNotFound):
		return "NoSuchVpcOrigin", true
	case errors.Is(err, ErrDistributionTenantNotFound):
		return "NoSuchDistributionTenant", true
	case errors.Is(err, ErrStreamingDistributionNotFound):
		return "NoSuchStreamingDistribution", true
	case errors.Is(err, ErrTrustStoreNotFound):
		return "NoSuchTrustStore", true
	case errors.Is(err, ErrResourcePolicyNotFound):
		return "NoSuchResourcePolicy", true
	case errors.Is(err, ErrMonitoringSubscriptionNotFound):
		return "NoSuchMonitoringSubscription", true
	}

	return "", false
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	if code, ok := notFoundCode(err); ok {
		return xmlResp(c, http.StatusNotFound, cfErrorXML(code, err.Error()))
	}

	switch {
	case errors.Is(err, ErrDistributionNotDisabled):
		return xmlResp(c, http.StatusConflict, cfErrorXML("DistributionNotDisabled", err.Error()))
	case errors.Is(err, ErrPublicKeyInUse):
		return xmlResp(c, http.StatusConflict, cfErrorXML("PublicKeyInUse", err.Error()))
	case errors.Is(err, ErrFLEProfileInUse):
		return xmlResp(c, http.StatusConflict, cfErrorXML("FieldLevelEncryptionProfileInUse", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return xmlResp(c, http.StatusConflict, cfErrorXML("DistributionAlreadyExists", err.Error()))
	case errors.Is(err, ErrConnectionGroupAlreadyExists):
		return xmlResp(c, http.StatusConflict, cfErrorXML("EntityAlreadyExists", err.Error()))
	case errors.Is(err, ErrInvalidTagging):
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("InvalidTagging", err.Error()))
	case errors.Is(err, ErrStreamingDistributionNotDisabled):
		return xmlResp(c, http.StatusConflict, cfErrorXML("StreamingDistributionNotDisabled", err.Error()))
	case errors.Is(err, ErrDomainConflict):
		return xmlResp(c, http.StatusConflict, cfErrorXML("DomainConflictException", err.Error()))
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

	return xmlResp(c, http.StatusCreated, distributionResponseXML(d, h.Backend.CountInProgressInvalidations(d.ID)))
}

func (h *Handler) handleGetDistribution(c *echo.Context, id string) error {
	d, err := h.Backend.GetDistribution(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", d.ETag)

	return xmlResp(c, http.StatusOK, distributionResponseXML(d, h.Backend.CountInProgressInvalidations(d.ID)))
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

	return xmlResp(c, http.StatusOK, distributionResponseXML(d, h.Backend.CountInProgressInvalidations(d.ID)))
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

// distributionSummaryPriceClass returns the PriceClass for a distribution, falling back to
// "PriceClass_All" when none was stored (legacy or minimal config).
func distributionSummaryPriceClass(d *Distribution) string {
	if d.PriceClass != "" {
		return d.PriceClass
	}

	if len(d.RawConfig) > 0 {
		var cfg distributionConfigMinimal
		if err := xml.Unmarshal(d.RawConfig, &cfg); err == nil && cfg.PriceClass != "" {
			return cfg.PriceClass
		}
	}

	return "PriceClass_All"
}

// distributionSummaryHTTPVersion returns the HttpVersion for a distribution.
func distributionSummaryHTTPVersion(d *Distribution) string {
	if d.HTTPVersion != "" {
		return d.HTTPVersion
	}

	if len(d.RawConfig) > 0 {
		var cfg distributionConfigMinimal
		if err := xml.Unmarshal(d.RawConfig, &cfg); err == nil && cfg.HTTPVersion != "" {
			return cfg.HTTPVersion
		}
	}

	return "http2"
}

// distributionSummaryIsIPV6(d) reads IsIPV6Enabled from the stored raw config when the
// Distribution field is false (zero value is ambiguous, so raw config is authoritative).
func distributionSummaryIsIPV6(d *Distribution) bool {
	if d.IsIPV6Enabled {
		return true
	}

	if len(d.RawConfig) > 0 {
		var cfg distributionConfigMinimal
		if err := xml.Unmarshal(d.RawConfig, &cfg); err == nil {
			return cfg.IsIPV6Enabled
		}
	}

	return false
}

func (h *Handler) handleListDistributions(c *echo.Context) error {
	dists := h.Backend.ListDistributions()

	// Parse pagination query params.
	marker := c.QueryParam("Marker")
	pageSize := maxItems
	if s := c.QueryParam("MaxItems"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 && n < maxItems {
			pageSize = n
		}
	}

	// Advance past the marker (marker == ID of first item on next page).
	if marker != "" {
		cut := 0
		for cut < len(dists) && dists[cut].ID <= marker {
			cut++
		}
		dists = dists[cut:]
	}

	isTruncated := len(dists) > pageSize
	if isTruncated {
		dists = dists[:pageSize]
	}

	nextMarker := ""
	if isTruncated && len(dists) > 0 {
		nextMarker = dists[len(dists)-1].ID
	}

	summaries := make([]distributionSummaryXML, 0, len(dists))
	for _, d := range dists {
		aliases := h.Backend.ListAliases(d.ID)
		s := distributionSummaryXML{
			ID:               d.ID,
			ARN:              d.ARN,
			Status:           d.Status,
			DomainName:       d.DomainName,
			Comment:          d.Comment,
			Enabled:          d.Enabled,
			IsIPV6Enabled:    distributionSummaryIsIPV6(d),
			LastModifiedTime: d.LastModifiedTime,
		}
		s.Aliases.Quantity = len(aliases)
		s.ViewerCertificate.CloudFrontDefaultCertificate = true
		s.Restrictions.GeoRestriction.RestrictionType = "none"
		s.PriceClass = distributionSummaryPriceClass(d)
		s.HTTPVersion = distributionSummaryHTTPVersion(d)
		summaries = append(summaries, s)
	}

	type distListXML struct {
		XMLName     xml.Name                 `xml:"DistributionList"`
		XMLNS       string                   `xml:"xmlns,attr"`
		NextMarker  string                   `xml:"NextMarker,omitempty"`
		Items       []distributionSummaryXML `xml:"Items>DistributionSummary"`
		MaxItems    int                      `xml:"MaxItems"`
		Quantity    int                      `xml:"Quantity"`
		IsTruncated bool                     `xml:"IsTruncated"`
	}

	list := distListXML{
		XMLNS:       cfNS,
		MaxItems:    pageSize,
		Quantity:    len(summaries),
		Items:       summaries,
		IsTruncated: isTruncated,
		NextMarker:  nextMarker,
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
	Tags    []tagXML `xml:"Tags>Items>Tag"`
	IPCount int32    `xml:"IPCount"`
}

type cachePolicyHeadersConfigXML struct {
	HeaderBehavior string   `xml:"HeaderBehavior"`
	Headers        []string `xml:"Headers>Header"`
}

type cachePolicyCookiesConfigXML struct {
	CookieBehavior string   `xml:"CookieBehavior"`
	Cookies        []string `xml:"Cookies>Cookie"`
}

type cachePolicyQueryStringsConfigXML struct {
	QueryStringBehavior string   `xml:"QueryStringBehavior"`
	QueryStrings        []string `xml:"QueryStrings>QueryString"`
}

type cachePolicyParamsXML struct {
	HeadersConfig      cachePolicyHeadersConfigXML      `xml:"HeadersConfig"`
	CookiesConfig      cachePolicyCookiesConfigXML      `xml:"CookiesConfig"`
	QueryStringsConfig cachePolicyQueryStringsConfigXML `xml:"QueryStringsConfig"`
	EnableGzip         bool                             `xml:"EnableAcceptEncodingGzip"`
	EnableBrotli       bool                             `xml:"EnableAcceptEncodingBrotli"`
}

type cachePolicyConfigXML struct {
	XMLName    xml.Name             `xml:"CachePolicyConfig"`
	Name       string               `xml:"Name"`
	Comment    string               `xml:"Comment"`
	Params     cachePolicyParamsXML `xml:"ParametersInCacheKeyAndForwardedToOrigin"`
	DefaultTTL int64                `xml:"DefaultTTL"`
	MaxTTL     int64                `xml:"MaxTTL"`
	MinTTL     int64                `xml:"MinTTL"`
}

// cachePolicyParamsFromXML converts the XML params struct to the backend model.
// Returns nil when no meaningful params were provided.
func cachePolicyParamsFromXML(x cachePolicyParamsXML) *CachePolicyParams {
	p := &CachePolicyParams{
		EnableAcceptEncodingGzip:   x.EnableGzip,
		EnableAcceptEncodingBrotli: x.EnableBrotli,
		HeadersConfig: CachePolicyHeadersConfig{
			HeaderBehavior: x.HeadersConfig.HeaderBehavior,
			Headers:        x.HeadersConfig.Headers,
		},
		CookiesConfig: CachePolicyCookiesConfig{
			CookieBehavior: x.CookiesConfig.CookieBehavior,
			Cookies:        x.CookiesConfig.Cookies,
		},
		QueryStringsConfig: CachePolicyQueryStringsConfig{
			QueryStringBehavior: x.QueryStringsConfig.QueryStringBehavior,
			QueryStrings:        x.QueryStringsConfig.QueryStrings,
		},
	}
	// Return nil when the params are entirely default (no config provided).
	if p.HeadersConfig.HeaderBehavior == "" && p.CookiesConfig.CookieBehavior == "" &&
		p.QueryStringsConfig.QueryStringBehavior == "" &&
		!p.EnableAcceptEncodingGzip && !p.EnableAcceptEncodingBrotli {
		return nil
	}

	return p
}

// cachePolicyResponseXML builds the full CachePolicy XML response.
func cachePolicyResponseXML(p *CachePolicy) string {
	var paramsXML string
	if p.Params != nil {
		paramsXML = fmt.Sprintf(
			`<ParametersInCacheKeyAndForwardedToOrigin>`+
				`<EnableAcceptEncodingGzip>%v</EnableAcceptEncodingGzip>`+
				`<EnableAcceptEncodingBrotli>%v</EnableAcceptEncodingBrotli>`+
				`<HeadersConfig><HeaderBehavior>%s</HeaderBehavior></HeadersConfig>`+
				`<CookiesConfig><CookieBehavior>%s</CookieBehavior></CookiesConfig>`+
				`<QueryStringsConfig><QueryStringBehavior>%s</QueryStringBehavior></QueryStringsConfig>`+
				`</ParametersInCacheKeyAndForwardedToOrigin>`,
			p.Params.EnableAcceptEncodingGzip,
			p.Params.EnableAcceptEncodingBrotli,
			p.Params.HeadersConfig.HeaderBehavior,
			p.Params.CookiesConfig.CookieBehavior,
			p.Params.QueryStringsConfig.QueryStringBehavior,
		)
	}

	return fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<CachePolicy xmlns="%s">`+
			`<Id>%s</Id>`+
			`<CachePolicyConfig>`+
			`<Name>%s</Name>`+
			`<Comment>%s</Comment>`+
			`<DefaultTTL>%d</DefaultTTL>`+
			`<MaxTTL>%d</MaxTTL>`+
			`<MinTTL>%d</MinTTL>`+
			`%s`+
			`</CachePolicyConfig>`+
			`</CachePolicy>`,
		cfNS,
		p.ID,
		p.Name,
		p.Comment,
		p.DefaultTTL,
		p.MaxTTL,
		p.MinTTL,
		paramsXML,
	)
}

// connectionFunctionConfigXML models the nested ConnectionFunctionConfig element carried by
// Create/UpdateConnectionFunctionRequest bodies (Comment + Runtime).
type connectionFunctionConfigXML struct {
	Comment string `xml:"Comment"`
	Runtime string `xml:"Runtime"`
}

// connectionFunctionRequestXML models a CreateConnectionFunctionRequest body. Comment is kept as
// both a top-level convenience field (for backward compatibility with earlier callers) and
// inside the nested, AWS-accurate ConnectionFunctionConfig element; the top-level value wins
// when both are present.
type connectionFunctionRequestXML struct {
	XMLName                  xml.Name                    `xml:"CreateConnectionFunctionRequest"`
	ConnectionFunctionConfig connectionFunctionConfigXML `xml:"ConnectionFunctionConfig"`
	Name                     string                      `xml:"Name"`
	Comment                  string                      `xml:"Comment"`
	// ConnectionFunctionCode is base64-encoded on the wire (matching real CloudFront); see
	// decodeConnectionFunctionCode.
	ConnectionFunctionCode string `xml:"ConnectionFunctionCode"`
}

// updateConnectionFunctionRequestXML models an UpdateConnectionFunctionRequest body.
type updateConnectionFunctionRequestXML struct {
	XMLName                  xml.Name                    `xml:"UpdateConnectionFunctionRequest"`
	ConnectionFunctionConfig connectionFunctionConfigXML `xml:"ConnectionFunctionConfig"`
	ConnectionFunctionCode   string                      `xml:"ConnectionFunctionCode"`
}

// decodeConnectionFunctionCode decodes a base64-encoded ConnectionFunctionCode payload, matching
// the real CloudFront wire format. If the payload is not valid base64 (e.g. a test sends raw
// text), it is used verbatim so the mock stays lenient.
func decodeConnectionFunctionCode(s string) []byte {
	if s == "" {
		return nil
	}
	if decoded, err := base64.StdEncoding.DecodeString(s); err == nil {
		return decoded
	}

	return []byte(s)
}

// connectionGroupRequestXML models a CreateConnectionGroupRequest body. Comment is a
// gopherstack-only convenience field kept for backward compatibility; it is not part of the
// real AWS ConnectionGroup shape.
type connectionGroupRequestXML struct {
	XMLName         xml.Name `xml:"CreateConnectionGroupRequest"`
	Name            string   `xml:"Name"`
	Comment         string   `xml:"Comment"`
	AnycastIPListID string   `xml:"AnycastIpListId"`
	Enabled         *bool    `xml:"Enabled"`
	Ipv6Enabled     *bool    `xml:"Ipv6Enabled"`
	Tags            []tagXML `xml:"Tags>Items>Tag"`
}

// updateConnectionGroupRequestXML models an UpdateConnectionGroupRequest body.
type updateConnectionGroupRequestXML struct {
	Enabled         *bool    `xml:"Enabled"`
	Ipv6Enabled     *bool    `xml:"Ipv6Enabled"`
	XMLName         xml.Name `xml:"UpdateConnectionGroupRequest"`
	Comment         string   `xml:"Comment"`
	AnycastIPListID string   `xml:"AnycastIpListId"`
}

type continuousDeploymentSessionStickinessConfigXML struct {
	IdleTTL    int32 `xml:"IdleTTL"`
	MaximumTTL int32 `xml:"MaximumTTL"`
}

type continuousDeploymentSingleWeightConfigXML struct {
	SessionStickinessConfig *continuousDeploymentSessionStickinessConfigXML `xml:"SessionStickinessConfig"`
	Weight                  float64                                         `xml:"Weight"`
}

type continuousDeploymentSingleHeaderConfigXML struct {
	Header string `xml:"Header"`
	Value  string `xml:"Value"`
}

type continuousDeploymentTrafficConfigXML struct {
	SingleWeightConfig *continuousDeploymentSingleWeightConfigXML `xml:"SingleWeightConfig"`
	SingleHeaderConfig *continuousDeploymentSingleHeaderConfigXML `xml:"SingleHeaderConfig"`
	Type               string                                     `xml:"Type"`
}

type continuousDeploymentPolicyConfigXML struct {
	XMLName                     xml.Name                              `xml:"ContinuousDeploymentPolicyConfig"`
	TrafficConfig               *continuousDeploymentTrafficConfigXML `xml:"TrafficConfig"`
	StagingDistributionDNS      string                                `xml:"StagingDistributionDnsNames>DnsName,omitempty"`
	StagingDistributionDNSNames []string                              `xml:"StagingDistributionDnsNames>Items>DnsName"`
	Enabled                     bool                                  `xml:"Enabled"`
}

// dnsNames returns the effective list of staging distribution DNS names from a parsed
// ContinuousDeploymentPolicyConfig request, preferring the real AWS Items>DnsName list shape
// and falling back to the legacy bare DnsName element for backward compatibility.
func (req continuousDeploymentPolicyConfigXML) dnsNames() []string {
	if len(req.StagingDistributionDNSNames) > 0 {
		return req.StagingDistributionDNSNames
	}

	return dnsNamesFromSingle(req.StagingDistributionDNS)
}

// trafficConfig converts a parsed TrafficConfig XML element into the backend model, returning
// the zero value if the request did not include one.
func (req continuousDeploymentPolicyConfigXML) trafficConfig() ContinuousDeploymentTrafficConfig {
	if req.TrafficConfig == nil {
		return ContinuousDeploymentTrafficConfig{}
	}

	out := ContinuousDeploymentTrafficConfig{Type: req.TrafficConfig.Type}
	if swc := req.TrafficConfig.SingleWeightConfig; swc != nil {
		out.SingleWeightConfig = &ContinuousDeploymentSingleWeightConfig{Weight: swc.Weight}
		if ssc := swc.SessionStickinessConfig; ssc != nil {
			out.SingleWeightConfig.SessionStickinessConfig = &ContinuousDeploymentSessionStickinessConfig{
				IdleTTL:    ssc.IdleTTL,
				MaximumTTL: ssc.MaximumTTL,
			}
		}
	}
	if shc := req.TrafficConfig.SingleHeaderConfig; shc != nil {
		out.SingleHeaderConfig = &ContinuousDeploymentSingleHeaderConfig{Header: shc.Header, Value: shc.Value}
	}

	return out
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

	return xmlResp(c, http.StatusCreated, distributionResponseXML(d, h.Backend.CountInProgressInvalidations(d.ID)))
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

	tags := make(map[string]string, len(req.Tags))
	for _, tag := range req.Tags {
		tags[tag.Key] = tag.Value
	}

	list, createErr := h.Backend.CreateAnycastIPList(req.Name, req.IPCount, tags)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	var ips strings.Builder
	for _, ip := range list.AnycastIPs {
		fmt.Fprintf(&ips, `<IpAddress>%s</IpAddress>`, ip)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<AnycastIPList xmlns="%s">`+
		`<Id>%s</Id>`+
		`<ARN>%s</ARN>`+
		`<Name>%s</Name>`+
		`<Status>%s</Status>`+
		`<IPCount>%d</IPCount>`+
		`<AnycastIps>%s</AnycastIps>`+
		`</AnycastIPList>`,
		cfNS, list.ID, list.ARN, list.Name, list.Status, list.IPCount, ips.String())

	c.Response().Header().Set("Location", cfPathPrefix+"anycast-ip-list/"+list.ID)
	c.Response().Header().Set("ETag", list.ETag)

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

	params := cachePolicyParamsFromXML(req.Params)
	policy, createErr := h.Backend.CreateCachePolicy(
		req.Name,
		req.Comment,
		req.DefaultTTL,
		req.MaxTTL,
		req.MinTTL,
		params,
	)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", policy.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"cache-policy/"+policy.ID)

	return xmlResp(c, http.StatusCreated, cachePolicyResponseXML(policy))
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

	comment := req.Comment
	if comment == "" {
		comment = req.ConnectionFunctionConfig.Comment
	}

	code := decodeConnectionFunctionCode(req.ConnectionFunctionCode)
	fn, createErr := h.Backend.CreateConnectionFunctionWithCode(
		req.Name, comment, req.ConnectionFunctionConfig.Runtime, code, nil,
	)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("Location", cfPathPrefix+"connection-function/"+fn.ID)
	c.Response().Header().Set("ETag", fn.ETag)

	return xmlResp(c, http.StatusCreated, connectionFunctionSummaryXML(fn))
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

	ipv6Enabled := true
	if req.Ipv6Enabled != nil {
		ipv6Enabled = *req.Ipv6Enabled
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	tags := make(map[string]string, len(req.Tags))
	for _, tag := range req.Tags {
		tags[tag.Key] = tag.Value
	}

	group, createErr := h.Backend.CreateConnectionGroupWithConfig(
		req.Name, req.Comment, req.AnycastIPListID, ipv6Enabled, enabled, tags,
	)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("Location", cfPathPrefix+"connection-group/"+group.ID)
	c.Response().Header().Set("ETag", group.ETag)

	return xmlResp(c, http.StatusCreated, connectionGroupXML(group))
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

	policy, createErr := h.Backend.CreateContinuousDeploymentPolicyWithConfig(
		req.Enabled, req.dnsNames(), req.trafficConfig(),
	)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	resp := continuousDeploymentPolicyXML(cfNS, policy)
	c.Response().Header().Set("Location", cfPathPrefix+"continuous-deployment-policy/"+policy.ID)
	c.Response().Header().Set("ETag", policy.ETag)

	return xmlResp(c, http.StatusCreated, resp)
}

func continuousDeploymentPolicyXML(ns string, policy *ContinuousDeploymentPolicy) string {
	var dnsNames strings.Builder
	for _, dns := range policy.StagingDistributionDNSNames {
		fmt.Fprintf(&dnsNames, `<DnsName>%s</DnsName>`, dns)
	}

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<ContinuousDeploymentPolicy xmlns="%s">`+
		`<Id>%s</Id>`+
		`<ARN>%s</ARN>`+
		`<LastModifiedTime>%s</LastModifiedTime>`+
		`<ContinuousDeploymentPolicyConfig>`+
		`<StagingDistributionDnsNames><Quantity>%d</Quantity><Items>%s</Items></StagingDistributionDnsNames>`+
		`<Enabled>%v</Enabled>`+
		`<TrafficConfig><Type>%s</Type></TrafficConfig>`+
		`</ContinuousDeploymentPolicyConfig>`+
		`</ContinuousDeploymentPolicy>`,
		ns, policy.ID, policy.ARN, policy.LastModifiedTime,
		len(policy.StagingDistributionDNSNames), dnsNames.String(),
		policy.Enabled, policy.TrafficConfig.Type)
}

func (h *Handler) handleGetContinuousDeploymentPolicy(c *echo.Context, id string) error {
	policy, err := h.Backend.GetContinuousDeploymentPolicy(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", policy.ETag)

	return xmlResp(c, http.StatusOK, continuousDeploymentPolicyXML(cfNS, policy))
}

func (h *Handler) handleUpdateContinuousDeploymentPolicy(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetContinuousDeploymentPolicy(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if ifMatch := c.Request().Header.Get("If-Match"); ifMatch != "" && ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed, cfErrorXML(
			"PreconditionFailed", "If-Match ETag did not match the current continuous deployment policy ETag",
		))
	}

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

	policy, updateErr := h.Backend.UpdateContinuousDeploymentPolicyWithConfig(
		id, req.Enabled, req.dnsNames(), req.trafficConfig(),
	)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", policy.ETag)

	return xmlResp(c, http.StatusOK, continuousDeploymentPolicyXML(cfNS, policy))
}

func (h *Handler) handleDeleteContinuousDeploymentPolicy(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetContinuousDeploymentPolicy(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if ifMatch := c.Request().Header.Get("If-Match"); ifMatch != "" && ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed, cfErrorXML(
			"PreconditionFailed", "If-Match ETag did not match the current continuous deployment policy ETag",
		))
	}

	if err := h.Backend.DeleteContinuousDeploymentPolicy(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListContinuousDeploymentPolicies(c *echo.Context) error {
	policies := h.Backend.ListContinuousDeploymentPolicies()

	var sb strings.Builder

	sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
	sb.WriteString(`<ContinuousDeploymentPolicyList xmlns="`)
	sb.WriteString(cfNS)
	sb.WriteString(`">`)
	count := strconv.Itoa(len(policies))
	sb.WriteString(`<MaxItems>`)
	sb.WriteString(count)
	sb.WriteString(`</MaxItems>`)
	sb.WriteString(`<Quantity>`)
	sb.WriteString(count)
	sb.WriteString(`</Quantity>`)
	sb.WriteString(`<IsTruncated>false</IsTruncated>`)
	sb.WriteString(`<Items>`)

	for _, p := range policies {
		fmt.Fprintf(
			&sb,
			`<ContinuousDeploymentPolicySummary><Id>%s</Id><Enabled>%v</Enabled></ContinuousDeploymentPolicySummary>`,
			p.ID, p.Enabled,
		)
	}

	sb.WriteString(`</Items>`)
	sb.WriteString(`</ContinuousDeploymentPolicyList>`)

	return xmlResp(c, http.StatusOK, sb.String())
}

// --- Function association handlers ---

type functionAssociationXML struct {
	FunctionARN string `xml:"FunctionARN"`
	EventType   string `xml:"EventType"`
}

type functionAssociationsXML struct {
	XMLName  xml.Name                 `xml:"FunctionAssociations"`
	Items    []functionAssociationXML `xml:"Items>FunctionAssociation"`
	Quantity int                      `xml:"Quantity"`
}

func (h *Handler) handleGetFunctionAssociations(c *echo.Context, distributionID string) error {
	assocs, err := h.Backend.GetDistributionFunctionAssociations(distributionID)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]functionAssociationXML, 0, len(assocs))
	for _, a := range assocs {
		items = append(items, functionAssociationXML(a))
	}

	resp := functionAssociationsXML{
		Quantity: len(items),
		Items:    items,
	}

	body, err := xml.MarshalIndent(resp, "", "  ")
	if err != nil {
		return xmlResp(c, http.StatusInternalServerError, cfErrorXML("InternalError", err.Error()))
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(body))
}

func (h *Handler) handleSetFunctionAssociations(c *echo.Context, distributionID string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req functionAssociationsXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(c, http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid FunctionAssociations XML"))
		}
	}

	associations := make([]FunctionAssociation, 0, len(req.Items))
	for _, item := range req.Items {
		associations = append(associations, FunctionAssociation(item))
	}

	if setErr := h.Backend.SetDistributionFunctionAssociations(distributionID, associations); setErr != nil {
		return h.handleError(c, setErr)
	}

	return c.NoContent(http.StatusOK)
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
	keys := collections.SortedKeys(kv)

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

	var pathsSB strings.Builder
	for _, p := range inv.Paths {
		fmt.Fprintf(&pathsSB, "<Path>%s</Path>", p)
	}

	resp := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<Invalidation xmlns="%s">`+
		`<Id>%s</Id>`+
		`<Status>%s</Status>`+
		`<CreateTime>%s</CreateTime>`+
		`<InvalidationBatch>`+
		`<CallerReference>%s</CallerReference>`+
		`<Paths><Quantity>%d</Quantity><Items>%s</Items></Paths>`+
		`</InvalidationBatch>`+
		`</Invalidation>`,
		cfNS, inv.ID, inv.Status, inv.CreateTime.Format(time.RFC3339),
		batch.CallerReference, len(inv.Paths), pathsSB.String())

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
		fmt.Fprintf(
			&sb,
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

// maxRequestBodyBytes caps the size of CloudFront request bodies. CloudFront
// configurations (distributions, functions, key value stores, etc.) are XML/JSON
// blobs well under 1 MiB; the cap prevents unbounded io.ReadAll DoS.
const maxRequestBodyBytes = 1 << 20 // 1 MiB

// readBody reads the entire request body, capped at maxRequestBodyBytes.
func readBody(c *echo.Context) ([]byte, error) {
	if c.Request().Body == nil {
		return []byte{}, nil
	}

	return io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, maxRequestBodyBytes))
}

// --- Cache Policy additional handlers ---

func (h *Handler) handleGetCachePolicy(c *echo.Context, id string) error {
	p, err := h.Backend.GetCachePolicy(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", p.ETag)

	return xmlResp(c, http.StatusOK, cachePolicyResponseXML(p))
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

	params := cachePolicyParamsFromXML(req.Params)
	p, updateErr := h.Backend.UpdateCachePolicy(
		id,
		req.Name,
		req.Comment,
		req.DefaultTTL,
		req.MaxTTL,
		req.MinTTL,
		params,
	)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", p.ETag)

	return xmlResp(c, http.StatusOK, cachePolicyResponseXML(p))
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

type rhpCorsConfigXML struct {
	AccessControlAllowOrigins     []string `xml:"AccessControlAllowOrigins>Items>Origin"`
	AccessControlAllowHeaders     []string `xml:"AccessControlAllowHeaders>Items>Header"`
	AccessControlAllowMethods     []string `xml:"AccessControlAllowMethods>Items>Method"`
	AccessControlExposeHeaders    []string `xml:"AccessControlExposeHeaders>Items>Header"`
	AccessControlMaxAgeSec        int64    `xml:"AccessControlMaxAgeSec"`
	AccessControlAllowCredentials bool     `xml:"AccessControlAllowCredentials"`
	OriginOverride                bool     `xml:"OriginOverride"`
}

type rhpSecurityHeadersXML struct {
	FrameOptionsValue              string `xml:"FrameOptions>FrameOption"`
	ReferrerPolicy                 string `xml:"ReferrerPolicy>ReferrerPolicy"`
	ContentSecurityPolicy          string `xml:"ContentSecurityPolicy>ContentSecurityPolicy"`
	XSSProtection                  string `xml:"XSSProtection>ReportUri"`
	StrictTransportSecuritySeconds int64  `xml:"StrictTransportSecurity>AccessControlMaxAgeSec"`
	IncludeSubdomains              bool   `xml:"StrictTransportSecurity>IncludeSubdomains"`
	Preload                        bool   `xml:"StrictTransportSecurity>Preload"`
	ContentTypeOptionsOverride     bool   `xml:"ContentTypeOptions>Override"`
}

type rhpCustomHeaderXML struct {
	Header   string `xml:"Header"`
	Value    string `xml:"Value"`
	Override bool   `xml:"Override"`
}

type rhpConfigXML struct {
	XMLName         xml.Name               `xml:"ResponseHeadersPolicyConfig"`
	Name            string                 `xml:"Name"`
	Comment         string                 `xml:"Comment"`
	CorsConfig      *rhpCorsConfigXML      `xml:"CorsConfig"`
	SecurityHeaders *rhpSecurityHeadersXML `xml:"SecurityHeadersConfig"`
	CustomHeaders   []rhpCustomHeaderXML   `xml:"CustomHeadersConfig>Items>ResponseHeadersPolicyCustomHeader"`
	RemoveHeaders   []string               `xml:"RemoveHeadersConfig>Items>ResponseHeadersPolicyRemoveHeader>Header"`
}

func rhpConfigFromXML(x rhpConfigXML) *ResponseHeadersPolicyConfig {
	cfg := &ResponseHeadersPolicyConfig{}
	if x.CorsConfig != nil {
		cfg.CorsConfig = &RHPCorsConfig{
			AccessControlAllowOrigins:     x.CorsConfig.AccessControlAllowOrigins,
			AccessControlAllowHeaders:     x.CorsConfig.AccessControlAllowHeaders,
			AccessControlAllowMethods:     x.CorsConfig.AccessControlAllowMethods,
			AccessControlExposeHeaders:    x.CorsConfig.AccessControlExposeHeaders,
			AccessControlMaxAgeSec:        x.CorsConfig.AccessControlMaxAgeSec,
			AccessControlAllowCredentials: x.CorsConfig.AccessControlAllowCredentials,
			OriginOverride:                x.CorsConfig.OriginOverride,
		}
	}
	if x.SecurityHeaders != nil {
		cfg.SecurityHeaders = &RHPSecurityHeaders{
			StrictTransportSecuritySeconds: x.SecurityHeaders.StrictTransportSecuritySeconds,
			ContentTypeOptionsOverride:     x.SecurityHeaders.ContentTypeOptionsOverride,
			FrameOptionsValue:              x.SecurityHeaders.FrameOptionsValue,
			ReferrerPolicy:                 x.SecurityHeaders.ReferrerPolicy,
			ContentSecurityPolicy:          x.SecurityHeaders.ContentSecurityPolicy,
			XSSProtection:                  x.SecurityHeaders.XSSProtection,
			IncludeSubdomains:              x.SecurityHeaders.IncludeSubdomains,
			Preload:                        x.SecurityHeaders.Preload,
		}
	}
	for _, h := range x.CustomHeaders {
		cfg.CustomHeaders = append(cfg.CustomHeaders, RHPCustomHeader(h))
	}
	cfg.RemoveHeaders = x.RemoveHeaders
	if cfg.CorsConfig == nil && cfg.SecurityHeaders == nil && len(cfg.CustomHeaders) == 0 &&
		len(cfg.RemoveHeaders) == 0 {
		return nil
	}

	return cfg
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

	p, createErr := h.Backend.CreateResponseHeadersPolicy(req.Name, req.Comment, rhpConfigFromXML(req))
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

	p, updateErr := h.Backend.UpdateResponseHeadersPolicy(id, req.Name, req.Comment, rhpConfigFromXML(req))
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
	var sb strings.Builder
	fmt.Fprintf(&sb, `<?xml version="1.0" encoding="UTF-8"?>`+
		`<ResponseHeadersPolicy xmlns="%s">`+
		`<Id>%s</Id>`+
		`<ResponseHeadersPolicyConfig>`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`,
		cfNS, p.ID, p.Name, p.Comment)
	if c := p.CorsConfig; c != nil {
		fmt.Fprintf(&sb,
			`<CorsConfig>`+
				`<AccessControlAllowCredentials>%v</AccessControlAllowCredentials>`+
				`<AccessControlMaxAgeSec>%d</AccessControlMaxAgeSec>`+
				`<OriginOverride>%v</OriginOverride>`+
				`</CorsConfig>`,
			c.AccessControlAllowCredentials, c.AccessControlMaxAgeSec, c.OriginOverride)
	}
	if s := p.SecurityHeaders; s != nil {
		fmt.Fprintf(&sb,
			`<SecurityHeadersConfig>`+
				`<StrictTransportSecurity>`+
				`<AccessControlMaxAgeSec>%d</AccessControlMaxAgeSec>`+
				`<IncludeSubdomains>%v</IncludeSubdomains>`+
				`<Preload>%v</Preload>`+
				`</StrictTransportSecurity>`+
				`<FrameOptions><FrameOption>%s</FrameOption></FrameOptions>`+
				`<ReferrerPolicy><ReferrerPolicy>%s</ReferrerPolicy></ReferrerPolicy>`+
				`</SecurityHeadersConfig>`,
			s.StrictTransportSecuritySeconds, s.IncludeSubdomains, s.Preload,
			s.FrameOptionsValue, s.ReferrerPolicy)
	}
	if len(p.CustomHeaders) > 0 {
		sb.WriteString(`<CustomHeadersConfig><Items>`)
		for _, h := range p.CustomHeaders {
			fmt.Fprintf(&sb,
				`<ResponseHeadersPolicyCustomHeader>`+
					`<Header>%s</Header>`+
					`<Value>%s</Value>`+
					`<Override>%v</Override>`+
					`</ResponseHeadersPolicyCustomHeader>`,
				h.Header, h.Value, h.Override)
		}
		fmt.Fprintf(&sb, `</Items><Quantity>%d</Quantity></CustomHeadersConfig>`, len(p.CustomHeaders))
	}
	if len(p.RemoveHeaders) > 0 {
		sb.WriteString(`<RemoveHeadersConfig><Items>`)
		for _, h := range p.RemoveHeaders {
			fmt.Fprintf(
				&sb,
				`<ResponseHeadersPolicyRemoveHeader><Header>%s</Header></ResponseHeadersPolicyRemoveHeader>`,
				h,
			)
		}
		fmt.Fprintf(&sb, `</Items><Quantity>%d</Quantity></RemoveHeadersConfig>`, len(p.RemoveHeaders))
	}
	sb.WriteString(`</ResponseHeadersPolicyConfig></ResponseHeadersPolicy>`)

	return sb.String()
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

type orpHeadersConfigXML struct {
	HeaderBehavior string   `xml:"HeaderBehavior"`
	Headers        []string `xml:"Headers>Items>Name"`
}

type orpCookiesConfigXML struct {
	CookieBehavior string   `xml:"CookieBehavior"`
	Cookies        []string `xml:"Cookies>Items>Name"`
}

type orpQueryStringsConfigXML struct {
	QueryStringBehavior string   `xml:"QueryStringBehavior"`
	QueryStrings        []string `xml:"QueryStrings>Items>Name"`
}

type orpConfigXML struct {
	HeadersConfig      *orpHeadersConfigXML      `xml:"HeadersConfig"`
	CookiesConfig      *orpCookiesConfigXML      `xml:"CookiesConfig"`
	QueryStringsConfig *orpQueryStringsConfigXML `xml:"QueryStringsConfig"`
	XMLName            xml.Name                  `xml:"OriginRequestPolicyConfig"`
	Name               string                    `xml:"Name"`
	Comment            string                    `xml:"Comment"`
}

func orpConfigFromXML(x orpConfigXML) *OriginRequestPolicyConfig {
	cfg := &OriginRequestPolicyConfig{}
	if x.HeadersConfig != nil {
		cfg.HeadersConfig = &ORPHeadersConfig{
			HeaderBehavior: x.HeadersConfig.HeaderBehavior,
			Headers:        x.HeadersConfig.Headers,
		}
	}
	if x.CookiesConfig != nil {
		cfg.CookiesConfig = &ORPCookiesConfig{
			CookieBehavior: x.CookiesConfig.CookieBehavior,
			Cookies:        x.CookiesConfig.Cookies,
		}
	}
	if x.QueryStringsConfig != nil {
		cfg.QueryStringsConfig = &ORPQueryStringsConfig{
			QueryStringBehavior: x.QueryStringsConfig.QueryStringBehavior,
			QueryStrings:        x.QueryStringsConfig.QueryStrings,
		}
	}
	if cfg.HeadersConfig == nil && cfg.CookiesConfig == nil && cfg.QueryStringsConfig == nil {
		return nil
	}

	return cfg
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

	p, createErr := h.Backend.CreateOriginRequestPolicy(req.Name, req.Comment, orpConfigFromXML(req))
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

	p, updateErr := h.Backend.UpdateOriginRequestPolicy(id, req.Name, req.Comment, orpConfigFromXML(req))
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
	var sb strings.Builder
	fmt.Fprintf(&sb, `<?xml version="1.0" encoding="UTF-8"?>`+
		`<OriginRequestPolicy xmlns="%s">`+
		`<Id>%s</Id>`+
		`<OriginRequestPolicyConfig>`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`,
		cfNS, p.ID, p.Name, p.Comment)
	if h := p.HeadersConfig; h != nil {
		fmt.Fprintf(&sb,
			`<HeadersConfig><HeaderBehavior>%s</HeaderBehavior><Quantity>%d</Quantity></HeadersConfig>`,
			h.HeaderBehavior, len(h.Headers))
	}
	if c := p.CookiesConfig; c != nil {
		fmt.Fprintf(&sb,
			`<CookiesConfig><CookieBehavior>%s</CookieBehavior><Quantity>%d</Quantity></CookiesConfig>`,
			c.CookieBehavior, len(c.Cookies))
	}
	if q := p.QueryStringsConfig; q != nil {
		fmt.Fprintf(
			&sb,
			`<QueryStringsConfig><QueryStringBehavior>%s</QueryStringBehavior><Quantity>%d</Quantity></QueryStringsConfig>`,
			q.QueryStringBehavior,
			len(q.QueryStrings),
		)
	}
	sb.WriteString(`</OriginRequestPolicyConfig></OriginRequestPolicy>`)

	return sb.String()
}

// --- Field Level Encryption handlers ---

// fleConfigRequestXML parses a FieldLevelEncryptionConfig request body.
type fleConfigRequestXML struct {
	XMLName          xml.Name `xml:"FieldLevelEncryptionConfig"`
	CallerReference  string   `xml:"CallerReference"`
	Comment          string   `xml:"Comment"`
	QueryArgProfiles []struct {
		QueryArg  string `xml:"QueryArg"`
		ProfileID string `xml:"ProfileId"`
	} `xml:"QueryArgProfileConfig>QueryArgProfiles>Items>QueryArgProfile"`
	ForwardWhenQueryArgProfileIsUnknown bool `xml:"QueryArgProfileConfig>ForwardWhenQueryArgProfileIsUnknown"`
}

// toBackend converts the parsed request into backend query-arg profiles.
func (r fleConfigRequestXML) toBackend() []FLEQueryArgProfile {
	if len(r.QueryArgProfiles) == 0 {
		return nil
	}

	out := make([]FLEQueryArgProfile, 0, len(r.QueryArgProfiles))
	for _, p := range r.QueryArgProfiles {
		out = append(out, FLEQueryArgProfile{QueryArg: p.QueryArg, ProfileID: p.ProfileID})
	}

	return out
}

// fleConfigInnerXML renders the inner fields of a FieldLevelEncryptionConfig
// element (without the enclosing element itself), so it can be reused for both
// the wrapped and config-only responses.
func fleConfigInnerXML(fle *FieldLevelEncryption) string {
	var items strings.Builder
	for _, p := range fle.QueryArgProfiles {
		items.WriteString(`<QueryArgProfile><QueryArg>`)
		items.WriteString(xmlEscape(p.QueryArg))
		items.WriteString(`</QueryArg><ProfileId>`)
		items.WriteString(xmlEscape(p.ProfileID))
		items.WriteString(`</ProfileId></QueryArgProfile>`)
	}

	return fmt.Sprintf(`<CallerReference>%s</CallerReference>`+
		`<Comment>%s</Comment>`+
		`<QueryArgProfileConfig>`+
		`<ForwardWhenQueryArgProfileIsUnknown>%t</ForwardWhenQueryArgProfileIsUnknown>`+
		`<QueryArgProfiles><Quantity>%d</Quantity><Items>%s</Items></QueryArgProfiles>`+
		`</QueryArgProfileConfig>`,
		xmlEscape(fle.Name), xmlEscape(fle.Comment),
		fle.ForwardWhenQueryArgProfileIsUnknown, len(fle.QueryArgProfiles), items.String())
}

func fleResponseXML(fle *FieldLevelEncryption) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<FieldLevelEncryption xmlns="%s"><Id>%s</Id>`+
		`<FieldLevelEncryptionConfig>%s</FieldLevelEncryptionConfig>`+
		`</FieldLevelEncryption>`,
		cfNS, fle.ID, fleConfigInnerXML(fle))
}

// fleConfigResponseXML renders the config-only response (root =
// FieldLevelEncryptionConfig) returned by GetFieldLevelEncryptionConfig.
func fleConfigResponseXML(fle *FieldLevelEncryption) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<FieldLevelEncryptionConfig xmlns="%s">%s</FieldLevelEncryptionConfig>`,
		cfNS, fleConfigInnerXML(fle))
}

func (h *Handler) handleCreateFieldLevelEncryption(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req fleConfigRequestXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	name := req.CallerReference
	if name == "" {
		name = generateID()
	}

	fle, createErr := h.Backend.CreateFieldLevelEncryption(name, req.Comment, req.toBackend())
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", fle.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"field-level-encryption/"+fle.ID)

	return xmlResp(c, http.StatusCreated, fleResponseXML(fle))
}

func (h *Handler) handleGetFieldLevelEncryption(c *echo.Context, id string) error {
	fle, err := h.Backend.GetFieldLevelEncryption(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", fle.ETag)

	return xmlResp(c, http.StatusOK, fleResponseXML(fle))
}

func (h *Handler) handleListFieldLevelEncryptions(c *echo.Context) error {
	items := h.Backend.ListFieldLevelEncryptions()

	type fleSummaryXML struct {
		XMLName xml.Name `xml:"FieldLevelEncryptionSummary"`
		ID      string   `xml:"Id"`
		Comment string   `xml:"Comment"`
	}

	type fleListXML struct {
		XMLName     xml.Name        `xml:"FieldLevelEncryptionList"`
		XMLNS       string          `xml:"xmlns,attr"`
		Items       []fleSummaryXML `xml:"Items>FieldLevelEncryptionSummary"`
		MaxItems    int             `xml:"MaxItems"`
		Quantity    int             `xml:"Quantity"`
		IsTruncated bool            `xml:"IsTruncated"`
	}

	summaries := make([]fleSummaryXML, 0, len(items))
	for _, fle := range items {
		summaries = append(summaries, fleSummaryXML{ID: fle.ID, Comment: fle.Comment})
	}

	list := fleListXML{XMLNS: cfNS, MaxItems: maxItems, Quantity: len(summaries), Items: summaries}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

//nolint:dupl // update FLE and FLE profile share structure but operate on distinct resource types
func (h *Handler) handleUpdateFieldLevelEncryption(c *echo.Context, id string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req fleConfigRequestXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	current, getErr := h.Backend.GetFieldLevelEncryption(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	name := req.CallerReference
	if name == "" {
		name = current.Name
	}

	fle, updateErr := h.Backend.UpdateFieldLevelEncryption(id, name, req.Comment, req.toBackend())
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", fle.ETag)

	return xmlResp(c, http.StatusOK, fleResponseXML(fle))
}

func (h *Handler) handleDeleteFieldLevelEncryption(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetFieldLevelEncryption(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current FieldLevelEncryption ETag"))
	}

	if err := h.Backend.DeleteFieldLevelEncryption(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Field Level Encryption Profile handlers ---

// fleProfileConfigRequestXML parses a FieldLevelEncryptionProfileConfig body.
type fleProfileConfigRequestXML struct {
	XMLName            xml.Name `xml:"FieldLevelEncryptionProfileConfig"`
	Name               string   `xml:"Name"`
	CallerReference    string   `xml:"CallerReference"`
	Comment            string   `xml:"Comment"`
	EncryptionEntities []struct {
		PublicKeyID   string   `xml:"PublicKeyId"`
		ProviderID    string   `xml:"ProviderId"`
		FieldPatterns []string `xml:"FieldPatterns>Items>FieldPattern"`
	} `xml:"EncryptionEntities>Items>EncryptionEntity"`
}

// toBackend converts the parsed request into backend encryption entities.
func (r fleProfileConfigRequestXML) toBackend() []EncryptionEntity {
	if len(r.EncryptionEntities) == 0 {
		return nil
	}

	out := make([]EncryptionEntity, 0, len(r.EncryptionEntities))
	for _, e := range r.EncryptionEntities {
		out = append(out, EncryptionEntity{
			PublicKeyID:   e.PublicKeyID,
			ProviderID:    e.ProviderID,
			FieldPatterns: append([]string(nil), e.FieldPatterns...),
		})
	}

	return out
}

// fleProfileConfigInnerXML renders the inner fields of a
// FieldLevelEncryptionProfileConfig element.
func fleProfileConfigInnerXML(p *FieldLevelEncryptionProfile) string {
	var entities strings.Builder
	for _, e := range p.EncryptionEntities {
		var patterns strings.Builder
		for _, fp := range e.FieldPatterns {
			patterns.WriteString(`<FieldPattern>`)
			patterns.WriteString(xmlEscape(fp))
			patterns.WriteString(`</FieldPattern>`)
		}

		fmt.Fprintf(&entities, `<EncryptionEntity>`+
			`<PublicKeyId>%s</PublicKeyId>`+
			`<ProviderId>%s</ProviderId>`+
			`<FieldPatterns><Quantity>%d</Quantity><Items>%s</Items></FieldPatterns>`+
			`</EncryptionEntity>`,
			xmlEscape(e.PublicKeyID), xmlEscape(e.ProviderID),
			len(e.FieldPatterns), patterns.String())
	}

	return fmt.Sprintf(`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`<EncryptionEntities><Quantity>%d</Quantity><Items>%s</Items></EncryptionEntities>`,
		xmlEscape(p.Name), xmlEscape(p.Comment),
		len(p.EncryptionEntities), entities.String())
}

func fleProfileResponseXML(p *FieldLevelEncryptionProfile) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<FieldLevelEncryptionProfile xmlns="%s"><Id>%s</Id>`+
		`<FieldLevelEncryptionProfileConfig>%s</FieldLevelEncryptionProfileConfig>`+
		`</FieldLevelEncryptionProfile>`,
		cfNS, p.ID, fleProfileConfigInnerXML(p))
}

// fleProfileConfigResponseXML renders the config-only response (root =
// FieldLevelEncryptionProfileConfig) for GetFieldLevelEncryptionProfileConfig.
func fleProfileConfigResponseXML(p *FieldLevelEncryptionProfile) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<FieldLevelEncryptionProfileConfig xmlns="%s">%s</FieldLevelEncryptionProfileConfig>`,
		cfNS, fleProfileConfigInnerXML(p))
}

func (h *Handler) handleCreateFieldLevelEncryptionProfile(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req fleProfileConfigRequestXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	if req.Name == "" {
		req.Name = generateID()
	}

	p, createErr := h.Backend.CreateFieldLevelEncryptionProfile(req.Name, req.Comment, req.toBackend())
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", p.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"field-level-encryption-profile/"+p.ID)

	return xmlResp(c, http.StatusCreated, fleProfileResponseXML(p))
}

func (h *Handler) handleGetFieldLevelEncryptionProfile(c *echo.Context, id string) error {
	p, err := h.Backend.GetFieldLevelEncryptionProfile(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", p.ETag)

	return xmlResp(c, http.StatusOK, fleProfileResponseXML(p))
}

//nolint:dupl // list handlers for different CloudFront resource types share XML list structure
func (h *Handler) handleListFieldLevelEncryptionProfiles(c *echo.Context) error {
	items := h.Backend.ListFieldLevelEncryptionProfiles()

	type flePSummaryXML struct {
		XMLName xml.Name `xml:"FieldLevelEncryptionProfileSummary"`
		ID      string   `xml:"Id"`
		Name    string   `xml:"Name"`
		Comment string   `xml:"Comment"`
	}

	type flePListXML struct {
		XMLName     xml.Name         `xml:"FieldLevelEncryptionProfileList"`
		XMLNS       string           `xml:"xmlns,attr"`
		Items       []flePSummaryXML `xml:"Items>FieldLevelEncryptionProfileSummary"`
		MaxItems    int              `xml:"MaxItems"`
		Quantity    int              `xml:"Quantity"`
		IsTruncated bool             `xml:"IsTruncated"`
	}

	summaries := make([]flePSummaryXML, 0, len(items))
	for _, p := range items {
		summaries = append(summaries, flePSummaryXML{ID: p.ID, Name: p.Name, Comment: p.Comment})
	}

	list := flePListXML{XMLNS: cfNS, MaxItems: maxItems, Quantity: len(summaries), Items: summaries}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

//nolint:dupl // update FLE profile and update FLE share structure but operate on distinct resource types
func (h *Handler) handleUpdateFieldLevelEncryptionProfile(c *echo.Context, id string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req fleProfileConfigRequestXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	current, getErr := h.Backend.GetFieldLevelEncryptionProfile(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	name := req.Name
	if name == "" {
		name = current.Name
	}

	p, updateErr := h.Backend.UpdateFieldLevelEncryptionProfile(id, name, req.Comment, req.toBackend())
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", p.ETag)

	return xmlResp(c, http.StatusOK, fleProfileResponseXML(p))
}

func (h *Handler) handleDeleteFieldLevelEncryptionProfile(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetFieldLevelEncryptionProfile(id)
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
				"If-Match ETag did not match the current FieldLevelEncryptionProfile ETag",
			),
		)
	}

	if err := h.Backend.DeleteFieldLevelEncryptionProfile(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Public Key handlers ---

func publicKeyResponseXML(pk *PublicKey) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<PublicKey xmlns="%s">`+
		`<Id>%s</Id>`+
		`<PublicKeyConfig>`+
		`<CallerReference>%s</CallerReference>`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`<EncodedKey>%s</EncodedKey>`+
		`</PublicKeyConfig>`+
		`</PublicKey>`,
		cfNS, pk.ID, pk.CallerReference, pk.Name, pk.Comment, pk.EncodedKey)
}

type publicKeyConfigXML struct {
	XMLName         xml.Name `xml:"PublicKeyConfig"`
	CallerReference string   `xml:"CallerReference"`
	Name            string   `xml:"Name"`
	Comment         string   `xml:"Comment"`
	EncodedKey      string   `xml:"EncodedKey"`
}

func (h *Handler) handleCreatePublicKey(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req publicKeyConfigXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	if req.Name == "" {
		req.Name = generateID()
	}

	pk, createErr := h.Backend.CreatePublicKey(req.CallerReference, req.Name, req.Comment, req.EncodedKey)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", pk.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"public-key/"+pk.ID)

	return xmlResp(c, http.StatusCreated, publicKeyResponseXML(pk))
}

func (h *Handler) handleGetPublicKey(c *echo.Context, id string) error {
	pk, err := h.Backend.GetPublicKey(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", pk.ETag)

	return xmlResp(c, http.StatusOK, publicKeyResponseXML(pk))
}

//nolint:dupl // list handlers for different CloudFront resource types share XML list structure
func (h *Handler) handleListPublicKeys(c *echo.Context) error {
	items := h.Backend.ListPublicKeys()

	type pkSummaryXML struct {
		XMLName xml.Name `xml:"PublicKeySummary"`
		ID      string   `xml:"Id"`
		Name    string   `xml:"Name"`
		Comment string   `xml:"Comment"`
	}

	type pkListXML struct {
		XMLName     xml.Name       `xml:"PublicKeyList"`
		XMLNS       string         `xml:"xmlns,attr"`
		Items       []pkSummaryXML `xml:"Items>PublicKeySummary"`
		MaxItems    int            `xml:"MaxItems"`
		Quantity    int            `xml:"Quantity"`
		IsTruncated bool           `xml:"IsTruncated"`
	}

	summaries := make([]pkSummaryXML, 0, len(items))
	for _, pk := range items {
		summaries = append(summaries, pkSummaryXML{ID: pk.ID, Name: pk.Name, Comment: pk.Comment})
	}

	list := pkListXML{XMLNS: cfNS, MaxItems: maxItems, Quantity: len(summaries), Items: summaries}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleUpdatePublicKey(c *echo.Context, id string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req publicKeyConfigXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	pk, updateErr := h.Backend.UpdatePublicKey(id, req.Comment)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", pk.ETag)

	return xmlResp(c, http.StatusOK, publicKeyResponseXML(pk))
}

func (h *Handler) handleDeletePublicKey(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetPublicKey(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current PublicKey ETag"))
	}

	if err := h.Backend.DeletePublicKey(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Key Group handlers ---

func keyGroupResponseXML(kg *KeyGroup) string {
	var sb strings.Builder
	for _, item := range kg.Items {
		sb.WriteString("<Key>")
		sb.WriteString(item)
		sb.WriteString("</Key>")
	}
	itemsXML := sb.String()

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<KeyGroup xmlns="%s">`+
		`<Id>%s</Id>`+
		`<KeyGroupConfig>`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`<Items>%s</Items>`+
		`</KeyGroupConfig>`+
		`</KeyGroup>`,
		cfNS, kg.ID, kg.Name, kg.Comment, itemsXML)
}

type keyGroupConfigXML struct {
	XMLName xml.Name `xml:"KeyGroupConfig"`
	Name    string   `xml:"Name"`
	Comment string   `xml:"Comment"`
	Items   []string `xml:"Items>PublicKey"`
}

func (h *Handler) handleCreateKeyGroup(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req keyGroupConfigXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	if req.Name == "" {
		req.Name = generateID()
	}

	kg, createErr := h.Backend.CreateKeyGroup(req.Name, req.Comment, req.Items)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", kg.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"key-group/"+kg.ID)

	return xmlResp(c, http.StatusCreated, keyGroupResponseXML(kg))
}

func (h *Handler) handleGetKeyGroup(c *echo.Context, id string) error {
	kg, err := h.Backend.GetKeyGroup(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", kg.ETag)

	return xmlResp(c, http.StatusOK, keyGroupResponseXML(kg))
}

//nolint:dupl // list handlers for different CloudFront resource types share XML list structure
func (h *Handler) handleListKeyGroups(c *echo.Context) error {
	items := h.Backend.ListKeyGroups()

	type kgSummaryXML struct {
		XMLName xml.Name `xml:"KeyGroupSummary"`
		ID      string   `xml:"Id"`
		Name    string   `xml:"Name"`
		Comment string   `xml:"Comment"`
	}

	type kgListXML struct {
		XMLName     xml.Name       `xml:"KeyGroupList"`
		XMLNS       string         `xml:"xmlns,attr"`
		Items       []kgSummaryXML `xml:"Items>KeyGroupSummary"`
		MaxItems    int            `xml:"MaxItems"`
		Quantity    int            `xml:"Quantity"`
		IsTruncated bool           `xml:"IsTruncated"`
	}

	summaries := make([]kgSummaryXML, 0, len(items))
	for _, kg := range items {
		summaries = append(summaries, kgSummaryXML{ID: kg.ID, Name: kg.Name, Comment: kg.Comment})
	}

	list := kgListXML{XMLNS: cfNS, MaxItems: maxItems, Quantity: len(summaries), Items: summaries}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleUpdateKeyGroup(c *echo.Context, id string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req keyGroupConfigXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	current, getErr := h.Backend.GetKeyGroup(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if req.Name == "" {
		req.Name = current.Name
	}

	kg, updateErr := h.Backend.UpdateKeyGroup(id, req.Name, req.Comment, req.Items)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", kg.ETag)

	return xmlResp(c, http.StatusOK, keyGroupResponseXML(kg))
}

func (h *Handler) handleDeleteKeyGroup(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetKeyGroup(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current KeyGroup ETag"))
	}

	if err := h.Backend.DeleteKeyGroup(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Realtime Log Config handlers ---

type realtimeLogConfigRequestXML struct {
	XMLName      xml.Name `xml:"RealtimeLogConfig"`
	Name         string   `xml:"Name"`
	Fields       []string `xml:"Fields>Field"`
	SamplingRate int64    `xml:"SamplingRate"`
}

func realtimeLogConfigResponseXML(cfg *RealtimeLogConfig) string {
	var sb strings.Builder
	for _, f := range cfg.Fields {
		sb.WriteString("<Field>")
		sb.WriteString(f)
		sb.WriteString("</Field>")
	}
	fieldsXML := sb.String()

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<RealtimeLogConfig xmlns="%s">`+
		`<ARN>%s</ARN>`+
		`<Name>%s</Name>`+
		`<SamplingRate>%d</SamplingRate>`+
		`<Fields>%s</Fields>`+
		`</RealtimeLogConfig>`,
		cfNS, cfg.ARN, cfg.Name, cfg.SamplingRate, fieldsXML)
}

func (h *Handler) handleCreateRealtimeLogConfig(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req realtimeLogConfigRequestXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	if req.Name == "" {
		req.Name = generateID()
	}

	cfg, createErr := h.Backend.CreateRealtimeLogConfig(req.Name, req.SamplingRate, req.Fields)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	return xmlResp(c, http.StatusCreated, realtimeLogConfigResponseXML(cfg))
}

func (h *Handler) handleGetRealtimeLogConfig(c *echo.Context, arn string) error {
	cfg, err := h.Backend.GetRealtimeLogConfig(arn)
	if err != nil {
		cfg, err = h.Backend.GetRealtimeLogConfigByName(arn)
		if err != nil {
			return h.handleError(c, err)
		}
	}

	return xmlResp(c, http.StatusOK, realtimeLogConfigResponseXML(cfg))
}

//nolint:dupl // list handlers for different CloudFront resource types share XML list structure
func (h *Handler) handleListRealtimeLogConfigs(c *echo.Context) error {
	items := h.Backend.ListRealtimeLogConfigs()

	type rlcItemXML struct {
		XMLName      xml.Name `xml:"member"`
		ARN          string   `xml:"ARN"`
		Name         string   `xml:"Name"`
		SamplingRate int64    `xml:"SamplingRate"`
	}

	type rlcListXML struct {
		XMLName     xml.Name     `xml:"RealtimeLogConfigs"`
		XMLNS       string       `xml:"xmlns,attr"`
		Items       []rlcItemXML `xml:"Items>member"`
		MaxItems    int          `xml:"MaxItems"`
		Quantity    int          `xml:"Quantity"`
		IsTruncated bool         `xml:"IsTruncated"`
	}

	summaries := make([]rlcItemXML, 0, len(items))
	for _, cfg := range items {
		summaries = append(summaries, rlcItemXML{ARN: cfg.ARN, Name: cfg.Name, SamplingRate: cfg.SamplingRate})
	}

	list := rlcListXML{XMLNS: cfNS, MaxItems: maxItems, Quantity: len(summaries), Items: summaries}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleUpdateRealtimeLogConfig(c *echo.Context, arn string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req realtimeLogConfigRequestXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	cfg, getErr := h.Backend.GetRealtimeLogConfig(arn)
	if getErr != nil {
		cfg, getErr = h.Backend.GetRealtimeLogConfigByName(arn)
		if getErr != nil {
			return h.handleError(c, getErr)
		}
	}

	updated, updateErr := h.Backend.UpdateRealtimeLogConfig(cfg.ARN, req.SamplingRate, req.Fields)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	return xmlResp(c, http.StatusOK, realtimeLogConfigResponseXML(updated))
}

func (h *Handler) handleDeleteRealtimeLogConfig(c *echo.Context, arn string) error {
	if err := h.Backend.DeleteRealtimeLogConfig(arn); err != nil {
		cfg, getErr := h.Backend.GetRealtimeLogConfigByName(arn)
		if getErr != nil {
			return h.handleError(c, err)
		}

		if delErr := h.Backend.DeleteRealtimeLogConfig(cfg.ARN); delErr != nil {
			return h.handleError(c, delErr)
		}
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Key Value Store handlers ---

func kvsResponseXML(kvs *KeyValueStore) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<KeyValueStore xmlns="%s">`+
		`<Id>%s</Id>`+
		`<ARN>%s</ARN>`+
		`<Name>%s</Name>`+
		`<Comment>%s</Comment>`+
		`<Status>%s</Status>`+
		`<LastModifiedTime>%s</LastModifiedTime>`+
		`</KeyValueStore>`,
		cfNS, kvs.ID, kvs.ARN, xmlEscape(kvs.Name), xmlEscape(kvs.Comment),
		kvs.Status, kvs.LastModifiedTime)
}

type keyValueStoreRequestXML struct {
	XMLName xml.Name `xml:"KeyValueStoreRequest"`
	Name    string   `xml:"Name"`
	Comment string   `xml:"Comment"`
}

func (h *Handler) handleCreateKeyValueStore(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req keyValueStoreRequestXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	if req.Name == "" {
		req.Name = generateID()
	}

	kvs, createErr := h.Backend.CreateKeyValueStore(req.Name, req.Comment)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", kvs.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"key-value-store/"+kvs.ID)

	return xmlResp(c, http.StatusCreated, kvsResponseXML(kvs))
}

func (h *Handler) handleGetKeyValueStore(c *echo.Context, id string) error {
	kvs, err := h.Backend.GetKeyValueStore(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", kvs.ETag)

	return xmlResp(c, http.StatusOK, kvsResponseXML(kvs))
}

func (h *Handler) handleListKeyValueStores(c *echo.Context) error {
	items := h.Backend.ListKeyValueStores()

	type kvsSummaryXML struct {
		XMLName          xml.Name `xml:"KeyValueStore"`
		ID               string   `xml:"Id"`
		ARN              string   `xml:"ARN"`
		Name             string   `xml:"Name"`
		Comment          string   `xml:"Comment"`
		Status           string   `xml:"Status"`
		LastModifiedTime string   `xml:"LastModifiedTime"`
	}

	type kvsListXML struct {
		XMLName     xml.Name        `xml:"KeyValueStoreList"`
		XMLNS       string          `xml:"xmlns,attr"`
		Items       []kvsSummaryXML `xml:"Items>KeyValueStore"`
		MaxItems    int             `xml:"MaxItems"`
		Quantity    int             `xml:"Quantity"`
		IsTruncated bool            `xml:"IsTruncated"`
	}

	summaries := make([]kvsSummaryXML, 0, len(items))
	for _, kvs := range items {
		summaries = append(summaries, kvsSummaryXML{
			ID:               kvs.ID,
			ARN:              kvs.ARN,
			Name:             kvs.Name,
			Comment:          kvs.Comment,
			Status:           kvs.Status,
			LastModifiedTime: kvs.LastModifiedTime,
		})
	}

	list := kvsListXML{XMLNS: cfNS, MaxItems: maxItems, Quantity: len(summaries), Items: summaries}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleDeleteKeyValueStore(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetKeyValueStore(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current KeyValueStore ETag"))
	}

	if err := h.Backend.DeleteKeyValueStore(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- VPC Origin handlers ---

func vpcOriginResponseXML(origin *VpcOrigin) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<VpcOrigin xmlns="%s">`+
		`<VpcOriginEndpointConfig>`+
		`<Id>%s</Id>`+
		`<ARN>%s</ARN>`+
		`<Name>%s</Name>`+
		`</VpcOriginEndpointConfig>`+
		`</VpcOrigin>`,
		cfNS, origin.ID, origin.ARN, origin.Name)
}

type vpcOriginRequestXML struct {
	XMLName xml.Name `xml:"VpcOriginRequest"`
	Name    string   `xml:"VpcOriginEndpointConfig>Name"`
}

func (h *Handler) handleCreateVpcOrigin(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req vpcOriginRequestXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	if req.Name == "" {
		req.Name = generateID()
	}

	origin, createErr := h.Backend.CreateVpcOrigin(req.Name)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	c.Response().Header().Set("ETag", origin.ETag)
	c.Response().Header().Set("Location", cfPathPrefix+"vpc-origin/"+origin.ID)

	return xmlResp(c, http.StatusCreated, vpcOriginResponseXML(origin))
}

func (h *Handler) handleGetVpcOrigin(c *echo.Context, id string) error {
	origin, err := h.Backend.GetVpcOrigin(id)
	if err != nil {
		return h.handleError(c, err)
	}

	c.Response().Header().Set("ETag", origin.ETag)

	return xmlResp(c, http.StatusOK, vpcOriginResponseXML(origin))
}

//nolint:dupl // list handlers for different CloudFront resource types share XML list structure
func (h *Handler) handleListVpcOrigins(c *echo.Context) error {
	items := h.Backend.ListVpcOrigins()

	type vpcSummaryXML struct {
		XMLName xml.Name `xml:"VpcOriginSummary"`
		ID      string   `xml:"Id"`
		ARN     string   `xml:"ARN"`
		Name    string   `xml:"Name"`
	}

	type vpcListXML struct {
		XMLName     xml.Name        `xml:"VpcOriginList"`
		XMLNS       string          `xml:"xmlns,attr"`
		Items       []vpcSummaryXML `xml:"Items>VpcOriginSummary"`
		MaxItems    int             `xml:"MaxItems"`
		Quantity    int             `xml:"Quantity"`
		IsTruncated bool            `xml:"IsTruncated"`
	}

	summaries := make([]vpcSummaryXML, 0, len(items))
	for _, origin := range items {
		summaries = append(summaries, vpcSummaryXML{ID: origin.ID, ARN: origin.ARN, Name: origin.Name})
	}

	list := vpcListXML{XMLNS: cfNS, MaxItems: maxItems, Quantity: len(summaries), Items: summaries}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleUpdateVpcOrigin(c *echo.Context, id string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req vpcOriginRequestXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	current, getErr := h.Backend.GetVpcOrigin(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	name := req.Name
	if name == "" {
		name = current.Name
	}

	origin, updateErr := h.Backend.UpdateVpcOrigin(id, name)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	c.Response().Header().Set("ETag", origin.ETag)

	return xmlResp(c, http.StatusOK, vpcOriginResponseXML(origin))
}

func (h *Handler) handleDeleteVpcOrigin(c *echo.Context, id string) error {
	current, getErr := h.Backend.GetVpcOrigin(id)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	ifMatch := c.Request().Header.Get("If-Match")
	if ifMatch == "" || ifMatch != current.ETag {
		return xmlResp(c, http.StatusPreconditionFailed,
			cfErrorXML("PreconditionFailed", "If-Match ETag did not match the current VpcOrigin ETag"))
	}

	if err := h.Backend.DeleteVpcOrigin(id); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// extractMonitoringDistID extracts distribution ID from monitoring subscription path.
func extractMonitoringDistID(path string) string {
	suffix := strings.TrimPrefix(path, cfPathPrefix+"distribution/")

	return strings.TrimSuffix(suffix, "/monitoring-subscription")
}

// extractResourceID extracts the resource ID from a CloudFront API path.
func extractResourceID(path, prefix string) string {
	suffix := strings.TrimPrefix(path, cfPathPrefix)
	trimmed := strings.TrimPrefix(suffix, prefix)
	if id, _, found := strings.Cut(trimmed, "/"); found {
		return id
	}

	return trimmed
}
