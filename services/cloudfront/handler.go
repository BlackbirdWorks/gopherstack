package cloudfront

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"

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

// opGetFunctionAssociations and opSetFunctionAssociations are internal-only
// route labels for a gopherstack-specific convenience endpoint
// (GET/PUT /2020-05-31/distribution/{id}/function-associations). They are NOT
// real AWS CloudFront API operations — the real SDK has no "FunctionAssociations"
// operation at all (verified against aws-sdk-go-v2/service/cloudfront: zero
// matches). Real clients manage function associations as the <FunctionAssociations>
// element nested inside a cache behavior in the DistributionConfig XML, submitted
// via the genuine CreateDistribution/UpdateDistribution operations; gopherstack
// stores that config as an opaque RawConfig blob and round-trips it correctly,
// so real SDK clients are fully served without ever touching this route. The
// FunctionInUse delete-guard (functions.go) also does not depend on this
// endpoint — it token-searches RawConfig via tokenReferencedByAnyDistribution.
// Deliberately excluded from GetSupportedOperations()/ChaosOperations() (see
// coreSupportedOperations) so gopherstack does not claim SDK support for an
// operation AWS does not have — see gopherstack-vhw2 category A. Left routed
// (handler_paths.go/handler_dispatch.go) as internal test/tooling scaffolding
// since nothing outside this package's own tests depends on it.
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
func (h *Handler) GetSupportedOperations() []string {
	ops := coreSupportedOperations()
	ops = append(ops, stubSupportedOperationsA()...)
	ops = append(ops, stubSupportedOperationsB()...)

	return ops
}

// coreSupportedOperations returns the CloudFront operations with a full, real implementation.
//
// opGetFunctionAssociations/opSetFunctionAssociations are intentionally NOT
// listed here — they are not real CloudFront SDK operations (see the comment
// on their const declaration above); listing them would misrepresent
// gopherstack's SDK completeness.
func coreSupportedOperations() []string {
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
	}
}

// stubSupportedOperationsA returns the first half of the stub (not-yet-implemented)
// CloudFront operations advertised by GetSupportedOperations.
func stubSupportedOperationsA() []string {
	return []string{
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
	}
}

// stubSupportedOperationsB returns the second half of the stub (not-yet-implemented)
// CloudFront operations advertised by GetSupportedOperations.
func stubSupportedOperationsB() []string {
	return []string{
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

// extractResourceID extracts the resource ID from a CloudFront API path.
func extractResourceID(path, prefix string) string {
	suffix := strings.TrimPrefix(path, cfPathPrefix)
	trimmed := strings.TrimPrefix(suffix, prefix)
	if id, _, found := strings.Cut(trimmed, "/"); found {
		return id
	}

	return trimmed
}

// xmlEscape escapes a string for safe inclusion as XML character data.
func xmlEscape(s string) string {
	if s == "" {
		return ""
	}

	var buf bytes.Buffer
	if err := xml.EscapeText(&buf, []byte(s)); err != nil {
		return ""
	}

	return buf.String()
}

// --- Config-only ("/config") GET handlers ---
//
// AWS returns the *Config element as the document root for the Get*Config
// operations (GetKeyGroupConfig, GetPublicKeyConfig,
// GetFieldLevelEncryptionConfig, GetFieldLevelEncryptionProfileConfig), not the
// wrapping resource element. These handlers render that config-only shape.
