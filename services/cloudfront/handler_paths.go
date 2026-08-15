package cloudfront

import (
	"net/http"
	"strings"
)

// parseCFPath maps HTTP method + path to (operationName, resourceID).
//
// parseCFPath maps an HTTP method + URL path to a CloudFront operation name and resource identifier.
// opParam is the request's "Operation" query value, used only to disambiguate
// TagResource ("Operation=Tag") from UntagResource ("Operation=Untag") -- both
// are POST /2020-05-31/tagging on the wire (cloudfront@v1.67.4 serializers.go:
// awsRestxml_serializeOp{Tag,Untag}Resource's SplitURI), never DELETE.
func parseCFPath(method, path, resourceParam, opParam string) (string, string) {
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

	if op, id := parseCFMonitoringSubscriptionPath(method, suffix); op != "" {
		return op, id
	}

	if op, id := parseCFManagedCertificatePath(method, suffix); op != "" {
		return op, id
	}

	return parseCFPathCore(method, suffix, resourceParam, opParam)
}

// parseCFMonitoringSubscriptionPath routes the monitoring-subscription trio.
// Real path is PLURAL "/2020-05-31/distributions/{DistributionId}/monitoring-subscription"
// (cloudfront@v1.67.4 serializers.go: awsRestxml_serializeOp{Create,Get,Delete}MonitoringSubscription's
// SplitURI) -- singular "distribution/{Id}/monitoring-subscription" never matches a real client.
func parseCFMonitoringSubscriptionPath(method, suffix string) (string, string) {
	const suffixPart = "/monitoring-subscription"

	if !strings.HasPrefix(suffix, "distributions/") || !strings.HasSuffix(suffix, suffixPart) {
		return "", ""
	}

	id := strings.TrimSuffix(strings.TrimPrefix(suffix, "distributions/"), suffixPart)

	switch method {
	case http.MethodPost:
		return opCreateMonitoringSubscription, id
	case http.MethodGet:
		return opGetMonitoringSubscription, id
	case http.MethodDelete:
		return opDeleteMonitoringSubscription, id
	}

	return "", ""
}

// parseCFManagedCertificatePath routes GetManagedCertificateDetails. Real
// path is "/2020-05-31/managed-certificate/{Identifier}" (cloudfront@v1.67.4
// serializers.go: awsRestxml_serializeOpGetManagedCertificateDetails's
// SplitURI) -- not nested under distribution-tenant.
func parseCFManagedCertificatePath(method, suffix string) (string, string) {
	const prefix = "managed-certificate/"

	if method == http.MethodGet && strings.HasPrefix(suffix, prefix) {
		return opGetManagedCertificateDetails, strings.TrimPrefix(suffix, prefix)
	}

	return "", ""
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
		switch {
		case strings.HasSuffix(suffix, "/associate-web-acl") && method == http.MethodPut:
			return opAssociateDistributionTenantWebACL, strings.TrimSuffix(rest, "/associate-web-acl")
		case strings.HasSuffix(suffix, "/disassociate-web-acl") && method == http.MethodPut:
			return opDisassociateDistributionTenantWebACL, strings.TrimSuffix(rest, "/disassociate-web-acl")
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
//
// UpdateOriginRequestPolicy's real request syntax is
// "PUT /2020-05-31/origin-request-policy/{Id}" -- the bare-ID path, with no
// "/config" suffix (verified against the API reference; matches the
// UpdateCachePolicy/UpdateResponseHeadersPolicy pattern used elsewhere in this
// file). PUT was previously only routed when the path ended in "/config", which
// no real SDK client ever sends: every UpdateOriginRequestPolicy call would 404
// with "unknown operation" against this emulator.
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
		if method == http.MethodGet {
			return opGetOriginRequestPolicyConfig, id
		}
	case strings.HasPrefix(suffix, prefix) && !strings.Contains(strings.TrimPrefix(suffix, prefix), "/"):
		id := strings.TrimPrefix(suffix, prefix)
		switch method {
		case http.MethodGet:
			return opGetOriginRequestPolicy, id
		case http.MethodPut:
			return opUpdateOriginRequestPolicy, id
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
		opGetFieldLevelEncryption, "", opDeleteFieldLevelEncryptionConfig,
		opGetFieldLevelEncryptionConfig, opUpdateFieldLevelEncryptionConfig); op != "" {
		return op, id
	}

	return parseCFResourcePath(method, suffix, "field-level-encryption-profile",
		opCreateFieldLevelEncryptionProfile, opListFieldLevelEncryptionProfiles,
		opGetFieldLevelEncryptionProfile, "", opDeleteFieldLevelEncryptionProfile,
		opGetFieldLevelEncryptionProfileConfig, opUpdateFieldLevelEncryptionProfile)
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

	return parseCFPublicKeyRealtimePath(method, suffix)
}

// parseCFPublicKeyRealtimePath routes public key and realtime log config paths.
func parseCFPublicKeyRealtimePath(method, suffix string) (string, string) {
	if op, id := parseCFResourcePath(method, suffix, "public-key",
		opCreatePublicKey, opListPublicKeys, opGetPublicKey, "", opDeletePublicKey,
		opGetPublicKeyConfig, opUpdatePublicKey); op != "" {
		return op, id
	}

	return parseCFRealtimeLogConfigPath(method, suffix)
}

// parseCFRealtimeLogConfigPath routes real-time log config paths. Unlike most
// CloudFront resources, Get/Delete are POST RPC-style calls to their own
// distinct paths and Update is a PUT to the base path -- none carry an ID path
// segment. ARN or Name travels in the body instead
// (api_op_{Get,Update,Delete}RealtimeLogConfig.go).
func parseCFRealtimeLogConfigPath(method, suffix string) (string, string) {
	switch suffix {
	case "realtime-log-config":
		switch method {
		case http.MethodPost:
			return opCreateRealtimeLogConfig, ""
		case http.MethodGet:
			return opListRealtimeLogConfigs, ""
		case http.MethodPut:
			return opUpdateRealtimeLogConfig, ""
		}
	case "get-realtime-log-config":
		if method == http.MethodPost {
			return opGetRealtimeLogConfig, ""
		}
	case "delete-realtime-log-config":
		if method == http.MethodPost {
			return opDeleteRealtimeLogConfig, ""
		}
	}

	return "", ""
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
// CreateStreamingDistributionWithTags variant (POST .../streaming-distribution?WithTags,
// resourceParam pre-resolved to resourceParamWithTags by cfResourceParam).
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
// ListConnectionFunctions is POST to the plural "connection-functions" path
// (cloudfront@v1.67.4 serializers.go: awsRestxml_serializeOpListConnectionFunctions's
// SplitURI) -- there is no bare GET "connection-function" op.
func parseCFConnectionFunctionPath(method, suffix string) (string, string) {
	const prefix = "connection-function/"

	switch {
	case suffix == "connection-functions" && method == http.MethodPost:
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
// GetConnectionGroupByRoutingEndpoint is the bare GET "connection-group"
// (RoutingEndpoint travels as a query value); ListConnectionGroups is POST
// to the plural "connection-groups" path instead (cloudfront@v1.67.4
// serializers.go: awsRestxml_serializeOp{GetConnectionGroupByRoutingEndpoint,
// ListConnectionGroups}'s SplitURI) -- there is no
// "connection-group-by-routing-endpoint" literal path in the real SDK.
func parseCFConnectionGroupPath(method, suffix string) (string, string) {
	const prefix = "connection-group/"
	const root = "connection-group"

	switch {
	case suffix == root && method == http.MethodGet:
		return opGetConnectionGroupByRoutingEndpoint, ""
	case suffix == "connection-groups" && method == http.MethodPost:
		return opListConnectionGroups, ""
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
	}

	_ = resourceParam

	return "", ""
}

// parseCFDistributionsByPath routes "distributionsBy*" paths -- a single
// camelCase path segment with no hyphens, verified per-op against
// cloudfront@v1.67.4 serializers.go's SplitURI (e.g.
// "/2020-05-31/distributionsByCachePolicyId/{CachePolicyId}"). There is no
// "distributions/by-*" hyphenated variant anywhere in the real SDK; every
// case below previously used that wrong shape and was unreachable by any
// real client. Most filter identifiers are a {Param} URI label (returned as
// the resource ID here); ListDistributionsByConnectionFunction and
// ListDistributionsByTrustStore carry theirs as a query value instead (no
// URI label at all -- see dispatchStubsDistributionListBy), and
// ListDistributionsByRealtimeLogConfig carries its ARN/Name in the XML body.
func parseCFDistributionsByPath(method, suffix string) (string, string) {
	if method == http.MethodPost && suffix == "distributionsByRealtimeLogConfig" {
		return opListDistributionsByRealtimeLogConfig, ""
	}

	if method != http.MethodGet {
		return "", ""
	}

	switch {
	case strings.HasPrefix(suffix, "distributionsByAnycastIpListId/"):
		return opListDistributionsByAnycastIPListID, strings.TrimPrefix(suffix, "distributionsByAnycastIpListId/")
	case strings.HasPrefix(suffix, "distributionsByCachePolicyId/"):
		return opListDistributionsByCachePolicyID, strings.TrimPrefix(suffix, "distributionsByCachePolicyId/")
	case suffix == "distributionsByConnectionFunction":
		return opListDistributionsByConnectionFunction, ""
	case strings.HasPrefix(suffix, "distributionsByConnectionMode/"):
		return opListDistributionsByConnectionMode, strings.TrimPrefix(suffix, "distributionsByConnectionMode/")
	case strings.HasPrefix(suffix, "distributionsByKeyGroupId/"):
		return opListDistributionsByKeyGroup, strings.TrimPrefix(suffix, "distributionsByKeyGroupId/")
	case strings.HasPrefix(suffix, "distributionsByOriginRequestPolicyId/"):
		return opListDistributionsByOriginRequestPol, strings.TrimPrefix(
			suffix,
			"distributionsByOriginRequestPolicyId/",
		)
	case strings.HasPrefix(suffix, "distributionsByOwnedResource/"):
		return opListDistributionsByOwnedResource, strings.TrimPrefix(suffix, "distributionsByOwnedResource/")
	case strings.HasPrefix(suffix, "distributionsByResponseHeadersPolicyId/"):
		return opListDistributionsByResponseHeadersPol, strings.TrimPrefix(
			suffix,
			"distributionsByResponseHeadersPolicyId/",
		)
	case suffix == "distributionsByTrustStore":
		return opListDistributionsByTrustStore, ""
	case strings.HasPrefix(suffix, "distributionsByVpcOriginId/"):
		return opListDistributionsByVpcOriginID, strings.TrimPrefix(suffix, "distributionsByVpcOriginId/")
	case strings.HasPrefix(suffix, "distributionsByWebACLId/"):
		return opListDistributionsByWebACLID, strings.TrimPrefix(suffix, "distributionsByWebACLId/")
	}

	return "", ""
}

// parseCFPathCore handles remaining distribution-tenant, create ops, tags, and resource policy paths.
func parseCFPathCore(method, suffix, resourceParam, opParam string) (string, string) {
	if op, id := parseCFCreateAndTagOps(method, suffix, resourceParam, opParam); op != "" {
		return op, id
	}

	if op, id := parseCFDistributionTenantOps(method, suffix); op != "" {
		return op, id
	}

	return parseCFDistributionExtPath(method, suffix)
}

// parseCFCreateAndTagOps handles create operations and tagging.
func parseCFCreateAndTagOps(method, suffix, resourceParam, opParam string) (string, string) {
	if op, id := parseCFCreateAndTagCoreOps(method, suffix, resourceParam, opParam); op != "" {
		return op, id
	}

	// Real clients POST to three distinct RPC-style paths for resource-policy
	// ops; ResourceArn travels in the body, never the URL (serializers.go:
	// awsRestxml_serializeOp{Get,Put,Delete}ResourcePolicy HandleSerialize).
	if method == http.MethodPost {
		switch suffix {
		case sfxGetResourcePolicy:
			return opGetResourcePolicy, resourceParam
		case sfxPutResourcePolicy:
			return opPutResourcePolicy, resourceParam
		case sfxDeleteResourcePolicy:
			return opDeleteResourcePolicy, resourceParam
		}
	}

	return "", ""
}

// parseCFCreateAndTagCoreOps handles create ops and tagging (without resource policy).
func parseCFCreateAndTagCoreOps(method, suffix, resourceParam, opParam string) (string, string) {
	if op, id := parseCFTaggingOps(method, suffix, resourceParam, opParam); op != "" {
		return op, id
	}

	return parseCFCreateOps(method, suffix, resourceParam)
}

// parseCFTaggingOps handles tagging and distribution-with-tags creation. Real
// TagResource and UntagResource are both POST /2020-05-31/tagging,
// disambiguated only by the "Operation=Tag"/"Operation=Untag" query value
// (cloudfront@v1.67.4 serializers.go); a bare POST with no recognized
// Operation value defaults to TagResource, and DELETE is never sent by a real
// client for either.
func parseCFTaggingOps(method, suffix, resourceParam, opParam string) (string, string) {
	if suffix == "tagging" {
		switch {
		case method == http.MethodGet:
			return opListTagsForResource, resourceParam
		case method == http.MethodPost && opParam == "Untag":
			return opUntagResource, resourceParam
		case method == http.MethodPost:
			return opTagResource, resourceParam
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
	// resource, POST method) and GetDistributionTenantByDomain as GET on the bare
	// singular /distribution-tenant resource (Domain travels as a "?domain="
	// query value, cloudfront@v1.67.4 serializers.go:
	// awsRestxml_serializeOpGetDistributionTenantByDomain's HttpBindings) --
	// distinct from both the plural List path and the /distribution-tenant/{Id}
	// path used by Get/Update/Delete.
	if suffix == "distribution-tenants" && method == http.MethodPost {
		return opListDistributionTenants, ""
	}

	if suffix == "distribution-tenant" {
		switch method {
		case http.MethodPost:
			return opCreateDistributionTenant, ""
		case http.MethodGet:
			return opGetDistributionTenantByDomain, ""
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

// parseCFDistributionMonitoringOps handles distribution staging and disassociate paths.
// The monitoring-subscription trio is handled separately by
// parseCFMonitoringSubscriptionPath, since it uses a plural "distributions/"
// prefix unlike every path handled here.
func parseCFDistributionMonitoringOps(method, suffix string) (string, string) {
	inner := strings.TrimPrefix(suffix, "distribution/")

	switch {
	// Real path is /distribution/{Id}/promote-staging-config (cloudfront@v1.67.4
	// serializers.go: awsRestxml_serializeOpUpdateDistributionWithStagingConfig's
	// SplitURI) -- the previous "/staging" suffix never matched a real client's PUT,
	// so every real UpdateDistributionWithStagingConfig call 404'd as NoSuchOperation.
	case strings.HasSuffix(inner, "/promote-staging-config") && method == http.MethodPut:
		return opUpdateDistributionWithStagingConfig, strings.TrimSuffix(inner, "/promote-staging-config")
	case strings.HasSuffix(inner, "/disassociate-web-acl") && method == http.MethodPut:
		return opDisassociateDistributionWebACL, strings.TrimSuffix(inner, "/disassociate-web-acl")
	}

	return "", ""
}

// parseCFDistributionTenantExtOps handles distribution-tenant extended paths.
// GetManagedCertificateDetails is NOT nested here despite the name --
// see parseCFManagedCertificatePath.
func parseCFDistributionTenantExtOps(method, suffix string) (string, string) {
	if strings.Contains(suffix, "/invalidation") {
		return parseCFDistributionTenantInvalidation(method, suffix)
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
		// Real path is /domain-conflicts (plural; cloudfront@v1.67.4 serializers.go:
		// awsRestxml_serializeOpListDomainConflicts's SplitURI) -- the previous singular
		// "domain-conflict" never matched a real client's POST, so every real
		// ListDomainConflicts call 404'd as NoSuchOperation.
		{"domain-conflicts", http.MethodPost, opListDomainConflicts},
		{"domain-association", http.MethodPost, opUpdateDomainAssociation},
		{"verify-dns-configuration", http.MethodPost, opVerifyDNSConfiguration},
		// cloudfront@v1.67.4 serializers.go awsRestxml_serializeOpListDistributionTenantsByCustomization:
		// POST to "distribution-tenants-by-customization" (one hyphenated segment), not GET to
		// "distribution-tenants/by-customization" (the old entry here never matched a real client).
		{"distribution-tenants-by-customization", http.MethodPost, opListDistributionTenantsByCustom},
	}

	for _, m := range exact {
		if suffix == m.suffix && method == m.method {
			return m.op
		}
	}

	return ""
}

// parseCFMiscPathByDistribution handles prefix-based distribution listing paths.
// The "distributions/by-*" ListDistributionsBy* family and
// GetManagedCertificateDetails no longer live here -- see
// parseCFDistributionsByPath and parseCFManagedCertificatePath, which route
// their real camelCase paths.
func parseCFMiscPathByDistribution(method, suffix string) (string, string) {
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
