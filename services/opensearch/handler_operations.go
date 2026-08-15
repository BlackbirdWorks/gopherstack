package opensearch

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func domainOperations() []string {
	return []string{
		"CancelDomainConfigChange",
		"CancelServiceSoftwareUpdate",
		"CreateDomain",
		"CreateIndex",
		"DeleteDomain",
		"DeleteIndex",
		"DescribeDomain",
		"DescribeDomainAutoTunes",
		"DescribeDomainChangeProgress",
		"DescribeDomainConfig",
		"DescribeDomainHealth",
		"DescribeDomainNodes",
		"DescribeDomains",
		"DescribeDryRunProgress",
		"GetDomainMaintenanceStatus",
		"GetIndex",
		"GetUpgradeHistory",
		"GetUpgradeStatus",
		"ListDomainMaintenances",
		"ListDomainNames",
		"StartDomainMaintenance",
		"StartServiceSoftwareUpdate",
		"UpdateDomainConfig",
		"UpdateIndex",
		"UpgradeDomain",
	}
}

func connectionAndTagOperations() []string {
	return []string{
		"AcceptInboundConnection",
		"AddTags",
		"AuthorizeVpcEndpointAccess",
		"CreateOutboundConnection",
		"CreateVpcEndpoint",
		"DeleteInboundConnection",
		"DeleteOutboundConnection",
		"DeleteVpcEndpoint",
		"DescribeInboundConnections",
		"DescribeOutboundConnections",
		"DescribeVpcEndpoints",
		"ListTags",
		"ListVpcEndpointAccess",
		"ListVpcEndpoints",
		"ListVpcEndpointsForDomain",
		"RejectInboundConnection",
		"RemoveTags",
		"RevokeVpcEndpointAccess",
	}
}

func packageAndDataOperations() []string {
	return []string{
		"AddDataSource",
		"AddDirectQueryDataSource",
		"AssociatePackage",
		"AssociatePackages",
		"CreatePackage",
		"DeleteDataSource",
		"DeleteDirectQueryDataSource",
		"DeletePackage",
		"DescribePackages",
		"DissociatePackage",
		"DissociatePackages",
		"GetDataSource",
		"GetDirectQueryDataSource",
		"GetPackageVersionHistory",
		"ListDataSources",
		"ListDirectQueryDataSources",
		"ListDomainsForPackage",
		"ListPackagesForDomain",
		"UpdateDataSource",
		"UpdateDirectQueryDataSource",
		"UpdatePackage",
		"UpdatePackageScope",
	}
}

func infraAndAppOperations() []string {
	return []string{
		"CreateApplication",
		"DeleteApplication",
		"DescribeInstanceTypeLimits",
		"DescribeReservedInstanceOfferings",
		"DescribeReservedInstances",
		"GetApplication",
		"GetCompatibleVersions",
		"GetDefaultApplicationSetting",
		"ListApplications",
		"ListInstanceTypeDetails",
		"ListScheduledActions",
		"ListVersions",
		"PurchaseReservedInstanceOffering",
		"PutDefaultApplicationSetting",
		"UpdateApplication",
		"UpdateScheduledAction",
		"UpdateVpcEndpoint",
	}
}

// applicationDataMigrationOperations lists the newer OpenSearch-application-
// scoped operations added in the aws-sdk-go-v2 bump this pass covers: data
// source attachments, capabilities, insights, migrations, and the
// domain-scoped RollbackServiceSoftwareUpdate.
func applicationDataMigrationOperations() []string {
	return []string{
		"AttachDataSource",
		"DetachDataSource",
		"DescribeDataSourceAttachment",
		"ListDataSourceAttachments",
		"RegisterCapability",
		"DeregisterCapability",
		"GetCapability",
		"ListInsights",
		"DescribeInsightDetails",
		"InsightFeedback",
		"StartMigration",
		"GetMigration",
		"ListMigrations",
		"RollbackServiceSoftwareUpdate",
	}
}

// serverlessOperations lists the OpenSearch Serverless (AOSS) operations this
// Handler advertises. These are real opensearchserverless.Client operations,
// not opensearch.Client (the "classic" managed-domain client checked
// elsewhere in this file's sibling operations) -- see sdk_completeness_test.go,
// which checks this subset against opensearchserverless.Client specifically.
//
// Note: AOSS models encryption and network security policies as a single
// CreateSecurityPolicy/GetSecurityPolicy/ListSecurityPolicies/
// UpdateSecurityPolicy/DeleteSecurityPolicy operation family discriminated by
// a "type" request field (encryption|network) -- there is no separate
// Create/Get/List/UpdateEncryptionPolicy or Create/List/DeleteNetworkPolicy
// operation in the real API at any SDK version. An earlier pass invented
// those names; they were never real. The route handlers underneath
// (handleServerlessEncryptionPolicyRoutes/handleServerlessNetworkPolicyRoutes)
// still work the same way over HTTP -- only the reported operation names
// here were corrected to match the real SDK.
func serverlessOperations() []string {
	return []string{
		"BatchGetCollection",
		"CreateAccessPolicy",
		"CreateCollection",
		"CreateSecurityConfig",
		"CreateSecurityPolicy",
		"DeleteAccessPolicy",
		"DeleteCollection",
		"DeleteSecurityConfig",
		"DeleteSecurityPolicy",
		"GetAccessPolicy",
		"GetSecurityConfig",
		"GetSecurityPolicy",
		"ListAccessPolicies",
		"ListCollections",
		"ListSecurityConfigs",
		"ListSecurityPolicies",
		"UpdateAccessPolicy",
		"UpdateSecurityConfig",
		"UpdateSecurityPolicy",
	}
}

// GetSupportedOperations returns supported operations.
func (h *Handler) GetSupportedOperations() []string {
	ops := domainOperations()
	ops = append(ops, connectionAndTagOperations()...)
	ops = append(ops, packageAndDataOperations()...)
	ops = append(ops, infraAndAppOperations()...)
	ops = append(ops, serverlessOperations()...)
	ops = append(ops, applicationDataMigrationOperations()...)

	return ops
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "es" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this OpenSearch instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// ExtractOperation returns the operation name from a request. It mirrors
// ServeHTTP's real dispatch tree op-for-op (gopherstack-l5ir): every branch
// here corresponds 1:1 to a route actually wired in handler.go and its
// per-family handler files, so this function's correctness is exercised by
// TestExtractOperation_SDKRouteTable in handler_paths_sdk_diff_test.go.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	if op, ok := strings.CutPrefix(c.Request().Header.Get("X-Amz-Target"), openSearchServerlessTargetPrefix); ok {
		return op
	}

	path := c.Request().URL.Path
	method := c.Request().Method

	if op := extractTagOrSoftwareOp(path, method); op != "" {
		return op
	}

	if path == openSearchDomainInfoPath && method == http.MethodPost {
		return "DescribeDomains"
	}

	if op := extractLegacyDomainOp(path, method); op != "" {
		return op
	}

	if op := extractNonDomainOperation(path, method); op != "" {
		return op
	}

	return extractDomainOperation(path, method)
}

// extractLegacyDomainOp handles the un-prefixed openSearchLegacyDomainPath
// root (ListDomainNames / ListPackagesForDomain).
func extractLegacyDomainOp(path, method string) string {
	if path == openSearchLegacyDomainPath && method == http.MethodGet {
		return "ListDomainNames"
	}

	if rest, ok := strings.CutPrefix(path, openSearchLegacyDomainPath+"/"); ok {
		if strings.HasSuffix(rest, "/packages") && method == http.MethodGet {
			return "ListPackagesForDomain"
		}
	}

	return ""
}

// extractNonDomainOperation derives the operation name from non-domain paths.
// Returns an empty string when the path does not match any known non-domain route.
func extractNonDomainOperation(path, method string) string {
	for _, fn := range []func(string, string) string{
		extractCCOp,
		extractDirectQueryOp,
		extractPackageOp,
		extractServiceSoftwareOp,
		extractDefaultAppSettingOp,
		extractApplicationOp,
		extractVersionsOrCompatibleOp,
		extractVpcEndpointsOp,
		extractReservedInstancesOp,
		extractUpgradeOp,
		extractApplicationDataMigrationOp,
	} {
		if op := fn(path, method); op != "" {
			return op
		}
	}

	return ""
}

// extractCCOp handles the cross-cluster inbound/outbound connection routes.
func extractCCOp(path, method string) string {
	rest, ok := strings.CutPrefix(path, openSearchCCPath)
	if !ok {
		return ""
	}

	if op := extractCCInboundOp(rest, method); op != "" {
		return op
	}

	return extractCCOutboundOp(rest, method)
}

// extractCCInboundOp handles the inboundConnection sub-routes.
func extractCCInboundOp(rest, method string) string {
	switch {
	case rest == "/inboundConnection/search" && method == http.MethodPost:
		return "DescribeInboundConnections"
	case strings.HasPrefix(rest, "/inboundConnection/") && strings.HasSuffix(rest, "/accept") &&
		method == http.MethodPut:
		return "AcceptInboundConnection"
	case strings.HasPrefix(rest, "/inboundConnection/") && strings.HasSuffix(rest, "/reject") &&
		method == http.MethodPut:
		return "RejectInboundConnection"
	case strings.HasPrefix(rest, "/inboundConnection/") && method == http.MethodDelete:
		return "DeleteInboundConnection"
	}

	return ""
}

// extractCCOutboundOp handles the outboundConnection sub-routes.
func extractCCOutboundOp(rest, method string) string {
	switch {
	case rest == "/outboundConnection/search" && method == http.MethodPost:
		return "DescribeOutboundConnections"
	case (rest == "/outboundConnection" || rest == "/outboundConnection/") && method == http.MethodPost:
		return "CreateOutboundConnection"
	case strings.HasPrefix(rest, "/outboundConnection/") && method == http.MethodDelete:
		return "DeleteOutboundConnection"
	}

	return ""
}

// extractDirectQueryOp handles the direct-query data source routes.
func extractDirectQueryOp(path, method string) string {
	rest, ok := strings.CutPrefix(path, openSearchDirectQueryPath)
	if !ok {
		return ""
	}

	switch {
	case (rest == "" || rest == "/") && method == http.MethodPost:
		return "AddDirectQueryDataSource"
	case (rest == "" || rest == "/") && method == http.MethodGet:
		return "ListDirectQueryDataSources"
	case strings.HasPrefix(rest, "/") && method == http.MethodGet:
		return "GetDirectQueryDataSource"
	case strings.HasPrefix(rest, "/") && method == http.MethodDelete:
		return "DeleteDirectQueryDataSource"
	case strings.HasPrefix(rest, "/") && method == http.MethodPut:
		return "UpdateDirectQueryDataSource"
	}

	return ""
}

// extractPackageOp handles package route operation extraction.
func extractPackageOp(path, method string) string {
	rest, ok := strings.CutPrefix(path, openSearchPackagesPath)
	if !ok {
		return ""
	}

	if op := extractPackageLiteralOrRootOp(rest, method); op != "" {
		return op
	}

	switch {
	case strings.HasPrefix(rest, "/associate/") && method == http.MethodPost:
		return "AssociatePackage"
	case strings.HasPrefix(rest, "/dissociate/") && method == http.MethodPost:
		return "DissociatePackage"
	case strings.HasSuffix(rest, "/history") && method == http.MethodGet:
		return "GetPackageVersionHistory"
	case strings.HasSuffix(rest, "/domains") && method == http.MethodGet:
		return "ListDomainsForPackage"
	case strings.HasPrefix(rest, "/") && !strings.Contains(strings.TrimPrefix(rest, "/"), "/") &&
		method == http.MethodDelete:
		return "DeletePackage"
	}

	return ""
}

// extractPackageLiteralOrRootOp handles the fixed-literal-action package
// paths and the bare /packages root.
func extractPackageLiteralOrRootOp(rest, method string) string {
	if (rest == "" || rest == "/") && method == http.MethodPost {
		return "CreatePackage"
	}

	if method != http.MethodPost {
		return ""
	}

	switch rest {
	case pathSuffixDescribe:
		return "DescribePackages"
	case pathSuffixUpdate:
		return "UpdatePackage"
	case "/updateScope":
		return "UpdatePackageScope"
	case "/associateMultiple":
		return "AssociatePackages"
	case "/dissociateMultiple":
		return "DissociatePackages"
	}

	return ""
}

// extractServiceSoftwareOp handles the serviceSoftwareUpdate prefix.
func extractServiceSoftwareOp(path, method string) string {
	rest, ok := strings.CutPrefix(path, openSearchServiceSwPath)
	if !ok || method != http.MethodPost {
		return ""
	}

	switch rest {
	case "/cancel":
		return "CancelServiceSoftwareUpdate"
	case "/rollback":
		return "RollbackServiceSoftwareUpdate"
	case "/start":
		return "StartServiceSoftwareUpdate"
	}

	return ""
}

// extractDefaultAppSettingOp handles the defaultApplicationSetting exact path.
func extractDefaultAppSettingOp(path, method string) string {
	if path != openSearchDefaultAppSettingPath {
		return ""
	}

	switch method {
	case http.MethodGet:
		return "GetDefaultApplicationSetting"
	case http.MethodPut:
		return "PutDefaultApplicationSetting"
	}

	return ""
}

// extractApplicationOp handles the /application prefix, including its
// data-source-attachment and capability sub-routes.
func extractApplicationOp(path, method string) string {
	if path == openSearchListApplicationsPath && method == http.MethodGet {
		return "ListApplications"
	}

	rest, ok := strings.CutPrefix(path, openSearchApplicationPath)
	if !ok {
		return ""
	}

	if (rest == "" || rest == "/") && method == http.MethodPost {
		return "CreateApplication"
	}

	_, subPath, hasSub := strings.Cut(strings.TrimPrefix(rest, "/"), "/")
	if !hasSub {
		return extractApplicationIDOp(rest, method)
	}

	if op := extractDataSourceAttachmentSubOp(subPath, method); op != "" {
		return op
	}

	return extractCapabilitySubOp(subPath, method)
}

// extractApplicationIDOp handles GET/DELETE/PUT on /application/{id}.
func extractApplicationIDOp(rest, method string) string {
	if rest == "" || strings.Contains(strings.TrimPrefix(rest, "/"), "/") {
		return ""
	}

	switch method {
	case http.MethodGet:
		return "GetApplication"
	case http.MethodDelete:
		return "DeleteApplication"
	case http.MethodPut:
		return "UpdateApplication"
	}

	return ""
}

// extractDataSourceAttachmentSubOp handles the four attach/detach/describe/
// list sub-paths.
func extractDataSourceAttachmentSubOp(subPath, method string) string {
	if method != http.MethodPost {
		return ""
	}

	switch subPath {
	case subPathAttachDataSource:
		return "AttachDataSource"
	case subPathDetachDataSource:
		return "DetachDataSource"
	case subPathDescribeDataSourceAttachment:
		return "DescribeDataSourceAttachment"
	case subPathListDataSourceAttachments:
		return "ListDataSourceAttachments"
	}

	return ""
}

// extractCapabilitySubOp handles the capability/{register,deregister/*,*} sub-paths.
func extractCapabilitySubOp(subPath, method string) string {
	switch {
	case subPath == "capability/register" && method == http.MethodPost:
		return "RegisterCapability"
	case strings.HasPrefix(subPath, "capability/deregister/") && method == http.MethodDelete:
		return "DeregisterCapability"
	case strings.HasPrefix(subPath, "capability/") && method == http.MethodGet:
		return "GetCapability"
	}

	return ""
}

// extractVersionsOrCompatibleOp handles the versions/compatibleVersions exact paths.
func extractVersionsOrCompatibleOp(path, method string) string {
	if method != http.MethodGet {
		return ""
	}

	switch path {
	case openSearchVersionsPath:
		return "ListVersions"
	case openSearchCompatiblePath:
		return "GetCompatibleVersions"
	}

	if strings.HasPrefix(path, openSearchInstanceTypesPath) {
		return "ListInstanceTypeDetails"
	}

	if strings.HasPrefix(path, openSearchInstanceTypeLimitsPath) {
		return "DescribeInstanceTypeLimits"
	}

	return ""
}

// extractVpcEndpointsOp handles the vpcEndpoints prefix.
func extractVpcEndpointsOp(path, method string) string {
	rest, ok := strings.CutPrefix(path, openSearchVpcEndpointsPath)
	if !ok {
		return ""
	}

	switch {
	case rest == pathSuffixDescribe && method == http.MethodPost:
		return "DescribeVpcEndpoints"
	case rest == pathSuffixUpdate && method == http.MethodPost:
		return "UpdateVpcEndpoint"
	case (rest == "" || rest == "/") && method == http.MethodPost:
		return "CreateVpcEndpoint"
	case (rest == "" || rest == "/") && method == http.MethodGet:
		return "ListVpcEndpoints"
	case strings.HasPrefix(rest, "/") && method == http.MethodDelete:
		return "DeleteVpcEndpoint"
	}

	return ""
}

// extractReservedInstancesOp handles reservedInstances and its sibling
// literal-action paths (offerings/purchase).
func extractReservedInstancesOp(path, method string) string {
	if method != http.MethodGet && method != http.MethodPost {
		return ""
	}

	switch path {
	case openSearchReservedOfferingsPath:
		if method == http.MethodGet {
			return "DescribeReservedInstanceOfferings"
		}
	case openSearchPurchaseReservedPath:
		if method == http.MethodPost {
			return "PurchaseReservedInstanceOffering"
		}
	case openSearchReservedPath:
		if method == http.MethodGet {
			return "DescribeReservedInstances"
		}
	}

	return ""
}

// extractUpgradeOp handles the upgradeDomain prefix.
func extractUpgradeOp(path, method string) string {
	rest, ok := strings.CutPrefix(path, openSearchUpgradePath)
	if !ok {
		return ""
	}

	switch {
	case (rest == "" || rest == "/") && method == http.MethodPost:
		return "UpgradeDomain"
	case strings.HasSuffix(rest, "/history") && method == http.MethodGet:
		return "GetUpgradeHistory"
	case strings.HasSuffix(rest, "/status") && method == http.MethodGet:
		return "GetUpgradeStatus"
	}

	return ""
}

// extractApplicationDataMigrationOp handles operation extraction for the
// insight and migration routes (see applicationDataMigrationOperations).
func extractApplicationDataMigrationOp(path, method string) string {
	if op := extractInsightOp(path, method); op != "" {
		return op
	}

	return extractMigrationOrRollbackOp(path, method)
}

// extractInsightOp handles the top-level insights/insight-details/
// insight-feedback routes.
func extractInsightOp(path, method string) string {
	switch {
	case path == openSearchInsightsPath && method == http.MethodPost:
		return "ListInsights"
	case path == openSearchInsightDetailsPath && method == http.MethodPost:
		return "DescribeInsightDetails"
	case path == openSearchInsightFeedbackPath && method == http.MethodPost:
		return "InsightFeedback"
	}

	return ""
}

// extractMigrationOrRollbackOp handles the app-migrations routes.
// RollbackServiceSoftwareUpdate is handled by extractServiceSoftwareOp, not
// here.
func extractMigrationOrRollbackOp(path, method string) string {
	switch {
	case path == openSearchAppMigrationsPath && method == http.MethodPost:
		return "StartMigration"
	case path == openSearchAppMigrationsPath && method == http.MethodGet:
		return "ListMigrations"
	case strings.HasPrefix(path, openSearchAppMigrationsPath+"/") && method == http.MethodGet:
		return "GetMigration"
	}

	return ""
}

// extractTagOrSoftwareOp handles tag route operation extraction.
func extractTagOrSoftwareOp(path, method string) string {
	switch {
	case path == openSearchTagsPath && method == http.MethodGet:
		return "ListTags"
	case path == openSearchTagsPath && method == http.MethodPost:
		return "AddTags"
	case path == openSearchTagsRemoval && method == http.MethodPost:
		return "RemoveTags"
	}

	return ""
}

// extractDomainOperation derives the operation name from domain-prefix paths.
func extractDomainOperation(path, method string) string {
	rest := strings.TrimPrefix(path, openSearchPathPrefix)

	switch {
	case rest == "" || rest == "/":
		if method == http.MethodPost {
			return "CreateDomain"
		}

		return opUnknown
	case strings.HasPrefix(rest, "/") && method == http.MethodGet:
		return extractDomainGetOp(rest)
	case strings.HasPrefix(rest, "/") && method == http.MethodDelete:
		return extractDomainDeleteOp(rest)
	case strings.HasPrefix(rest, "/") && method == http.MethodPost:
		return extractDomainPostOp(rest)
	case strings.HasPrefix(rest, "/") && method == http.MethodPut:
		return extractDomainPutOp(rest)
	}

	return opUnknown
}

// extractDomainGetOp derives the operation from a domain GET sub-route.
func extractDomainGetOp(rest string) string {
	trimmed := strings.TrimPrefix(rest, "/")

	if op := extractDomainGetStatusOp(trimmed); op != "" {
		return op
	}

	if op := extractDomainGetResourceOp(trimmed); op != "" {
		return op
	}

	if !strings.Contains(trimmed, "/") {
		return "DescribeDomain"
	}

	return opUnknown
}

// extractDomainGetStatusOp handles the status/health/vpc-access GET sub-routes.
func extractDomainGetStatusOp(trimmed string) string {
	switch {
	case strings.HasSuffix(trimmed, "/config"):
		return "DescribeDomainConfig"
	case strings.HasSuffix(trimmed, "/progress"):
		return "DescribeDomainChangeProgress"
	case strings.HasSuffix(trimmed, "/health"):
		return "DescribeDomainHealth"
	case strings.HasSuffix(trimmed, "/nodes"):
		return "DescribeDomainNodes"
	case strings.HasSuffix(trimmed, "/dryRun"):
		return "DescribeDryRunProgress"
	case strings.HasSuffix(trimmed, "/autoTunes"):
		return "DescribeDomainAutoTunes"
	case strings.HasSuffix(trimmed, "/vpcEndpoints"):
		return "ListVpcEndpointsForDomain"
	case strings.HasSuffix(trimmed, "/listVpcEndpointAccess"):
		return "ListVpcEndpointAccess"
	}

	return ""
}

// extractDomainGetResourceOp handles the data-source/maintenance/index/
// scheduled-action GET sub-routes.
func extractDomainGetResourceOp(trimmed string) string {
	switch {
	case strings.Contains(trimmed, "/dataSource/"):
		return "GetDataSource"
	case strings.HasSuffix(trimmed, "/domainMaintenance"):
		return "GetDomainMaintenanceStatus"
	case strings.Contains(trimmed, "/index/"):
		return "GetIndex"
	case strings.HasSuffix(trimmed, "/dataSource"):
		return "ListDataSources"
	case strings.HasSuffix(trimmed, "/domainMaintenances"):
		return "ListDomainMaintenances"
	case strings.HasSuffix(trimmed, "/scheduledActions"):
		return "ListScheduledActions"
	}

	return ""
}

// extractDomainDeleteOp derives the operation from a domain DELETE sub-route.
func extractDomainDeleteOp(rest string) string {
	trimmed := strings.TrimPrefix(rest, "/")

	switch {
	case strings.Contains(trimmed, "/dataSource/"):
		return "DeleteDataSource"
	case strings.Contains(trimmed, "/index/"):
		return "DeleteIndex"
	case !strings.Contains(trimmed, "/"):
		return "DeleteDomain"
	}

	return opUnknown
}

// extractDomainPostOp derives the operation from a domain POST sub-route.
func extractDomainPostOp(rest string) string {
	trimmed := strings.TrimPrefix(rest, "/")

	switch {
	case strings.HasSuffix(trimmed, "/dataSource"):
		return "AddDataSource"
	case strings.HasSuffix(trimmed, "/authorizeVpcEndpointAccess"):
		return "AuthorizeVpcEndpointAccess"
	case strings.HasSuffix(trimmed, "/config/cancel"):
		return "CancelDomainConfigChange"
	case strings.HasSuffix(trimmed, "/config"):
		return "UpdateDomainConfig"
	case strings.HasSuffix(trimmed, "/domainMaintenance"):
		return "StartDomainMaintenance"
	case strings.HasSuffix(trimmed, "/revokeVpcEndpointAccess"):
		return "RevokeVpcEndpointAccess"
	case strings.HasSuffix(trimmed, "/index"):
		return "CreateIndex"
	}

	return opUnknown
}

// extractDomainPutOp derives the operation from a domain PUT sub-route.
func extractDomainPutOp(rest string) string {
	trimmed := strings.TrimPrefix(rest, "/")

	switch {
	case strings.Contains(trimmed, "/index/"):
		return "UpdateIndex"
	case strings.HasSuffix(trimmed, "/scheduledAction/update"):
		return "UpdateScheduledAction"
	case strings.Contains(trimmed, "/dataSource/"):
		return "UpdateDataSource"
	}

	return opUnknown
}

// ExtractResource returns the primary resource identifier from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path

	// Cross-cluster: extract connection ID
	if strings.HasPrefix(path, openSearchCCPath) {
		rest := strings.TrimPrefix(path, openSearchCCPath+"/inboundConnection/")

		return strings.TrimSuffix(rest, "/accept")
	}

	// Direct query data source: no ID in path
	if strings.HasPrefix(path, openSearchDirectQueryPath) {
		return ""
	}

	// Packages: extract package ID
	if strings.HasPrefix(path, openSearchPackagesPath) {
		rest := strings.TrimPrefix(path, openSearchPackagesPath+"/associate/")
		parts := strings.SplitN(rest, "/", pkgPathParts)
		if len(parts) > 0 {
			return parts[0]
		}

		return ""
	}

	// Tag routes: no domain name in path
	if path == openSearchTagsPath || path == openSearchTagsRemoval {
		return ""
	}

	// Domain path prefix
	rest := strings.TrimPrefix(path, openSearchPathPrefix+"/")
	if rest == path {
		return ""
	}

	// Extract domain name (first segment)
	if before, _, ok := strings.Cut(rest, "/"); ok {
		return before
	}

	return strings.TrimSuffix(rest, "/")
}
