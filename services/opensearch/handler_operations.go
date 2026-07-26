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

func serverlessOperations() []string {
	return []string{
		"BatchGetCollection",
		"CreateAccessPolicy",
		"CreateCollection",
		"CreateEncryptionPolicy",
		"CreateNetworkPolicy",
		"CreateSecurityConfig",
		"DeleteAccessPolicy",
		"DeleteCollection",
		"DeleteEncryptionPolicy",
		"DeleteNetworkPolicy",
		"DeleteSecurityConfig",
		"GetAccessPolicy",
		"GetEncryptionPolicy",
		"GetSecurityConfig",
		"ListAccessPolicies",
		"ListCollections",
		"ListEncryptionPolicies",
		"ListNetworkPolicies",
		"ListSecurityConfigs",
		"UpdateAccessPolicy",
		"UpdateEncryptionPolicy",
		"UpdateSecurityConfig",
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

// ExtractOperation returns the operation name from a request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

	if op := extractNonDomainOperation(path, method); op != "" {
		return op
	}

	return extractDomainOperation(path, method)
}

// extractNonDomainOperation derives the operation name from non-domain paths.
// Returns an empty string when the path does not match any known non-domain route.
func extractNonDomainOperation(path, method string) string {
	if op := extractCCOrDirectQueryOp(path, method); op != "" {
		return op
	}

	if op := extractPackageOp(path, method); op != "" {
		return op
	}

	if op := extractTagOrSoftwareOp(path, method); op != "" {
		return op
	}

	return extractApplicationDataMigrationOp(path, method)
}

// extractApplicationDataMigrationOp handles operation extraction for the
// newer data source attachment, capability, insight, and migration routes
// (see applicationDataMigrationOperations).
func extractApplicationDataMigrationOp(path, method string) string {
	if op := extractDataSourceAttachmentOrCapabilityOp(path, method); op != "" {
		return op
	}

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

// extractMigrationOrRollbackOp handles the app-migrations routes and
// RollbackServiceSoftwareUpdate.
func extractMigrationOrRollbackOp(path, method string) string {
	switch {
	case path == openSearchAppMigrationsPath && method == http.MethodPost:
		return "StartMigration"
	case path == openSearchAppMigrationsPath && method == http.MethodGet:
		return "ListMigrations"
	case strings.HasPrefix(path, openSearchAppMigrationsPath+"/") && method == http.MethodGet:
		return "GetMigration"
	case path == openSearchServiceSwPath+"/rollback" && method == http.MethodPost:
		return "RollbackServiceSoftwareUpdate"
	}

	return ""
}

// extractDataSourceAttachmentOrCapabilityOp handles the
// /application/{id}/{attachDataSource,...,capability/...} sub-routes.
func extractDataSourceAttachmentOrCapabilityOp(path, method string) string {
	after, ok := strings.CutPrefix(path, openSearchApplicationPath+"/")
	if !ok {
		return ""
	}

	_, subPath, ok := strings.Cut(after, "/")
	if !ok {
		return ""
	}

	if op := extractDataSourceAttachmentSubOp(subPath, method); op != "" {
		return op
	}

	return extractCapabilitySubOp(subPath, method)
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

// extractCCOrDirectQueryOp handles cross-cluster and direct-query operation extraction.
func extractCCOrDirectQueryOp(path, method string) string {
	switch {
	case strings.HasPrefix(path, openSearchCCPath) &&
		strings.Contains(path, "/inboundConnection/") && strings.HasSuffix(path, "/accept") &&
		method == http.MethodPut:
		return "AcceptInboundConnection"
	case strings.HasPrefix(path, openSearchDirectQueryPath) && method == http.MethodPost:
		return "AddDirectQueryDataSource"
	case (path == openSearchApplicationPath || path == openSearchApplicationPath+"/") && method == http.MethodPost:
		return "CreateApplication"
	case path == openSearchServiceSwPath+"/cancel" && method == http.MethodPost:
		return "CancelServiceSoftwareUpdate"
	}

	return ""
}

// extractPackageOp handles package route operation extraction.
func extractPackageOp(path, method string) string {
	after, ok := strings.CutPrefix(path, openSearchPackagesPath)
	if !ok {
		return ""
	}

	if strings.HasPrefix(after, "/associate/") && method == http.MethodPost {
		return "AssociatePackage"
	}

	if after == "/associateMultiple" && method == http.MethodPost {
		return "AssociatePackages"
	}

	return ""
}

// extractTagOrSoftwareOp handles tag and service-software route operation extraction.
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

		if method == http.MethodGet {
			return "ListDomainNames"
		}

		return opUnknown
	case strings.HasPrefix(rest, "/") && method == http.MethodGet:
		return "DescribeDomain"
	case strings.HasPrefix(rest, "/") && method == http.MethodDelete:
		return "DeleteDomain"
	case strings.HasPrefix(rest, "/") && method == http.MethodPost:
		return extractDomainSubOperation(rest)
	}

	return opUnknown
}

// extractDomainSubOperation derives the operation from a domain POST sub-route.
func extractDomainSubOperation(rest string) string {
	trimmed := strings.TrimPrefix(rest, "/")

	switch {
	case strings.HasSuffix(trimmed, "/dataSource"):
		return "AddDataSource"
	case strings.HasSuffix(trimmed, "/authorizeVpcEndpointAccess"):
		return "AuthorizeVpcEndpointAccess"
	case strings.HasSuffix(trimmed, "/config/cancel"):
		return "CancelDomainConfigChange"
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
