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

	return extractTagOrSoftwareOp(path, method)
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
	case strings.HasPrefix(path, openSearchApplicationPath) && method == http.MethodPost:
		return "CreateApplication"
	case strings.HasPrefix(path, openSearchServiceSwPath) && method == http.MethodPost:
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
