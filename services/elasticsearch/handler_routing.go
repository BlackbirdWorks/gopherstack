package elasticsearch

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// ExtractOperation returns the operation name from a request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

	if op := extractNonDomainOperation(path, method); op != "" {
		return op
	}

	return extractDomainOperation(strings.TrimPrefix(path, elasticsearchPathPrefix), method)
}

// extractNonDomainOperation returns the operation name for paths not under the domain prefix.
func extractNonDomainOperation(path, method string) string {
	if op := extractTagAndServiceOperation(path, method); op != "" {
		return op
	}

	return extractCCSVpcPackageOperation(path, method)
}

// extractTagAndServiceOperation handles tag, domain-info, role, and software-update paths.
func extractTagAndServiceOperation(path, method string) string {
	switch {
	case path == elasticsearchDomainInfo && method == http.MethodPost:
		return "DescribeElasticsearchDomains"
	case path == elasticsearchTagsPath && method == http.MethodGet:
		return "ListTags"
	case path == elasticsearchTagsPath && method == http.MethodPost:
		return "AddTags"
	case path == elasticsearchTagsRemove && method == http.MethodPost:
		return "RemoveTags"
	case path == elasticsearchServiceRole && method == http.MethodDelete:
		return "DeleteElasticsearchServiceRole"
	case path == elasticsearchSoftwareUpdate+"/cancel" && method == http.MethodPost:
		return "CancelElasticsearchServiceSoftwareUpdate"
	}

	return ""
}

// extractCCSVpcPackageOperation handles CCS, VPC endpoint, and package paths.
func extractCCSVpcPackageOperation(path, method string) string {
	if op := extractCCSOperation(path, method); op != "" {
		return op
	}

	if op := extractVpcPackageOperation(path, method); op != "" {
		return op
	}

	return extractUpgradeAndMiscOperation(path, method)
}

// extractCCSOperation handles Cross-Cluster Search operations.
func extractCCSOperation(path, method string) string {
	if strings.HasPrefix(path, elasticsearchCCSInbound+"/") {
		return extractCCSInboundOp(path, method)
	}

	return extractCCSOutboundOp(path, method)
}

// extractCCSInboundOp handles inbound CCS operations.
func extractCCSInboundOp(path, method string) string {
	switch {
	case strings.HasSuffix(path, "/accept") && method == http.MethodPut:
		return "AcceptInboundCrossClusterSearchConnection"
	case strings.HasSuffix(path, "/reject") && method == http.MethodPut:
		return "RejectInboundCrossClusterSearchConnection"
	case method == http.MethodDelete:
		return "DeleteInboundCrossClusterSearchConnection"
	}

	return ""
}

// extractCCSOutboundOp handles outbound CCS operations.
func extractCCSOutboundOp(path, method string) string {
	switch {
	case path == elasticsearchCCSInboundSearch && method == http.MethodPost:
		return "DescribeInboundCrossClusterSearchConnections"
	case path == elasticsearchCCSOutbound && method == http.MethodPost:
		return "CreateOutboundCrossClusterSearchConnection"
	case strings.HasPrefix(path, elasticsearchCCSOutbound+"/") && method == http.MethodDelete:
		return "DeleteOutboundCrossClusterSearchConnection"
	case path == elasticsearchCCSOutboundSearch && method == http.MethodPost:
		return "DescribeOutboundCrossClusterSearchConnections"
	}

	return ""
}

// extractVpcPackageOperation handles VPC endpoint and package operations.
func extractVpcPackageOperation(path, method string) string {
	if op := extractVpcEndpointOperation(path, method); op != "" {
		return op
	}

	return extractPackageOperation(path, method)
}

// extractVpcEndpointOperation handles VPC endpoint operations.
func extractVpcEndpointOperation(path, method string) string {
	switch {
	case path == elasticsearchVpcEndpoints && method == http.MethodPost:
		return "CreateVpcEndpoint"
	case path == elasticsearchVpcEndpoints && method == http.MethodGet:
		return "ListVpcEndpoints"
	case path == elasticsearchVpcEndpoints+"/describe" && method == http.MethodPost:
		return "DescribeVpcEndpoints"
	case path == elasticsearchVpcEndpoints+"/update" && method == http.MethodPost:
		return "UpdateVpcEndpoint"
	case strings.HasPrefix(path, elasticsearchVpcEndpoints+"/") && method == http.MethodDelete:
		return "DeleteVpcEndpoint"
	}

	return ""
}

// extractPackageOperation handles package operations.
func extractPackageOperation(path, method string) string {
	if op := extractPackageCRUDOp(path, method); op != "" {
		return op
	}

	return extractPackageDomainOp(path, method)
}

// extractPackageCRUDOp handles package CRUD and association operations.
func extractPackageCRUDOp(path, method string) string {
	switch {
	case strings.HasPrefix(path, elasticsearchPackages+"/associate/") && method == http.MethodPost:
		return "AssociatePackage"
	case strings.HasPrefix(path, elasticsearchPackages+"/dissociate/") && method == http.MethodPost:
		return "DissociatePackage"
	case path == elasticsearchPackages && method == http.MethodPost:
		return "CreatePackage"
	case path == elasticsearchPackages+"/describe" && method == http.MethodPost:
		return "DescribePackages"
	case path == elasticsearchPackages+"/update" && method == http.MethodPost:
		return "UpdatePackage"
	}

	return ""
}

// extractPackageDomainOp handles package history, domain listing, and domain-package operations.
func extractPackageDomainOp(path, method string) string {
	switch {
	case strings.HasPrefix(path, elasticsearchPackages+"/") &&
		strings.HasSuffix(path, "/history") &&
		method == http.MethodGet:
		return "GetPackageVersionHistory"
	case strings.HasPrefix(path, elasticsearchPackages+"/") &&
		strings.HasSuffix(path, "/domains") &&
		method == http.MethodGet:
		return "ListDomainsForPackage"
	case strings.HasPrefix(path, elasticsearchPackages+"/") && method == http.MethodDelete:
		return "DeletePackage"
	case strings.HasPrefix(path, elasticsearchDomainPackages+"/") &&
		strings.HasSuffix(path, "/packages") &&
		method == http.MethodGet:
		return "ListPackagesForDomain"
	case path == elasticsearchDomainPackages && method == http.MethodGet:
		return opListDomainNames
	}

	return ""
}

// extractUpgradeAndMiscOperation handles upgrade, instance, reserved, and software update operations.
func extractUpgradeAndMiscOperation(path, method string) string {
	if op := extractVersionAndInstanceOp(path, method); op != "" {
		return op
	}

	return extractUpgradeOp(path, method)
}

// extractVersionAndInstanceOp handles version, instance type, and reserved instance operations.
func extractVersionAndInstanceOp(path, method string) string {
	switch {
	case path == elasticsearchCompatibleVersions && method == http.MethodGet:
		return "GetCompatibleElasticsearchVersions"
	case path == elasticsearchVersions && method == http.MethodGet:
		return "ListElasticsearchVersions"
	case strings.HasPrefix(path, elasticsearchInstanceTypes+"/") && method == http.MethodGet:
		return "ListElasticsearchInstanceTypes"
	case strings.HasPrefix(path, elasticsearchInstanceTypeLimits+"/") && method == http.MethodGet:
		return "DescribeElasticsearchInstanceTypeLimits"
	case path == elasticsearchReservedOfferings && method == http.MethodGet:
		return "DescribeReservedElasticsearchInstanceOfferings"
	case path == elasticsearchReservedInstances && method == http.MethodGet:
		return "DescribeReservedElasticsearchInstances"
	case path == elasticsearchPurchaseReserved && method == http.MethodPost:
		return "PurchaseReservedElasticsearchInstanceOffering"
	}

	return ""
}

// extractUpgradeOp handles upgrade and software update operations.
func extractUpgradeOp(path, method string) string {
	switch {
	case path == elasticsearchUpgradeDomain && method == http.MethodPost:
		return "UpgradeElasticsearchDomain"
	case strings.HasPrefix(path, elasticsearchUpgradeDomain+"/") &&
		strings.HasSuffix(path, "/history") &&
		method == http.MethodGet:
		return "GetUpgradeHistory"
	case strings.HasPrefix(path, elasticsearchUpgradeDomain+"/") &&
		strings.HasSuffix(path, "/status") &&
		method == http.MethodGet:
		return "GetUpgradeStatus"
	case path == elasticsearchSoftwareUpdate+"/start" && method == http.MethodPost:
		return "StartElasticsearchServiceSoftwareUpdate"
	}

	return ""
}

// extractDomainOperation returns the operation name for paths under the domain prefix.
// rest is the path with the domain prefix stripped.
func extractDomainOperation(rest, method string) string {
	if rest == "" || rest == "/" {
		return extractRootDomainOperation(method)
	}

	if !strings.HasPrefix(rest, "/") {
		return opUnknown
	}

	return extractSubDomainOperation(rest, method)
}

// extractSubDomainOperation resolves operations on specific domain paths (rest starts with "/").
func extractSubDomainOperation(rest, method string) string {
	if op := extractSubDomainSuffixOp(rest, method); op != opUnknown {
		return op
	}

	return extractSubDomainMethodOp(method)
}

// extractSubDomainSuffixOp matches sub-domain operations by path suffix.
func extractSubDomainSuffixOp(rest, method string) string {
	if op := extractSubDomainConfigOp(rest, method); op != opUnknown {
		return op
	}

	return extractSubDomainListingOp(rest, method)
}

// extractSubDomainConfigOp handles config, cancel, and VPC authorization operations.
func extractSubDomainConfigOp(rest, method string) string {
	switch {
	case strings.HasSuffix(rest, "/config/cancel") && method == http.MethodPost:
		return "CancelDomainConfigChange"
	case strings.HasSuffix(rest, "/authorizeVpcEndpointAccess") && method == http.MethodPost:
		return "AuthorizeVpcEndpointAccess"
	case strings.HasSuffix(rest, "/revokeVpcEndpointAccess") && method == http.MethodPost:
		return "RevokeVpcEndpointAccess"
	case strings.HasSuffix(rest, "/config") && method == http.MethodPost:
		return "UpdateElasticsearchDomainConfig"
	case strings.HasSuffix(rest, "/config"):
		return "DescribeElasticsearchDomainConfig"
	}

	return opUnknown
}

// extractSubDomainListingOp handles auto-tunes, progress, and VPC endpoint listing operations.
func extractSubDomainListingOp(rest, method string) string {
	switch {
	case strings.HasSuffix(rest, "/autoTunes") && method == http.MethodGet:
		return "DescribeDomainAutoTunes"
	case strings.HasSuffix(rest, "/progress") && method == http.MethodGet:
		return "DescribeDomainChangeProgress"
	case strings.HasSuffix(rest, "/listVpcEndpointAccess") && method == http.MethodGet:
		return "ListVpcEndpointAccess"
	case strings.HasSuffix(rest, "/vpcEndpoints") && method == http.MethodGet:
		return "ListVpcEndpointsForDomain"
	}

	return opUnknown
}

// extractSubDomainMethodOp matches sub-domain operations by HTTP method alone.
func extractSubDomainMethodOp(method string) string {
	switch method {
	case http.MethodGet:
		return "DescribeElasticsearchDomain"
	case http.MethodDelete:
		return "DeleteElasticsearchDomain"
	}

	return opUnknown
}

// extractRootDomainOperation returns the operation for the bare domain-prefix
// path ("/2015-01-01/es/domain", no name segment). POST creates a domain.
// GET is ListDomainNames served as a same-resource convenience alias of the
// real AWS "/2015-01-01/domain" resource (see extractPackageDomainOp and the
// matching buildOps entry) -- both paths report the same operation name.
func extractRootDomainOperation(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateElasticsearchDomain"
	case http.MethodGet:
		return opListDomainNames
	}

	return opUnknown
}
