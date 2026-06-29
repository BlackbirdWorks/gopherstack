package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	keyInstanceType           = "InstanceType"
	keyInstanceCount          = "InstanceCount"
	keyEBSEnabled             = "EBSEnabled"
	keyVolumeSize             = "VolumeSize"
	keyVolumeType             = "VolumeType"
	keyIops                   = "Iops"
	keyThroughput             = "Throughput"
	keyDedicatedMasterEnabled = "DedicatedMasterEnabled"
	keyDedicatedMasterType    = "DedicatedMasterType"
	keyDedicatedMasterCount   = "DedicatedMasterCount"
	keyZoneAwarenessEnabled   = "ZoneAwarenessEnabled"
	keyZoneAwarenessConfig    = "ZoneAwarenessConfig"
	keyWarmEnabled            = "WarmEnabled"
	keyWarmType               = "WarmType"
	keyWarmCount              = "WarmCount"
	keyColdStorageEnabled     = "ColdStorageEnabled"

	keyCrossClusterSearchConnection = "CrossClusterSearchConnection"
	minimumInstanceCount            = 1
	maximumInstanceCount            = 20

	maxTagKeyLen           = 128
	maxTagValueLen         = 256
	maxTagsPerResource     = 50
	maxDescribeDomainNames = 5
)

const (
	elasticsearchPathPrefix     = "/2015-01-01/es/domain"
	elasticsearchTagsPath       = "/2015-01-01/tags"
	elasticsearchTagsRemove     = "/2015-01-01/tags-removal"
	elasticsearchDomainInfo     = "/2015-01-01/es/domain-info"
	elasticsearchServiceRole    = "/2015-01-01/es/role"
	elasticsearchSoftwareUpdate = "/2015-01-01/es/serviceSoftwareUpdate"
	elasticsearchCCSInbound     = "/2015-01-01/es/ccs/inboundConnection"
	elasticsearchCCSOutbound    = "/2015-01-01/es/ccs/outboundConnection"
	elasticsearchVpcEndpoints   = "/2015-01-01/es/vpcEndpoints"
	elasticsearchPackages       = "/2015-01-01/packages"
	elasticsearchDomainPackages = "/2015-01-01/domain"

	opUnknown = "Unknown"
)

const (
	elasticsearchCCSInboundSearch   = "/2015-01-01/es/ccs/inboundConnection/search"
	elasticsearchCCSOutboundSearch  = "/2015-01-01/es/ccs/outboundConnection/search"
	elasticsearchUpgradeDomain      = "/2015-01-01/es/upgradeDomain"
	elasticsearchCompatibleVersions = "/2015-01-01/es/compatibleVersions"
	elasticsearchVersions           = "/2015-01-01/es/versions"
	elasticsearchInstanceTypes      = "/2015-01-01/es/instanceTypes"
	elasticsearchInstanceTypeLimits = "/2015-01-01/es/instanceTypeLimits"
	elasticsearchReservedOfferings  = "/2015-01-01/es/reservedInstanceOfferings"
	elasticsearchReservedInstances  = "/2015-01-01/es/reservedInstances"
	elasticsearchPurchaseReserved   = "/2015-01-01/es/purchaseReservedInstanceOffering"
)

// Handler is the HTTP handler for Elasticsearch operations.
type Handler struct {
	Backend   *InMemoryBackend
	ops       map[string]http.HandlerFunc
	AccountID string
	Region    string
}

// NewHandler creates a new Elasticsearch Handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// buildOps builds the cached dispatch table for fixed-path Elasticsearch routes.
// Routes with dynamic path segments (e.g., domain name, connection ID) are
// handled separately via the domain and prefix routers.
func (h *Handler) buildOps() map[string]http.HandlerFunc {
	describeInbound := h.handleDescribeInboundCrossClusterSearchConnections
	describeOutbound := h.handleDescribeOutboundCrossClusterSearchConnections
	describeReservedOfferings := h.handleDescribeReservedElasticsearchInstanceOfferings
	purchaseReserved := h.handlePurchaseReservedElasticsearchInstanceOffering

	return map[string]http.HandlerFunc{
		http.MethodPost + " " + elasticsearchDomainInfo:                 h.handleDescribeElasticsearchDomains,
		http.MethodGet + " " + elasticsearchTagsPath:                    h.handleListTags,
		http.MethodPost + " " + elasticsearchTagsPath:                   h.handleAddTags,
		http.MethodPost + " " + elasticsearchTagsRemove:                 h.handleRemoveTags,
		http.MethodDelete + " " + elasticsearchServiceRole:              h.handleDeleteElasticsearchServiceRole,
		http.MethodPost + " " + elasticsearchSoftwareUpdate + "/cancel": h.handleCancelElasticsearchServiceSoftwareUpdate,
		http.MethodPost + " " + elasticsearchSoftwareUpdate:             h.handleStartElasticsearchServiceSoftwareUpdate,
		http.MethodPost + " " + elasticsearchCCSOutbound:                h.handleCreateOutboundCrossClusterSearchConnection,
		http.MethodPost + " " + elasticsearchVpcEndpoints:               h.handleCreateVpcEndpoint,
		http.MethodPost + " " + elasticsearchPackages:                   h.handleCreatePackage,
		http.MethodPost + " " + elasticsearchCCSInboundSearch:           describeInbound,
		http.MethodPost + " " + elasticsearchCCSOutboundSearch:          describeOutbound,
		http.MethodPost + " " + elasticsearchPackages + "/describe":     h.handleDescribePackages,
		http.MethodPost + " " + elasticsearchPackages + "/update":       h.handleUpdatePackage,
		http.MethodPost + " " + elasticsearchVpcEndpoints + "/describe": h.handleDescribeVpcEndpoints,
		http.MethodPost + " " + elasticsearchVpcEndpoints + "/update":   h.handleUpdateVpcEndpoint,
		http.MethodGet + " " + elasticsearchVpcEndpoints:                h.handleListVpcEndpoints,
		http.MethodGet + " " + elasticsearchCompatibleVersions:          h.handleGetCompatibleElasticsearchVersions,
		http.MethodGet + " " + elasticsearchVersions:                    h.handleListElasticsearchVersions,
		http.MethodGet + " " + elasticsearchReservedOfferings:           describeReservedOfferings,
		http.MethodGet + " " + elasticsearchReservedInstances:           h.handleDescribeReservedElasticsearchInstances,
		http.MethodPost + " " + elasticsearchPurchaseReserved:           purchaseReserved,
		http.MethodPost + " " + elasticsearchUpgradeDomain:              h.handleUpgradeElasticsearchDomain,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Elasticsearch" }

// reqContext returns the request context with the SigV4-derived AWS region
// attached so backend operations route to the correct per-region store.
func (h *Handler) reqContext(r *http.Request) context.Context {
	region := httputils.ExtractRegionFromRequest(r, h.Backend.Region())

	return context.WithValue(r.Context(), regionContextKey{}, region)
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathSubdomain }

// RouteMatcher returns a matcher that selects Elasticsearch requests by path prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return matchElasticsearchPath(c.Request().URL.Path)
	}
}

// matchElasticsearchPath returns true if the path matches a known Elasticsearch API path.
func matchElasticsearchPath(path string) bool {
	return matchElasticsearchCorePaths(path) || matchElasticsearchExtPaths(path)
}

// matchElasticsearchCorePaths returns true if path matches core Elasticsearch paths.
func matchElasticsearchCorePaths(path string) bool {
	return strings.HasPrefix(path, elasticsearchPathPrefix) ||
		path == elasticsearchDomainInfo ||
		path == elasticsearchTagsPath ||
		path == elasticsearchTagsRemove ||
		path == elasticsearchServiceRole ||
		strings.HasPrefix(path, elasticsearchSoftwareUpdate) ||
		strings.HasPrefix(path, elasticsearchCCSInbound) ||
		path == elasticsearchCCSOutbound ||
		path == elasticsearchVpcEndpoints ||
		strings.HasPrefix(path, elasticsearchVpcEndpoints+"/")
}

// matchElasticsearchExtPaths returns true if path matches extended Elasticsearch paths.
func matchElasticsearchExtPaths(path string) bool {
	return strings.HasPrefix(path, elasticsearchPackages) ||
		strings.HasPrefix(path, elasticsearchDomainPackages+"/") ||
		strings.HasPrefix(path, elasticsearchUpgradeDomain) ||
		path == elasticsearchCompatibleVersions ||
		path == elasticsearchVersions ||
		strings.HasPrefix(path, elasticsearchInstanceTypes) ||
		strings.HasPrefix(path, elasticsearchInstanceTypeLimits) ||
		path == elasticsearchReservedOfferings ||
		path == elasticsearchReservedInstances ||
		path == elasticsearchPurchaseReserved
}

// GetSupportedOperations returns supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AcceptInboundCrossClusterSearchConnection",
		"AddTags",
		"AssociatePackage",
		"AuthorizeVpcEndpointAccess",
		"CancelDomainConfigChange",
		"CancelElasticsearchServiceSoftwareUpdate",
		"CreateElasticsearchDomain",
		"CreateOutboundCrossClusterSearchConnection",
		"CreatePackage",
		"CreateVpcEndpoint",
		"DeleteElasticsearchDomain",
		"DeleteElasticsearchServiceRole",
		"DeleteInboundCrossClusterSearchConnection",
		"DeleteOutboundCrossClusterSearchConnection",
		"DeletePackage",
		"DeleteVpcEndpoint",
		"DescribeDomainAutoTunes",
		"DescribeDomainChangeProgress",
		"DescribeElasticsearchDomain",
		"DescribeElasticsearchDomainConfig",
		"DescribeElasticsearchDomains",
		"DescribeElasticsearchInstanceTypeLimits",
		"DescribeInboundCrossClusterSearchConnections",
		"DescribeOutboundCrossClusterSearchConnections",
		"DescribePackages",
		"DescribeReservedElasticsearchInstanceOfferings",
		"DescribeReservedElasticsearchInstances",
		"DescribeVpcEndpoints",
		"DissociatePackage",
		"GetCompatibleElasticsearchVersions",
		"GetPackageVersionHistory",
		"GetUpgradeHistory",
		"GetUpgradeStatus",
		"ListDomainNames",
		"ListDomainsForPackage",
		"ListElasticsearchInstanceTypes",
		"ListElasticsearchVersions",
		"ListPackagesForDomain",
		"ListTags",
		"ListVpcEndpointAccess",
		"ListVpcEndpoints",
		"ListVpcEndpointsForDomain",
		"PurchaseReservedElasticsearchInstanceOffering",
		"RejectInboundCrossClusterSearchConnection",
		"RemoveTags",
		"RevokeVpcEndpointAccess",
		"StartElasticsearchServiceSoftwareUpdate",
		"UpdateElasticsearchDomainConfig",
		"UpdatePackage",
		"UpdateVpcEndpoint",
		"UpgradeElasticsearchDomain",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "es" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Elasticsearch instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

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
	case path == elasticsearchSoftwareUpdate && method == http.MethodPost:
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

func extractRootDomainOperation(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateElasticsearchDomain"
	case http.MethodGet:
		return "ListDomainNames"
	}

	return opUnknown
}

// ExtractResource returns the domain name from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path
	rest := strings.TrimPrefix(path, elasticsearchPathPrefix+"/")

	if rest == path {
		return ""
	}

	return strings.TrimSuffix(rest, "/")
}

// domainZoneAwarenessConfig holds zone awareness sub-config.
type domainZoneAwarenessConfig struct {
	AvailabilityZoneCount int `json:"AvailabilityZoneCount"`
}

// domainClusterConfig holds the cluster configuration request parameters.
type domainClusterConfig struct {
	ZoneAwarenessConfig    *domainZoneAwarenessConfig `json:"ZoneAwarenessConfig,omitempty"`
	InstanceType           string                     `json:"InstanceType"`
	DedicatedMasterType    string                     `json:"DedicatedMasterType,omitempty"`
	WarmType               string                     `json:"WarmType,omitempty"`
	InstanceCount          int                        `json:"InstanceCount"`
	DedicatedMasterCount   int                        `json:"DedicatedMasterCount,omitempty"`
	WarmCount              int                        `json:"WarmCount,omitempty"`
	DedicatedMasterEnabled bool                       `json:"DedicatedMasterEnabled"`
	ZoneAwarenessEnabled   bool                       `json:"ZoneAwarenessEnabled"`
	WarmEnabled            bool                       `json:"WarmEnabled"`
	ColdStorageEnabled     bool                       `json:"ColdStorageEnabled"`
}

// domainEBSOptions holds the EBS options request parameters.
type domainEBSOptions struct {
	VolumeType string `json:"VolumeType"`
	VolumeSize int    `json:"VolumeSize"`
	Iops       int    `json:"Iops"`
	Throughput int    `json:"Throughput"`
	EBSEnabled bool   `json:"EBSEnabled"`
}

// domainSnapshotOptions holds snapshot configuration in requests/responses.
type domainSnapshotOptions struct {
	AutomatedSnapshotStartHour int `json:"AutomatedSnapshotStartHour"`
}

// domainEncryptionAtRestOptions holds encryption at rest configuration.
type domainEncryptionAtRestOptions struct {
	KmsKeyID string `json:"KmsKeyId,omitempty"`
	Enabled  bool   `json:"Enabled"`
}

// domainNodeToNodeEncryptionOptions holds node-to-node encryption configuration.
type domainNodeToNodeEncryptionOptions struct {
	Enabled bool `json:"Enabled"`
}

// domainEndpointOptions holds HTTPS/TLS endpoint configuration.
type domainEndpointOptions struct {
	TLSSecurityPolicy string `json:"TLSSecurityPolicy,omitempty"`
	EnforceHTTPS      bool   `json:"EnforceHTTPS"`
}

// domainJSON is the JSON request body for CreateElasticsearchDomain.
type domainJSON struct {
	ClusterConfig        *domainClusterConfig               `json:"ElasticsearchClusterConfig"`
	EBSOptions           *domainEBSOptions                  `json:"EBSOptions"`
	SnapshotOptions      *domainSnapshotOptions             `json:"SnapshotOptions"`
	EncryptionAtRest     *domainEncryptionAtRestOptions     `json:"EncryptionAtRestOptions"`
	NodeToNodeEncryption *domainNodeToNodeEncryptionOptions `json:"NodeToNodeEncryptionOptions"`
	DomainEndpointOpts   *domainEndpointOptions             `json:"DomainEndpointOptions"`
	AdvancedOptions      map[string]string                  `json:"AdvancedOptions"`
	DomainName           string                             `json:"DomainName"`
	ElasticsearchVersion string                             `json:"ElasticsearchVersion"`
	AccessPolicies       string                             `json:"AccessPolicies"`
}

// domainStatusJSON is the JSON response for domain operations.
type domainStatusJSON struct { //nolint:govet // fieldalignment: readability over micro-optimization
	ElasticsearchClusterConfig  clusterConfigJSON                 `json:"ElasticsearchClusterConfig"`
	EBSOptions                  ebsOptionsJSON                    `json:"EBSOptions"`
	CognitoOptions              cognitoOptionsJSON                `json:"CognitoOptions"`
	SnapshotOptions             domainSnapshotOptions             `json:"SnapshotOptions"`
	EncryptionAtRestOptions     domainEncryptionAtRestOptions     `json:"EncryptionAtRestOptions"`
	NodeToNodeEncryptionOptions domainNodeToNodeEncryptionOptions `json:"NodeToNodeEncryptionOptions"`
	DomainEndpointOptions       domainEndpointOptions             `json:"DomainEndpointOptions"`
	AdvancedOptions             map[string]string                 `json:"AdvancedOptions"`
	DomainName                  string                            `json:"DomainName"`
	DomainID                    string                            `json:"DomainId"`
	ARN                         string                            `json:"ARN"`
	ElasticsearchVersion        string                            `json:"ElasticsearchVersion"`
	Endpoint                    string                            `json:"Endpoint"`
	DomainProcessingStatus      string                            `json:"DomainProcessingStatus"`
	AccessPolicies              string                            `json:"AccessPolicies"`
	Processing                  bool                              `json:"Processing"`
}

// cognitoOptionsJSON is the JSON representation of Cognito options.
// The Terraform provider's flattenCognitoOptions does not guard against nil,
// so we always return this field with Enabled=false when Cognito is not configured.
type cognitoOptionsJSON struct {
	Enabled bool `json:"Enabled"`
}

// ebsOptionsJSON is the JSON representation of EBS options.
type ebsOptionsJSON struct {
	VolumeType string `json:"VolumeType"`
	VolumeSize int    `json:"VolumeSize"`
	Iops       int    `json:"Iops"`
	Throughput int    `json:"Throughput"`
	EBSEnabled bool   `json:"EBSEnabled"`
}

// clusterConfigJSON is the JSON representation of cluster config.
type clusterConfigJSON struct {
	ZoneAwarenessConfig    *domainZoneAwarenessConfig `json:"ZoneAwarenessConfig,omitempty"`
	InstanceType           string                     `json:"InstanceType"`
	DedicatedMasterType    string                     `json:"DedicatedMasterType,omitempty"`
	WarmType               string                     `json:"WarmType,omitempty"`
	InstanceCount          int                        `json:"InstanceCount"`
	DedicatedMasterCount   int                        `json:"DedicatedMasterCount,omitempty"`
	WarmCount              int                        `json:"WarmCount,omitempty"`
	DedicatedMasterEnabled bool                       `json:"DedicatedMasterEnabled"`
	ZoneAwarenessEnabled   bool                       `json:"ZoneAwarenessEnabled"`
	WarmEnabled            bool                       `json:"WarmEnabled"`
	ColdStorageEnabled     bool                       `json:"ColdStorageEnabled"`
}

// domainStatusWrapJSON wraps the domain status in a DomainStatus key.
type domainStatusWrapJSON struct {
	DomainStatus domainStatusJSON `json:"DomainStatus"`
}

// domainListJSON is the response for ListDomainNames.
type domainListJSON struct {
	DomainNames []domainNameEntry `json:"DomainNames"`
}

// domainNameEntry is an element of the ListDomainNames response.
type domainNameEntry struct {
	DomainName           string `json:"DomainName"`
	ElasticsearchVersion string `json:"ElasticsearchVersion"`
}

// describeDomainsRequest is the request body for DescribeElasticsearchDomains.
type describeDomainsRequest struct {
	DomainNames []string `json:"DomainNames"`
}

// describeDomainsResponse is the response for DescribeElasticsearchDomains.
type describeDomainsResponse struct {
	DomainStatusList   []domainStatusJSON      `json:"DomainStatusList"`
	UnprocessedDomains []unprocessedDomainJSON `json:"UnprocessedDomains"`
}

// unprocessedDomainJSON represents a domain name that could not be described,
// matching the AWS DescribeElasticsearchDomains UnprocessedDomains field.
type unprocessedDomainJSON struct {
	DomainName   string             `json:"DomainName"`
	ErrorDetails domainErrorDetails `json:"ErrorDetails"`
}

// domainErrorDetails carries the error type and message for unprocessed domains.
type domainErrorDetails struct {
	ErrorType    string `json:"ErrorType"`
	ErrorMessage string `json:"ErrorMessage"`
}

// updateDomainConfigRequest is the request body for UpdateElasticsearchDomainConfig.
type updateDomainConfigRequest struct {
	ClusterConfig        *domainClusterConfig               `json:"ElasticsearchClusterConfig"`
	EBSOptions           *domainEBSOptions                  `json:"EBSOptions"`
	SnapshotOptions      *domainSnapshotOptions             `json:"SnapshotOptions"`
	EncryptionAtRest     *domainEncryptionAtRestOptions     `json:"EncryptionAtRestOptions"`
	NodeToNodeEncryption *domainNodeToNodeEncryptionOptions `json:"NodeToNodeEncryptionOptions"`
	DomainEndpointOpts   *domainEndpointOptions             `json:"DomainEndpointOptions"`
	AdvancedOptions      map[string]string                  `json:"AdvancedOptions"`
	AccessPolicies       *string                            `json:"AccessPolicies"`
}

// ServeHTTP implements [http.Handler] for the Elasticsearch service.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Fast O(1) lookup for fixed-path routes.
	key := r.Method + " " + r.URL.Path
	if fn, ok := h.ops[key]; ok {
		fn(w, r)

		return
	}

	if h.handlePrefixRoutes(w, r) {
		return
	}

	h.handleDomainRoutes(w, r)
}

// handlePrefixRoutes handles routes that require prefix matching with path params.
// Returns true if the request was handled.
func (h *Handler) handlePrefixRoutes(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path

	if h.handleCCSPrefixRoutes(w, r, path) {
		return true
	}

	if h.handlePackagePrefixRoutes(w, r, path) {
		return true
	}

	return h.handleInstanceUpgradePrefixRoutes(w, r, path)
}

// handleCCSPrefixRoutes handles CCS and VPC endpoint prefix routes.
func (h *Handler) handleCCSPrefixRoutes(w http.ResponseWriter, r *http.Request, path string) bool {
	// CCS inbound routes (ordered most-specific first)
	if strings.HasPrefix(path, elasticsearchCCSInbound+"/") {
		switch {
		case strings.HasSuffix(path, "/accept") && r.Method == http.MethodPut:
			h.handleAcceptInboundCrossClusterSearchConnection(w, r)

			return true
		case strings.HasSuffix(path, "/reject") && r.Method == http.MethodPut:
			h.handleRejectInboundCrossClusterSearchConnection(w, r)

			return true
		case r.Method == http.MethodDelete:
			h.handleDeleteInboundCrossClusterSearchConnection(w, r)

			return true
		}
	}

	if strings.HasPrefix(path, elasticsearchCCSOutbound+"/") && r.Method == http.MethodDelete {
		h.handleDeleteOutboundCrossClusterSearchConnection(w, r)

		return true
	}

	if strings.HasPrefix(path, elasticsearchVpcEndpoints+"/") && r.Method == http.MethodDelete {
		h.handleDeleteVpcEndpoint(w, r)

		return true
	}

	return false
}

// handlePackagePrefixRoutes handles package and domain-package prefix routes.
func (h *Handler) handlePackagePrefixRoutes(w http.ResponseWriter, r *http.Request, path string) bool {
	if h.handlePackageAssocDisassoc(w, r, path) {
		return true
	}

	return h.handlePackageHistoryAndDelete(w, r, path)
}

// handlePackageAssocDisassoc handles package association and history listing operations.
func (h *Handler) handlePackageAssocDisassoc(w http.ResponseWriter, r *http.Request, path string) bool {
	switch {
	case strings.HasPrefix(path, elasticsearchPackages+"/associate/") && r.Method == http.MethodPost:
		h.handleAssociatePackage(w, r)

		return true
	case strings.HasPrefix(path, elasticsearchPackages+"/dissociate/") && r.Method == http.MethodPost:
		h.handleDissociatePackage(w, r)

		return true
	case strings.HasPrefix(path, elasticsearchPackages+"/") &&
		strings.HasSuffix(path, "/history") &&
		r.Method == http.MethodGet:
		h.handleGetPackageVersionHistory(w, r)

		return true
	case strings.HasPrefix(path, elasticsearchPackages+"/") &&
		strings.HasSuffix(path, "/domains") &&
		r.Method == http.MethodGet:
		h.handleListDomainsForPackage(w, r)

		return true
	}

	return false
}

// handlePackageHistoryAndDelete handles package delete and domain-package listing.
func (h *Handler) handlePackageHistoryAndDelete(w http.ResponseWriter, r *http.Request, path string) bool {
	if strings.HasPrefix(path, elasticsearchPackages+"/") && r.Method == http.MethodDelete {
		rest := strings.TrimPrefix(path, elasticsearchPackages+"/")
		if !strings.Contains(rest, "/") {
			h.handleDeletePackage(w, r)

			return true
		}
	}

	if strings.HasPrefix(path, elasticsearchDomainPackages+"/") &&
		strings.HasSuffix(path, "/packages") &&
		r.Method == http.MethodGet {
		h.handleListPackagesForDomain(w, r)

		return true
	}

	return false
}

// handleInstanceUpgradePrefixRoutes handles instance type and upgrade prefix routes.
func (h *Handler) handleInstanceUpgradePrefixRoutes(w http.ResponseWriter, r *http.Request, path string) bool {
	if strings.HasPrefix(path, elasticsearchInstanceTypes+"/") && r.Method == http.MethodGet {
		h.handleListElasticsearchInstanceTypes(w, r)

		return true
	}

	if strings.HasPrefix(path, elasticsearchInstanceTypeLimits+"/") && r.Method == http.MethodGet {
		h.handleDescribeElasticsearchInstanceTypeLimits(w, r)

		return true
	}

	if strings.HasPrefix(path, elasticsearchUpgradeDomain+"/") {
		switch {
		case strings.HasSuffix(path, "/history") && r.Method == http.MethodGet:
			h.handleGetUpgradeHistory(w, r)

			return true
		case strings.HasSuffix(path, "/status") && r.Method == http.MethodGet:
			h.handleGetUpgradeStatus(w, r)

			return true
		}
	}

	return false
}

func (h *Handler) handleDomainRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, elasticsearchPathPrefix)

	switch {
	case (rest == "" || rest == "/") && r.Method == http.MethodPost:
		h.handleCreateDomain(w, r)
	case (rest == "" || rest == "/") && r.Method == http.MethodGet:
		h.handleListDomainNames(w, r)
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodGet:
		h.handleGetDomainRoute(w, r, rest)
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodDelete:
		h.handleDeleteDomain(w, r, domainNameFromRest(rest))
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodPost:
		h.handlePostDomainRoute(w, r, rest)
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

func (h *Handler) handleGetDomainRoute(w http.ResponseWriter, r *http.Request, rest string) {
	trimmed := domainNameFromRest(rest)
	switch {
	case strings.HasSuffix(trimmed, "/config"):
		domainName, _ := strings.CutSuffix(trimmed, "/config")
		h.handleDescribeDomainConfig(w, r, domainName)
	case strings.HasSuffix(trimmed, "/autoTunes"):
		domainName, _ := strings.CutSuffix(trimmed, "/autoTunes")
		h.handleDescribeDomainAutoTunes(w, r, domainName)
	case strings.HasSuffix(trimmed, "/progress"):
		domainName, _ := strings.CutSuffix(trimmed, "/progress")
		h.handleDescribeDomainChangeProgress(w, r, domainName)
	case strings.HasSuffix(trimmed, "/listVpcEndpointAccess"):
		domainName, _ := strings.CutSuffix(trimmed, "/listVpcEndpointAccess")
		h.handleListVpcEndpointAccess(w, r, domainName)
	case strings.HasSuffix(trimmed, "/vpcEndpoints"):
		domainName, _ := strings.CutSuffix(trimmed, "/vpcEndpoints")
		h.handleListVpcEndpointsForDomain(w, r, domainName)
	default:
		h.handleDescribeDomain(w, r, trimmed)
	}
}

func (h *Handler) handlePostDomainRoute(w http.ResponseWriter, r *http.Request, rest string) {
	trimmed := domainNameFromRest(rest)

	switch {
	case strings.HasSuffix(trimmed, "/config/cancel"):
		domainName, _ := strings.CutSuffix(trimmed, "/config/cancel")
		h.handleCancelDomainConfigChange(w, r, domainName)
	case strings.HasSuffix(trimmed, "/config"):
		domainName, _ := strings.CutSuffix(trimmed, "/config")
		h.handleUpdateDomainConfig(w, r, domainName)
	case strings.HasSuffix(trimmed, "/authorizeVpcEndpointAccess"):
		domainName, _ := strings.CutSuffix(trimmed, "/authorizeVpcEndpointAccess")
		h.handleAuthorizeVpcEndpointAccess(w, r, domainName)
	case strings.HasSuffix(trimmed, "/revokeVpcEndpointAccess"):
		domainName, _ := strings.CutSuffix(trimmed, "/revokeVpcEndpointAccess")
		h.handleRevokeVpcEndpointAccess(w, r, domainName)
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

func domainNameFromRest(rest string) string {
	name := strings.TrimPrefix(rest, "/")

	return strings.TrimSuffix(name, "/")
}

// Handle satisfies the Echo handler interface.
func (h *Handler) Handle(c *echo.Context) error {
	h.ServeHTTP(c.Response(), c.Request())

	return nil
}

// Handler returns the Echo HandlerFunc for this service.
func (h *Handler) Handler() echo.HandlerFunc {
	return h.Handle
}

func (h *Handler) handleCreateDomain(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req domainJSON
	if err = json.Unmarshal(body, &req); err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	inp := CreateDomainInput{
		Name:                 req.DomainName,
		ElasticsearchVersion: req.ElasticsearchVersion,
		AccessPolicies:       req.AccessPolicies,
		AdvancedOptions:      req.AdvancedOptions,
	}

	if req.ClusterConfig != nil {
		inp.ClusterConfig = clusterConfigFromRequest(req.ClusterConfig)
	}

	if req.EBSOptions != nil {
		inp.EBSOptions = ebsOptsFromRequest(req.EBSOptions)
	}

	if req.SnapshotOptions != nil {
		inp.SnapshotOptions = SnapshotOptions{
			AutomatedSnapshotStartHour: req.SnapshotOptions.AutomatedSnapshotStartHour,
		}
	}

	if req.EncryptionAtRest != nil {
		inp.EncryptionAtRestEnabled = req.EncryptionAtRest.Enabled
	}

	if req.NodeToNodeEncryption != nil {
		inp.NodeToNodeEncryptionEnabled = req.NodeToNodeEncryption.Enabled
	}

	if req.DomainEndpointOpts != nil {
		inp.EnforceHTTPS = req.DomainEndpointOpts.EnforceHTTPS
		inp.TLSSecurityPolicy = req.DomainEndpointOpts.TLSSecurityPolicy
	}

	domain, err := h.Backend.CreateDomain(h.reqContext(r), inp)
	if err != nil {
		h.handleDomainError(r, w, err)

		return
	}

	h.writeJSON(r, w, domainStatusWrapJSON{
		DomainStatus: toDomainStatusJSON(domain),
	})
}

// handleDomainError maps backend domain errors to HTTP responses.
func (h *Handler) handleDomainError(r *http.Request, w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrDomainAlreadyExists):
		h.writeError(r, w, http.StatusConflict, "ResourceAlreadyExistsException", err.Error())
	case errors.Is(err, ErrValidation):
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
	case errors.Is(err, ErrDomainNotFound):
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	default:
		h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
	}
}

func (h *Handler) handleDescribeDomain(w http.ResponseWriter, r *http.Request, name string) {
	domain, err := h.Backend.DescribeDomain(h.reqContext(r), name)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, domainStatusWrapJSON{
		DomainStatus: toDomainStatusJSON(domain),
	})
}

func (h *Handler) handleDeleteDomain(w http.ResponseWriter, r *http.Request, name string) {
	domain, err := h.Backend.DeleteDomain(h.reqContext(r), name)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, domainStatusWrapJSON{
		DomainStatus: toDomainStatusJSON(domain),
	})
}

func (h *Handler) handleListDomainNames(w http.ResponseWriter, r *http.Request) {
	ctx := h.reqContext(r)
	names := h.Backend.ListDomainNames(ctx)
	entries := make([]domainNameEntry, 0, len(names))

	for _, name := range names {
		d, err := h.Backend.DescribeDomain(ctx, name)
		if err != nil {
			continue
		}

		entries = append(entries, domainNameEntry{
			DomainName:           name,
			ElasticsearchVersion: d.ElasticsearchVersion,
		})
	}

	// Ensure the slice is non-nil so JSON marshals as [] not null.
	if entries == nil {
		entries = []domainNameEntry{}
	}

	h.writeJSON(r, w, domainListJSON{DomainNames: entries})
}

func (h *Handler) handleDescribeElasticsearchDomains(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req describeDomainsRequest
	if err = json.Unmarshal(body, &req); err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	if len(req.DomainNames) > maxDescribeDomainNames {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException",
			fmt.Sprintf("DescribeElasticsearchDomains accepts a maximum of %d domain names", maxDescribeDomainNames))

		return
	}

	list := make([]domainStatusJSON, 0, len(req.DomainNames))
	var unprocessed []unprocessedDomainJSON
	ctx := h.reqContext(r)

	for _, name := range req.DomainNames {
		d, descErr := h.Backend.DescribeDomain(ctx, name)
		if descErr != nil {
			unprocessed = append(unprocessed, unprocessedDomainJSON{
				DomainName: name,
				ErrorDetails: domainErrorDetails{
					ErrorType:    "ResourceNotFoundException",
					ErrorMessage: fmt.Sprintf("Domain not found: %s", name),
				},
			})

			continue
		}

		list = append(list, toDomainStatusJSON(d))
	}

	// AWS always emits both arrays (never null), even when empty.
	if unprocessed == nil {
		unprocessed = []unprocessedDomainJSON{}
	}

	h.writeJSON(r, w, describeDomainsResponse{DomainStatusList: list, UnprocessedDomains: unprocessed})
}

func (h *Handler) handleUpdateDomainConfig(w http.ResponseWriter, r *http.Request, name string) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req updateDomainConfigRequest
	if err = json.Unmarshal(body, &req); err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	upd := UpdateConfig{}

	if req.ClusterConfig != nil {
		cfg := clusterConfigFromRequest(req.ClusterConfig)
		upd.ClusterConfig = &cfg
	}

	if req.EBSOptions != nil {
		opts := ebsOptsFromRequest(req.EBSOptions)
		upd.EBSOptions = &opts
	}

	if req.SnapshotOptions != nil {
		so := SnapshotOptions{AutomatedSnapshotStartHour: req.SnapshotOptions.AutomatedSnapshotStartHour}
		upd.SnapshotOptions = &so
	}

	if req.EncryptionAtRest != nil {
		upd.EncryptionAtRestEnabled = &req.EncryptionAtRest.Enabled
	}

	if req.NodeToNodeEncryption != nil {
		upd.NodeToNodeEncryptionEnabled = &req.NodeToNodeEncryption.Enabled
	}

	if req.DomainEndpointOpts != nil {
		upd.EnforceHTTPS = &req.DomainEndpointOpts.EnforceHTTPS
		upd.TLSSecurityPolicy = &req.DomainEndpointOpts.TLSSecurityPolicy
	}

	if req.AdvancedOptions != nil {
		upd.AdvancedOptions = req.AdvancedOptions
	}

	upd.AccessPolicies = req.AccessPolicies

	domain, err := h.Backend.UpdateDomainConfig(h.reqContext(r), name, upd)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, buildDomainConfigOutput(domain))
}

// clusterConfigFromRequest converts a request cluster config into a backend ClusterConfig.
func clusterConfigFromRequest(req *domainClusterConfig) ClusterConfig {
	cfg := ClusterConfig{
		InstanceType:           req.InstanceType,
		InstanceCount:          req.InstanceCount,
		DedicatedMasterEnabled: req.DedicatedMasterEnabled,
		DedicatedMasterType:    req.DedicatedMasterType,
		DedicatedMasterCount:   req.DedicatedMasterCount,
		ZoneAwarenessEnabled:   req.ZoneAwarenessEnabled,
		WarmEnabled:            req.WarmEnabled,
		WarmType:               req.WarmType,
		WarmCount:              req.WarmCount,
		ColdStorageEnabled:     req.ColdStorageEnabled,
	}

	if req.ZoneAwarenessConfig != nil {
		cfg.ZoneAwarenessConfig = ZoneAwarenessConfig{
			AvailabilityZoneCount: req.ZoneAwarenessConfig.AvailabilityZoneCount,
		}
	}

	return cfg
}

// ebsOptsFromRequest converts a request EBS options struct into a backend EBSOptions.
func ebsOptsFromRequest(req *domainEBSOptions) EBSOptions {
	return EBSOptions{
		EBSEnabled: req.EBSEnabled,
		VolumeSize: req.VolumeSize,
		VolumeType: req.VolumeType,
		Iops:       req.Iops,
		Throughput: req.Throughput,
	}
}

// toClusterConfigJSON converts a backend ClusterConfig to its JSON representation.
func toClusterConfigJSON(c ClusterConfig) clusterConfigJSON {
	cfg := clusterConfigJSON{
		InstanceType:           c.InstanceType,
		InstanceCount:          c.InstanceCount,
		DedicatedMasterEnabled: c.DedicatedMasterEnabled,
		DedicatedMasterType:    c.DedicatedMasterType,
		DedicatedMasterCount:   c.DedicatedMasterCount,
		ZoneAwarenessEnabled:   c.ZoneAwarenessEnabled,
		WarmEnabled:            c.WarmEnabled,
		WarmType:               c.WarmType,
		WarmCount:              c.WarmCount,
		ColdStorageEnabled:     c.ColdStorageEnabled,
	}

	if c.ZoneAwarenessEnabled {
		cfg.ZoneAwarenessConfig = &domainZoneAwarenessConfig{
			AvailabilityZoneCount: c.ZoneAwarenessConfig.AvailabilityZoneCount,
		}
	}

	return cfg
}

func toDomainStatusJSON(d *Domain) domainStatusJSON {
	advOpts := d.AdvancedOptions
	if advOpts == nil {
		advOpts = map[string]string{}
	}

	return domainStatusJSON{
		DomainName:             d.Name,
		DomainID:               d.DomainID,
		ARN:                    d.ARN,
		ElasticsearchVersion:   d.ElasticsearchVersion,
		Endpoint:               d.Endpoint,
		Processing:             false,
		DomainProcessingStatus: statusActiveCap,
		AccessPolicies:         d.AccessPolicies,
		AdvancedOptions:        advOpts,
		EBSOptions: ebsOptionsJSON{
			EBSEnabled: d.EBSOptions.EBSEnabled,
			VolumeSize: d.EBSOptions.VolumeSize,
			VolumeType: d.EBSOptions.VolumeType,
			Iops:       d.EBSOptions.Iops,
			Throughput: d.EBSOptions.Throughput,
		},
		ElasticsearchClusterConfig: toClusterConfigJSON(d.ClusterConfig),
		CognitoOptions:             cognitoOptionsJSON{Enabled: false},
		SnapshotOptions: domainSnapshotOptions{
			AutomatedSnapshotStartHour: d.SnapshotOptions.AutomatedSnapshotStartHour,
		},
		EncryptionAtRestOptions:     domainEncryptionAtRestOptions{Enabled: d.EncryptionAtRestEnabled},
		NodeToNodeEncryptionOptions: domainNodeToNodeEncryptionOptions{Enabled: d.NodeToNodeEncryptionEnabled},
		DomainEndpointOptions: domainEndpointOptions{
			EnforceHTTPS:      d.EnforceHTTPS,
			TLSSecurityPolicy: d.TLSSecurityPolicy,
		},
	}
}

// buildDomainConfigOutput builds the DescribeDomainConfig/UpdateDomainConfig response.
func buildDomainConfigOutput(d *Domain) *describeDomainConfigOutput {
	activeStatus := elasticsearchConfigStatus{State: statusActiveCap}
	out := &describeDomainConfigOutput{}
	out.DomainConfig.ElasticsearchVersion = elasticsearchConfigValue{
		Options: d.ElasticsearchVersion,
		Status:  activeStatus,
	}

	clusterOpts := map[string]any{
		keyInstanceType:           d.ClusterConfig.InstanceType,
		keyInstanceCount:          d.ClusterConfig.InstanceCount,
		keyDedicatedMasterEnabled: d.ClusterConfig.DedicatedMasterEnabled,
		keyZoneAwarenessEnabled:   d.ClusterConfig.ZoneAwarenessEnabled,
		keyWarmEnabled:            d.ClusterConfig.WarmEnabled,
		keyColdStorageEnabled:     d.ClusterConfig.ColdStorageEnabled,
	}

	if d.ClusterConfig.DedicatedMasterEnabled {
		clusterOpts[keyDedicatedMasterType] = d.ClusterConfig.DedicatedMasterType
		clusterOpts[keyDedicatedMasterCount] = d.ClusterConfig.DedicatedMasterCount
	}

	if d.ClusterConfig.WarmEnabled {
		clusterOpts[keyWarmType] = d.ClusterConfig.WarmType
		clusterOpts[keyWarmCount] = d.ClusterConfig.WarmCount
	}

	if d.ClusterConfig.ZoneAwarenessEnabled {
		clusterOpts[keyZoneAwarenessConfig] = map[string]any{
			"AvailabilityZoneCount": d.ClusterConfig.ZoneAwarenessConfig.AvailabilityZoneCount,
		}
	}

	out.DomainConfig.ElasticsearchClusterConfig = elasticsearchConfigValue{Options: clusterOpts, Status: activeStatus}
	out.DomainConfig.EBSOptions = elasticsearchConfigValue{Options: map[string]any{
		keyEBSEnabled: d.EBSOptions.EBSEnabled,
		keyVolumeSize: d.EBSOptions.VolumeSize,
		keyVolumeType: d.EBSOptions.VolumeType,
		keyIops:       d.EBSOptions.Iops,
		keyThroughput: d.EBSOptions.Throughput,
	}, Status: activeStatus}
	out.DomainConfig.AccessPolicies = elasticsearchConfigValue{Options: d.AccessPolicies, Status: activeStatus}

	advOpts := d.AdvancedOptions
	if advOpts == nil {
		advOpts = map[string]string{}
	}

	out.DomainConfig.AdvancedOptions = elasticsearchConfigValue{Options: advOpts, Status: activeStatus}
	out.DomainConfig.SnapshotOptions = elasticsearchConfigValue{
		Options: map[string]any{"AutomatedSnapshotStartHour": d.SnapshotOptions.AutomatedSnapshotStartHour},
		Status:  activeStatus,
	}
	out.DomainConfig.EncryptionAtRestOptions = elasticsearchConfigValue{
		Options: map[string]any{"Enabled": d.EncryptionAtRestEnabled},
		Status:  activeStatus,
	}
	out.DomainConfig.NodeToNodeEncryptionOptions = elasticsearchConfigValue{
		Options: map[string]any{"Enabled": d.NodeToNodeEncryptionEnabled},
		Status:  activeStatus,
	}
	out.DomainConfig.DomainEndpointOptions = elasticsearchConfigValue{
		Options: map[string]any{
			"EnforceHTTPS":      d.EnforceHTTPS,
			"TLSSecurityPolicy": d.TLSSecurityPolicy,
		},
		Status: activeStatus,
	}

	return out
}

type errorResponseJSON struct {
	Message string `json:"message"`
}

func (h *Handler) writeError(r *http.Request, w http.ResponseWriter, status int, code, message string) {
	ctx := r.Context()
	logger.Load(ctx).ErrorContext(r.Context(), "elasticsearch error", "code", code, "message", message)
	w.Header().Set("x-amzn-ErrorType", code)
	httputils.WriteJSON(ctx, w, status, errorResponseJSON{Message: message})
}

func (h *Handler) writeJSON(r *http.Request, w http.ResponseWriter, v any) {
	httputils.WriteJSON(r.Context(), w, http.StatusOK, v)
}

type listTagsOutput struct {
	TagList []svcTags.KV `json:"TagList"`
}

type elasticsearchConfigStatus struct {
	State string `json:"State"`
}

type elasticsearchConfigValue struct {
	Options any                       `json:"Options"`
	Status  elasticsearchConfigStatus `json:"Status"`
}

// domainConfigFields holds the per-feature configuration values for a domain.
type domainConfigFields struct {
	ElasticsearchVersion        elasticsearchConfigValue `json:"ElasticsearchVersion"`
	ElasticsearchClusterConfig  elasticsearchConfigValue `json:"ElasticsearchClusterConfig"`
	EBSOptions                  elasticsearchConfigValue `json:"EBSOptions"`
	AccessPolicies              elasticsearchConfigValue `json:"AccessPolicies"`
	AdvancedOptions             elasticsearchConfigValue `json:"AdvancedOptions"`
	SnapshotOptions             elasticsearchConfigValue `json:"SnapshotOptions"`
	EncryptionAtRestOptions     elasticsearchConfigValue `json:"EncryptionAtRestOptions"`
	NodeToNodeEncryptionOptions elasticsearchConfigValue `json:"NodeToNodeEncryptionOptions"`
	DomainEndpointOptions       elasticsearchConfigValue `json:"DomainEndpointOptions"`
}

type describeDomainConfigOutput struct {
	DomainConfig domainConfigFields `json:"DomainConfig"`
}

func (h *Handler) handleListTags(w http.ResponseWriter, r *http.Request) {
	domainARN := r.URL.Query().Get("arn")

	tags, err := h.Backend.ListTags(h.reqContext(r), domainARN)
	if err != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

		return
	}

	tagList := make([]svcTags.KV, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, svcTags.KV{Key: k, Value: v})
	}

	slices.SortFunc(tagList, func(a, b svcTags.KV) int {
		return strings.Compare(a.Key, b.Key)
	})

	h.writeJSON(r, w, &listTagsOutput{TagList: tagList})
}

type addTagsInput struct {
	ARN     string       `json:"ARN"`
	TagList []svcTags.KV `json:"TagList"`
}

func (h *Handler) handleAddTags(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req addTagsInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	seen := make(map[string]bool, len(req.TagList))
	for _, t := range req.TagList {
		if len(t.Key) == 0 || len(t.Key) > maxTagKeyLen {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException",
				fmt.Sprintf("tag key must be 1-%d characters", maxTagKeyLen))

			return
		}

		if len(t.Value) > maxTagValueLen {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException",
				fmt.Sprintf("tag value must be 0-%d characters", maxTagValueLen))

			return
		}

		if seen[t.Key] {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException",
				fmt.Sprintf("Duplicate tag key: %s", t.Key))

			return
		}

		seen[t.Key] = true
	}

	tagMap := make(map[string]string, len(req.TagList))
	for _, t := range req.TagList {
		tagMap[t.Key] = t.Value
	}

	ctx := h.reqContext(r)
	existing, _ := h.Backend.ListTags(ctx, req.ARN)
	maps.Copy(existing, tagMap)

	if len(existing) > maxTagsPerResource {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException",
			fmt.Sprintf("resource cannot have more than %d tags", maxTagsPerResource))

		return
	}

	_ = h.Backend.AddTags(ctx, req.ARN, tagMap)
	w.WriteHeader(http.StatusOK)
}

type removeTagsInput struct {
	ARN     string   `json:"ARN"`
	TagKeys []string `json:"TagKeys"`
}

func (h *Handler) handleRemoveTags(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req removeTagsInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	_ = h.Backend.RemoveTags(h.reqContext(r), req.ARN, req.TagKeys)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleDescribeDomainConfig(w http.ResponseWriter, r *http.Request, name string) {
	d, err := h.Backend.DescribeDomain(h.reqContext(r), name)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("domain %s/config not found", name))
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, buildDomainConfigOutput(d))
}

// --- New operations ---

// createPackageRequest is the JSON body for CreatePackage.
type createPackageRequest struct {
	PackageName        string `json:"PackageName"`
	PackageType        string `json:"PackageType"`
	PackageDescription string `json:"PackageDescription"`
}

// packageJSON is the JSON representation of an Elasticsearch package.
type packageJSON struct {
	PackageID          string `json:"PackageID"`
	PackageName        string `json:"PackageName"`
	PackageType        string `json:"PackageType"`
	PackageDescription string `json:"PackageDescription"`
	PackageStatus      string `json:"PackageStatus"`
}

// createPackageOutput is the response for CreatePackage.
type createPackageOutput struct {
	PackageDetails packageJSON `json:"PackageDetails"`
}

func (h *Handler) handleCreatePackage(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req createPackageRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	pkg, createErr := h.Backend.CreatePackage(h.reqContext(r), req.PackageName, req.PackageType, req.PackageDescription)
	if createErr != nil {
		if errors.Is(createErr, ErrDomainAlreadyExists) {
			h.writeError(r, w, http.StatusConflict, "ResourceAlreadyExistsException", createErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", createErr.Error())
		}

		return
	}

	h.writeJSON(r, w, createPackageOutput{PackageDetails: toPackageJSON(pkg)})
}

func toPackageJSON(p *Package) packageJSON {
	return packageJSON{
		PackageID:          p.ID,
		PackageName:        p.Name,
		PackageType:        p.PackageType,
		PackageDescription: p.Description,
		PackageStatus:      p.Status,
	}
}

// associatePackageOutput is the response for AssociatePackage.
type associatePackageOutput struct {
	DomainPackageDetails domainPackageJSON `json:"DomainPackageDetails"`
}

type domainPackageJSON struct {
	PackageID           string `json:"PackageID"`
	PackageName         string `json:"PackageName,omitempty"`
	DomainName          string `json:"DomainName"`
	PackageType         string `json:"PackageType,omitempty"`
	DomainPackageStatus string `json:"DomainPackageStatus"`
}

// associatePackagePathParts is the expected number of path segments after /associate/.
const associatePackagePathParts = 2

func (h *Handler) handleAssociatePackage(w http.ResponseWriter, r *http.Request) {
	// Path: /2015-01-01/packages/associate/{packageID}/{domainName}
	rest := strings.TrimPrefix(r.URL.Path, elasticsearchPackages+"/associate/")
	parts := strings.SplitN(rest, "/", associatePackagePathParts)

	if len(parts) != associatePackagePathParts {
		h.writeError(
			r,
			w,
			http.StatusBadRequest,
			"ValidationException",
			"invalid path: expected /associate/{packageID}/{domainName}",
		)

		return
	}

	packageID, domainName := parts[0], parts[1]

	if assocErr := h.Backend.AssociatePackage(h.reqContext(r), packageID, domainName); assocErr != nil {
		switch {
		case errors.Is(assocErr, ErrDomainNotFound) || errors.Is(assocErr, ErrPackageNotFound):
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", assocErr.Error())
		case errors.Is(assocErr, ErrPackageAlreadyAssociated):
			h.writeError(r, w, http.StatusConflict, "ConflictException", assocErr.Error())
		default:
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", assocErr.Error())
		}

		return
	}

	var out associatePackageOutput
	out.DomainPackageDetails.PackageID = packageID
	out.DomainPackageDetails.DomainName = domainName
	out.DomainPackageDetails.DomainPackageStatus = "ACTIVE"

	h.writeJSON(r, w, &out)
}

// inboundConnectionJSON is the JSON representation of an inbound cross-cluster connection.
type inboundConnectionJSON struct {
	CrossClusterSearchConnectionID string                      `json:"CrossClusterSearchConnectionId"`
	ConnectionStatus               inboundConnectionStatusJSON `json:"ConnectionStatus"`
	SourceDomainInfo               crossClusterDomainInfoJSON  `json:"SourceDomainInfo"`
	DestinationDomainInfo          crossClusterDomainInfoJSON  `json:"DestinationDomainInfo"`
}

type inboundConnectionStatusJSON struct {
	StatusCode string `json:"StatusCode"`
}

type crossClusterDomainInfoJSON struct {
	OwnerID    string `json:"OwnerId"`
	DomainName string `json:"DomainName"`
	Region     string `json:"Region"`
}

// acceptInboundConnectionOutput wraps the accepted connection.
type acceptInboundConnectionOutput struct {
	CrossClusterSearchConnection inboundConnectionJSON `json:"CrossClusterSearchConnection"`
}

func (h *Handler) handleAcceptInboundCrossClusterSearchConnection(w http.ResponseWriter, r *http.Request) {
	// Path: /2015-01-01/es/ccs/inboundConnection/{connectionId}/accept
	rest := strings.TrimPrefix(r.URL.Path, elasticsearchCCSInbound+"/")
	connectionID, _ := strings.CutSuffix(rest, "/accept")

	conn, err := h.Backend.AcceptInboundCrossClusterSearchConnection(h.reqContext(r), connectionID)
	if err != nil {
		if errors.Is(err, ErrConnectionNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, acceptInboundConnectionOutput{
		CrossClusterSearchConnection: toInboundConnectionJSON(conn),
	})
}

func toInboundConnectionJSON(c *InboundConnection) inboundConnectionJSON {
	return inboundConnectionJSON{
		CrossClusterSearchConnectionID: c.ConnectionID,
		ConnectionStatus:               inboundConnectionStatusJSON{StatusCode: c.ConnectionStatus},
		SourceDomainInfo: crossClusterDomainInfoJSON{
			OwnerID:    c.SourceDomainInfo.OwnerID,
			DomainName: c.SourceDomainInfo.DomainName,
			Region:     c.SourceDomainInfo.Region,
		},
		DestinationDomainInfo: crossClusterDomainInfoJSON{
			OwnerID:    c.DestDomainInfo.OwnerID,
			DomainName: c.DestDomainInfo.DomainName,
			Region:     c.DestDomainInfo.Region,
		},
	}
}

// outboundConnectionJSON is the JSON representation of an outbound cross-cluster connection.
type outboundConnectionJSON struct {
	CrossClusterSearchConnectionID string                       `json:"CrossClusterSearchConnectionId"`
	ConnectionAlias                string                       `json:"ConnectionAlias"`
	ConnectionStatus               outboundConnectionStatusJSON `json:"ConnectionStatus"`
	LocalDomainInfo                crossClusterDomainInfoJSON   `json:"LocalDomainInfo"`
	RemoteDomainInfo               crossClusterDomainInfoJSON   `json:"RemoteDomainInfo"`
}

type outboundConnectionStatusJSON struct {
	StatusCode string `json:"StatusCode"`
}

// createOutboundConnectionRequest is the JSON body for CreateOutboundCrossClusterSearchConnection.
type createOutboundConnectionRequest struct {
	LocalDomainInfo  crossClusterDomainInfoJSON `json:"LocalDomainInfo"`
	RemoteDomainInfo crossClusterDomainInfoJSON `json:"RemoteDomainInfo"`
	ConnectionAlias  string                     `json:"ConnectionAlias"`
}

// createOutboundConnectionOutput wraps the new outbound connection.
type createOutboundConnectionOutput struct {
	CrossClusterSearchConnection outboundConnectionJSON `json:"CrossClusterSearchConnection"`
}

func (h *Handler) handleCreateOutboundCrossClusterSearchConnection(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req createOutboundConnectionRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	localDomain := CrossClusterDomainInfo{
		OwnerID:    req.LocalDomainInfo.OwnerID,
		DomainName: req.LocalDomainInfo.DomainName,
		Region:     req.LocalDomainInfo.Region,
	}
	remoteDomain := CrossClusterDomainInfo{
		OwnerID:    req.RemoteDomainInfo.OwnerID,
		DomainName: req.RemoteDomainInfo.DomainName,
		Region:     req.RemoteDomainInfo.Region,
	}

	conn, createErr := h.Backend.CreateOutboundCrossClusterSearchConnection(
		h.reqContext(r),
		localDomain,
		remoteDomain,
		req.ConnectionAlias,
	)
	if createErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", createErr.Error())

		return
	}

	h.writeJSON(r, w, createOutboundConnectionOutput{
		CrossClusterSearchConnection: toOutboundConnectionJSON(conn),
	})
}

func toOutboundConnectionJSON(c *OutboundConnection) outboundConnectionJSON {
	return outboundConnectionJSON{
		CrossClusterSearchConnectionID: c.ConnectionID,
		ConnectionAlias:                c.ConnectionAlias,
		ConnectionStatus:               outboundConnectionStatusJSON{StatusCode: c.ConnectionStatus},
		LocalDomainInfo: crossClusterDomainInfoJSON{
			OwnerID:    c.LocalDomainInfo.OwnerID,
			DomainName: c.LocalDomainInfo.DomainName,
			Region:     c.LocalDomainInfo.Region,
		},
		RemoteDomainInfo: crossClusterDomainInfoJSON{
			OwnerID:    c.RemoteDomainInfo.OwnerID,
			DomainName: c.RemoteDomainInfo.DomainName,
			Region:     c.RemoteDomainInfo.Region,
		},
	}
}

// vpcEndpointJSON is the JSON representation of a VPC endpoint.
type vpcEndpointJSON struct {
	VpcOptions       map[string]string `json:"VpcOptions"`
	VpcEndpointID    string            `json:"VpcEndpointId"`
	VpcEndpointOwner string            `json:"VpcEndpointOwner"`
	DomainArn        string            `json:"DomainArn"`
	Endpoint         string            `json:"Endpoint"`
	Status           string            `json:"Status"`
}

// createVpcEndpointRequest is the JSON body for CreateVpcEndpoint.
type createVpcEndpointRequest struct {
	VpcOptions map[string]string `json:"VpcOptions"`
	DomainArn  string            `json:"DomainArn"`
}

// createVpcEndpointOutput wraps the new VPC endpoint.
type createVpcEndpointOutput struct {
	VpcEndpoint vpcEndpointJSON `json:"VpcEndpoint"`
}

func (h *Handler) handleCreateVpcEndpoint(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req createVpcEndpointRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	endpoint, createErr := h.Backend.CreateVpcEndpoint(h.reqContext(r), req.DomainArn, req.VpcOptions)
	if createErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", createErr.Error())

		return
	}

	h.writeJSON(r, w, createVpcEndpointOutput{VpcEndpoint: toVpcEndpointJSON(endpoint)})
}

func toVpcEndpointJSON(e *VpcEndpoint) vpcEndpointJSON {
	return vpcEndpointJSON{
		VpcEndpointID:    e.ID,
		VpcEndpointOwner: e.OwnerAccountID,
		DomainArn:        e.DomainARN,
		Endpoint:         e.Endpoint,
		Status:           e.Status,
		VpcOptions:       e.VpcOptions,
	}
}

// authorizeVpcEndpointAccessRequest is the JSON body for AuthorizeVpcEndpointAccess.
type authorizeVpcEndpointAccessRequest struct {
	Account string `json:"Account"`
}

// authorizedPrincipalJSON is the JSON representation of an authorized principal.
type authorizedPrincipalJSON struct {
	PrincipalType string `json:"PrincipalType"`
	Principal     string `json:"Principal"`
}

// authorizeVpcEndpointAccessOutput is the response for AuthorizeVpcEndpointAccess.
type authorizeVpcEndpointAccessOutput struct {
	AuthorizedPrincipal authorizedPrincipalJSON `json:"AuthorizedPrincipal"`
}

func (h *Handler) handleAuthorizeVpcEndpointAccess(w http.ResponseWriter, r *http.Request, domainName string) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req authorizeVpcEndpointAccessRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	if authErr := h.Backend.AuthorizeVpcEndpointAccess(h.reqContext(r), domainName, req.Account); authErr != nil {
		if errors.Is(authErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", authErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", authErr.Error())
		}

		return
	}

	h.writeJSON(r, w, authorizeVpcEndpointAccessOutput{
		AuthorizedPrincipal: authorizedPrincipalJSON{
			PrincipalType: "AWS_ACCOUNT",
			Principal:     req.Account,
		},
	})
}

func (h *Handler) handleCancelDomainConfigChange(w http.ResponseWriter, r *http.Request, domainName string) {
	d, err := h.Backend.CancelDomainConfigChange(h.reqContext(r), domainName)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, buildDomainConfigOutput(d))
}

// cancelSoftwareUpdateRequest is the JSON body for CancelElasticsearchServiceSoftwareUpdate.
type cancelSoftwareUpdateRequest struct {
	DomainName string `json:"DomainName"`
}

// serviceSoftwareOptionsJSON is the JSON representation of software update options.
type serviceSoftwareOptionsJSON struct {
	CurrentVersion      string `json:"CurrentVersion"`
	NewVersion          string `json:"NewVersion"`
	UpdateStatus        string `json:"UpdateStatus"`
	Description         string `json:"Description"`
	AutomatedUpdateDate string `json:"AutomatedUpdateDate"`
	UpdateAvailable     bool   `json:"UpdateAvailable"`
	Cancellable         bool   `json:"Cancellable"`
	OptionalDeployment  bool   `json:"OptionalDeployment"`
}

// cancelSoftwareUpdateOutput is the response for CancelElasticsearchServiceSoftwareUpdate.
type cancelSoftwareUpdateOutput struct {
	ServiceSoftwareOptions serviceSoftwareOptionsJSON `json:"ServiceSoftwareOptions"`
}

func (h *Handler) handleCancelElasticsearchServiceSoftwareUpdate(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req cancelSoftwareUpdateRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	_, cancelErr := h.Backend.CancelElasticsearchServiceSoftwareUpdate(h.reqContext(r), req.DomainName)
	if cancelErr != nil {
		if errors.Is(cancelErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", cancelErr.Error())
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", cancelErr.Error())
		}

		return
	}

	h.writeJSON(r, w, cancelSoftwareUpdateOutput{
		ServiceSoftwareOptions: serviceSoftwareOptionsJSON{
			UpdateAvailable: false,
			Cancellable:     false,
			UpdateStatus:    "NOT_ELIGIBLE",
			Description:     "No software update scheduled",
		},
	})
}

func (h *Handler) handleDeleteElasticsearchServiceRole(w http.ResponseWriter, _ *http.Request) {
	_ = h.Backend.DeleteElasticsearchServiceRole()
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleStartElasticsearchServiceSoftwareUpdate(w http.ResponseWriter, r *http.Request) {
	var req cancelSoftwareUpdateRequest
	if !h.decodeRequest(w, r, &req) {
		return
	}

	if _, err := h.Backend.StartElasticsearchServiceSoftwareUpdate(h.reqContext(r), req.DomainName); err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{"ServiceSoftwareOptions": map[string]any{
		"UpdateStatus": "PENDING_UPDATE",
		"Cancellable":  true,
	}})
}

func (h *Handler) handleDescribeInboundCrossClusterSearchConnections(w http.ResponseWriter, r *http.Request) {
	connections := h.Backend.DescribeInboundCrossClusterSearchConnections(h.reqContext(r))
	result := make([]inboundConnectionJSON, 0, len(connections))
	for _, connection := range connections {
		result = append(result, toInboundConnectionJSON(connection))
	}

	h.writeJSON(r, w, map[string]any{"CrossClusterSearchConnections": result})
}

func (h *Handler) handleDescribeOutboundCrossClusterSearchConnections(w http.ResponseWriter, r *http.Request) {
	connections := h.Backend.DescribeOutboundCrossClusterSearchConnections(h.reqContext(r))
	result := make([]outboundConnectionJSON, 0, len(connections))
	for _, connection := range connections {
		result = append(result, toOutboundConnectionJSON(connection))
	}

	h.writeJSON(r, w, map[string]any{"CrossClusterSearchConnections": result})
}

func (h *Handler) handleDescribePackages(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PackageIDs []string `json:"PackageIDs"`
	}
	if !h.decodeRequest(w, r, &req) {
		return
	}

	packages := h.Backend.DescribePackages(h.reqContext(r), req.PackageIDs)
	result := make([]packageJSON, 0, len(packages))
	for _, pkg := range packages {
		result = append(result, toPackageJSON(pkg))
	}

	h.writeJSON(r, w, map[string]any{"PackageDetailsList": result})
}

func (h *Handler) handleUpdatePackage(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PackageID          string `json:"PackageID"`
		PackageDescription string `json:"PackageDescription"`
	}
	if !h.decodeRequest(w, r, &req) {
		return
	}

	pkg, err := h.Backend.UpdatePackage(h.reqContext(r), req.PackageID, req.PackageDescription)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{"PackageDetails": toPackageJSON(pkg)})
}

func (h *Handler) handleDescribeVpcEndpoints(w http.ResponseWriter, r *http.Request) {
	var req struct {
		VpcEndpointIDs []string `json:"VpcEndpointIds"`
	}
	if !h.decodeRequest(w, r, &req) {
		return
	}

	h.writeJSON(r, w, map[string]any{
		"VpcEndpoints":      toVpcEndpointsJSON(h.Backend.DescribeVpcEndpoints(h.reqContext(r), req.VpcEndpointIDs)),
		"VpcEndpointErrors": []any{},
	})
}

func (h *Handler) handleUpdateVpcEndpoint(w http.ResponseWriter, r *http.Request) {
	var req struct {
		VpcOptions    map[string]string `json:"VpcOptions"`
		VpcEndpointID string            `json:"VpcEndpointId"`
	}
	if !h.decodeRequest(w, r, &req) {
		return
	}

	endpoint, err := h.Backend.UpdateVpcEndpoint(h.reqContext(r), req.VpcEndpointID, req.VpcOptions)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{"VpcEndpoint": toVpcEndpointJSON(endpoint)})
}

func (h *Handler) handleListVpcEndpoints(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(r, w, map[string]any{
		"VpcEndpointSummaryList": toVpcEndpointsJSON(h.Backend.ListVpcEndpoints(h.reqContext(r))),
	})
}

// compatibleVersionEntry is the JSON representation of a compatible version pair.
type compatibleVersionEntry struct {
	SourceVersion  string   `json:"SourceVersion"`
	TargetVersions []string `json:"TargetVersions"`
}

// compatibleVersionsFor returns the valid upgrade targets for a given Elasticsearch version.
func compatibleVersionsFor(version string) []string {
	switch version {
	case elasticsearchVersion51, elasticsearchVersion53, elasticsearchVersion55:
		return []string{elasticsearchVersion56}
	case elasticsearchVersion56:
		return []string{elasticsearchVersion68}
	case elasticsearchVersion60, elasticsearchVersion62, elasticsearchVersion63,
		elasticsearchVersion64, elasticsearchVersion65, elasticsearchVersion67:
		return []string{elasticsearchVersion68}
	case elasticsearchVersion68:
		return []string{elasticsearchVersion71, defaultElasticsearchVersion}
	case elasticsearchVersion71, elasticsearchVersion74,
		elasticsearchVersion77, elasticsearchVersion78, elasticsearchVersion79:
		return []string{defaultElasticsearchVersion}
	case elasticsearchVersion713:
		return []string{elasticsearchVersion716, elasticsearchVersion717}
	case elasticsearchVersion716:
		return []string{elasticsearchVersion717}
	default:
		return []string{}
	}
}

func (h *Handler) handleGetCompatibleElasticsearchVersions(w http.ResponseWriter, r *http.Request) {
	domainName := r.URL.Query().Get("domainName")

	if domainName != "" {
		d, err := h.Backend.DescribeDomain(h.reqContext(r), domainName)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}

		targets := compatibleVersionsFor(d.ElasticsearchVersion)
		h.writeJSON(r, w, map[string]any{
			"CompatibleElasticsearchVersions": []compatibleVersionEntry{
				{SourceVersion: d.ElasticsearchVersion, TargetVersions: targets},
			},
		})

		return
	}

	h.writeJSON(r, w, map[string]any{"CompatibleElasticsearchVersions": []compatibleVersionEntry{
		{
			SourceVersion:  elasticsearchVersion68,
			TargetVersions: []string{elasticsearchVersion71, defaultElasticsearchVersion},
		},
		{SourceVersion: elasticsearchVersion71, TargetVersions: []string{defaultElasticsearchVersion}},
	}})
}

func (h *Handler) handleListElasticsearchVersions(w http.ResponseWriter, r *http.Request) {
	versions := []string{
		elasticsearchVersion717, elasticsearchVersion716, elasticsearchVersion713,
		defaultElasticsearchVersion, elasticsearchVersion79, elasticsearchVersion78,
		elasticsearchVersion77, elasticsearchVersion74, elasticsearchVersion71,
		elasticsearchVersion68, elasticsearchVersion67, elasticsearchVersion65,
		elasticsearchVersion64, elasticsearchVersion63, elasticsearchVersion62,
		elasticsearchVersion60, elasticsearchVersion56, elasticsearchVersion55,
		elasticsearchVersion53, elasticsearchVersion51, "2.3", "1.5",
	}
	h.writeJSON(r, w, map[string]any{"ElasticsearchVersions": versions})
}

func (h *Handler) handleDeleteInboundCrossClusterSearchConnection(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, elasticsearchCCSInbound+"/")
	connection, err := h.Backend.DeleteInboundCrossClusterSearchConnection(h.reqContext(r), id)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{keyCrossClusterSearchConnection: toInboundConnectionJSON(connection)})
}

func (h *Handler) handleDeleteOutboundCrossClusterSearchConnection(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, elasticsearchCCSOutbound+"/")
	connection, err := h.Backend.DeleteOutboundCrossClusterSearchConnection(h.reqContext(r), id)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{keyCrossClusterSearchConnection: toOutboundConnectionJSON(connection)})
}

func (h *Handler) handleDeleteVpcEndpoint(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, elasticsearchVpcEndpoints+"/")
	endpoint, err := h.Backend.DeleteVpcEndpoint(h.reqContext(r), id)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{"VpcEndpointSummary": toVpcEndpointJSON(endpoint)})
}

func (h *Handler) handleDescribeReservedElasticsearchInstanceOfferings(w http.ResponseWriter, r *http.Request) {
	offerings := h.Backend.DescribeReservedElasticsearchInstanceOfferings()
	result := make([]map[string]any, 0, len(offerings))
	for _, offering := range offerings {
		result = append(result, map[string]any{
			"ReservedElasticsearchInstanceOfferingId": offering.OfferingID,
			"ElasticsearchInstanceType":               offering.InstanceType,
			"PaymentOption":                           offering.PaymentOption,
			"CurrencyCode":                            offering.Currency,
			"FixedPrice":                              offering.FixedPrice,
			"UsagePrice":                              offering.UsagePrice,
			"Duration":                                offering.Duration,
		})
	}

	h.writeJSON(r, w, map[string]any{
		"ReservedElasticsearchInstanceOfferings": result,
	})
}

func (h *Handler) handleDescribeReservedElasticsearchInstances(w http.ResponseWriter, r *http.Request) {
	instances := h.Backend.DescribeReservedElasticsearchInstances(h.reqContext(r))
	result := make([]map[string]any, 0, len(instances))
	for _, instance := range instances {
		result = append(result, map[string]any{
			"ReservedElasticsearchInstanceId":         instance.ReservationID,
			"ReservationName":                         instance.ReservationName,
			"ReservedElasticsearchInstanceOfferingId": instance.OfferingID,
			"ElasticsearchInstanceType":               instance.InstanceType,
			"State":                                   instance.State,
			"ElasticsearchInstanceCount":              instance.Count,
		})
	}

	h.writeJSON(r, w, map[string]any{
		"ReservedElasticsearchInstances": result,
	})
}

func (h *Handler) handleDissociatePackage(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, elasticsearchPackages+"/dissociate/")
	parts := strings.SplitN(rest, "/", associatePackagePathParts)
	if len(parts) != associatePackagePathParts {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid package dissociation path")

		return
	}

	if err := h.Backend.DissociatePackage(h.reqContext(r), parts[0], parts[1]); err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{"DomainPackageDetails": map[string]any{
		"PackageID":           parts[0],
		"DomainName":          parts[1],
		"DomainPackageStatus": "DISSOCIATED",
	}})
}

func (h *Handler) handleGetPackageVersionHistory(w http.ResponseWriter, r *http.Request) {
	id := pathID(r.URL.Path, elasticsearchPackages+"/", "/history")
	packages, err := h.Backend.GetPackageVersionHistory(h.reqContext(r), id)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	history := make([]packageJSON, 0, len(packages))
	for _, pkg := range packages {
		history = append(history, toPackageJSON(pkg))
	}

	h.writeJSON(r, w, map[string]any{"PackageVersionHistoryList": history})
}

func (h *Handler) handlePurchaseReservedElasticsearchInstanceOffering(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OfferingID      string `json:"ReservedElasticsearchInstanceOfferingId"`
		ReservationName string `json:"ReservationName"`
		InstanceCount   int    `json:"InstanceCount"`
	}
	if !h.decodeRequest(w, r, &req) {
		return
	}

	instance, err := h.Backend.PurchaseReservedElasticsearchInstanceOffering(
		h.reqContext(r),
		req.OfferingID,
		req.ReservationName,
		req.InstanceCount,
	)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{
		"ReservedElasticsearchInstanceId": instance.ReservationID,
		"ReservationName":                 instance.ReservationName,
	})
}

func (h *Handler) handleRejectInboundCrossClusterSearchConnection(w http.ResponseWriter, r *http.Request) {
	id := pathID(r.URL.Path, elasticsearchCCSInbound+"/", "/reject")
	connection, err := h.Backend.RejectInboundCrossClusterSearchConnection(h.reqContext(r), id)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{keyCrossClusterSearchConnection: toInboundConnectionJSON(connection)})
}

func (h *Handler) handleUpgradeElasticsearchDomain(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DomainName       string `json:"DomainName"`
		TargetVersion    string `json:"TargetVersion"`
		PerformCheckOnly bool   `json:"PerformCheckOnly"`
	}
	if !h.decodeRequest(w, r, &req) {
		return
	}

	ctx := h.reqContext(r)
	if !req.PerformCheckOnly {
		if _, err := h.Backend.UpgradeElasticsearchDomain(ctx, req.DomainName, req.TargetVersion); err != nil {
			h.writeOperationError(r, w, err)

			return
		}
	} else if _, err := h.Backend.DescribeDomain(ctx, req.DomainName); err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, req)
}

func (h *Handler) handleDeletePackage(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, elasticsearchPackages+"/")
	pkg, err := h.Backend.DeletePackage(h.reqContext(r), id)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{"PackageDetails": toPackageJSON(pkg)})
}

func (h *Handler) handleDescribeDomainAutoTunes(w http.ResponseWriter, r *http.Request, domainName string) {
	if err := h.Backend.DescribeDomainAutoTunes(h.reqContext(r), domainName); err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{"AutoTunes": []any{}})
}

func (h *Handler) handleDescribeDomainChangeProgress(w http.ResponseWriter, r *http.Request, domainName string) {
	if err := h.Backend.DescribeDomainChangeProgress(h.reqContext(r), domainName); err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{"ChangeProgressStatus": map[string]any{"Status": "COMPLETED"}})
}

func (h *Handler) handleDescribeElasticsearchInstanceTypeLimits(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(r, w, map[string]any{"LimitsByRole": map[string]any{
		"data": map[string]any{"InstanceLimits": map[string]any{"InstanceCountLimits": map[string]any{
			"MinimumInstanceCount": minimumInstanceCount,
			"MaximumInstanceCount": maximumInstanceCount,
		}}},
	}})
}

func (h *Handler) handleGetUpgradeHistory(w http.ResponseWriter, r *http.Request) {
	domainName := pathID(r.URL.Path, elasticsearchUpgradeDomain+"/", "/history")
	if err := h.Backend.GetUpgradeHistory(h.reqContext(r), domainName); err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{"UpgradeHistories": []any{}})
}

func (h *Handler) handleGetUpgradeStatus(w http.ResponseWriter, r *http.Request) {
	domainName := pathID(r.URL.Path, elasticsearchUpgradeDomain+"/", "/status")
	if err := h.Backend.GetUpgradeStatus(h.reqContext(r), domainName); err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{"UpgradeStep": "UPGRADE", "StepStatus": "SUCCEEDED"})
}

func (h *Handler) handleListDomainsForPackage(w http.ResponseWriter, r *http.Request) {
	id := pathID(r.URL.Path, elasticsearchPackages+"/", "/domains")
	domains, err := h.Backend.ListDomainsForPackage(h.reqContext(r), id)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	result := make([]domainPackageJSON, 0, len(domains))
	for _, domainName := range domains {
		result = append(result, domainPackageJSON{
			PackageID: id, DomainName: domainName, DomainPackageStatus: statusActive,
		})
	}

	h.writeJSON(r, w, map[string]any{"DomainPackageDetailsList": result})
}

func (h *Handler) handleListElasticsearchInstanceTypes(w http.ResponseWriter, r *http.Request) {
	h.writeJSON(r, w, map[string]any{"ElasticsearchInstanceTypes": []string{
		defaultInstanceType,
		largeInstanceType,
	}})
}

func (h *Handler) handleListPackagesForDomain(w http.ResponseWriter, r *http.Request) {
	domainName := pathID(r.URL.Path, elasticsearchDomainPackages+"/", "/packages")
	packages := h.Backend.ListPackagesForDomain(h.reqContext(r), domainName)
	result := make([]domainPackageJSON, 0, len(packages))
	for _, pkg := range packages {
		result = append(result, domainPackageJSON{
			PackageID:           pkg.ID,
			PackageName:         pkg.Name,
			PackageType:         pkg.PackageType,
			DomainName:          domainName,
			DomainPackageStatus: statusActive,
		})
	}

	h.writeJSON(r, w, map[string]any{"DomainPackageDetailsList": result})
}

func (h *Handler) handleListVpcEndpointAccess(w http.ResponseWriter, r *http.Request, domainName string) {
	accounts, err := h.Backend.ListVpcEndpointAccess(h.reqContext(r), domainName)
	if err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	principals := make([]authorizedPrincipalJSON, 0, len(accounts))
	for _, account := range accounts {
		principals = append(principals, authorizedPrincipalJSON{PrincipalType: "AWS_ACCOUNT", Principal: account})
	}

	h.writeJSON(r, w, map[string]any{"AuthorizedPrincipalList": principals})
}

func (h *Handler) handleListVpcEndpointsForDomain(w http.ResponseWriter, r *http.Request, domainName string) {
	h.writeJSON(r, w, map[string]any{
		"VpcEndpointSummaryList": toVpcEndpointsJSON(h.Backend.ListVpcEndpointsForDomain(h.reqContext(r), domainName)),
	})
}

func (h *Handler) handleRevokeVpcEndpointAccess(w http.ResponseWriter, r *http.Request, domainName string) {
	var req authorizeVpcEndpointAccessRequest
	if !h.decodeRequest(w, r, &req) {
		return
	}

	if err := h.Backend.RevokeVpcEndpointAccess(h.reqContext(r), domainName, req.Account); err != nil {
		h.writeOperationError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{})
}

func (h *Handler) decodeRequest(w http.ResponseWriter, r *http.Request, out any) bool {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return false
	}

	if len(body) == 0 {
		return true
	}

	if err = json.Unmarshal(body, out); err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return false
	}

	return true
}

func (h *Handler) writeOperationError(r *http.Request, w http.ResponseWriter, err error) {
	if errors.Is(err, ErrDomainNotFound) || errors.Is(err, ErrPackageNotFound) ||
		errors.Is(err, ErrVpcEndpointNotFound) || errors.Is(err, ErrConnectionNotFound) {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

		return
	}

	h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
}

func toVpcEndpointsJSON(endpoints []*VpcEndpoint) []vpcEndpointJSON {
	result := make([]vpcEndpointJSON, 0, len(endpoints))
	for _, endpoint := range endpoints {
		result = append(result, toVpcEndpointJSON(endpoint))
	}

	return result
}

func pathID(path, prefix, suffix string) string {
	id := strings.TrimPrefix(path, prefix)
	id, _ = strings.CutSuffix(id, suffix)

	return id
}
