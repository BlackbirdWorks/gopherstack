package opensearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	openSearchPathPrefix             = "/2021-01-01/opensearch/domain"
	openSearchTagsPath               = "/2021-01-01/tags"
	openSearchTagsRemoval            = "/2021-01-01/tags-removal"
	openSearchCCPath                 = "/2021-01-01/opensearch/cc"
	openSearchDirectQueryPath        = "/2021-01-01/opensearch/directQueryDataSource"
	openSearchPackagesPath           = "/2021-01-01/packages"
	openSearchServiceSwPath          = "/2021-01-01/opensearch/serviceSoftwareUpdate"
	openSearchApplicationPath        = "/2021-01-01/opensearch/application"
	openSearchVersionsPath           = "/2021-01-01/opensearch/versions"
	openSearchInstanceTypesPath      = "/2021-01-01/opensearch/instanceTypeDetails"
	openSearchCompatiblePath         = "/2021-01-01/opensearch/compatibleVersions"
	openSearchVpcEndpointsPath       = "/2021-01-01/opensearch/vpcEndpoints"
	openSearchScheduledActionsPath   = "/2021-01-01/opensearch/scheduledActions"
	openSearchReservedPath           = "/2021-01-01/opensearch/reservedInstances"
	openSearchUpgradePath            = "/2021-01-01/opensearch/upgradeDomain"
	openSearchInstanceTypeLimitsPath = "/2021-01-01/opensearch/instanceTypeLimits"
	openSearchServerlessPath         = "/2021-11-01/opensearch/serverless"
	openSearchServiceName            = "OpenSearch"
	// pkgPathParts is the number of path segments after the associate prefix (PackageID/DomainName).
	pkgPathParts = 2
	// opUnknown is the sentinel returned when no operation can be determined from a request.
	opUnknown = "Unknown"
	// JSON field name constants reused across stub responses.
	jsonKeyStatus           = "Status"
	jsonKeyConnection       = "Connection"
	jsonKeyConnectionID     = "ConnectionId"
	jsonKeyConnectionStatus = "ConnectionStatus"
	jsonKeyPkgDetailsList   = "DomainPackageDetailsList"
	jsonKeyPackageID        = "PackageID"
	jsonKeyPackageDetails   = "PackageDetails"
	jsonKeyPackageName      = "PackageName"
	jsonKeyPackageStatus    = "PackageStatus"
	jsonKeyVpcEndpointID    = "VpcEndpointId"
	jsonKeyStatusCode       = "StatusCode"
	jsonKeyAppName          = "Name"
	jsonKeyAppArn           = "Arn"
	jsonKeyDataSource       = "DataSource"
)

// Handler is the HTTP handler for OpenSearch operations.
type Handler struct {
	Backend   StorageBackend
	AccountID string
	Region    string
}

// NewHandler creates a new OpenSearch Handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return openSearchServiceName }

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathSubdomain }

// openSearchPathPrefixes holds all path prefixes handled by the OpenSearch service.
//
//nolint:gochecknoglobals // intentional package-level prefix table
var openSearchPathPrefixes = []string{
	openSearchPathPrefix,
	openSearchCCPath,
	openSearchDirectQueryPath,
	openSearchPackagesPath,
	openSearchServiceSwPath,
	openSearchApplicationPath,
	openSearchVersionsPath,
	openSearchInstanceTypesPath,
	openSearchCompatiblePath,
	openSearchVpcEndpointsPath,
	openSearchScheduledActionsPath,
	openSearchReservedPath,
	openSearchUpgradePath,
	openSearchInstanceTypeLimitsPath,
	openSearchServerlessPath,
}

// isOpenSearchPath returns true when the given path belongs to the OpenSearch service.
func isOpenSearchPath(path string) bool {
	if path == openSearchTagsPath || path == openSearchTagsRemoval {
		return true
	}

	for _, prefix := range openSearchPathPrefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}

	return false
}

// RouteMatcher returns a matcher that selects OpenSearch requests by path prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return isOpenSearchPath(c.Request().URL.Path)
	}
}

// GetSupportedOperations returns supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AcceptInboundConnection",
		"AddDataSource",
		"AddDirectQueryDataSource",
		"AddTags",
		"AssociatePackage",
		"AssociatePackages",
		"AuthorizeVpcEndpointAccess",
		"CancelDomainConfigChange",
		"CancelServiceSoftwareUpdate",
		"CreateApplication",
		"CreateDomain",
		"CreateIndex",
		"CreateOutboundConnection",
		"CreatePackage",
		"CreateVpcEndpoint",
		"DeleteApplication",
		"DeleteDataSource",
		"DeleteDirectQueryDataSource",
		"DeleteDomain",
		"DeleteInboundConnection",
		"DeleteIndex",
		"DeleteOutboundConnection",
		"DeletePackage",
		"DeleteVpcEndpoint",
		"DescribeDomain",
		"DescribeDomainAutoTunes",
		"DescribeDomainChangeProgress",
		"DescribeDomainConfig",
		"DescribeDomainHealth",
		"DescribeDomainNodes",
		"DescribeDomains",
		"DescribeDryRunProgress",
		"DescribeInboundConnections",
		"DescribeInstanceTypeLimits",
		"DescribeOutboundConnections",
		"DescribePackages",
		"DescribeReservedInstanceOfferings",
		"DescribeReservedInstances",
		"DescribeVpcEndpoints",
		"DissociatePackage",
		"DissociatePackages",
		"GetApplication",
		"GetCompatibleVersions",
		"GetDataSource",
		"GetDefaultApplicationSetting",
		"GetDirectQueryDataSource",
		"GetDomainMaintenanceStatus",
		"GetIndex",
		"GetPackageVersionHistory",
		"GetUpgradeHistory",
		"GetUpgradeStatus",
		"ListApplications",
		"ListDataSources",
		"ListDirectQueryDataSources",
		"ListDomainMaintenances",
		"ListDomainNames",
		"ListDomainsForPackage",
		"ListInstanceTypeDetails",
		"ListPackagesForDomain",
		"ListScheduledActions",
		"ListTags",
		"ListVersions",
		"ListVpcEndpointAccess",
		"ListVpcEndpoints",
		"ListVpcEndpointsForDomain",
		"PurchaseReservedInstanceOffering",
		"PutDefaultApplicationSetting",
		"RejectInboundConnection",
		"RemoveTags",
		"RevokeVpcEndpointAccess",
		"StartDomainMaintenance",
		"StartServiceSoftwareUpdate",
		"UpdateApplication",
		"UpdateDataSource",
		"UpdateDirectQueryDataSource",
		"UpdateDomainConfig",
		"UpdateIndex",
		"UpdatePackage",
		"UpdatePackageScope",
		"UpdateScheduledAction",
		"UpdateVpcEndpoint",
		"UpgradeDomain",
		// Serverless operations
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

// Reset clears the handler's backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

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

// domainClusterConfig holds the cluster configuration request parameters for a domain.
type domainClusterConfig struct {
	ZoneAwarenessConfig        *zoneAwarenessConfigJSON        `json:"ZoneAwarenessConfig,omitempty"`
	BlueGreenDeploymentOptions *blueGreenDeploymentOptionsJSON `json:"BlueGreenDeploymentOptions,omitempty"`
	InstanceType               string                          `json:"InstanceType"`
	DedicatedMasterType        string                          `json:"DedicatedMasterType,omitempty"`
	WarmType                   string                          `json:"WarmType,omitempty"`
	InstanceCount              int                             `json:"InstanceCount"`
	DedicatedMasterCount       int                             `json:"DedicatedMasterCount,omitempty"`
	WarmCount                  int                             `json:"WarmCount,omitempty"`
	DedicatedMasterEnabled     bool                            `json:"DedicatedMasterEnabled,omitempty"`
	ZoneAwarenessEnabled       bool                            `json:"ZoneAwarenessEnabled,omitempty"`
	WarmEnabled                bool                            `json:"WarmEnabled,omitempty"`
	ColdStorageEnabled         bool                            `json:"ColdStorageEnabled,omitempty"`
	MultiAZWithStandbyEnabled  bool                            `json:"MultiAZWithStandbyEnabled,omitempty"`
}

// zoneAwarenessConfigJSON holds zone awareness config in JSON.
type zoneAwarenessConfigJSON struct {
	AvailabilityZoneCount int `json:"AvailabilityZoneCount"`
}

// ebsOptionsJSON is the JSON representation of EBS options.
type ebsOptionsJSON struct {
	VolumeType string `json:"VolumeType,omitempty"`
	KMSKeyID   string `json:"KMSKeyId,omitempty"`
	VolumeSize int    `json:"VolumeSize,omitempty"`
	IOPS       int    `json:"Iops,omitempty"`
	Throughput int    `json:"Throughput,omitempty"`
	EBSEnabled bool   `json:"EBSEnabled"`
}

// snapshotOptionsJSON is the JSON representation of snapshot options.
type snapshotOptionsJSON struct {
	AutomatedSnapshotStartHour int `json:"AutomatedSnapshotStartHour"`
}

// encryptAtRestOptionsJSON is the JSON representation of encryption at rest options.
type encryptAtRestOptionsJSON struct {
	KMSKeyID string `json:"KMSKeyId,omitempty"`
	Enabled  bool   `json:"Enabled"`
}

// nodeToNodeEncryptJSON is the JSON representation of node-to-node encryption options.
type nodeToNodeEncryptJSON struct {
	Enabled bool `json:"Enabled"`
}

// domainEndpointOptionsJSON is the JSON representation of domain endpoint options.
type domainEndpointOptionsJSON struct {
	CustomEndpointCertificateArn string `json:"CustomEndpointCertificateArn,omitempty"`
	CustomEndpoint               string `json:"CustomEndpoint,omitempty"`
	TLSSecurityPolicy            string `json:"TLSSecurityPolicy,omitempty"`
	EnforceHTTPS                 bool   `json:"EnforceHTTPS"`
	CustomEndpointEnabled        bool   `json:"CustomEndpointEnabled"`
}

// samlOptionsJSON is the JSON representation of SAML options.
type samlOptionsJSON struct {
	IDPEntityID           string `json:"Idp,omitempty"`
	IDPMetadataContent    string `json:"MasterBackendRole,omitempty"`
	RolesKey              string `json:"RolesKey,omitempty"`
	SubjectKey            string `json:"SubjectKey,omitempty"`
	SessionTimeoutMinutes int    `json:"SessionTimeoutMinutes,omitempty"`
	Enabled               bool   `json:"Enabled"`
}

// advancedSecurityOptionsJSON is the JSON representation of advanced security options.
type advancedSecurityOptionsJSON struct {
	SAMLOptions                 *samlOptionsJSON `json:"SAMLOptions,omitempty"`
	AnonymousAuthEnabled        bool             `json:"AnonymousAuthEnabled"`
	Enabled                     bool             `json:"Enabled"`
	InternalUserDatabaseEnabled bool             `json:"InternalUserDatabaseEnabled"`
}

// vpcOptionsJSON is the JSON representation of VPC options.
type vpcOptionsJSON struct {
	VPCID            string   `json:"VPCId,omitempty"`
	SecurityGroupIDs []string `json:"SecurityGroupIds,omitempty"`
	SubnetIDs        []string `json:"SubnetIds,omitempty"`
}

// cognitoOptionsJSON is the JSON representation of Cognito options.
type cognitoOptionsJSON struct {
	IdentityPoolID string `json:"IdentityPoolId,omitempty"`
	RoleARN        string `json:"RoleArn,omitempty"`
	UserPoolID     string `json:"UserPoolId,omitempty"`
	Enabled        bool   `json:"Enabled"`
}

// logPublishingOptionJSON is the JSON representation of a log publishing option.
type logPublishingOptionJSON struct {
	CloudWatchLogsLogGroupARN string `json:"CloudWatchLogsLogGroupArn,omitempty"`
	Enabled                   bool   `json:"Enabled"`
}

// packageSourceJSON is the JSON representation of a package S3 source.
type packageSourceJSON struct {
	S3BucketName string `json:"S3BucketName,omitempty"`
	S3Key        string `json:"S3Key,omitempty"`
}

// packageEncryptionOptionsJSON is the JSON representation of package encryption options.
type packageEncryptionOptionsJSON struct {
	KmsKeyIdentifier  string `json:"KmsKeyIdentifier,omitempty"`
	EncryptionEnabled bool   `json:"EncryptionEnabled"`
}

// offPeakWindowOptionsJSON is the JSON representation of off-peak window options.
type offPeakWindowOptionsJSON struct {
	OffPeakWindow *offPeakWindowJSON `json:"OffPeakWindow,omitempty"`
	Enabled       bool               `json:"Enabled"`
}

// offPeakWindowJSON is the JSON representation of an off-peak window.
type offPeakWindowJSON struct {
	WindowStartTime *windowStartTimeJSON `json:"WindowStartTime,omitempty"`
}

// windowStartTimeJSON is the JSON representation of a window start time.
type windowStartTimeJSON struct {
	Hours   int `json:"Hours"`
	Minutes int `json:"Minutes"`
}

// iamIdentityCenterOptionsJSON is the JSON representation of IAM Identity Center options.
type iamIdentityCenterOptionsJSON struct {
	IamIdentityCenterArn                   string `json:"IamIdentityCenterArn,omitempty"`
	IamRoleForIdentityCenterApplicationArn string `json:"IamRoleForIdentityCenterApplicationArn,omitempty"`
	EnabledAPIAccess                       bool   `json:"EnabledAPIAccess"`
}

// enableSoftwareUpdateOptionsJSON is the JSON representation of enable software update options.
type enableSoftwareUpdateOptionsJSON struct {
	AutoSoftwareUpdateEnabled bool `json:"AutoSoftwareUpdateEnabled"`
}

// blueGreenDeploymentOptionsJSON is the JSON representation of blue-green deployment options.
type blueGreenDeploymentOptionsJSON struct {
	Enabled bool `json:"Enabled"`
}

// domainNamePattern matches valid OpenSearch domain names: starts with a lowercase letter,
// 3–28 characters, only lowercase letters, digits, and hyphens.
var domainNamePattern = regexp.MustCompile(`^[a-z][a-z0-9\-]{2,27}$`)

// engineVersionPattern matches valid engine version strings like OpenSearch_2.11 or Elasticsearch_7.10.
var engineVersionPattern = regexp.MustCompile(`^(OpenSearch|Elasticsearch)_\d+\.\d+$`)

// validateDomainName checks that a domain name meets AWS OpenSearch naming rules.
func validateDomainName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	if !domainNamePattern.MatchString(name) {
		return fmt.Errorf("%w: DomainName %q is not valid. Domain names must start with a lowercase letter "+
			"and be between 3 and 28 characters. Valid characters are a-z (lowercase only), 0-9, and - (hyphen)",
			ErrInvalidParameter, name)
	}

	return nil
}

// domainJSON is the JSON request body for CreateDomain.
type domainJSON struct {
	ClusterConfig               *domainClusterConfig                `json:"ClusterConfig,omitempty"`
	EBSOptions                  *ebsOptionsJSON                     `json:"EBSOptions,omitempty"`
	SnapshotOptions             *snapshotOptionsJSON                `json:"SnapshotOptions,omitempty"`
	EncryptionAtRestOptions     *encryptAtRestOptionsJSON           `json:"EncryptionAtRestOptions,omitempty"`
	NodeToNodeEncryptionOptions *nodeToNodeEncryptJSON              `json:"NodeToNodeEncryptionOptions,omitempty"`
	DomainEndpointOptions       *domainEndpointOptionsJSON          `json:"DomainEndpointOptions,omitempty"`
	AdvancedSecurityOptions     *advancedSecurityOptionsJSON        `json:"AdvancedSecurityOptions,omitempty"`
	VPCOptions                  *vpcOptionsJSON                     `json:"VPCOptions,omitempty"`
	CognitoOptions              *cognitoOptionsJSON                 `json:"CognitoOptions,omitempty"`
	OffPeakWindowOptions        *offPeakWindowOptionsJSON           `json:"OffPeakWindowOptions,omitempty"`
	IamIdentityCenterOptions    *iamIdentityCenterOptionsJSON       `json:"IamIdentityCenterOptions,omitempty"`
	EnableSoftwareUpdateOptions *enableSoftwareUpdateOptionsJSON    `json:"EnableSoftwareUpdateOptions,omitempty"`
	LogPublishingOptions        map[string]*logPublishingOptionJSON `json:"LogPublishingOptions,omitempty"`
	Tags                        map[string]string                   `json:"TagList,omitempty"`
	DomainName                  string                              `json:"DomainName"`
	EngineVersion               string                              `json:"EngineVersion"`
	AccessPolicies              string                              `json:"AccessPolicies,omitempty"`
}

// domainStatusJSON is the JSON response for domain operations.
type domainStatusJSON struct {
	EBSOptions                  *ebsOptionsJSON                     `json:"EBSOptions,omitempty"`
	SnapshotOptions             *snapshotOptionsJSON                `json:"SnapshotOptions,omitempty"`
	EncryptionAtRestOptions     *encryptAtRestOptionsJSON           `json:"EncryptionAtRestOptions,omitempty"`
	NodeToNodeEncryptionOptions *nodeToNodeEncryptJSON              `json:"NodeToNodeEncryptionOptions,omitempty"`
	DomainEndpointOptions       *domainEndpointOptionsJSON          `json:"DomainEndpointOptions,omitempty"`
	AdvancedSecurityOptions     *advancedSecurityOptionsJSON        `json:"AdvancedSecurityOptions,omitempty"`
	VPCOptions                  *vpcOptionsJSON                     `json:"VPCOptions,omitempty"`
	CognitoOptions              *cognitoOptionsJSON                 `json:"CognitoOptions,omitempty"`
	OffPeakWindowOptions        *offPeakWindowOptionsJSON           `json:"OffPeakWindowOptions,omitempty"`
	IamIdentityCenterOptions    *iamIdentityCenterOptionsJSON       `json:"IamIdentityCenterOptions,omitempty"`
	EnableSoftwareUpdateOptions *enableSoftwareUpdateOptionsJSON    `json:"EnableSoftwareUpdateOptions,omitempty"`
	LogPublishingOptions        map[string]*logPublishingOptionJSON `json:"LogPublishingOptions,omitempty"`
	DomainName                  string                              `json:"DomainName"`
	ARN                         string                              `json:"ARN"`
	EngineVersion               string                              `json:"EngineVersion"`
	Endpoint                    string                              `json:"Endpoint"`
	DomainProcessingStatus      string                              `json:"DomainProcessingStatus"`
	AccessPolicies              string                              `json:"AccessPolicies,omitempty"`
	ClusterConfig               clusterConfigJSON                   `json:"ClusterConfig"`
	Processing                  bool                                `json:"Processing"`
}

// clusterConfigJSON is the JSON representation of cluster config.
type clusterConfigJSON struct {
	ZoneAwarenessConfig        *zoneAwarenessConfigJSON        `json:"ZoneAwarenessConfig,omitempty"`
	BlueGreenDeploymentOptions *blueGreenDeploymentOptionsJSON `json:"BlueGreenDeploymentOptions,omitempty"`
	InstanceType               string                          `json:"InstanceType"`
	DedicatedMasterType        string                          `json:"DedicatedMasterType,omitempty"`
	WarmType                   string                          `json:"WarmType,omitempty"`
	InstanceCount              int                             `json:"InstanceCount"`
	DedicatedMasterCount       int                             `json:"DedicatedMasterCount,omitempty"`
	WarmCount                  int                             `json:"WarmCount,omitempty"`
	DedicatedMasterEnabled     bool                            `json:"DedicatedMasterEnabled"`
	ZoneAwarenessEnabled       bool                            `json:"ZoneAwarenessEnabled"`
	WarmEnabled                bool                            `json:"WarmEnabled"`
	ColdStorageEnabled         bool                            `json:"ColdStorageEnabled"`
	MultiAZWithStandbyEnabled  bool                            `json:"MultiAZWithStandbyEnabled"`
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
	DomainName    string `json:"DomainName"`
	EngineVersion string `json:"EngineVersion"`
}

// ServeHTTP implements [http.Handler] for the OpenSearch service.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.handleTagRoutes(w, r) {
		return
	}

	if h.dispatchNonDomainRoutes(w, r) {
		return
	}

	h.dispatchDomainRoutes(w, r)
}

// dispatchNonDomainRoutes handles all non-domain prefix paths.
// Returns true if the request was handled.
func (h *Handler) dispatchNonDomainRoutes(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path

	switch {
	case strings.HasPrefix(path, openSearchCCPath):
		h.handleCCRoutes(w, r)
	case strings.HasPrefix(path, openSearchDirectQueryPath):
		h.handleDirectQueryRoutes(w, r)
	case strings.HasPrefix(path, openSearchPackagesPath):
		h.handlePackageRoutes(w, r)
	case strings.HasPrefix(path, openSearchServiceSwPath):
		h.handleServiceSoftwareRoutes(w, r)
	case strings.HasPrefix(path, openSearchApplicationPath):
		h.handleApplicationRoutes(w, r)
	case strings.HasPrefix(path, openSearchVersionsPath):
		h.handleVersionsRoutes(w, r)
	case strings.HasPrefix(path, openSearchInstanceTypesPath):
		h.handleInstanceTypeDetailsRoutes(w, r)
	case strings.HasPrefix(path, openSearchCompatiblePath):
		h.handleCompatibleVersionsRoutes(w, r)
	case strings.HasPrefix(path, openSearchVpcEndpointsPath):
		h.handleVpcEndpointsRoutes(w, r)
	case strings.HasPrefix(path, openSearchScheduledActionsPath):
		h.handleScheduledActionsRoutes(w, r)
	case strings.HasPrefix(path, openSearchReservedPath):
		h.handleReservedInstancesRoutes(w, r)
	case strings.HasPrefix(path, openSearchInstanceTypeLimitsPath):
		h.handleInstanceTypeLimitsRoutes(w, r)
	case strings.HasPrefix(path, openSearchUpgradePath):
		h.handleUpgradeDomainRoutes(w, r)
	case strings.HasPrefix(path, openSearchServerlessPath):
		h.handleServerlessRoutes(w, r)
	default:
		return false
	}

	return true
}

// dispatchDomainRoutes handles requests under /2021-01-01/opensearch/domain/...
func (h *Handler) dispatchDomainRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchPathPrefix)

	// Root-level domain list/create.
	if rest == "" || rest == "/" {
		h.dispatchDomainRootRoutes(w, r)

		return
	}

	// Bulk describe: GET /domain/describe → DescribeDomains.
	if rest == "/describe" && r.Method == http.MethodGet {
		h.handleDescribeDomains(w, r)

		return
	}

	if !strings.HasPrefix(rest, "/") {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	switch r.Method {
	case http.MethodGet:
		h.dispatchDomainGetRoutes(w, r, rest)
	case http.MethodDelete:
		h.dispatchDomainDeleteRoutes(w, r, rest)
	case http.MethodPost:
		h.handleDomainSubRoutes(w, r, rest)
	case http.MethodPut:
		h.dispatchDomainPutRoutes(w, r, rest)
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// dispatchDomainRootRoutes handles POST/GET on the domain root.
func (h *Handler) dispatchDomainRootRoutes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.handleCreateDomain(w, r)
	case http.MethodGet:
		h.handleListDomainNames(w, r)
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// dispatchDomainDeleteRoutes handles DELETE under a domain path.
func (h *Handler) dispatchDomainDeleteRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	trimmed := strings.TrimPrefix(rest, "/")
	if h.dispatchDomainDeleteRoutesExtended(w, r, trimmed) {
		return
	}

	h.handleDeleteDomain(w, r, domainNameFromRest(rest))
}

// handleUpdateIndexRoute handles PUT {domainName}/index/{indexName}.
func (h *Handler) handleUpdateIndexRoute(w http.ResponseWriter, r *http.Request, trimmed string) {
	parts := strings.SplitN(trimmed, "/index/", 2) //nolint:mnd // path split count
	if len(parts) != 2 {                           //nolint:mnd // path split count
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "invalid index path")

		return
	}

	body, _ := httputils.ReadBody(r)
	var req struct {
		Mappings map[string]any `json:"Mappings"`
		Settings map[string]any `json:"Settings"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}
	idx, err := h.Backend.UpdateIndex(parts[0], parts[1], req.Mappings, req.Settings)
	if err != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

		return
	}
	h.writeJSON(r, w, idx)
}

// dispatchDomainPutRoutes handles PUT under a domain path (UpdateDomainConfig, UpdateIndex).
func (h *Handler) dispatchDomainPutRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	trimmed := domainNameFromRest(rest)

	// UpdateIndex: PUT {domainName}/index/{indexName}
	if strings.Contains(trimmed, "/index/") {
		h.handleUpdateIndexRoute(w, r, trimmed)

		return
	}

	name, ok := strings.CutSuffix(trimmed, "/config")
	if !ok {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	body, _ := httputils.ReadBody(r)
	var req domainJSON
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	input := applyReqToUpdateInput(&req)

	domain, err := h.Backend.UpdateDomainConfig(name, input)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, map[string]any{"DomainConfig": toDomainConfigJSON(domain)})
}

// dispatchDomainGetRoutes handles GET requests under a domain path.
func (h *Handler) dispatchDomainGetRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	trimmed := domainNameFromRest(rest)

	if before, ok := strings.CutSuffix(trimmed, "/config"); ok {
		h.handleDescribeDomainConfig(w, r, before)

		return
	}

	if h.dispatchDomainGetRoutesExtended(w, r, trimmed) {
		return
	}

	// Plain domain name — DescribeDomain.
	h.handleDescribeDomain(w, r, trimmed)
}

func domainNameFromRest(rest string) string {
	name := strings.TrimPrefix(rest, "/")

	return strings.TrimSuffix(name, "/")
}

// handleTagRoutes processes /2021-01-01/tags and /2021-01-01/tags-removal requests.
// Returns true if the request was handled.
func (h *Handler) handleTagRoutes(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path

	switch {
	case path == openSearchTagsPath && r.Method == http.MethodGet:
		h.handleListTags(w, r)

		return true
	case path == openSearchTagsPath && r.Method == http.MethodPost:
		h.handleAddTags(w, r)

		return true
	case path == openSearchTagsRemoval && r.Method == http.MethodPost:
		h.handleRemoveTags(w, r)

		return true
	}

	return false
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

// parseClusterConfigFromReq converts a JSON cluster config to backend ClusterConfig.
func parseClusterConfigFromReq(cc *domainClusterConfig) ClusterConfig {
	if cc == nil {
		return ClusterConfig{}
	}
	cfg := ClusterConfig{
		InstanceType:              cc.InstanceType,
		InstanceCount:             cc.InstanceCount,
		DedicatedMasterEnabled:    cc.DedicatedMasterEnabled,
		DedicatedMasterType:       cc.DedicatedMasterType,
		DedicatedMasterCount:      cc.DedicatedMasterCount,
		ZoneAwarenessEnabled:      cc.ZoneAwarenessEnabled,
		WarmEnabled:               cc.WarmEnabled,
		WarmType:                  cc.WarmType,
		WarmCount:                 cc.WarmCount,
		ColdStorageEnabled:        cc.ColdStorageEnabled,
		MultiAZWithStandbyEnabled: cc.MultiAZWithStandbyEnabled,
	}
	if cc.ZoneAwarenessConfig != nil {
		cfg.ZoneAwarenessConfig = &ZoneAwarenessConfig{
			AvailabilityZoneCount: cc.ZoneAwarenessConfig.AvailabilityZoneCount,
		}
	}
	if cc.BlueGreenDeploymentOptions != nil {
		cfg.BlueGreenDeploymentOptions = &BlueGreenDeploymentOptions{
			Enabled: cc.BlueGreenDeploymentOptions.Enabled,
		}
	}

	return cfg
}

// parseAdvancedSecurityOptsFromReq converts JSON advanced security options to backend type.
func parseAdvancedSecurityOptsFromReq(aso *advancedSecurityOptionsJSON) *AdvancedSecurityOptions {
	if aso == nil {
		return nil
	}
	out := &AdvancedSecurityOptions{
		Enabled:                     aso.Enabled,
		InternalUserDatabaseEnabled: aso.InternalUserDatabaseEnabled,
		AnonymousAuthEnabled:        aso.AnonymousAuthEnabled,
	}
	if aso.SAMLOptions != nil {
		out.SAMLOptions = &SAMLOptionsInput{
			Enabled:               aso.SAMLOptions.Enabled,
			IDPEntityID:           aso.SAMLOptions.IDPEntityID,
			IDPMetadataContent:    aso.SAMLOptions.IDPMetadataContent,
			RolesKey:              aso.SAMLOptions.RolesKey,
			SubjectKey:            aso.SAMLOptions.SubjectKey,
			SessionTimeoutMinutes: aso.SAMLOptions.SessionTimeoutMinutes,
		}
	}

	return out
}

// parseLogPublishingOptsFromReq converts JSON log publishing options to backend type.
func parseLogPublishingOptsFromReq(opts map[string]*logPublishingOptionJSON) map[string]*LogPublishingOption {
	if len(opts) == 0 {
		return nil
	}
	out := make(map[string]*LogPublishingOption, len(opts))
	for k, v := range opts {
		out[k] = &LogPublishingOption{
			Enabled:                   v.Enabled,
			CloudWatchLogsLogGroupARN: v.CloudWatchLogsLogGroupARN,
		}
	}

	return out
}

// applyReqToUpdateInput maps parsed domainJSON fields onto an UpdateDomainConfigInput.
func applyReqToUpdateInput(req *domainJSON) UpdateDomainConfigInput {
	input := UpdateDomainConfigInput{
		EngineVersion:  req.EngineVersion,
		AccessPolicies: req.AccessPolicies,
	}
	if req.ClusterConfig != nil {
		cc := parseClusterConfigFromReq(req.ClusterConfig)
		input.ClusterConfig = &cc
	}
	if req.EBSOptions != nil {
		input.EBSOptions = &EBSOptions{
			EBSEnabled: req.EBSOptions.EBSEnabled,
			VolumeType: req.EBSOptions.VolumeType,
			VolumeSize: req.EBSOptions.VolumeSize,
			IOPS:       req.EBSOptions.IOPS,
			Throughput: req.EBSOptions.Throughput,
			KMSKeyID:   req.EBSOptions.KMSKeyID,
		}
	}
	if req.SnapshotOptions != nil {
		input.SnapshotOptions = &SnapshotOptions{
			AutomatedSnapshotStartHour: req.SnapshotOptions.AutomatedSnapshotStartHour,
		}
	}
	if req.EncryptionAtRestOptions != nil {
		input.EncryptionAtRestOptions = &EncryptionAtRestOptions{
			Enabled:  req.EncryptionAtRestOptions.Enabled,
			KMSKeyID: req.EncryptionAtRestOptions.KMSKeyID,
		}
	}
	if req.NodeToNodeEncryptionOptions != nil {
		input.NodeToNodeEncryptionOptions = &NodeToNodeEncryptionOptions{
			Enabled: req.NodeToNodeEncryptionOptions.Enabled,
		}
	}
	if req.DomainEndpointOptions != nil {
		input.DomainEndpointOptions = &DomainEndpointOptions{
			EnforceHTTPS:                 req.DomainEndpointOptions.EnforceHTTPS,
			TLSSecurityPolicy:            req.DomainEndpointOptions.TLSSecurityPolicy,
			CustomEndpointEnabled:        req.DomainEndpointOptions.CustomEndpointEnabled,
			CustomEndpoint:               req.DomainEndpointOptions.CustomEndpoint,
			CustomEndpointCertificateArn: req.DomainEndpointOptions.CustomEndpointCertificateArn,
		}
	}
	input.AdvancedSecurityOptions = parseAdvancedSecurityOptsFromReq(req.AdvancedSecurityOptions)
	if req.VPCOptions != nil {
		input.VPCOptions = &VPCOptions{
			VPCID:            req.VPCOptions.VPCID,
			SubnetIDs:        req.VPCOptions.SubnetIDs,
			SecurityGroupIDs: req.VPCOptions.SecurityGroupIDs,
		}
	}
	if req.CognitoOptions != nil {
		input.CognitoOptions = &CognitoOptions{
			Enabled:        req.CognitoOptions.Enabled,
			UserPoolID:     req.CognitoOptions.UserPoolID,
			IdentityPoolID: req.CognitoOptions.IdentityPoolID,
			RoleARN:        req.CognitoOptions.RoleARN,
		}
	}
	input.LogPublishingOptions = parseLogPublishingOptsFromReq(req.LogPublishingOptions)

	if req.OffPeakWindowOptions != nil {
		input.OffPeakWindowOptions = parseOffPeakWindowOptionsFromReq(req.OffPeakWindowOptions)
	}

	if req.IamIdentityCenterOptions != nil {
		input.IamIdentityCenterOptions = &IamIdentityCenterOptions{
			EnabledAPIAccess:                       req.IamIdentityCenterOptions.EnabledAPIAccess,
			IamIdentityCenterArn:                   req.IamIdentityCenterOptions.IamIdentityCenterArn,
			IamRoleForIdentityCenterApplicationArn: req.IamIdentityCenterOptions.IamRoleForIdentityCenterApplicationArn,
		}
	}

	if req.EnableSoftwareUpdateOptions != nil {
		input.EnableSoftwareUpdateOptions = &EnableSoftwareUpdateOptions{
			AutoSoftwareUpdateEnabled: req.EnableSoftwareUpdateOptions.AutoSoftwareUpdateEnabled,
		}
	}

	return input
}

// parseOffPeakWindowOptionsFromReq converts JSON off-peak window options to backend type.
func parseOffPeakWindowOptionsFromReq(opts *offPeakWindowOptionsJSON) *OffPeakWindowOptions {
	if opts == nil {
		return nil
	}
	out := &OffPeakWindowOptions{Enabled: opts.Enabled}
	if opts.OffPeakWindow != nil {
		out.OffPeakWindow = &OffPeakWindow{}
		if opts.OffPeakWindow.WindowStartTime != nil {
			out.OffPeakWindow.WindowStartTime = &WindowStartTime{
				Hours:   opts.OffPeakWindow.WindowStartTime.Hours,
				Minutes: opts.OffPeakWindow.WindowStartTime.Minutes,
			}
		}
	}

	return out
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

	if vErr := validateDomainName(req.DomainName); vErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", vErr.Error())

		return
	}

	if req.EngineVersion != "" && !engineVersionPattern.MatchString(req.EngineVersion) {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException",
			fmt.Sprintf("EngineVersion %q is not valid", req.EngineVersion))

		return
	}

	upd := applyReqToUpdateInput(&req)
	input := CreateDomainInput{
		Name:                        req.DomainName,
		EngineVersion:               upd.EngineVersion,
		AccessPolicies:              upd.AccessPolicies,
		Tags:                        req.Tags,
		ClusterConfig:               parseClusterConfigFromReq(req.ClusterConfig),
		EBSOptions:                  upd.EBSOptions,
		SnapshotOptions:             upd.SnapshotOptions,
		EncryptionAtRestOptions:     upd.EncryptionAtRestOptions,
		NodeToNodeEncryptionOptions: upd.NodeToNodeEncryptionOptions,
		DomainEndpointOptions:       upd.DomainEndpointOptions,
		AdvancedSecurityOptions:     upd.AdvancedSecurityOptions,
		VPCOptions:                  upd.VPCOptions,
		CognitoOptions:              upd.CognitoOptions,
		OffPeakWindowOptions:        upd.OffPeakWindowOptions,
		IamIdentityCenterOptions:    upd.IamIdentityCenterOptions,
		EnableSoftwareUpdateOptions: upd.EnableSoftwareUpdateOptions,
		LogPublishingOptions:        upd.LogPublishingOptions,
	}

	domain, err := h.Backend.CreateDomain(input)
	if err != nil {
		if errors.Is(err, ErrDomainAlreadyExists) {
			h.writeError(r, w, http.StatusConflict, "ResourceAlreadyExistsException", err.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, domainStatusWrapJSON{
		DomainStatus: toDomainStatusJSON(domain),
	})
}

func (h *Handler) handleDescribeDomain(w http.ResponseWriter, r *http.Request, name string) {
	domain, err := h.Backend.DescribeDomain(name)
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
	domain, err := h.Backend.DeleteDomain(name)
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
	engineTypeFilter := r.URL.Query().Get("engineType")
	names := h.Backend.ListDomainNames()
	entries := make([]domainNameEntry, 0, len(names))

	for _, name := range names {
		d, err := h.Backend.DescribeDomain(name)
		if err != nil {
			continue
		}

		if engineTypeFilter != "" && !strings.HasPrefix(d.EngineVersion, engineTypeFilter+"_") {
			continue
		}

		entries = append(entries, domainNameEntry{
			DomainName:    name,
			EngineVersion: d.EngineVersion,
		})
	}

	h.writeJSON(r, w, domainListJSON{DomainNames: entries})
}

func (h *Handler) handleDescribeDomains(w http.ResponseWriter, r *http.Request) {
	body, _ := httputils.ReadBody(r)
	var req struct {
		DomainNames []string `json:"DomainNames"`
	}

	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	domains, err := h.Backend.DescribeDomains(req.DomainNames)
	if err != nil {
		h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())

		return
	}

	list := make([]map[string]any, 0, len(domains))

	for _, d := range domains {
		entry := map[string]any{
			"DomainName":    d.Name,
			"ARN":           d.ARN,
			"Endpoint":      d.Endpoint,
			"Endpoints":     map[string]any{"vpc": d.Endpoint},
			"EngineVersion": d.EngineVersion,
			"ClusterConfig": map[string]any{
				jsonKeyInstanceType: d.ClusterConfig.InstanceType,
				"InstanceCount":     d.ClusterConfig.InstanceCount,
			},
		}

		if d.EBSOptions != nil {
			entry["EBSOptions"] = d.EBSOptions
		}

		list = append(list, entry)
	}

	h.writeJSON(r, w, map[string]any{"DomainStatusList": list})
}

func (h *Handler) handleDissociatePackage(w http.ResponseWriter, r *http.Request, packageID, domainName string) {
	details, err := h.Backend.DissociatePackage(packageID, domainName)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, map[string]any{"DomainPackageDetails": domainPackageDetailsJSON{
		PackageID:           details.PackageID,
		DomainName:          details.DomainName,
		DomainPackageStatus: details.State,
	}})
}

func (h *Handler) handleDissociatePackages(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req struct {
		DomainName  string            `json:"DomainName"`
		PackageList []packageForAssoc `json:"PackageList"`
	}

	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	packageIDs := make([]string, 0, len(req.PackageList))

	for _, p := range req.PackageList {
		packageIDs = append(packageIDs, p.PackageID)
	}

	details, dissocErr := h.Backend.DissociatePackages(req.DomainName, packageIDs)
	if dissocErr != nil {
		if errors.Is(dissocErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", dissocErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", dissocErr.Error())
		}

		return
	}

	outList := make([]domainPackageDetailsJSON, 0, len(details))

	for _, d := range details {
		outList = append(outList, domainPackageDetailsJSON{
			PackageID:           d.PackageID,
			DomainName:          d.DomainName,
			DomainPackageStatus: d.State,
		})
	}

	h.writeJSON(r, w, map[string]any{"DomainPackageDetailsList": outList})
}

func toClusterConfigJSON(cc ClusterConfig) clusterConfigJSON {
	out := clusterConfigJSON{
		InstanceType:              cc.InstanceType,
		InstanceCount:             cc.InstanceCount,
		DedicatedMasterEnabled:    cc.DedicatedMasterEnabled,
		DedicatedMasterType:       cc.DedicatedMasterType,
		DedicatedMasterCount:      cc.DedicatedMasterCount,
		ZoneAwarenessEnabled:      cc.ZoneAwarenessEnabled,
		WarmEnabled:               cc.WarmEnabled,
		WarmType:                  cc.WarmType,
		WarmCount:                 cc.WarmCount,
		ColdStorageEnabled:        cc.ColdStorageEnabled,
		MultiAZWithStandbyEnabled: cc.MultiAZWithStandbyEnabled,
	}
	if cc.ZoneAwarenessConfig != nil {
		out.ZoneAwarenessConfig = &zoneAwarenessConfigJSON{
			AvailabilityZoneCount: cc.ZoneAwarenessConfig.AvailabilityZoneCount,
		}
	}
	if cc.BlueGreenDeploymentOptions != nil {
		out.BlueGreenDeploymentOptions = &blueGreenDeploymentOptionsJSON{
			Enabled: cc.BlueGreenDeploymentOptions.Enabled,
		}
	}

	return out
}

func toAdvancedSecurityOptionsJSON(aso *AdvancedSecurityOptions) *advancedSecurityOptionsJSON {
	if aso == nil {
		return &advancedSecurityOptionsJSON{}
	}
	out := &advancedSecurityOptionsJSON{
		Enabled:                     aso.Enabled,
		InternalUserDatabaseEnabled: aso.InternalUserDatabaseEnabled,
		AnonymousAuthEnabled:        aso.AnonymousAuthEnabled,
	}
	if aso.SAMLOptions != nil {
		out.SAMLOptions = &samlOptionsJSON{
			Enabled:               aso.SAMLOptions.Enabled,
			IDPEntityID:           aso.SAMLOptions.IDPEntityID,
			IDPMetadataContent:    aso.SAMLOptions.IDPMetadataContent,
			RolesKey:              aso.SAMLOptions.RolesKey,
			SubjectKey:            aso.SAMLOptions.SubjectKey,
			SessionTimeoutMinutes: aso.SAMLOptions.SessionTimeoutMinutes,
		}
	}

	return out
}

func toLogPublishingOptionsJSON(opts map[string]*LogPublishingOption) map[string]*logPublishingOptionJSON {
	if len(opts) == 0 {
		return nil
	}
	out := make(map[string]*logPublishingOptionJSON, len(opts))
	for k, v := range opts {
		out[k] = &logPublishingOptionJSON{
			Enabled:                   v.Enabled,
			CloudWatchLogsLogGroupARN: v.CloudWatchLogsLogGroupARN,
		}
	}

	return out
}

func toDomainStatusJSON(d *Domain) domainStatusJSON {
	out := domainStatusJSON{
		DomainName:             d.Name,
		ARN:                    d.ARN,
		EngineVersion:          d.EngineVersion,
		Endpoint:               d.Endpoint,
		Processing:             false,
		DomainProcessingStatus: domainStatusActive,
		AccessPolicies:         d.AccessPolicies,
		ClusterConfig:          toClusterConfigJSON(d.ClusterConfig),
		// Always emit these fields so providers see a consistent response shape.
		EBSOptions:                  &ebsOptionsJSON{},
		EncryptionAtRestOptions:     &encryptAtRestOptionsJSON{},
		NodeToNodeEncryptionOptions: &nodeToNodeEncryptJSON{},
		CognitoOptions:              &cognitoOptionsJSON{},
		AdvancedSecurityOptions:     toAdvancedSecurityOptionsJSON(nil),
	}
	applyDomainOptionalFields(d, &out)

	return out
}

func applyDomainOptionalFields(d *Domain, out *domainStatusJSON) {
	if d.EBSOptions != nil {
		out.EBSOptions = &ebsOptionsJSON{
			EBSEnabled: d.EBSOptions.EBSEnabled,
			VolumeType: d.EBSOptions.VolumeType,
			VolumeSize: d.EBSOptions.VolumeSize,
			IOPS:       d.EBSOptions.IOPS,
			Throughput: d.EBSOptions.Throughput,
			KMSKeyID:   d.EBSOptions.KMSKeyID,
		}
	}
	if d.SnapshotOptions != nil {
		out.SnapshotOptions = &snapshotOptionsJSON{
			AutomatedSnapshotStartHour: d.SnapshotOptions.AutomatedSnapshotStartHour,
		}
	}
	if d.EncryptionAtRestOptions != nil {
		out.EncryptionAtRestOptions = &encryptAtRestOptionsJSON{
			Enabled:  d.EncryptionAtRestOptions.Enabled,
			KMSKeyID: d.EncryptionAtRestOptions.KMSKeyID,
		}
	}
	if d.NodeToNodeEncryptionOptions != nil {
		out.NodeToNodeEncryptionOptions = &nodeToNodeEncryptJSON{Enabled: d.NodeToNodeEncryptionOptions.Enabled}
	}
	if d.DomainEndpointOptions != nil {
		out.DomainEndpointOptions = &domainEndpointOptionsJSON{
			EnforceHTTPS:                 d.DomainEndpointOptions.EnforceHTTPS,
			TLSSecurityPolicy:            d.DomainEndpointOptions.TLSSecurityPolicy,
			CustomEndpointEnabled:        d.DomainEndpointOptions.CustomEndpointEnabled,
			CustomEndpoint:               d.DomainEndpointOptions.CustomEndpoint,
			CustomEndpointCertificateArn: d.DomainEndpointOptions.CustomEndpointCertificateArn,
		}
	}
	if d.AdvancedSecurityOptions != nil {
		out.AdvancedSecurityOptions = toAdvancedSecurityOptionsJSON(d.AdvancedSecurityOptions)
	}
	if d.VPCOptions != nil {
		out.VPCOptions = &vpcOptionsJSON{
			VPCID:            d.VPCOptions.VPCID,
			SubnetIDs:        d.VPCOptions.SubnetIDs,
			SecurityGroupIDs: d.VPCOptions.SecurityGroupIDs,
		}
	}
	if d.CognitoOptions != nil {
		out.CognitoOptions = &cognitoOptionsJSON{
			Enabled:        d.CognitoOptions.Enabled,
			UserPoolID:     d.CognitoOptions.UserPoolID,
			IdentityPoolID: d.CognitoOptions.IdentityPoolID,
			RoleARN:        d.CognitoOptions.RoleARN,
		}
	}
	out.LogPublishingOptions = toLogPublishingOptionsJSON(d.LogPublishingOptions)

	if d.OffPeakWindowOptions != nil {
		out.OffPeakWindowOptions = toOffPeakWindowOptionsJSON(d.OffPeakWindowOptions)
	}

	if d.IamIdentityCenterOptions != nil {
		out.IamIdentityCenterOptions = &iamIdentityCenterOptionsJSON{
			EnabledAPIAccess:                       d.IamIdentityCenterOptions.EnabledAPIAccess,
			IamIdentityCenterArn:                   d.IamIdentityCenterOptions.IamIdentityCenterArn,
			IamRoleForIdentityCenterApplicationArn: d.IamIdentityCenterOptions.IamRoleForIdentityCenterApplicationArn,
		}
	}

	if d.EnableSoftwareUpdateOptions != nil {
		out.EnableSoftwareUpdateOptions = &enableSoftwareUpdateOptionsJSON{
			AutoSoftwareUpdateEnabled: d.EnableSoftwareUpdateOptions.AutoSoftwareUpdateEnabled,
		}
	}
}

// toOffPeakWindowOptionsJSON converts backend OffPeakWindowOptions to JSON representation.
func toOffPeakWindowOptionsJSON(opts *OffPeakWindowOptions) *offPeakWindowOptionsJSON {
	if opts == nil {
		return nil
	}
	out := &offPeakWindowOptionsJSON{Enabled: opts.Enabled}
	if opts.OffPeakWindow != nil {
		out.OffPeakWindow = &offPeakWindowJSON{}
		if opts.OffPeakWindow.WindowStartTime != nil {
			out.OffPeakWindow.WindowStartTime = &windowStartTimeJSON{
				Hours:   opts.OffPeakWindow.WindowStartTime.Hours,
				Minutes: opts.OffPeakWindow.WindowStartTime.Minutes,
			}
		}
	}

	return out
}

type errorResponseJSON struct {
	Message string `json:"message"`
}

// toDomainConfigJSON builds the DescribeDomainConfig / UpdateDomainConfig response body.
func toDomainConfigJSON(d *Domain) domainConfigFields {
	active := opensearchConfigStatus{State: domainStatusActive}
	st := toDomainStatusJSON(d)

	cfg := domainConfigFields{
		EngineVersion:   opensearchConfigValue{Options: d.EngineVersion, Status: active},
		ClusterConfig:   opensearchConfigValue{Options: st.ClusterConfig, Status: active},
		EBSOptions:      opensearchConfigValue{Options: map[string]any{}, Status: active},
		AccessPolicies:  opensearchConfigValue{Options: d.AccessPolicies, Status: active},
		AdvancedOptions: opensearchConfigValue{Options: map[string]any{}, Status: active},
	}

	if st.EBSOptions != nil {
		cfg.EBSOptions = opensearchConfigValue{Options: st.EBSOptions, Status: active}
	}

	if st.SnapshotOptions != nil {
		cfg.SnapshotOptions = opensearchConfigValue{Options: st.SnapshotOptions, Status: active}
	}

	if st.EncryptionAtRestOptions != nil {
		cfg.EncryptionAtRestOptions = opensearchConfigValue{Options: st.EncryptionAtRestOptions, Status: active}
	}

	if st.NodeToNodeEncryptionOptions != nil {
		cfg.NodeToNodeEncryptionOptions = opensearchConfigValue{Options: st.NodeToNodeEncryptionOptions, Status: active}
	}

	if st.DomainEndpointOptions != nil {
		cfg.DomainEndpointOptions = opensearchConfigValue{Options: st.DomainEndpointOptions, Status: active}
	}

	if st.AdvancedSecurityOptions != nil {
		cfg.AdvancedSecurityOptions = opensearchConfigValue{Options: st.AdvancedSecurityOptions, Status: active}
	}

	if st.VPCOptions != nil {
		cfg.VPCOptions = opensearchConfigValue{Options: st.VPCOptions, Status: active}
	}

	if st.CognitoOptions != nil {
		cfg.CognitoOptions = opensearchConfigValue{Options: st.CognitoOptions, Status: active}
	}

	if len(st.LogPublishingOptions) > 0 {
		cfg.LogPublishingOptions = opensearchConfigValue{Options: st.LogPublishingOptions, Status: active}
	}

	if st.OffPeakWindowOptions != nil {
		cfg.OffPeakWindowOptions = opensearchConfigValue{Options: st.OffPeakWindowOptions, Status: active}
	}

	if st.IamIdentityCenterOptions != nil {
		cfg.IamIdentityCenterOptions = opensearchConfigValue{Options: st.IamIdentityCenterOptions, Status: active}
	}

	if st.EnableSoftwareUpdateOptions != nil {
		cfg.EnableSoftwareUpdateOptions = opensearchConfigValue{Options: st.EnableSoftwareUpdateOptions, Status: active}
	}

	return cfg
}

func (h *Handler) writeError(r *http.Request, w http.ResponseWriter, status int, code, message string) {
	ctx := r.Context()
	logger.Load(ctx).ErrorContext(r.Context(), "opensearch error", "code", code, "message", message)
	w.Header().Set("x-amzn-ErrorType", code)
	httputils.WriteJSON(ctx, w, status, errorResponseJSON{Message: message})
}

func (h *Handler) writeJSON(r *http.Request, w http.ResponseWriter, v any) {
	httputils.WriteJSON(r.Context(), w, http.StatusOK, v)
}

type listTagsOutput struct {
	TagList []svcTags.KV `json:"TagList"`
}

type opensearchConfigStatus struct {
	State string `json:"State"`
}

type opensearchConfigValue struct {
	Options any                    `json:"Options"`
	Status  opensearchConfigStatus `json:"Status"`
}

// domainConfigFields holds the per-feature configuration values for a domain.
type domainConfigFields struct {
	EngineVersion               opensearchConfigValue `json:"EngineVersion"`
	ClusterConfig               opensearchConfigValue `json:"ClusterConfig"`
	EBSOptions                  opensearchConfigValue `json:"EBSOptions"`
	AccessPolicies              opensearchConfigValue `json:"AccessPolicies"`
	AdvancedOptions             opensearchConfigValue `json:"AdvancedOptions"`
	SnapshotOptions             opensearchConfigValue `json:"SnapshotOptions"`
	EncryptionAtRestOptions     opensearchConfigValue `json:"EncryptionAtRestOptions"`
	NodeToNodeEncryptionOptions opensearchConfigValue `json:"NodeToNodeEncryptionOptions"`
	DomainEndpointOptions       opensearchConfigValue `json:"DomainEndpointOptions"`
	AdvancedSecurityOptions     opensearchConfigValue `json:"AdvancedSecurityOptions"`
	VPCOptions                  opensearchConfigValue `json:"VPCOptions"`
	CognitoOptions              opensearchConfigValue `json:"CognitoOptions"`
	LogPublishingOptions        opensearchConfigValue `json:"LogPublishingOptions"`
	OffPeakWindowOptions        opensearchConfigValue `json:"OffPeakWindowOptions,omitempty"`
	IamIdentityCenterOptions    opensearchConfigValue `json:"IamIdentityCenterOptions,omitempty"`
	EnableSoftwareUpdateOptions opensearchConfigValue `json:"EnableSoftwareUpdateOptions,omitempty"`
}

func (h *Handler) handleListTags(w http.ResponseWriter, r *http.Request) {
	domainARN := r.URL.Query().Get("arn")

	tags, err := h.Backend.ListTags(domainARN)
	if err != nil {
		h.writeJSON(r, w, &listTagsOutput{TagList: []svcTags.KV{}})

		return
	}

	tagList := make([]svcTags.KV, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, svcTags.KV{Key: k, Value: v})
	}

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

	tagMap := make(map[string]string, len(req.TagList))
	for _, t := range req.TagList {
		tagMap[t.Key] = t.Value
	}

	if addErr := h.Backend.AddTags(req.ARN, tagMap); addErr != nil {
		if errors.Is(addErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", addErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", addErr.Error())
		}

		return
	}

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

	if removeErr := h.Backend.RemoveTags(req.ARN, req.TagKeys); removeErr != nil {
		if errors.Is(removeErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", removeErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", removeErr.Error())
		}

		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleDescribeDomainConfig(w http.ResponseWriter, r *http.Request, name string) {
	domain, err := h.Backend.DescribeDomain(name)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("domain %s/config not found", name))
		} else {
			h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, map[string]any{"DomainConfig": toDomainConfigJSON(domain)})
}

// handleDomainSubRoutes handles POST requests under /2021-01-01/opensearch/domain/{name}/...
func (h *Handler) handleDomainSubRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	// rest looks like "/{domainName}/dataSource", "/{domainName}/authorizeVpcEndpointAccess", etc.
	trimmed := strings.TrimPrefix(rest, "/")

	switch {
	case strings.HasSuffix(trimmed, "/dataSource"):
		domainName := strings.TrimSuffix(trimmed, "/dataSource")
		h.handleAddDataSource(w, r, domainName)
	case strings.HasSuffix(trimmed, "/authorizeVpcEndpointAccess"):
		domainName := strings.TrimSuffix(trimmed, "/authorizeVpcEndpointAccess")
		h.handleAuthorizeVpcEndpointAccess(w, r, domainName)
	case strings.HasSuffix(trimmed, "/config/cancel"):
		domainName := strings.TrimSuffix(trimmed, "/config/cancel")
		h.handleCancelDomainConfigChange(w, r, domainName)
	default:
		if h.dispatchDomainPostRoutesExtended(w, r, trimmed) {
			return
		}
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleCCRoutes handles cross-cluster connection routes.
func (h *Handler) handleCCRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchCCPath)

	if strings.HasPrefix(rest, "/inboundConnection") {
		h.handleCCInboundRoutes(w, r, rest)

		return
	}

	if strings.HasPrefix(rest, "/outboundConnection") {
		h.handleCCOutboundRoutes(w, r, rest)

		return
	}

	h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
}

// handleCCInboundRoutes handles inbound cross-cluster connection sub-routes.
func (h *Handler) handleCCInboundRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	const prefix = "/inboundConnection/"

	switch {
	// GET /inboundConnection → DescribeInboundConnections
	case (rest == "/inboundConnection" || rest == "/inboundConnection/") && r.Method == http.MethodGet:
		conns := h.Backend.DescribeInboundConnections()
		h.writeJSON(r, w, map[string]any{"Connections": conns})
	// PUT /inboundConnection/{id}/accept → AcceptInboundConnection
	case strings.HasPrefix(rest, prefix) && strings.HasSuffix(rest, "/accept") &&
		r.Method == http.MethodPut:
		connID := strings.TrimSuffix(strings.TrimPrefix(rest, prefix), "/accept")
		h.handleAcceptInboundConnection(w, r, connID)
	// PUT /inboundConnection/{id}/reject → RejectInboundConnection
	case strings.HasPrefix(rest, prefix) && strings.HasSuffix(rest, "/reject") &&
		r.Method == http.MethodPut:
		connID := strings.TrimSuffix(strings.TrimPrefix(rest, prefix), "/reject")
		conn, err := h.Backend.RejectInboundConnection(connID)
		if err != nil {
			conn = &InboundConnection{ConnectionID: connID, Status: "REJECTED"}
		}
		h.writeJSON(r, w, map[string]any{jsonKeyConnection: map[string]any{
			jsonKeyConnectionID:     conn.ConnectionID,
			jsonKeyConnectionStatus: map[string]any{jsonKeyStatusCode: conn.Status},
		}})
	// DELETE /inboundConnection/{id} → DeleteInboundConnection
	case strings.HasPrefix(rest, prefix) && r.Method == http.MethodDelete:
		connID := strings.TrimPrefix(rest, prefix)
		conn, err := h.Backend.DeleteInboundConnection(connID)
		if err != nil {
			conn = &InboundConnection{ConnectionID: connID, Status: statusDeleted}
		}
		h.writeJSON(r, w, map[string]any{jsonKeyConnection: map[string]any{
			jsonKeyConnectionID:     conn.ConnectionID,
			jsonKeyConnectionStatus: map[string]any{jsonKeyStatusCode: conn.Status},
		}})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleCCOutboundRoutes handles outbound cross-cluster connection sub-routes.
func (h *Handler) handleCCOutboundRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	const prefix = "/outboundConnection/"

	switch {
	// GET /outboundConnection → DescribeOutboundConnections
	case (rest == "/outboundConnection" || rest == "/outboundConnection/") &&
		r.Method == http.MethodGet:
		conns := h.Backend.DescribeOutboundConnections()
		h.writeJSON(r, w, map[string]any{"Connections": conns})
	// POST /outboundConnection → CreateOutboundConnection
	case (rest == "/outboundConnection" || rest == "/outboundConnection/") &&
		r.Method == http.MethodPost:
		body, err := httputils.ReadBody(r)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

			return
		}
		var req struct {
			LocalDomainInfo  map[string]any `json:"LocalDomainInfo"`
			RemoteDomainInfo map[string]any `json:"RemoteDomainInfo"`
			ConnectionAlias  string         `json:"ConnectionAlias"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		conn, createErr := h.Backend.CreateOutboundConnection(
			req.ConnectionAlias,
			req.LocalDomainInfo,
			req.RemoteDomainInfo,
		)
		if createErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", createErr.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{
			"ConnectionId":     conn.ConnectionID,
			"ConnectionAlias":  conn.ConnectionAlias,
			"ConnectionStatus": map[string]any{jsonKeyStatusCode: conn.Status},
		})
	// DELETE /outboundConnection/{id} → DeleteOutboundConnection
	case strings.HasPrefix(rest, prefix) && r.Method == http.MethodDelete:
		connID := strings.TrimPrefix(rest, prefix)
		conn, err := h.Backend.DeleteOutboundConnection(connID)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{jsonKeyConnection: map[string]any{
			jsonKeyConnectionID:     conn.ConnectionID,
			jsonKeyConnectionStatus: map[string]any{jsonKeyStatusCode: conn.Status},
		}})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleDirectQueryRoutes handles direct query data source routes.
func (h *Handler) handleDirectQueryRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchDirectQueryPath)

	switch {
	// POST /2021-01-01/opensearch/directQueryDataSource → AddDirectQueryDataSource
	case (rest == "" || rest == "/") && r.Method == http.MethodPost:
		h.handleAddDirectQueryDataSource(w, r)
	// GET /2021-01-01/opensearch/directQueryDataSource → ListDirectQueryDataSources
	case (rest == "" || rest == "/") && r.Method == http.MethodGet:
		sources := h.Backend.ListDirectQueryDataSources()
		h.writeJSON(r, w, map[string]any{"DirectQueryDataSources": sources})
	// GET /2021-01-01/opensearch/directQueryDataSource/{dataSourceName} → GetDirectQueryDataSource
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodGet:
		h.handleGetDirectQueryDataSource(w, r, strings.TrimPrefix(rest, "/"))
	// DELETE /2021-01-01/opensearch/directQueryDataSource/{dataSourceName} → DeleteDirectQueryDataSource
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodDelete:
		h.handleDeleteDirectQueryDataSource(w, r, strings.TrimPrefix(rest, "/"))
	// PUT /2021-01-01/opensearch/directQueryDataSource/{dataSourceName} → UpdateDirectQueryDataSource
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodPut:
		h.handleUpdateDirectQueryDataSource(w, r, strings.TrimPrefix(rest, "/"))
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

func (h *Handler) handleGetDirectQueryDataSource(w http.ResponseWriter, r *http.Request, name string) {
	ds, err := h.Backend.GetDirectQueryDataSource(name)
	if err != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

		return
	}
	h.writeJSON(r, w, ds)
}

func (h *Handler) handleDeleteDirectQueryDataSource(w http.ResponseWriter, _ *http.Request, name string) {
	_ = h.Backend.DeleteDirectQueryDataSource(name)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleUpdateDirectQueryDataSource(w http.ResponseWriter, r *http.Request, name string) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}
	var req struct {
		Description    string   `json:"Description"`
		OpenSearchArns []string `json:"OpenSearchArns"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}
	ds, updateErr := h.Backend.UpdateDirectQueryDataSource(name, req.Description, req.OpenSearchArns)
	if updateErr != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", updateErr.Error())

		return
	}
	h.writeJSON(r, w, map[string]any{"DataSourceArn": ds.DataSourceArn})
}

// handlePackageRoutes handles package routes.
func (h *Handler) handlePackageRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchPackagesPath)

	// Root paths first.
	if rest == "" || rest == "/" {
		h.handlePackageRootRoutes(w, r)

		return
	}

	// Named sub-paths: associate, dissociate.
	if h.handlePackageAssocRoutes(w, r, rest) {
		return
	}

	// Sub-resource paths: history, domains, scope.
	if h.handlePackageSubResourceRoutes(w, r, rest) {
		return
	}

	// Fallback: single-segment package-ID routes.
	h.handlePackageIDRoutes(w, r, rest)
}

// handlePackageAssocRoutes handles associate/dissociate package routes.
// Returns true if the request was handled.
func (h *Handler) handlePackageAssocRoutes(w http.ResponseWriter, r *http.Request, rest string) bool {
	switch {
	// POST /packages/associate/{PackageID}/{DomainName} → AssociatePackage
	case strings.HasPrefix(rest, "/associate/") && r.Method == http.MethodPost:
		parts := strings.SplitN(strings.TrimPrefix(rest, "/associate/"), "/", pkgPathParts)
		if len(parts) != pkgPathParts {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid associate package path")

			return true
		}

		h.handleAssociatePackage(w, r, parts[0], parts[1])

		return true
	// POST /packages/associateMultiple → AssociatePackages
	case rest == "/associateMultiple" && r.Method == http.MethodPost:
		h.handleAssociatePackages(w, r)

		return true
	// DELETE /packages/dissociate/{PackageID}/{DomainName} → DissociatePackage
	case strings.HasPrefix(rest, "/dissociate/") && r.Method == http.MethodDelete:
		parts := strings.SplitN(strings.TrimPrefix(rest, "/dissociate/"), "/", pkgPathParts)
		if len(parts) != pkgPathParts {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid dissociate package path")

			return true
		}

		h.handleDissociatePackage(w, r, parts[0], parts[1])

		return true
	// POST /packages/dissociateMultiple → DissociatePackages
	case rest == "/dissociateMultiple" && r.Method == http.MethodPost:
		h.handleDissociatePackages(w, r)

		return true
	}

	return false
}

// handlePackageSubResourceRoutes handles package sub-resource routes (history, domains, scope).
// Returns true if the request was handled.
func (h *Handler) handlePackageSubResourceRoutes(w http.ResponseWriter, r *http.Request, rest string) bool {
	switch {
	// GET /packages/{packageId}/history → GetPackageVersionHistory
	case strings.HasSuffix(rest, "/history") && r.Method == http.MethodGet:
		pkgID := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/history")
		history, err := h.Backend.GetPackageVersionHistory(pkgID)
		if err != nil {
			history = []*PackageVersionHistory{}
		}
		h.writeJSON(r, w, map[string]any{"PackageVersionHistoryList": history})

		return true
	// GET /packages/{packageId}/domains → ListDomainsForPackage
	case strings.HasSuffix(rest, "/domains") && r.Method == http.MethodGet:
		pkgID := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/domains")
		domains := h.Backend.ListDomainsForPackage(pkgID)
		h.writeJSON(r, w, map[string]any{jsonKeyPkgDetailsList: domains})

		return true
	// PUT /packages/{packageId}/scope → UpdatePackageScope
	case strings.HasSuffix(rest, "/scope") && r.Method == http.MethodPut:
		pkgID := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/scope")
		body, _ := httputils.ReadBody(r)
		var req struct {
			Operation   string   `json:"Operation"`
			DomainNames []string `json:"PackageScopeOperationConfig"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		pkg, err := h.Backend.UpdatePackageScope(pkgID, req.Operation, req.DomainNames)
		var retPkgID string
		if pkg != nil {
			retPkgID = pkg.PackageID
		}
		_ = err
		h.writeJSON(r, w, map[string]any{
			jsonKeyPackageID:              retPkgID,
			"Operation":                   req.Operation,
			"PackageScopeOperationStatus": softwareUpdateCompleted,
		})

		return true
	}

	return false
}

// handlePackageRootRoutes handles /packages and /packages/ requests.
func (h *Handler) handlePackageRootRoutes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	// POST /packages → CreatePackage
	case http.MethodPost:
		body, err := httputils.ReadBody(r)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

			return
		}
		var req struct {
			PackageSource            *packageSourceJSON            `json:"PackageSource,omitempty"`
			PackageEncryptionOptions *packageEncryptionOptionsJSON `json:"PackageEncryptionOptions,omitempty"`
			PackageName              string                        `json:"PackageName"`
			PackageType              string                        `json:"PackageType"`
			PackageDescription       string                        `json:"PackageDescription"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		var pkgSource *PackageSource
		if req.PackageSource != nil {
			pkgSource = &PackageSource{
				S3BucketName: req.PackageSource.S3BucketName,
				S3Key:        req.PackageSource.S3Key,
			}
		}
		var pkgEncOpts *PackageEncryptionOptions
		if req.PackageEncryptionOptions != nil {
			pkgEncOpts = &PackageEncryptionOptions{
				KmsKeyIdentifier:  req.PackageEncryptionOptions.KmsKeyIdentifier,
				EncryptionEnabled: req.PackageEncryptionOptions.EncryptionEnabled,
			}
		}
		pkg, createErr := h.Backend.CreatePackage(req.PackageName, req.PackageType, req.PackageDescription, pkgSource, pkgEncOpts)
		if createErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", createErr.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{jsonKeyPackageDetails: pkg})
	// GET /packages → DescribePackages
	case http.MethodGet:
		var ids []string
		if q := r.URL.Query().Get("PackageID"); q != "" {
			ids = append(ids, q)
		}
		pkgs, _ := h.Backend.DescribePackages(ids)
		if pkgs == nil {
			pkgs = []*Package{}
		}
		h.writeJSON(r, w, map[string]any{"PackageDetailsList": pkgs})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handlePackageIDRoutes handles /packages/{packageId} requests.
func (h *Handler) handlePackageIDRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	pkgID := strings.TrimPrefix(rest, "/")
	if strings.Contains(pkgID, "/") {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	switch r.Method {
	// DELETE /packages/{packageId} → DeletePackage
	case http.MethodDelete:
		pkg, err := h.Backend.DeletePackage(pkgID)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{jsonKeyPackageDetails: pkg})
	// POST /packages/{packageId} → UpdatePackage
	case http.MethodPost:
		body, err := httputils.ReadBody(r)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

			return
		}
		var req struct {
			PackageDescription string `json:"PackageDescription"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		pkg, updateErr := h.Backend.UpdatePackage(pkgID, req.PackageDescription)
		if updateErr != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", updateErr.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{jsonKeyPackageDetails: pkg})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleServiceSoftwareRoutes handles service software update routes.
func (h *Handler) handleServiceSoftwareRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchServiceSwPath)

	// POST /2021-01-01/opensearch/serviceSoftwareUpdate/cancel
	if rest == "/cancel" && r.Method == http.MethodPost {
		h.handleCancelServiceSoftwareUpdate(w, r)

		return
	}

	h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
}

// handleApplicationRoutes handles application routes.
func (h *Handler) handleApplicationRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchApplicationPath)

	// Root: Create/List applications.
	if rest == "" || rest == "/" {
		h.handleApplicationRootRoutes(w, r)

		return
	}

	// Settings sub-path.
	if rest == "/settings/default" {
		h.handleApplicationSettingsRoutes(w, r)

		return
	}

	// Per-app-ID routes.
	h.handleApplicationIDRoutes(w, r, rest)
}

// handleApplicationRootRoutes handles /application and /application/ requests.
func (h *Handler) handleApplicationRootRoutes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.handleCreateApplication(w, r)
	case http.MethodGet:
		apps := h.Backend.ListApplications()
		summaries := make([]map[string]any, 0, len(apps))
		for _, app := range apps {
			summaries = append(summaries, map[string]any{
				"Id":           app.ID,
				jsonKeyAppName: app.Name,
				jsonKeyAppArn:  app.ARN,
				"Status":       pkgStateActive,
			})
		}
		h.writeJSON(r, w, map[string]any{"ApplicationSummaries": summaries})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleApplicationSettingsRoutes handles /application/settings/default requests.
func (h *Handler) handleApplicationSettingsRoutes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		appType := r.URL.Query().Get("ApplicationType")
		if appType == "" {
			appType = "OpenSearchDashboards"
		}

		settings, _ := h.Backend.GetDefaultApplicationSettings(appType)
		if settings == nil {
			settings = []AppSetting{}
		}

		h.writeJSON(r, w, map[string]any{
			"ApplicationType":            appType,
			"DefaultApplicationSettings": settings,
		})
	case http.MethodPut:
		body, err := httputils.ReadBody(r)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

			return
		}

		var req struct {
			ApplicationType            string       `json:"ApplicationType"`
			DefaultApplicationSettings []AppSetting `json:"DefaultApplicationSettings"`
		}

		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}

		appType := req.ApplicationType
		if appType == "" {
			appType = "OpenSearchDashboards"
		}

		_ = h.Backend.PutDefaultApplicationSettings(appType, req.DefaultApplicationSettings)
		w.WriteHeader(http.StatusOK)
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleApplicationIDRoutes handles /application/{appId} requests.
func (h *Handler) handleApplicationIDRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	appID := strings.TrimPrefix(rest, "/")
	if strings.Contains(appID, "/") {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	switch r.Method {
	case http.MethodGet:
		app, err := h.Backend.GetApplication(appID)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{
			"Id": app.ID, jsonKeyAppName: app.Name, jsonKeyAppArn: app.ARN,
			"AppConfigs": app.AppConfigs, "DataSources": app.DataSources,
			jsonKeyStatus: pkgStateActive,
		})
	case http.MethodDelete:
		if err := h.Backend.DeleteApplication(appID); err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodPut:
		body, err := httputils.ReadBody(r)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

			return
		}
		var req struct {
			AppConfigs  []appConfigJSON `json:"AppConfigs"`
			DataSources []appDSJSON     `json:"DataSources"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		appConfigs := make([]AppConfig, len(req.AppConfigs))
		for i, ac := range req.AppConfigs {
			appConfigs[i] = AppConfig(ac)
		}
		dataSources := make([]AppDataSource, len(req.DataSources))
		for i, ds := range req.DataSources {
			dataSources[i] = AppDataSource(ds)
		}
		app, updateErr := h.Backend.UpdateApplication(appID, appConfigs, dataSources)
		if updateErr != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", updateErr.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{
			"Id": app.ID, jsonKeyAppName: app.Name, jsonKeyAppArn: app.ARN,
			jsonKeyStatus: pkgStateActive,
		})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// acceptInboundConnectionOutput is the JSON response for AcceptInboundConnection.
type acceptInboundConnectionOutput struct {
	Connection inboundConnectionJSON `json:"Connection"`
}

// inboundConnectionJSON is the JSON representation of an inbound connection.
type inboundConnectionJSON struct {
	ConnectionID     string                `json:"ConnectionId"`
	ConnectionStatus inboundConnStatusJSON `json:"ConnectionStatus"`
}

// inboundConnStatusJSON is the JSON representation of a connection status.
type inboundConnStatusJSON struct {
	StatusCode string `json:"StatusCode"`
}

func (h *Handler) handleAcceptInboundConnection(w http.ResponseWriter, r *http.Request, connectionID string) {
	conn, err := h.Backend.AcceptInboundConnection(connectionID)
	if err != nil {
		if errors.Is(err, ErrInvalidParameter) {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
		} else {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, acceptInboundConnectionOutput{
		Connection: inboundConnectionJSON{
			ConnectionID: conn.ConnectionID,
			ConnectionStatus: inboundConnStatusJSON{
				StatusCode: conn.Status,
			},
		},
	})
}

// addDataSourceRequest is the JSON request body for AddDataSource.
type addDataSourceRequest struct {
	DataSourceType any    `json:"DataSourceType"`
	Name           string `json:"Name"`
	Description    string `json:"Description"`
}

// addDataSourceOutput is the JSON response for AddDataSource.
type addDataSourceOutput struct {
	Message string `json:"Message"`
}

func (h *Handler) handleAddDataSource(w http.ResponseWriter, r *http.Request, domainName string) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req addDataSourceRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	msg, addErr := h.Backend.AddDataSource(domainName, req.Name, req.Description, fmt.Sprintf("%v", req.DataSourceType))
	if addErr != nil {
		switch {
		case errors.Is(addErr, ErrDomainNotFound):
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", addErr.Error())
		case errors.Is(addErr, ErrDataSourceAlreadyExists):
			h.writeError(r, w, http.StatusConflict, "ResourceAlreadyExistsException", addErr.Error())
		default:
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", addErr.Error())
		}

		return
	}

	h.writeJSON(r, w, addDataSourceOutput{Message: msg})
}

// addDirectQueryDataSourceRequest is the JSON request body for AddDirectQueryDataSource.
type addDirectQueryDataSourceRequest struct {
	DataSourceName string   `json:"DataSourceName"`
	Description    string   `json:"Description"`
	DataSourceType any      `json:"DataSourceType"`
	OpenSearchArns []string `json:"OpenSearchArns"`
}

// addDirectQueryDataSourceOutput is the JSON response for AddDirectQueryDataSource.
type addDirectQueryDataSourceOutput struct {
	DataSourceArn string `json:"DataSourceArn"`
}

func (h *Handler) handleAddDirectQueryDataSource(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req addDirectQueryDataSourceRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	dsARN, addErr := h.Backend.AddDirectQueryDataSource(
		req.DataSourceName,
		req.Description,
		fmt.Sprintf("%v", req.DataSourceType),
		req.OpenSearchArns,
	)
	if addErr != nil {
		if errors.Is(addErr, ErrDataSourceAlreadyExists) {
			h.writeError(r, w, http.StatusConflict, "ResourceAlreadyExistsException", addErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", addErr.Error())
		}

		return
	}

	h.writeJSON(r, w, addDirectQueryDataSourceOutput{DataSourceArn: dsARN})
}

// associatePackageOutput is the JSON response for AssociatePackage.
type associatePackageOutput struct {
	DomainPackageDetails domainPackageDetailsJSON `json:"DomainPackageDetails"`
}

// domainPackageDetailsJSON is the JSON representation of package domain details.
type domainPackageDetailsJSON struct {
	PackageID           string `json:"PackageID"`
	DomainName          string `json:"DomainName"`
	DomainPackageStatus string `json:"DomainPackageStatus"`
}

func (h *Handler) handleAssociatePackage(w http.ResponseWriter, r *http.Request, packageID, domainName string) {
	details, err := h.Backend.AssociatePackage(packageID, domainName)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) || errors.Is(err, ErrPackageNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, associatePackageOutput{
		DomainPackageDetails: domainPackageDetailsJSON{
			PackageID:           details.PackageID,
			DomainName:          details.DomainName,
			DomainPackageStatus: details.State,
		},
	})
}

// associatePackagesRequest is the JSON request body for AssociatePackages.
type associatePackagesRequest struct {
	DomainName  string            `json:"DomainName"`
	PackageList []packageForAssoc `json:"PackageList"`
}

// packageForAssoc is a package entry in AssociatePackages request.
type packageForAssoc struct {
	PackageID string `json:"PackageID"`
}

// associatePackagesOutput is the JSON response for AssociatePackages.
type associatePackagesOutput struct {
	DomainPackageDetailsList []domainPackageDetailsJSON `json:"DomainPackageDetailsList"`
}

func (h *Handler) handleAssociatePackages(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req associatePackagesRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	packageIDs := make([]string, 0, len(req.PackageList))
	for _, p := range req.PackageList {
		packageIDs = append(packageIDs, p.PackageID)
	}

	details, assocErr := h.Backend.AssociatePackages(req.DomainName, packageIDs)
	if assocErr != nil {
		if errors.Is(assocErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", assocErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", assocErr.Error())
		}

		return
	}

	outList := make([]domainPackageDetailsJSON, 0, len(details))
	for _, d := range details {
		outList = append(outList, domainPackageDetailsJSON{
			PackageID:           d.PackageID,
			DomainName:          d.DomainName,
			DomainPackageStatus: d.State,
		})
	}

	h.writeJSON(r, w, associatePackagesOutput{DomainPackageDetailsList: outList})
}

// authorizeVpcEndpointAccessRequest is the JSON request body for AuthorizeVpcEndpointAccess.
type authorizeVpcEndpointAccessRequest struct {
	Account string `json:"Account"`
	Service string `json:"Service"`
}

// authorizeVpcEndpointAccessOutput is the JSON response for AuthorizeVpcEndpointAccess.
type authorizeVpcEndpointAccessOutput struct {
	AuthorizedPrincipal authorizedPrincipalJSON `json:"AuthorizedPrincipal"`
}

// authorizedPrincipalJSON is the JSON representation of an authorized principal.
type authorizedPrincipalJSON struct {
	Principal     string `json:"Principal"`
	PrincipalType string `json:"PrincipalType"`
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

	principal, authErr := h.Backend.AuthorizeVpcEndpointAccess(domainName, req.Account, req.Service)
	if authErr != nil {
		if errors.Is(authErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", authErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", authErr.Error())
		}

		return
	}

	h.writeJSON(r, w, authorizeVpcEndpointAccessOutput{
		AuthorizedPrincipal: authorizedPrincipalJSON{
			Principal:     principal.Principal,
			PrincipalType: principal.PrincipalType,
		},
	})
}

// cancelDomainConfigChangeRequest is the JSON request body for CancelDomainConfigChange.
type cancelDomainConfigChangeRequest struct {
	DryRun bool `json:"DryRun"`
}

// cancelDomainConfigChangeOutput is the JSON response for CancelDomainConfigChange.
type cancelDomainConfigChangeOutput struct {
	CancelledChangeIDs        []string `json:"CancelledChangeIds"`
	CancelledChangeProperties []any    `json:"CancelledChangeProperties"`
	DryRun                    bool     `json:"DryRun"`
}

func (h *Handler) handleCancelDomainConfigChange(w http.ResponseWriter, r *http.Request, domainName string) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req cancelDomainConfigChangeRequest
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

			return
		}
	}

	ids, dryRun, cancelErr := h.Backend.CancelDomainConfigChange(domainName, req.DryRun)
	if cancelErr != nil {
		if errors.Is(cancelErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", cancelErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", cancelErr.Error())
		}

		return
	}

	h.writeJSON(r, w, cancelDomainConfigChangeOutput{
		CancelledChangeIDs:        ids,
		CancelledChangeProperties: []any{},
		DryRun:                    dryRun,
	})
}

// cancelServiceSoftwareUpdateRequest is the JSON request body for CancelServiceSoftwareUpdate.
type cancelServiceSoftwareUpdateRequest struct {
	DomainName string `json:"DomainName"`
}

// serviceSoftwareOptionsJSON is the JSON representation of service software options.
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

// cancelServiceSoftwareUpdateOutput is the JSON response for CancelServiceSoftwareUpdate.
type cancelServiceSoftwareUpdateOutput struct {
	ServiceSoftwareOptions serviceSoftwareOptionsJSON `json:"ServiceSoftwareOptions"`
}

func (h *Handler) handleCancelServiceSoftwareUpdate(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req cancelServiceSoftwareUpdateRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	opts, cancelErr := h.Backend.CancelServiceSoftwareUpdate(req.DomainName)
	if cancelErr != nil {
		if errors.Is(cancelErr, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", cancelErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", cancelErr.Error())
		}

		return
	}

	h.writeJSON(r, w, cancelServiceSoftwareUpdateOutput{
		ServiceSoftwareOptions: serviceSoftwareOptionsJSON{
			CurrentVersion:      opts.CurrentVersion,
			NewVersion:          opts.NewVersion,
			UpdateAvailable:     opts.UpdateAvailable,
			Cancellable:         opts.Cancellable,
			UpdateStatus:        opts.UpdateStatus,
			Description:         opts.Description,
			AutomatedUpdateDate: opts.AutomatedUpdateDate,
			OptionalDeployment:  opts.OptionalDeployment,
		},
	})
}

// createApplicationRequest is the JSON request body for CreateApplication.
type createApplicationRequest struct {
	Name        string          `json:"Name"`
	AppConfigs  []appConfigJSON `json:"AppConfigs"`
	DataSources []appDSJSON     `json:"DataSources"`
}

// appConfigJSON is the JSON representation of an AppConfig.
type appConfigJSON struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// appDSJSON is the JSON representation of an application data source.
type appDSJSON struct {
	DataSourceArn string `json:"DataSourceArn"`
}

// createApplicationOutput is the JSON response for CreateApplication.
type createApplicationOutput struct {
	ID          string          `json:"Id"`
	Name        string          `json:"Name"`
	ARN         string          `json:"Arn"`
	AppConfigs  []appConfigJSON `json:"AppConfigs"`
	DataSources []appDSJSON     `json:"DataSources"`
}

func (h *Handler) handleCreateApplication(w http.ResponseWriter, r *http.Request) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req createApplicationRequest
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	appConfigs := make([]AppConfig, 0, len(req.AppConfigs))
	for _, ac := range req.AppConfigs {
		appConfigs = append(appConfigs, AppConfig(ac))
	}

	dataSources := make([]AppDataSource, 0, len(req.DataSources))
	for _, ds := range req.DataSources {
		dataSources = append(dataSources, AppDataSource(ds))
	}

	app, createErr := h.Backend.CreateApplication(req.Name, appConfigs, dataSources)
	if createErr != nil {
		if errors.Is(createErr, ErrApplicationAlreadyExists) {
			h.writeError(r, w, http.StatusConflict, "ResourceAlreadyExistsException", createErr.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", createErr.Error())
		}

		return
	}

	outConfigs := make([]appConfigJSON, 0, len(app.AppConfigs))
	for _, ac := range app.AppConfigs {
		outConfigs = append(outConfigs, appConfigJSON(ac))
	}

	outDS := make([]appDSJSON, 0, len(app.DataSources))
	for _, ds := range app.DataSources {
		outDS = append(outDS, appDSJSON(ds))
	}

	h.writeJSON(r, w, createApplicationOutput{
		ID:          app.ID,
		Name:        app.Name,
		ARN:         app.ARN,
		AppConfigs:  outConfigs,
		DataSources: outDS,
	})
}

// handleVersionsRoutes handles GET /2021-01-01/opensearch/versions → ListVersions.
func (h *Handler) handleVersionsRoutes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	versions := []string{
		"OpenSearch_2.17", "OpenSearch_2.15", "OpenSearch_2.13",
		engineVersionOpenSearch211, "OpenSearch_2.10",
		engineVersionOpenSearch29, "OpenSearch_2.8",
		engineVersionOpenSearch27, "Elasticsearch_8.11",
		"Elasticsearch_7.10", "Elasticsearch_6.8",
	}

	// Support nextToken-based pagination offset.
	if tok := r.URL.Query().Get("nextToken"); tok != "" {
		for i, v := range versions {
			if v == tok {
				versions = versions[i:]

				break
			}
		}
	}

	// Support maxResults limit.
	maxResults := len(versions)
	if mr := r.URL.Query().Get("maxResults"); mr != "" {
		if n, err := strconv.Atoi(mr); err == nil && n > 0 && n < maxResults {
			maxResults = n
		}
	}

	result := map[string]any{
		"Versions": versions[:maxResults],
	}

	if maxResults < len(versions) {
		result["NextToken"] = versions[maxResults]
	}

	h.writeJSON(r, w, result)
}

// handleInstanceTypeLimitsRoutes handles DescribeInstanceTypeLimits requests.
// Path: GET /2021-01-01/opensearch/instanceTypeLimits/{EngineVersion}/{InstanceType}.
func (h *Handler) handleInstanceTypeLimitsRoutes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	// Path: /2021-01-01/opensearch/instanceTypeLimits/{EngineVersion}/{InstanceType}
	rest := strings.TrimPrefix(r.URL.Path, openSearchInstanceTypeLimitsPath)
	rest = strings.TrimPrefix(rest, "/")
	parts := strings.SplitN(rest, "/", 2) //nolint:mnd // split into 2: engineVersion, instanceType

	engineVersion := ""
	instanceType := ""

	if len(parts) >= 1 {
		engineVersion = parts[0]
	}

	if len(parts) >= 2 { //nolint:mnd // 2 path segments: engineVersion and instanceType
		instanceType = parts[1]
	}

	limits, err := h.Backend.DescribeInstanceTypeLimits(instanceType, engineVersion)
	if err != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

		return
	}

	dataMap := map[string]any{
		"InstanceLimits": limits.InstanceLimits,
		"StorageTypes":   limits.StorageTypes,
	}

	if len(limits.AdditionalLimits) > 0 {
		dataMap["AdditionalLimits"] = limits.AdditionalLimits
	}

	h.writeJSON(r, w, map[string]any{
		"LimitsByRole": map[string]any{
			"data": dataMap,
		},
	})
}

// handleInstanceTypeDetailsRoutes handles GET /2021-01-01/opensearch/instanceTypeDetails → ListInstanceTypeDetails.
func (h *Handler) handleInstanceTypeDetailsRoutes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	engineVersion := r.URL.Query().Get("engineVersion")
	instanceType := r.URL.Query().Get("instanceType")
	details := h.Backend.ListInstanceTypeDetails(engineVersion, instanceType)
	h.writeJSON(r, w, map[string]any{"InstanceTypeDetails": details})
}

// handleCompatibleVersionsRoutes handles GET /2021-01-01/opensearch/compatibleVersions → GetCompatibleVersions.
func (h *Handler) handleCompatibleVersionsRoutes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	domainName := r.URL.Query().Get("domainName")
	versions := h.Backend.GetCompatibleVersions(domainName)
	h.writeJSON(r, w, map[string]any{"CompatibleVersions": versions})
}

// handleVpcEndpointsRoutes handles VPC endpoint routes.
func (h *Handler) handleVpcEndpointsRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchVpcEndpointsPath)

	switch {
	// POST /vpcEndpoints/describe → DescribeVpcEndpoints
	case rest == "/describe" && r.Method == http.MethodPost:
		body, _ := httputils.ReadBody(r)
		var req struct {
			VpcEndpointIDs []string `json:"VpcEndpointIds"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		endpoints, errs := h.Backend.DescribeVpcEndpoints(req.VpcEndpointIDs)
		h.writeJSON(r, w, map[string]any{"VpcEndpoints": endpoints, "VpcEndpointErrors": errs})
	// Root: Create/List.
	case rest == "" || rest == "/":
		h.handleVpcEndpointRootRoutes(w, r)
	// Per-ID: Delete/Update.
	case strings.HasPrefix(rest, "/"):
		h.handleVpcEndpointIDRoutes(w, r, strings.TrimPrefix(rest, "/"))
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleVpcEndpointRootRoutes handles /vpcEndpoints and /vpcEndpoints/ requests.
func (h *Handler) handleVpcEndpointRootRoutes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		body, err := httputils.ReadBody(r)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

			return
		}
		var req struct {
			VpcOptions map[string]any `json:"VpcOptions"`
			DomainArn  string         `json:"DomainArn"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		ep, createErr := h.Backend.CreateVpcEndpoint(req.DomainArn, req.VpcOptions)
		if createErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", createErr.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{"VpcEndpoint": ep})
	case http.MethodGet:
		endpoints := h.Backend.ListVpcEndpoints()
		if endpoints == nil {
			endpoints = []*VpcEndpoint{}
		}
		h.writeJSON(r, w, map[string]any{"VpcEndpoints": endpoints})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleVpcEndpointIDRoutes handles /vpcEndpoints/{id} requests.
func (h *Handler) handleVpcEndpointIDRoutes(w http.ResponseWriter, r *http.Request, endpointID string) {
	switch r.Method {
	case http.MethodDelete:
		ep, err := h.Backend.DeleteVpcEndpoint(endpointID)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{
			"VpcEndpointSummary": map[string]any{jsonKeyVpcEndpointID: ep.VpcEndpointID, jsonKeyStatus: ep.Status},
		})
	case http.MethodPut:
		body, err := httputils.ReadBody(r)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

			return
		}
		var req struct {
			VpcOptions map[string]any `json:"VpcOptions"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		ep, updateErr := h.Backend.UpdateVpcEndpoint(endpointID, req.VpcOptions)
		if updateErr != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", updateErr.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{"VpcEndpoint": ep})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleScheduledActionsRoutes handles scheduled action routes.
func (h *Handler) handleScheduledActionsRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchScheduledActionsPath)

	switch {
	// GET /scheduledActions → ListScheduledActions
	case (rest == "" || rest == "/") && r.Method == http.MethodGet:
		domainName := r.URL.Query().Get("DomainName")
		actions := h.Backend.ListScheduledActions(domainName)
		if actions == nil {
			actions = []*ScheduledAction{}
		}
		h.writeJSON(r, w, map[string]any{"ScheduledActions": actions})
	// PUT /scheduledActions/update → UpdateScheduledAction
	case rest == "/update" && r.Method == http.MethodPut:
		body, err := httputils.ReadBody(r)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

			return
		}
		var req struct {
			ScheduledAction *ScheduledAction `json:"ScheduledAction"`
			DomainName      string           `json:"DomainName"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		if req.ScheduledAction == nil {
			req.ScheduledAction = &ScheduledAction{}
		}
		action, _ := h.Backend.UpdateScheduledAction(req.DomainName, req.ScheduledAction)
		h.writeJSON(r, w, map[string]any{"ScheduledAction": action})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleReservedInstancesRoutes handles reserved instance routes.
func (h *Handler) handleReservedInstancesRoutes(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, openSearchReservedPath)

	switch {
	// GET /reservedInstances → DescribeReservedInstances
	case (rest == "" || rest == "/") && r.Method == http.MethodGet:
		instances := h.Backend.DescribeReservedInstances()
		if instances == nil {
			instances = []*ReservedInstance{}
		}
		h.writeJSON(r, w, map[string]any{"ReservedInstances": instances})
	// GET /reservedInstances/offerings → DescribeReservedInstanceOfferings
	case rest == "/offerings" && r.Method == http.MethodGet:
		offerings := h.Backend.DescribeReservedInstanceOfferings()
		h.writeJSON(r, w, map[string]any{"ReservedInstanceOfferings": offerings})
	// POST /reservedInstances/offerings/{offeringId} → PurchaseReservedInstanceOffering
	case strings.HasPrefix(rest, "/offerings/") && r.Method == http.MethodPost:
		offeringID := strings.TrimPrefix(rest, "/offerings/")
		body, err := httputils.ReadBody(r)
		if err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

			return
		}
		var req struct {
			ReservationName string `json:"ReservationName"`
			InstanceCount   int    `json:"InstanceCount"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		if req.InstanceCount == 0 {
			req.InstanceCount = 1
		}
		ri, purchaseErr := h.Backend.PurchaseReservedInstanceOffering(
			offeringID,
			req.ReservationName,
			req.InstanceCount,
		)
		if purchaseErr != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", purchaseErr.Error())

			return
		}
		h.writeJSON(r, w, map[string]any{
			"ReservedInstanceId": ri.ReservedInstanceID,
			"ReservationName":    ri.ReservationName,
		})
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// handleUpgradeDomainRoutes handles POST /2021-01-01/opensearch/upgradeDomain → UpgradeDomain.
func (h *Handler) handleUpgradeDomainRoutes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

		return
	}

	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req struct {
		DomainName    string `json:"DomainName"`
		TargetVersion string `json:"TargetVersion"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	if upgradeErr := h.Backend.UpgradeDomain(req.DomainName, req.TargetVersion); upgradeErr != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", upgradeErr.Error())

		return
	}

	h.writeJSON(r, w, map[string]any{
		"UpgradeId":     fmt.Sprintf("upgrade-%s", req.DomainName),
		"DomainName":    req.DomainName,
		"TargetVersion": req.TargetVersion,
		"StepStatus":    "REQUESTED",
	})
}

// dispatchDomainGetRoutesExtended handles additional GET sub-routes on a domain.
// Returns true if handled.
func (h *Handler) dispatchDomainGetRoutesExtended(w http.ResponseWriter, r *http.Request, trimmed string) bool {
	if h.dispatchDomainGetStatusRoutes(w, r, trimmed) {
		return true
	}

	return h.dispatchDomainGetResourceRoutes(w, r, trimmed)
}

// dispatchDomainGetStatusRoutes handles status/health/upgrade/vpc GET sub-routes on a domain.
// Returns true if handled.
func (h *Handler) dispatchDomainGetStatusRoutes(w http.ResponseWriter, r *http.Request, trimmed string) bool {
	if h.dispatchDomainGetHealthRoutes(w, r, trimmed) {
		return true
	}

	if h.dispatchDomainGetUpgradeRoutes(w, r, trimmed) {
		return true
	}

	switch {
	case strings.HasSuffix(trimmed, "/autoTunes"):
		// DescribeDomainAutoTunes
		domainName, _ := strings.CutSuffix(trimmed, "/autoTunes")
		autoTunes, err := h.Backend.GetAutoTune(domainName)
		if err != nil {
			autoTunes = []*AutoTune{}
		}

		h.writeJSON(r, w, map[string]any{"AutoTunes": autoTunes})

		return true
	default:
		return h.dispatchDomainGetVpcRoutes(w, trimmed)
	}
}

// dispatchDomainGetHealthRoutes handles health/nodes/progress/dryRun GET sub-routes on a domain.
// Returns true if handled.
func (h *Handler) dispatchDomainGetHealthRoutes(w http.ResponseWriter, r *http.Request, trimmed string) bool {
	switch {
	case strings.HasSuffix(trimmed, "/progress"):
		// DescribeDomainChangeProgress
		domainName, _ := strings.CutSuffix(trimmed, "/progress")
		progress, err := h.Backend.GetChangeProgress(domainName)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return true
		}

		h.writeJSON(r, w, map[string]any{"ChangeProgressStatus": progress})
	case strings.HasSuffix(trimmed, "/health"):
		// DescribeDomainHealth
		domainName, _ := strings.CutSuffix(trimmed, "/health")
		health, err := h.Backend.GetDomainHealth(domainName)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return true
		}

		h.writeJSON(r, w, health)
	case strings.HasSuffix(trimmed, "/nodes"):
		// DescribeDomainNodes
		domainName, _ := strings.CutSuffix(trimmed, "/nodes")
		nodes, err := h.Backend.GetDomainNodes(domainName)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return true
		}

		h.writeJSON(r, w, map[string]any{"DomainNodesStatusList": nodes})
	case strings.HasSuffix(trimmed, "/dryRun"):
		// DescribeDryRunProgress
		domainName, _ := strings.CutSuffix(trimmed, "/dryRun")
		dr, err := h.Backend.GetDryRunProgress(domainName)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return true
		}

		h.writeJSON(r, w, map[string]any{"DryRunProgressStatus": dr})
	default:
		return false
	}

	return true
}

// dispatchDomainGetUpgradeRoutes handles upgrade-related GET sub-routes on a domain.
// Returns true if handled.
func (h *Handler) dispatchDomainGetUpgradeRoutes(w http.ResponseWriter, r *http.Request, trimmed string) bool {
	switch {
	case strings.HasSuffix(trimmed, "/upgradeHistory"):
		// GetUpgradeHistory
		domainName, _ := strings.CutSuffix(trimmed, "/upgradeHistory")
		history, err := h.Backend.GetUpgradeHistory(domainName)
		if err != nil {
			history = []*UpgradeHistory{}
		}

		h.writeJSON(r, w, map[string]any{"UpgradeHistories": history})
	case strings.HasSuffix(trimmed, "/upgrades"):
		// GetUpgradeStatus
		domainName, _ := strings.CutSuffix(trimmed, "/upgrades")
		upgradeName, upgradeStatus, upgradeStep, err := h.Backend.GetUpgradeStatus(domainName)

		if err != nil {
			upgradeName, upgradeStatus, upgradeStep = "INITIAL", upgradeStatusSucceeded, upgradeStepUpgrade
		}

		h.writeJSON(r, w, map[string]any{
			"UpgradeName": upgradeName,
			"StepStatus":  upgradeStatus,
			"UpgradeStep": upgradeStep,
		})
	default:
		return false
	}

	return true
}

// dispatchDomainGetVpcRoutes handles VPC-related GET sub-routes on a domain.
// Returns true if handled.
func (h *Handler) dispatchDomainGetVpcRoutes(w http.ResponseWriter, trimmed string) bool {
	switch {
	case strings.HasSuffix(trimmed, "/vpcEndpoints"):
		// ListVpcEndpointsForDomain
		domainName, _ := strings.CutSuffix(trimmed, "/vpcEndpoints")
		domain, descErr := h.Backend.DescribeDomain(domainName)
		var domainArn string
		if descErr == nil {
			domainArn = domain.ARN
		}
		endpoints := h.Backend.ListVpcEndpointsForDomain(domainArn)
		httputils.WriteJSON(context.Background(), w, http.StatusOK, map[string]any{"VpcEndpointSummaryList": endpoints})
	case strings.HasSuffix(trimmed, "/listVpcEndpointAccess"):
		// ListVpcEndpointAccess
		domainName, _ := strings.CutSuffix(trimmed, "/listVpcEndpointAccess")
		principals, _ := h.Backend.ListVpcEndpointAccess(domainName)
		if principals == nil {
			principals = []AuthorizedPrincipal{}
		}
		httputils.WriteJSON(
			context.Background(),
			w,
			http.StatusOK,
			map[string]any{"AuthorizedPrincipalList": principals},
		)
	default:
		return false
	}

	return true
}

// dispatchDomainGetResourceRoutes handles resource-listing GET sub-routes on a domain.
// Returns true if handled.
func (h *Handler) dispatchDomainGetResourceRoutes(w http.ResponseWriter, r *http.Request, trimmed string) bool {
	if h.dispatchDomainGetResourceByID(w, r, trimmed) {
		return true
	}

	switch {
	case strings.HasSuffix(trimmed, "/dataSource"):
		domainName, _ := strings.CutSuffix(trimmed, "/dataSource")
		sources, _ := h.Backend.ListDataSources(domainName)
		if sources == nil {
			sources = []*DataSource{}
		}
		h.writeJSON(r, w, map[string]any{"DataSources": sources})
	case strings.HasSuffix(trimmed, "/packages"):
		domainName, _ := strings.CutSuffix(trimmed, "/packages")
		h.writeJSON(r, w, map[string]any{jsonKeyPkgDetailsList: h.Backend.ListPackagesForDomain(domainName)})
	case strings.HasSuffix(trimmed, "/maintenance"):
		domainName, _ := strings.CutSuffix(trimmed, "/maintenance")
		maintenances, _ := h.Backend.ListDomainMaintenances(domainName)
		if maintenances == nil {
			maintenances = []*DomainMaintenance{}
		}
		h.writeJSON(r, w, map[string]any{"DomainMaintenances": maintenances})
	default:
		return false
	}

	return true
}

// dispatchDomainGetResourceByID handles GET sub-routes that address a specific resource by ID.
// Returns true if handled.
func (h *Handler) dispatchDomainGetResourceByID(w http.ResponseWriter, r *http.Request, trimmed string) bool {
	switch {
	case strings.Contains(trimmed, "/dataSource/"):
		parts := strings.SplitN(trimmed, "/dataSource/", 2) //nolint:mnd // path split count
		if len(parts) != 2 || parts[1] == "" {
			h.writeJSON(r, w, map[string]any{jsonKeyDataSource: map[string]any{}})

			return true
		}
		ds, err := h.Backend.GetDataSource(parts[0], parts[1])
		if err != nil {
			h.writeJSON(r, w, map[string]any{jsonKeyDataSource: map[string]any{}})

			return true
		}
		h.writeJSON(r, w, map[string]any{jsonKeyDataSource: ds})
	case strings.Contains(trimmed, "/maintenance/"):
		parts := strings.SplitN(trimmed, "/maintenance/", 2) //nolint:mnd // path split count
		if len(parts) != 2 || parts[1] == "" {
			h.writeJSON(r, w, map[string]any{jsonKeyStatus: softwareUpdateCompleted})

			return true
		}
		m, err := h.Backend.GetDomainMaintenanceStatus(parts[0], parts[1])
		if err != nil {
			h.writeJSON(r, w, map[string]any{jsonKeyStatus: softwareUpdateCompleted})

			return true
		}
		h.writeJSON(r, w, m)
	case strings.Contains(trimmed, "/index/"):
		parts := strings.SplitN(trimmed, "/index/", 2) //nolint:mnd // path split count
		if len(parts) != 2 || parts[1] == "" {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "invalid index path")

			return true
		}
		idx, err := h.Backend.GetIndex(parts[0], parts[1])
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return true
		}
		h.writeJSON(r, w, idx)
	default:
		return false
	}

	return true
}

// dispatchDomainPostRoutesExtended handles additional POST sub-routes on a domain.
// Returns true if handled.
func (h *Handler) dispatchDomainPostRoutesExtended(w http.ResponseWriter, r *http.Request, trimmed string) bool {
	switch {
	case strings.HasSuffix(trimmed, "/maintenance"):
		// StartDomainMaintenance
		domainName, _ := strings.CutSuffix(trimmed, "/maintenance")
		body, _ := httputils.ReadBody(r)
		var req struct {
			Action string `json:"Action"`
			NodeID string `json:"NodeId"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		m, err := h.Backend.StartDomainMaintenance(domainName, req.Action, req.NodeID)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return true
		}
		h.writeJSON(r, w, map[string]any{"MaintenanceId": m.MaintenanceID})
	case strings.HasSuffix(trimmed, "/revokeVpcEndpointAccess"):
		// RevokeVpcEndpointAccess
		domainName, _ := strings.CutSuffix(trimmed, "/revokeVpcEndpointAccess")
		body, _ := httputils.ReadBody(r)
		var req struct {
			Account string `json:"Account"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		_ = h.Backend.RevokeVpcEndpointAccess(domainName, req.Account)
		w.WriteHeader(http.StatusOK)
	case strings.HasSuffix(trimmed, "/serviceSoftwareUpdate"):
		// StartServiceSoftwareUpdate
		domainName, _ := strings.CutSuffix(trimmed, "/serviceSoftwareUpdate")
		opts, err := h.Backend.StartServiceSoftwareUpdate(domainName)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return true
		}
		h.writeJSON(r, w, map[string]any{
			"ServiceSoftwareOptions": map[string]any{
				"UpdateStatus": opts.UpdateStatus, "UpdateAvailable": opts.UpdateAvailable,
			},
		})
	case strings.HasSuffix(trimmed, "/updateDataSource"):
		// UpdateDataSource
		domainName, _ := strings.CutSuffix(trimmed, "/updateDataSource")
		body, _ := httputils.ReadBody(r)
		var req struct {
			Name        string `json:"Name"`
			Description string `json:"Description"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &req)
		}
		_ = h.Backend.UpdateDataSource(domainName, req.Name, req.Description)
		h.writeJSON(r, w, map[string]any{"Message": "DataSource updated"})
	case strings.Contains(trimmed, "/index/"):
		return h.handleCreateIndexRoute(w, r, trimmed)
	default:
		return false
	}

	return true
}

// handleCreateIndexRoute handles the POST {domainName}/index/{indexName} route.
func (h *Handler) handleCreateIndexRoute(w http.ResponseWriter, r *http.Request, trimmed string) bool {
	parts := strings.SplitN(trimmed, "/index/", 2) //nolint:mnd // path split count
	if len(parts) != 2 {                           //nolint:mnd // path split count
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "invalid index path")

		return true
	}
	body, _ := httputils.ReadBody(r)
	var req struct {
		Mappings map[string]any `json:"Mappings"`
		Settings map[string]any `json:"Settings"`
		Aliases  map[string]any `json:"Aliases"`
	}
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}
	idx, err := h.Backend.CreateIndex(parts[0], parts[1], req.Mappings, req.Settings, req.Aliases)
	if err != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

		return true
	}
	h.writeJSON(r, w, idx)

	return true
}

// dispatchDomainDeleteRoutesExtended handles DELETE sub-routes on a domain.
// Returns true if handled.
func (h *Handler) dispatchDomainDeleteRoutesExtended(w http.ResponseWriter, r *http.Request, trimmed string) bool {
	if strings.Contains(trimmed, "/dataSource/") {
		// DeleteDataSource: {domainName}/dataSource/{name}
		parts := strings.SplitN(trimmed, "/dataSource/", 2) //nolint:mnd // path split count
		if len(parts) == 2 {                                //nolint:mnd // path split count
			_ = h.Backend.DeleteDataSource(parts[0], parts[1])
		}
		h.writeJSON(r, w, map[string]any{"Message": "DataSource deleted"})

		return true
	}

	if strings.Contains(trimmed, "/index/") {
		// DeleteIndex: {domainName}/index/{indexName}
		parts := strings.SplitN(trimmed, "/index/", 2) //nolint:mnd // path split count
		if len(parts) == 2 {                           //nolint:mnd // path split count
			idx, err := h.Backend.DeleteIndex(parts[0], parts[1])
			if err != nil {
				h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

				return true
			}
			h.writeJSON(r, w, idx)
		} else {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "invalid index path")
		}

		return true
	}

	return false
}
