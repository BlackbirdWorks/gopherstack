package opensearch

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
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
	openSearchDefaultAppSettingPath  = "/2021-01-01/opensearch/defaultApplicationSetting"
	openSearchVersionsPath           = "/2021-01-01/opensearch/versions"
	openSearchInstanceTypesPath      = "/2021-01-01/opensearch/instanceTypeDetails"
	openSearchCompatiblePath         = "/2021-01-01/opensearch/compatibleVersions"
	openSearchVpcEndpointsPath       = "/2021-01-01/opensearch/vpcEndpoints"
	openSearchReservedPath           = "/2021-01-01/opensearch/reservedInstances"
	openSearchUpgradePath            = "/2021-01-01/opensearch/upgradeDomain"
	openSearchInstanceTypeLimitsPath = "/2021-01-01/opensearch/instanceTypeLimits"
	openSearchServerlessPath         = "/2021-11-01/opensearch/serverless"
	openSearchInsightsPath           = "/2021-01-01/opensearch/insights"
	openSearchInsightDetailsPath     = "/2021-01-01/opensearch/insight-details"
	openSearchInsightFeedbackPath    = "/2021-01-01/opensearch/insight-feedback"
	openSearchAppMigrationsPath      = "/2021-01-01/opensearch/app-migrations"
	// openSearchDomainInfoPath is DescribeDomains' real literal path
	// (api_op_DescribeDomains.go, opensearch@v1.75.4 serializers.go: POST
	// /2021-01-01/opensearch/domain-info, DomainNames in the body) -- handled as
	// an exact-path check before dispatchDomainRoutes, not as a domain-prefix
	// sub-route, because it is a sibling of openSearchPathPrefix's "domain" path
	// segment, not nested under it (gopherstack-l5ir).
	openSearchDomainInfoPath = "/2021-01-01/opensearch/domain-info"
	// openSearchLegacyDomainPath is the un-prefixed root that ListDomainNames
	// and ListPackagesForDomain still use (api_op_ListDomainNames.go /
	// api_op_ListPackagesForDomain.go, opensearch@v1.75.4 serializers.go:
	// "/2021-01-01/domain" and "/2021-01-01/domain/{DomainName}/packages" --
	// no "/opensearch/" segment, unlike every other domain op) -- gopherstack-l5ir.
	openSearchLegacyDomainPath = "/2021-01-01/domain"
	// openSearchListApplicationsPath is ListApplications' real literal path
	// (api_op_ListApplications.go, opensearch@v1.75.4 serializers.go: GET
	// /2021-01-01/opensearch/list-applications) -- a sibling of, not nested
	// under, openSearchApplicationPath -- gopherstack-l5ir.
	openSearchListApplicationsPath = "/2021-01-01/opensearch/list-applications"
	// openSearchReservedOfferingsPath and openSearchPurchaseReservedPath are
	// DescribeReservedInstanceOfferings' and PurchaseReservedInstanceOffering's
	// real literal paths (api_op_DescribeReservedInstanceOfferings.go /
	// api_op_PurchaseReservedInstanceOffering.go, opensearch@v1.75.4
	// serializers.go) -- siblings of, not nested under, openSearchReservedPath
	// -- gopherstack-l5ir.
	openSearchReservedOfferingsPath = "/2021-01-01/opensearch/reservedInstanceOfferings"
	openSearchPurchaseReservedPath  = "/2021-01-01/opensearch/purchaseReservedInstanceOffering"
	openSearchServiceName           = "OpenSearch"
	// pathSuffixDescribe and pathSuffixUpdate are the fixed-literal-action
	// path suffixes shared by several op families (DescribePackages/
	// DescribeVpcEndpoints, UpdatePackage/UpdateVpcEndpoint, etc).
	pathSuffixDescribe = "/describe"
	pathSuffixUpdate   = "/update"
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
	jsonKeyNextToken        = "NextToken"
	// jsonKeyAppName/jsonKeyAppArn/jsonKeyCreatedAt/jsonKeyLastUpdatedAt are
	// lowerCamelCase for the newer Applications API (verified against
	// GetApplication/UpdateApplication/ListApplications in opensearch's own
	// deserializers.go) -- unlike jsonKeyDataSources below, which serves the
	// older, PascalCase domain-scoped ListDataSources.
	// jsonKeyStatusLower is the lowerCamel "status" key shared by the
	// Applications and capability sub-APIs (as opposed to jsonKeyStatus
	// above, which is PascalCase "Status" for the older Domain API).
	jsonKeyStatusLower   = "status"
	jsonKeyAppName       = "name"
	jsonKeyAppArn        = "arn"
	jsonKeyDomainConfig  = "DomainConfig"
	jsonKeyDataSources   = "DataSources"
	jsonKeyCreatedAt     = "createdAt"
	jsonKeyLastUpdatedAt = "lastUpdatedAt"
	// Index data-plane operation segments and document response keys.
	indexOpDoc      = "_doc"
	indexOpSearch   = "_search"
	indexOpCount    = "_count"
	jsonKeyDocIndex = "_index"
	jsonKeyDocID    = "_id"
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
	openSearchReservedPath,
	openSearchUpgradePath,
	openSearchInstanceTypeLimitsPath,
	openSearchServerlessPath,
	openSearchInsightsPath,
	openSearchInsightDetailsPath,
	openSearchInsightFeedbackPath,
	openSearchAppMigrationsPath,
	openSearchLegacyDomainPath,
	openSearchListApplicationsPath,
	openSearchReservedOfferingsPath,
	openSearchPurchaseReservedPath,
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

// RouteMatcher returns a matcher that selects OpenSearch requests by path
// prefix (classic control-plane, REST-JSON) or by the real AOSS
// X-Amz-Target prefix (JSON-RPC 1.0, always POST /) -- see
// openSearchServerlessTargetPrefix's doc comment (gopherstack-92ft). The
// target prefix alone fully discriminates AOSS requests: X-Amz-Target
// values are unique per AWS service by construction (SSM's and
// Personalize's RouteMatchers scope on target prefix the same way, with no
// SigV4 check), unlike iot/iotdataplane's shared generic path segments
// (gopherstack-61i8) where SigV4 scoping was needed to break a real
// ambiguity.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), openSearchServerlessTargetPrefix) {
			return true
		}

		return isOpenSearchPath(c.Request().URL.Path)
	}
}

// Reset clears the handler's backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// ServeHTTP implements [http.Handler] for the OpenSearch service.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.handleTagRoutes(w, r) {
		return
	}

	if r.URL.Path == openSearchDomainInfoPath && r.Method == http.MethodPost {
		h.handleDescribeDomains(w, r)

		return
	}

	if h.dispatchLegacyDomainRoutes(w, r) {
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
	if h.dispatchNonDomainCoreRoutes(w, r) {
		return true
	}

	return h.dispatchNonDomainExtRoutes(w, r)
}

// dispatchNonDomainCoreRoutes handles cross-cluster, package, application, and instance-type routes.
func (h *Handler) dispatchNonDomainCoreRoutes(w http.ResponseWriter, r *http.Request) bool {
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
	case strings.HasPrefix(path, openSearchDefaultAppSettingPath):
		h.handleDefaultApplicationSettingRoutes(w, r)
	case path == openSearchListApplicationsPath:
		h.handleListApplications(w, r)
	case strings.HasPrefix(path, openSearchApplicationPath):
		h.handleApplicationRoutes(w, r)
	case strings.HasPrefix(path, openSearchVersionsPath):
		h.handleVersionsRoutes(w, r)
	case strings.HasPrefix(path, openSearchInstanceTypesPath):
		h.handleInstanceTypeDetailsRoutes(w, r)
	default:
		return false
	}

	return true
}

// dispatchNonDomainExtRoutes handles VPC, reserved instances, upgrade, and serverless routes.
func (h *Handler) dispatchNonDomainExtRoutes(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path

	switch {
	case strings.HasPrefix(path, openSearchCompatiblePath):
		h.handleCompatibleVersionsRoutes(w, r)
	case strings.HasPrefix(path, openSearchVpcEndpointsPath):
		h.handleVpcEndpointsRoutes(w, r)
	case strings.HasPrefix(path, openSearchReservedPath):
		h.handleReservedInstancesRoutes(w, r)
	case path == openSearchReservedOfferingsPath:
		h.handleReservedInstanceOfferings(w, r)
	case path == openSearchPurchaseReservedPath:
		h.handlePurchaseReservedInstanceOffering(w, r)
	case strings.HasPrefix(path, openSearchInstanceTypeLimitsPath):
		h.handleInstanceTypeLimitsRoutes(w, r)
	case strings.HasPrefix(path, openSearchUpgradePath):
		h.handleUpgradeDomainRoutes(w, r)
	case strings.HasPrefix(path, openSearchServerlessPath):
		h.handleServerlessRoutes(w, r)
	case path == openSearchInsightsPath:
		h.handleListInsights(w, r)
	case path == openSearchInsightDetailsPath:
		h.handleDescribeInsightDetails(w, r)
	case path == openSearchInsightFeedbackPath:
		h.handleInsightFeedback(w, r)
	case strings.HasPrefix(path, openSearchAppMigrationsPath):
		h.handleAppMigrationsRoutes(w, r)
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

// dispatchDomainRootRoutes handles POST on the domain root (CreateDomain).
// ListDomainNames is NOT here: real clients GET the un-prefixed
// openSearchLegacyDomainPath, not this /opensearch/domain root -- see
// dispatchLegacyDomainRoutes.
func (h *Handler) dispatchDomainRootRoutes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.handleCreateDomain(w, r)
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// dispatchLegacyDomainRoutes handles the un-prefixed openSearchLegacyDomainPath
// root: ListDomainNames (GET, exact) and ListPackagesForDomain (GET
// {DomainName}/packages). Returns false when the path doesn't belong here at
// all, so the caller can fall through to the ordinary dispatch tree.
func (h *Handler) dispatchLegacyDomainRoutes(w http.ResponseWriter, r *http.Request) bool {
	path := r.URL.Path

	switch {
	case path == openSearchLegacyDomainPath:
		if r.Method == http.MethodGet {
			h.handleListDomainNames(w, r)
		} else {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
		}

		return true
	case strings.HasPrefix(path, openSearchLegacyDomainPath+"/"):
		rest := strings.TrimPrefix(path, openSearchLegacyDomainPath+"/")
		if domainName, ok := strings.CutSuffix(rest, "/packages"); ok && r.Method == http.MethodGet {
			h.handleListPackagesForDomainRoute(w, r, domainName)
		} else {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
		}

		return true
	default:
		return false
	}
}

// handleListPackagesForDomainRoute serves ListPackagesForDomain.
func (h *Handler) handleListPackagesForDomainRoute(w http.ResponseWriter, r *http.Request, domainName string) {
	pkgs := h.Backend.ListPackagesForDomain(domainName)
	outList := make([]domainPackageDetailsJSON, 0, len(pkgs))

	for _, pkg := range pkgs {
		outList = append(outList, domainPackageDetailsJSON{
			PackageID:           pkg.PackageID,
			DomainName:          domainName,
			DomainPackageStatus: pkgStateActive,
			PackageName:         pkg.PackageName,
			PackageType:         pkg.PackageType,
		})
	}

	h.writeJSON(r, w, map[string]any{jsonKeyPkgDetailsList: outList})
}

// dispatchDomainDeleteRoutes handles DELETE under a domain path.
func (h *Handler) dispatchDomainDeleteRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	trimmed := strings.TrimPrefix(rest, "/")
	if h.dispatchDomainDeleteRoutesExtended(w, r, trimmed) {
		return
	}

	h.handleDeleteDomain(w, r, domainNameFromRest(rest))
}

// dispatchDomainPutRoutes handles PUT under a domain path (UpdateIndex,
// UpdateScheduledAction, UpdateDataSource). UpdateDomainConfig is NOT here:
// the real SDK sends POST, not PUT (api_op_UpdateDomainConfig.go /
// opensearch@v1.75.4 serializers.go), so it is handled by handleConfigPostRoute
// off the POST path instead -- gopherstack-l5ir.
func (h *Handler) dispatchDomainPutRoutes(w http.ResponseWriter, r *http.Request, rest string) {
	trimmed := domainNameFromRest(rest)

	// UpdateIndex: PUT {domainName}/index/{indexName}
	if strings.Contains(trimmed, "/index/") {
		h.handleUpdateIndexRoute(w, r, trimmed)

		return
	}

	// UpdateScheduledAction: PUT {domainName}/scheduledAction/update
	if domainName, ok := strings.CutSuffix(trimmed, "/scheduledAction/update"); ok {
		h.handleUpdateScheduledAction(w, r, domainName)

		return
	}

	// UpdateDataSource: PUT {domainName}/dataSource/{name}
	if domainName, dsName, ok := strings.Cut(trimmed, "/dataSource/"); ok && dsName != "" {
		h.handleUpdateDataSource(w, r, domainName, dsName)

		return
	}

	h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
}

// handleConfigPostRoute handles UpdateDomainConfig: POST {domainName}/config
// (real method per the SDK serializer -- see dispatchDomainPutRoutes).
func (h *Handler) handleConfigPostRoute(w http.ResponseWriter, r *http.Request, name string) {
	body, _ := httputils.ReadBody(r)
	var req domainJSON
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	input := applyReqToUpdateInput(&req)

	if req.DryRun {
		h.handleUpdateDomainConfigDryRun(w, r, name, input)

		return
	}

	domain, err := h.Backend.UpdateDomainConfig(name, input)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
		}

		return
	}

	h.writeJSON(r, w, map[string]any{jsonKeyDomainConfig: toDomainConfigJSON(domain)})
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

// Handle satisfies the Echo handler interface.
func (h *Handler) Handle(c *echo.Context) error {
	if op, ok := strings.CutPrefix(c.Request().Header.Get("X-Amz-Target"), openSearchServerlessTargetPrefix); ok {
		return h.handleServerlessJSONRPC(c, op)
	}

	h.ServeHTTP(c.Response(), c.Request())

	return nil
}

// Handler returns the Echo HandlerFunc for this service.
func (h *Handler) Handler() echo.HandlerFunc {
	return h.Handle
}

type errorResponseJSON struct {
	Message string `json:"message"`
}

func (h *Handler) writeError(
	r *http.Request,
	w http.ResponseWriter,
	status int,
	code, message string,
) {
	ctx := r.Context()
	logger.Load(ctx).ErrorContext(r.Context(), "opensearch error", "code", code, "message", message)
	w.Header().Set("X-Amzn-Errortype", code)
	httputils.WriteJSON(ctx, w, status, errorResponseJSON{Message: message})
}

func (h *Handler) writeJSON(r *http.Request, w http.ResponseWriter, v any) {
	httputils.WriteJSON(r.Context(), w, http.StatusOK, v)
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
	case strings.HasSuffix(trimmed, "/config"):
		domainName := strings.TrimSuffix(trimmed, "/config")
		h.handleConfigPostRoute(w, r, domainName)
	default:
		if h.dispatchDomainPostRoutesExtended(w, r, trimmed) {
			return
		}
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// dispatchDomainGetRoutesExtended handles additional GET sub-routes on a domain.
// Returns true if handled.
func (h *Handler) dispatchDomainGetRoutesExtended(
	w http.ResponseWriter,
	r *http.Request,
	trimmed string,
) bool {
	if h.dispatchDomainGetStatusRoutes(w, r, trimmed) {
		return true
	}

	return h.dispatchDomainGetResourceRoutes(w, r, trimmed)
}

// dispatchDomainGetStatusRoutes handles status/health/vpc GET sub-routes on a
// domain. Upgrade history/status are NOT here: their real paths are nested
// under openSearchUpgradePath, not the domain prefix -- see
// dispatchUpgradeStatusRoutes.
func (h *Handler) dispatchDomainGetStatusRoutes(
	w http.ResponseWriter,
	r *http.Request,
	trimmed string,
) bool {
	if h.dispatchDomainGetHealthRoutes(w, r, trimmed) {
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
		return h.dispatchDomainGetVpcRoutes(w, r, trimmed)
	}
}

// dispatchDomainGetResourceRoutes handles resource-listing GET sub-routes on a domain.
// Returns true if handled.
func (h *Handler) dispatchDomainGetResourceRoutes(
	w http.ResponseWriter,
	r *http.Request,
	trimmed string,
) bool {
	if h.dispatchDomainGetResourceByID(w, r, trimmed) {
		return true
	}

	switch {
	case strings.HasSuffix(trimmed, "/dataSource"):
		domainName, _ := strings.CutSuffix(trimmed, "/dataSource")
		sources, _ := h.Backend.ListDataSources(domainName)
		items := make([]dataSourceJSON, 0, len(sources))
		for _, ds := range sources {
			items = append(items, toDataSourceJSON(ds))
		}
		h.writeJSON(r, w, map[string]any{jsonKeyDataSources: items})
	// ListDomainMaintenances: GET {domainName}/domainMaintenances (plural,
	// api_op_ListDomainMaintenances.go, opensearch@v1.75.4 serializers.go) --
	// gopherstack-l5ir.
	case strings.HasSuffix(trimmed, "/domainMaintenances"):
		domainName, _ := strings.CutSuffix(trimmed, "/domainMaintenances")
		maintenances, _ := h.Backend.ListDomainMaintenances(domainName)
		if maintenances == nil {
			maintenances = []*DomainMaintenance{}
		}
		h.writeJSON(r, w, map[string]any{"DomainMaintenances": maintenances})
	case strings.HasSuffix(trimmed, "/scheduledActions"):
		domainName, _ := strings.CutSuffix(trimmed, "/scheduledActions")
		actions := h.Backend.ListScheduledActions(domainName)
		if actions == nil {
			actions = []*ScheduledAction{}
		}
		h.writeJSON(r, w, map[string]any{"ScheduledActions": actions})
	default:
		return false
	}

	return true
}

// dispatchDomainGetResourceByID handles GET sub-routes that address a specific resource by ID.
// Returns true if handled.
func (h *Handler) dispatchDomainGetResourceByID(
	w http.ResponseWriter,
	r *http.Request,
	trimmed string,
) bool {
	switch {
	case strings.Contains(trimmed, "/dataSource/"):
		domainName, dsName, ok := strings.Cut(trimmed, "/dataSource/")
		if !ok || dsName == "" {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")

			return true
		}
		ds, err := h.Backend.GetDataSource(domainName, dsName)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return true
		}
		h.writeJSON(r, w, toDataSourceJSON(ds))
	// GetDomainMaintenanceStatus: GET {domainName}/domainMaintenance
	// (singular, api_op_GetDomainMaintenanceStatus.go, opensearch@v1.75.4
	// serializers.go) -- the maintenance ID is a "maintenanceId" query
	// param, not a URL segment -- gopherstack-l5ir.
	case strings.HasSuffix(trimmed, "/domainMaintenance"):
		domainName, _ := strings.CutSuffix(trimmed, "/domainMaintenance")
		maintenanceID := r.URL.Query().Get("maintenanceId")
		if maintenanceID == "" {
			h.writeJSON(r, w, map[string]any{jsonKeyStatus: softwareUpdateCompleted})

			return true
		}
		m, err := h.Backend.GetDomainMaintenanceStatus(domainName, maintenanceID)
		if err != nil {
			h.writeJSON(r, w, map[string]any{jsonKeyStatus: softwareUpdateCompleted})

			return true
		}
		h.writeJSON(r, w, m)
	case strings.Contains(trimmed, "/index/"):
		return h.handleIndexGetRoute(w, r, trimmed)
	default:
		return false
	}

	return true
}

// dispatchDomainPostRoutesExtended handles additional POST sub-routes on a domain.
// Returns true if handled.
func (h *Handler) dispatchDomainPostRoutesExtended(
	w http.ResponseWriter,
	r *http.Request,
	trimmed string,
) bool {
	switch {
	// StartDomainMaintenance: POST {domainName}/domainMaintenance (singular,
	// api_op_StartDomainMaintenance.go, opensearch@v1.75.4 serializers.go) --
	// gopherstack-l5ir.
	case strings.HasSuffix(trimmed, "/domainMaintenance"):
		domainName, _ := strings.CutSuffix(trimmed, "/domainMaintenance")
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
	// CreateIndex: POST {domainName}/index, IndexName in the body -- unlike
	// GetIndex/DeleteIndex/UpdateIndex, the real CreateIndex path has NO
	// {IndexName} URL segment at all (api_op_CreateIndex.go,
	// opensearch@v1.75.4: only DomainName is URI-bound) -- gopherstack-l5ir.
	case strings.HasSuffix(trimmed, "/index"):
		return h.handleCreateIndexRealRoute(w, r, trimmed)
	case strings.Contains(trimmed, "/index/"):
		return h.handleCreateIndexRoute(w, r, trimmed)
	default:
		return false
	}

	return true
}

// dispatchDomainDeleteRoutesExtended handles DELETE sub-routes on a domain.
// Returns true if handled.
func (h *Handler) dispatchDomainDeleteRoutesExtended(
	w http.ResponseWriter,
	r *http.Request,
	trimmed string,
) bool {
	if strings.Contains(trimmed, "/dataSource/") {
		// DeleteDataSource: {domainName}/dataSource/{name}
		domainName, dsName, ok := strings.Cut(trimmed, "/dataSource/")
		if ok {
			_ = h.Backend.DeleteDataSource(domainName, dsName)
		}
		h.writeJSON(r, w, map[string]any{"Message": "DataSource deleted"})

		return true
	}

	if strings.Contains(trimmed, "/index/") {
		return h.handleIndexDeleteRoute(w, r, trimmed)
	}

	return false
}
