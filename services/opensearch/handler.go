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
	jsonKeyDomainConfig     = "DomainConfig"
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

// Reset clears the handler's backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

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
	w.Header().Set("x-amzn-ErrorType", code)
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

// dispatchDomainGetStatusRoutes handles status/health/upgrade/vpc GET sub-routes on a domain.
// Returns true if handled.
func (h *Handler) dispatchDomainGetStatusRoutes(
	w http.ResponseWriter,
	r *http.Request,
	trimmed string,
) bool {
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
		if sources == nil {
			sources = []*DataSource{}
		}
		h.writeJSON(r, w, map[string]any{"DataSources": sources})
	case strings.HasSuffix(trimmed, "/packages"):
		domainName, _ := strings.CutSuffix(trimmed, "/packages")
		h.writeJSON(
			r,
			w,
			map[string]any{jsonKeyPkgDetailsList: h.Backend.ListPackagesForDomain(domainName)},
		)
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
func (h *Handler) dispatchDomainGetResourceByID(
	w http.ResponseWriter,
	r *http.Request,
	trimmed string,
) bool {
	switch {
	case strings.Contains(trimmed, "/dataSource/"):
		domainName, dsName, ok := strings.Cut(trimmed, "/dataSource/")
		if !ok || dsName == "" {
			h.writeJSON(r, w, map[string]any{jsonKeyDataSource: map[string]any{}})

			return true
		}
		ds, err := h.Backend.GetDataSource(domainName, dsName)
		if err != nil {
			h.writeJSON(r, w, map[string]any{jsonKeyDataSource: map[string]any{}})

			return true
		}
		h.writeJSON(r, w, map[string]any{jsonKeyDataSource: ds})
	case strings.Contains(trimmed, "/maintenance/"):
		domainName, maintenanceID, ok := strings.Cut(trimmed, "/maintenance/")
		if !ok || maintenanceID == "" {
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
		body, _ := httputils.ReadBody(r)
		var sswReq struct {
			DesiredStartTime *int64 `json:"DesiredStartTime"`
			ScheduleAt       string `json:"ScheduleAt"`
		}
		if len(body) > 0 {
			_ = json.Unmarshal(body, &sswReq)
		}
		opts, err := h.Backend.StartServiceSoftwareUpdate(domainName, sswReq.ScheduleAt)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return true
		}
		h.writeJSON(r, w, map[string]any{
			"ServiceSoftwareOptions": map[string]any{
				"UpdateStatus":    opts.UpdateStatus,
				"UpdateAvailable": opts.UpdateAvailable,
				"Description":     opts.Description,
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
