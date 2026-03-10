package appconfig

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	appConfigMatchPriority = 86

	// pathParts* constants define the expected segment counts for route matching.
	pathPartsBase      = 2 // /resource/{id}
	pathPartsSubLevel  = 3 // /applications/{id}/subresource
	pathPartsSubItem   = 4 // /applications/{id}/subresource/{subId}
	pathPartsDeepLevel = 5 // /applications/{id}/subresource/{subId}/nested
)

// Handler is the Echo HTTP handler for AppConfig operations.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new AppConfig Handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "AppConfig" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateApplication",
		"GetApplication",
		"ListApplications",
		"UpdateApplication",
		"DeleteApplication",
		"CreateEnvironment",
		"GetEnvironment",
		"ListEnvironments",
		"UpdateEnvironment",
		"DeleteEnvironment",
		"CreateConfigurationProfile",
		"GetConfigurationProfile",
		"ListConfigurationProfiles",
		"UpdateConfigurationProfile",
		"DeleteConfigurationProfile",
		"CreateHostedConfigurationVersion",
		"GetHostedConfigurationVersion",
		"ListHostedConfigurationVersions",
		"DeleteHostedConfigurationVersion",
		"CreateDeploymentStrategy",
		"GetDeploymentStrategy",
		"ListDeploymentStrategies",
		"UpdateDeploymentStrategy",
		"DeleteDeploymentStrategy",
		"StartDeployment",
		"GetDeployment",
		"ListDeployments",
		"StopDeployment",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "appconfig" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function matching AppConfig REST API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, "/applications") || strings.HasPrefix(path, "/deploymentstrategies")
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return appConfigMatchPriority }

// ExtractOperation returns the operation name based on the parsed path and HTTP method.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	route := parseAppConfigPath(c.Request().Method, c.Request().URL.Path)

	return route.operation
}

// ExtractResource extracts the primary resource ID from the URL path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	route := parseAppConfigPath(c.Request().Method, c.Request().URL.Path)
	if route.applicationID != "" {
		return route.applicationID
	}

	return route.strategyID
}

// appConfigRoute holds parsed path segments and the derived operation name.
type appConfigRoute struct {
	applicationID string
	environmentID string
	profileID     string
	strategyID    string
	operation     string
	versionNumber int32
	deploymentNum int32
}

// parseAppConfigPath parses an HTTP method and URL path into an appConfigRoute,
// identifying the resource type, IDs, and operation name for AppConfig REST API requests.
// It maps REST path segments to their corresponding CRUD operations.
func parseAppConfigPath(method, path string) appConfigRoute {
	// Trim leading slash and split.
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")

	if len(parts) == 0 {
		return appConfigRoute{operation: "Unknown"}
	}

	switch parts[0] {
	case "deploymentstrategies":
		return parseDeploymentStrategyRoute(method, parts)
	case "applications":
		return parseApplicationRoute(method, parts)
	}

	return appConfigRoute{operation: "Unknown"}
}

func parseDeploymentStrategyRoute(method string, parts []string) appConfigRoute {
	if len(parts) == 1 {
		switch method {
		case http.MethodPost:
			return appConfigRoute{operation: "CreateDeploymentStrategy"}
		case http.MethodGet:
			return appConfigRoute{operation: "ListDeploymentStrategies"}
		}

		return appConfigRoute{operation: "Unknown"}
	}

	strategyID := parts[1]

	switch method {
	case http.MethodGet:
		return appConfigRoute{strategyID: strategyID, operation: "GetDeploymentStrategy"}
	case http.MethodPatch:
		return appConfigRoute{strategyID: strategyID, operation: "UpdateDeploymentStrategy"}
	case http.MethodDelete:
		return appConfigRoute{strategyID: strategyID, operation: "DeleteDeploymentStrategy"}
	}

	return appConfigRoute{strategyID: strategyID, operation: "Unknown"}
}

// parseApplicationRoute parses routes starting with /applications.
//

func parseApplicationRoute(method string, parts []string) appConfigRoute {
	if len(parts) == 1 {
		switch method {
		case http.MethodPost:
			return appConfigRoute{operation: "CreateApplication"}
		case http.MethodGet:
			return appConfigRoute{operation: "ListApplications"}
		}

		return appConfigRoute{operation: "Unknown"}
	}

	appID := parts[1]

	if len(parts) == pathPartsBase {
		return parseAppIDRoute(method, appID)
	}

	switch parts[2] {
	case "environments":
		return parseEnvironmentRoute(method, appID, parts)
	case "configurationprofiles":
		return parseConfigProfileRoute(method, appID, parts)
	}

	return appConfigRoute{applicationID: appID, operation: "Unknown"}
}

func parseAppIDRoute(method, appID string) appConfigRoute {
	switch method {
	case http.MethodGet:
		return appConfigRoute{applicationID: appID, operation: "GetApplication"}
	case http.MethodPatch:
		return appConfigRoute{applicationID: appID, operation: "UpdateApplication"}
	case http.MethodDelete:
		return appConfigRoute{applicationID: appID, operation: "DeleteApplication"}
	}

	return appConfigRoute{applicationID: appID, operation: "Unknown"}
}

func parseEnvironmentRoute(method, appID string, parts []string) appConfigRoute {
	if len(parts) == pathPartsSubLevel {
		switch method {
		case http.MethodPost:
			return appConfigRoute{applicationID: appID, operation: "CreateEnvironment"}
		case http.MethodGet:
			return appConfigRoute{applicationID: appID, operation: "ListEnvironments"}
		}

		return appConfigRoute{applicationID: appID, operation: "Unknown"}
	}

	envID := parts[3]

	if len(parts) == pathPartsSubItem {
		return parseEnvIDRoute(method, appID, envID)
	}

	if len(parts) >= pathPartsDeepLevel && parts[4] == "deployments" {
		return parseDeploymentRoute(method, appID, envID, parts)
	}

	return appConfigRoute{applicationID: appID, environmentID: envID, operation: "Unknown"}
}

func parseEnvIDRoute(method, appID, envID string) appConfigRoute {
	switch method {
	case http.MethodGet:
		return appConfigRoute{applicationID: appID, environmentID: envID, operation: "GetEnvironment"}
	case http.MethodPatch:
		return appConfigRoute{applicationID: appID, environmentID: envID, operation: "UpdateEnvironment"}
	case http.MethodDelete:
		return appConfigRoute{applicationID: appID, environmentID: envID, operation: "DeleteEnvironment"}
	}

	return appConfigRoute{applicationID: appID, environmentID: envID, operation: "Unknown"}
}

// parseDeploymentRoute parses deployment routes under /environments/{envId}/deployments.
//
//nolint:dupl // similar structure to parseHostedVersionRoute by design; different resource fields
func parseDeploymentRoute(method, appID, envID string, parts []string) appConfigRoute {
	if len(parts) == pathPartsDeepLevel {
		switch method {
		case http.MethodPost:
			return appConfigRoute{applicationID: appID, environmentID: envID, operation: "StartDeployment"}
		case http.MethodGet:
			return appConfigRoute{applicationID: appID, environmentID: envID, operation: "ListDeployments"}
		}

		return appConfigRoute{applicationID: appID, environmentID: envID, operation: "Unknown"}
	}

	depNum, err := strconv.ParseInt(parts[5], 10, 32)
	if err != nil {
		return appConfigRoute{applicationID: appID, environmentID: envID, operation: "Unknown"}
	}

	num := int32(depNum)

	switch method {
	case http.MethodGet:
		return appConfigRoute{
			applicationID: appID,
			environmentID: envID,
			deploymentNum: num,
			operation:     "GetDeployment",
		}
	case http.MethodDelete:
		return appConfigRoute{
			applicationID: appID,
			environmentID: envID,
			deploymentNum: num,
			operation:     "StopDeployment",
		}
	}

	return appConfigRoute{applicationID: appID, environmentID: envID, deploymentNum: num, operation: "Unknown"}
}

// parseConfigProfileRoute parses configuration profile routes.
//

func parseConfigProfileRoute(method, appID string, parts []string) appConfigRoute {
	if len(parts) == pathPartsSubLevel {
		switch method {
		case http.MethodPost:
			return appConfigRoute{applicationID: appID, operation: "CreateConfigurationProfile"}
		case http.MethodGet:
			return appConfigRoute{applicationID: appID, operation: "ListConfigurationProfiles"}
		}

		return appConfigRoute{applicationID: appID, operation: "Unknown"}
	}

	profileID := parts[3]

	if len(parts) == pathPartsSubItem {
		return parseProfileIDRoute(method, appID, profileID)
	}

	if len(parts) >= pathPartsDeepLevel && parts[4] == "hostedconfigurationversions" {
		return parseHostedVersionRoute(method, appID, profileID, parts)
	}

	return appConfigRoute{applicationID: appID, profileID: profileID, operation: "Unknown"}
}

func parseProfileIDRoute(method, appID, profileID string) appConfigRoute {
	switch method {
	case http.MethodGet:
		return appConfigRoute{applicationID: appID, profileID: profileID, operation: "GetConfigurationProfile"}
	case http.MethodPatch:
		return appConfigRoute{applicationID: appID, profileID: profileID, operation: "UpdateConfigurationProfile"}
	case http.MethodDelete:
		return appConfigRoute{applicationID: appID, profileID: profileID, operation: "DeleteConfigurationProfile"}
	}

	return appConfigRoute{applicationID: appID, profileID: profileID, operation: "Unknown"}
}

// parseHostedVersionRoute parses hosted configuration version routes.
//
//nolint:dupl // similar structure to parseDeploymentRoute by design; different resource fields
func parseHostedVersionRoute(method, appID, profileID string, parts []string) appConfigRoute {
	if len(parts) == pathPartsDeepLevel {
		switch method {
		case http.MethodPost:
			return appConfigRoute{
				applicationID: appID,
				profileID:     profileID,
				operation:     "CreateHostedConfigurationVersion",
			}
		case http.MethodGet:
			return appConfigRoute{
				applicationID: appID,
				profileID:     profileID,
				operation:     "ListHostedConfigurationVersions",
			}
		}

		return appConfigRoute{applicationID: appID, profileID: profileID, operation: "Unknown"}
	}

	verNum, err := strconv.ParseInt(parts[5], 10, 32)
	if err != nil {
		return appConfigRoute{applicationID: appID, profileID: profileID, operation: "Unknown"}
	}

	num := int32(verNum)

	switch method {
	case http.MethodGet:
		return appConfigRoute{
			applicationID: appID,
			profileID:     profileID,
			versionNumber: num,
			operation:     "GetHostedConfigurationVersion",
		}
	case http.MethodDelete:
		return appConfigRoute{
			applicationID: appID,
			profileID:     profileID,
			versionNumber: num,
			operation:     "DeleteHostedConfigurationVersion",
		}
	}

	return appConfigRoute{applicationID: appID, profileID: profileID, versionNumber: num, operation: "Unknown"}
}

// Handler returns the Echo handler function for AppConfig operations.
//
//nolint:cyclop // dispatch table requires multiple branches
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		log := logger.Load(c.Request().Context())
		route := parseAppConfigPath(c.Request().Method, c.Request().URL.Path)

		switch route.operation {
		case "CreateApplication":
			return h.handleCreateApplication(c)
		case "GetApplication":
			return h.handleGetApplication(c, route.applicationID)
		case "ListApplications":
			return h.handleListApplications(c)
		case "UpdateApplication":
			return h.handleUpdateApplication(c, route.applicationID)
		case "DeleteApplication":
			return h.handleDeleteApplication(c, route.applicationID)
		case "CreateEnvironment":
			return h.handleCreateEnvironment(c, route.applicationID)
		case "GetEnvironment":
			return h.handleGetEnvironment(c, route.applicationID, route.environmentID)
		case "ListEnvironments":
			return h.handleListEnvironments(c, route.applicationID)
		case "UpdateEnvironment":
			return h.handleUpdateEnvironment(c, route.applicationID, route.environmentID)
		case "DeleteEnvironment":
			return h.handleDeleteEnvironment(c, route.applicationID, route.environmentID)
		case "CreateConfigurationProfile":
			return h.handleCreateConfigurationProfile(c, route.applicationID)
		case "GetConfigurationProfile":
			return h.handleGetConfigurationProfile(c, route.applicationID, route.profileID)
		case "ListConfigurationProfiles":
			return h.handleListConfigurationProfiles(c, route.applicationID)
		case "UpdateConfigurationProfile":
			return h.handleUpdateConfigurationProfile(c, route.applicationID, route.profileID)
		case "DeleteConfigurationProfile":
			return h.handleDeleteConfigurationProfile(c, route.applicationID, route.profileID)
		case "CreateHostedConfigurationVersion":
			return h.handleCreateHostedConfigurationVersion(c, route.applicationID, route.profileID)
		case "GetHostedConfigurationVersion":
			return h.handleGetHostedConfigurationVersion(c, route.applicationID, route.profileID, route.versionNumber)
		case "ListHostedConfigurationVersions":
			return h.handleListHostedConfigurationVersions(c, route.applicationID, route.profileID)
		case "DeleteHostedConfigurationVersion":
			return h.handleDeleteHostedConfigurationVersion(
				c,
				route.applicationID,
				route.profileID,
				route.versionNumber,
			)
		case "CreateDeploymentStrategy":
			return h.handleCreateDeploymentStrategy(c)
		case "GetDeploymentStrategy":
			return h.handleGetDeploymentStrategy(c, route.strategyID)
		case "ListDeploymentStrategies":
			return h.handleListDeploymentStrategies(c)
		case "UpdateDeploymentStrategy":
			return h.handleUpdateDeploymentStrategy(c, route.strategyID)
		case "DeleteDeploymentStrategy":
			return h.handleDeleteDeploymentStrategy(c, route.strategyID)
		case "StartDeployment":
			return h.handleStartDeployment(c, route.applicationID, route.environmentID)
		case "GetDeployment":
			return h.handleGetDeployment(c, route.applicationID, route.environmentID, route.deploymentNum)
		case "ListDeployments":
			return h.handleListDeployments(c, route.applicationID, route.environmentID)
		case "StopDeployment":
			return h.handleStopDeployment(c, route.applicationID, route.environmentID, route.deploymentNum)
		default:
			log.Warn("appconfig: unmatched route", "method", c.Request().Method, "path", c.Request().URL.Path)

			return c.JSON(http.StatusNotFound, map[string]string{"message": "not found"})
		}
	}
}

func notFoundResponse(c *echo.Context, err error) error {
	return c.JSON(http.StatusNotFound, map[string]string{"message": err.Error()})
}

func (h *Handler) handleCreateApplication(c *echo.Context) error {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request body"})
	}

	app, err := h.Backend.CreateApplication(req.Name, req.Description)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusCreated, app)
}

func (h *Handler) handleGetApplication(c *echo.Context, applicationID string) error {
	app, err := h.Backend.GetApplication(applicationID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, app)
}

func (h *Handler) handleListApplications(c *echo.Context) error {
	apps := h.Backend.ListApplications()

	return c.JSON(http.StatusOK, map[string]any{"items": apps})
}

func (h *Handler) handleUpdateApplication(c *echo.Context, applicationID string) error {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request body"})
	}

	app, err := h.Backend.UpdateApplication(applicationID, req.Name, req.Description)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, app)
}

func (h *Handler) handleDeleteApplication(c *echo.Context, applicationID string) error {
	if err := h.Backend.DeleteApplication(applicationID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateEnvironment(c *echo.Context, applicationID string) error {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request body"})
	}

	env, err := h.Backend.CreateEnvironment(applicationID, req.Name, req.Description)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusCreated, env)
}

func (h *Handler) handleGetEnvironment(c *echo.Context, applicationID, environmentID string) error {
	env, err := h.Backend.GetEnvironment(applicationID, environmentID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, env)
}

func (h *Handler) handleListEnvironments(c *echo.Context, applicationID string) error {
	envs, err := h.Backend.ListEnvironments(applicationID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{"items": envs})
}

func (h *Handler) handleUpdateEnvironment(c *echo.Context, applicationID, environmentID string) error {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request body"})
	}

	env, err := h.Backend.UpdateEnvironment(applicationID, environmentID, req.Name, req.Description)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, env)
}

func (h *Handler) handleDeleteEnvironment(c *echo.Context, applicationID, environmentID string) error {
	if err := h.Backend.DeleteEnvironment(applicationID, environmentID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateConfigurationProfile(c *echo.Context, applicationID string) error {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
		LocationURI string `json:"LocationUri"`
		Type        string `json:"Type"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request body"})
	}

	profile, err := h.Backend.CreateConfigurationProfile(
		applicationID,
		req.Name,
		req.Description,
		req.LocationURI,
		req.Type,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusCreated, profile)
}

func (h *Handler) handleGetConfigurationProfile(c *echo.Context, applicationID, profileID string) error {
	profile, err := h.Backend.GetConfigurationProfile(applicationID, profileID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, profile)
}

func (h *Handler) handleListConfigurationProfiles(c *echo.Context, applicationID string) error {
	profiles, err := h.Backend.ListConfigurationProfiles(applicationID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{"items": profiles})
}

func (h *Handler) handleUpdateConfigurationProfile(c *echo.Context, applicationID, profileID string) error {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request body"})
	}

	profile, err := h.Backend.UpdateConfigurationProfile(applicationID, profileID, req.Name, req.Description)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, profile)
}

func (h *Handler) handleDeleteConfigurationProfile(c *echo.Context, applicationID, profileID string) error {
	if err := h.Backend.DeleteConfigurationProfile(applicationID, profileID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateHostedConfigurationVersion(c *echo.Context, applicationID, profileID string) error {
	contentType := c.Request().Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	content, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": "failed to read request body"})
	}

	v, err := h.Backend.CreateHostedConfigurationVersion(applicationID, profileID, contentType, content)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	c.Response().Header().Set("Appconfig-Configuration-Version", strconv.Itoa(int(v.VersionNumber)))

	return c.JSON(http.StatusCreated, v)
}

func (h *Handler) handleGetHostedConfigurationVersion(
	c *echo.Context,
	applicationID, profileID string,
	versionNumber int32,
) error {
	v, err := h.Backend.GetHostedConfigurationVersion(applicationID, profileID, versionNumber)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	c.Response().Header().Set("Content-Type", v.ContentType)
	c.Response().Header().Set("Appconfig-Configuration-Version", strconv.Itoa(int(v.VersionNumber)))

	return c.Blob(http.StatusOK, v.ContentType, v.Content)
}

func (h *Handler) handleListHostedConfigurationVersions(c *echo.Context, applicationID, profileID string) error {
	versions, err := h.Backend.ListHostedConfigurationVersions(applicationID, profileID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{"items": versions})
}

func (h *Handler) handleDeleteHostedConfigurationVersion(
	c *echo.Context,
	applicationID, profileID string,
	versionNumber int32,
) error {
	if err := h.Backend.DeleteHostedConfigurationVersion(applicationID, profileID, versionNumber); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateDeploymentStrategy(c *echo.Context) error {
	var req struct {
		Name                        string  `json:"Name"`
		Description                 string  `json:"Description"`
		GrowthType                  string  `json:"GrowthType"`
		ReplicateTo                 string  `json:"ReplicateTo"`
		DeploymentDurationInMinutes int32   `json:"DeploymentDurationInMinutes"`
		FinalBakeTimeInMinutes      int32   `json:"FinalBakeTimeInMinutes"`
		GrowthFactor                float32 `json:"GrowthFactor"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request body"})
	}

	strategy, err := h.Backend.CreateDeploymentStrategy(
		req.Name, req.Description,
		req.DeploymentDurationInMinutes, req.FinalBakeTimeInMinutes,
		req.GrowthFactor, req.GrowthType, req.ReplicateTo,
	)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusCreated, strategy)
}

func (h *Handler) handleGetDeploymentStrategy(c *echo.Context, strategyID string) error {
	strategy, err := h.Backend.GetDeploymentStrategy(strategyID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, strategy)
}

func (h *Handler) handleListDeploymentStrategies(c *echo.Context) error {
	strategies := h.Backend.ListDeploymentStrategies()

	return c.JSON(http.StatusOK, map[string]any{"items": strategies})
}

func (h *Handler) handleUpdateDeploymentStrategy(c *echo.Context, strategyID string) error {
	var req struct {
		DeploymentDurationInMinutes *int32   `json:"DeploymentDurationInMinutes"`
		FinalBakeTimeInMinutes      *int32   `json:"FinalBakeTimeInMinutes"`
		GrowthFactor                *float32 `json:"GrowthFactor"`
		Name                        string   `json:"Name"`
		Description                 string   `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request body"})
	}

	// Fetch current values to use as defaults for omitted pointer fields.
	existing, err := h.Backend.GetDeploymentStrategy(strategyID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	deployDur := existing.DeploymentDurationInMinutes
	if req.DeploymentDurationInMinutes != nil {
		deployDur = *req.DeploymentDurationInMinutes
	}

	bakeTime := existing.FinalBakeTimeInMinutes
	if req.FinalBakeTimeInMinutes != nil {
		bakeTime = *req.FinalBakeTimeInMinutes
	}

	growthFactor := existing.GrowthFactor
	if req.GrowthFactor != nil {
		growthFactor = *req.GrowthFactor
	}

	strategy, err := h.Backend.UpdateDeploymentStrategy(
		strategyID, req.Name, req.Description,
		deployDur, bakeTime,
		growthFactor,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, strategy)
}

func (h *Handler) handleDeleteDeploymentStrategy(c *echo.Context, strategyID string) error {
	if err := h.Backend.DeleteDeploymentStrategy(strategyID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleStartDeployment(c *echo.Context, applicationID, environmentID string) error {
	var req struct {
		ConfigurationProfileID string `json:"ConfigurationProfileId"`
		DeploymentStrategyID   string `json:"DeploymentStrategyId"`
		ConfigurationVersion   string `json:"ConfigurationVersion"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request body"})
	}

	deployment, err := h.Backend.StartDeployment(
		applicationID, environmentID,
		req.ConfigurationProfileID, req.DeploymentStrategyID,
		req.ConfigurationVersion,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusCreated, deployment)
}

func (h *Handler) handleGetDeployment(
	c *echo.Context,
	applicationID, environmentID string,
	deploymentNumber int32,
) error {
	deployment, err := h.Backend.GetDeployment(applicationID, environmentID, deploymentNumber)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, deployment)
}

func (h *Handler) handleListDeployments(c *echo.Context, applicationID, environmentID string) error {
	deployments, err := h.Backend.ListDeployments(applicationID, environmentID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{"items": deployments})
}

func (h *Handler) handleStopDeployment(
	c *echo.Context,
	applicationID, environmentID string,
	deploymentNumber int32,
) error {
	if err := h.Backend.StopDeployment(applicationID, environmentID, deploymentNumber); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.NoContent(http.StatusNoContent)
}
