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
	keyMessageField       = "message"
	errInvalidRequestBody = "invalid request body"
)

const (
	opUnknown = "Unknown"
	keyItems  = "Items"
)

const (
	opCreateApplication                = "CreateApplication"
	opCreateConfigurationProfile       = "CreateConfigurationProfile"
	opCreateDeploymentStrategy         = "CreateDeploymentStrategy"
	opCreateEnvironment                = "CreateEnvironment"
	opCreateExtension                  = "CreateExtension"
	opCreateExtensionAssociation       = "CreateExtensionAssociation"
	opCreateHostedConfigurationVersion = "CreateHostedConfigurationVersion"
	opDeleteApplication                = "DeleteApplication"
	opDeleteConfigurationProfile       = "DeleteConfigurationProfile"
	opDeleteDeploymentStrategy         = "DeleteDeploymentStrategy"
	opDeleteEnvironment                = "DeleteEnvironment"
	opDeleteExtension                  = "DeleteExtension"
	opDeleteExtensionAssociation       = "DeleteExtensionAssociation"
	opDeleteHostedConfigurationVersion = "DeleteHostedConfigurationVersion"
	opGetAccountSettings               = "GetAccountSettings"
	opGetApplication                   = "GetApplication"
	opGetConfiguration                 = "GetConfiguration"
	opGetConfigurationProfile          = "GetConfigurationProfile"
	opGetDeployment                    = "GetDeployment"
	opGetDeploymentStrategy            = "GetDeploymentStrategy"
	opGetEnvironment                   = "GetEnvironment"
	opGetExtension                     = "GetExtension"
	opGetExtensionAssociation          = "GetExtensionAssociation"
	opGetHostedConfigurationVersion    = "GetHostedConfigurationVersion"
	opListApplications                 = "ListApplications"
	opListConfigurationProfiles        = "ListConfigurationProfiles"
	opListDeploymentStrategies         = "ListDeploymentStrategies"
	opListDeployments                  = "ListDeployments"
	opListEnvironments                 = "ListEnvironments"
	opListExtensionAssociations        = "ListExtensionAssociations"
	opListExtensions                   = "ListExtensions"
	opListHostedConfigurationVersions  = "ListHostedConfigurationVersions"
	opListTagsForResource              = "ListTagsForResource"
	opStartDeployment                  = "StartDeployment"
	opStopDeployment                   = "StopDeployment"
	opTagResource                      = "TagResource"
	opUntagResource                    = "UntagResource"
	opUpdateAccountSettings            = "UpdateAccountSettings"
	opUpdateApplication                = "UpdateApplication"
	opUpdateConfigurationProfile       = "UpdateConfigurationProfile"
	opUpdateDeploymentStrategy         = "UpdateDeploymentStrategy"
	opUpdateEnvironment                = "UpdateEnvironment"
	opUpdateExtension                  = "UpdateExtension"
	opUpdateExtensionAssociation       = "UpdateExtensionAssociation"
	opValidateConfiguration            = "ValidateConfiguration"
)

const (
	appConfigMatchPriority = 86

	// pathParts* constants define the expected segment counts for route matching.
	pathPartsBase      = 2 // /resource/{id}
	pathPartsSubLevel  = 3 // /applications/{id}/subresource
	pathPartsSubItem   = 4 // /applications/{id}/subresource/{subId}
	pathPartsDeepLevel = 5 // /applications/{id}/subresource/{subId}/nested
	pathPartsDeepItem  = 6 // /applications/{id}/subresource/{subId}/nested/{nestedId}

	// maxHostedConfigurationVersionBytes caps the request-body size accepted by
	// CreateHostedConfigurationVersion. AWS AppConfig allows hosted configuration
	// versions up to 1 MiB; clamp here to prevent unbounded io.ReadAll DoS.
	maxHostedConfigurationVersionBytes = 1 << 20 // 1 MiB
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
		opCreateApplication,
		opGetApplication,
		opListApplications,
		opUpdateApplication,
		opDeleteApplication,
		opCreateEnvironment,
		opGetEnvironment,
		opListEnvironments,
		opUpdateEnvironment,
		opDeleteEnvironment,
		opCreateConfigurationProfile,
		opGetConfigurationProfile,
		opListConfigurationProfiles,
		opUpdateConfigurationProfile,
		opDeleteConfigurationProfile,
		opCreateHostedConfigurationVersion,
		opGetHostedConfigurationVersion,
		opListHostedConfigurationVersions,
		opDeleteHostedConfigurationVersion,
		opCreateDeploymentStrategy,
		opGetDeploymentStrategy,
		opListDeploymentStrategies,
		opUpdateDeploymentStrategy,
		opDeleteDeploymentStrategy,
		opStartDeployment,
		opGetDeployment,
		opListDeployments,
		opStopDeployment,
		opListTagsForResource,
		opTagResource,
		opUntagResource,
		opCreateExtension,
		opGetExtension,
		opListExtensions,
		opUpdateExtension,
		opDeleteExtension,
		opCreateExtensionAssociation,
		opGetExtensionAssociation,
		opListExtensionAssociations,
		opUpdateExtensionAssociation,
		opDeleteExtensionAssociation,
		opGetAccountSettings,
		opUpdateAccountSettings,
		opGetConfiguration,
		opValidateConfiguration,
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

		return strings.HasPrefix(path, "/applications") ||
			strings.HasPrefix(path, "/deploymentstrategies") ||
			strings.HasPrefix(path, "/extensions") ||
			strings.HasPrefix(path, "/extensionassociations") ||
			path == "/settings" ||
			strings.HasPrefix(path, "/tags/arn:aws:appconfig:")
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
	applicationID          string
	environmentID          string
	profileID              string
	strategyID             string
	resourceArn            string
	extensionID            string
	extensionAssociationID string
	configurationID        string
	operation              string
	versionNumber          int32
	deploymentNum          int32
}

// parseAppConfigPath parses an HTTP method and URL path into an appConfigRoute,
// identifying the resource type, IDs, and operation name for AppConfig REST API requests.
// It maps REST path segments to their corresponding CRUD operations.
func parseAppConfigPath(method, path string) appConfigRoute {
	// Trim leading slash and split.
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")

	if len(parts) == 0 {
		return appConfigRoute{operation: opUnknown}
	}

	switch parts[0] {
	case "deploymentstrategies":
		return parseDeploymentStrategyRoute(method, parts)
	case "applications":
		return parseApplicationRoute(method, parts)
	case "extensions":
		return parseExtensionRoute(method, parts)
	case "extensionassociations":
		return parseExtensionAssociationRoute(method, parts)
	case "settings":
		if len(parts) == 1 {
			switch method {
			case http.MethodGet:
				return appConfigRoute{operation: opGetAccountSettings}
			case http.MethodPatch:
				return appConfigRoute{operation: opUpdateAccountSettings}
			}
		}
	case "tags":
		// ARN spans all remaining path segments joined by "/"
		return parseTagRoute(method, strings.Join(parts[1:], "/"))
	}

	return appConfigRoute{operation: opUnknown}
}

func parseTagRoute(method, resourceArn string) appConfigRoute {
	base := appConfigRoute{resourceArn: resourceArn}

	switch method {
	case http.MethodGet:
		base.operation = opListTagsForResource
	case http.MethodPost:
		base.operation = opTagResource
	case http.MethodDelete:
		base.operation = opUntagResource
	default:
		base.operation = opUnknown
	}

	return base
}

func parseExtensionRoute(method string, parts []string) appConfigRoute {
	if len(parts) == 1 {
		switch method {
		case http.MethodPost:
			return appConfigRoute{operation: opCreateExtension}
		case http.MethodGet:
			return appConfigRoute{operation: opListExtensions}
		}

		return appConfigRoute{operation: opUnknown}
	}

	extID := parts[1]

	switch method {
	case http.MethodGet:
		return appConfigRoute{extensionID: extID, operation: opGetExtension}
	case http.MethodPatch:
		return appConfigRoute{extensionID: extID, operation: opUpdateExtension}
	case http.MethodDelete:
		return appConfigRoute{extensionID: extID, operation: opDeleteExtension}
	}

	return appConfigRoute{extensionID: extID, operation: opUnknown}
}

func parseExtensionAssociationRoute(method string, parts []string) appConfigRoute {
	if len(parts) == 1 {
		switch method {
		case http.MethodPost:
			return appConfigRoute{operation: opCreateExtensionAssociation}
		case http.MethodGet:
			return appConfigRoute{operation: opListExtensionAssociations}
		}

		return appConfigRoute{operation: opUnknown}
	}

	assocID := parts[1]

	switch method {
	case http.MethodGet:
		return appConfigRoute{extensionAssociationID: assocID, operation: opGetExtensionAssociation}
	case http.MethodPatch:
		return appConfigRoute{
			extensionAssociationID: assocID,
			operation:              opUpdateExtensionAssociation,
		}
	case http.MethodDelete:
		return appConfigRoute{
			extensionAssociationID: assocID,
			operation:              opDeleteExtensionAssociation,
		}
	}

	return appConfigRoute{extensionAssociationID: assocID, operation: opUnknown}
}

func parseDeploymentStrategyRoute(method string, parts []string) appConfigRoute {
	if len(parts) == 1 {
		switch method {
		case http.MethodPost:
			return appConfigRoute{operation: opCreateDeploymentStrategy}
		case http.MethodGet:
			return appConfigRoute{operation: opListDeploymentStrategies}
		}

		return appConfigRoute{operation: opUnknown}
	}

	strategyID := parts[1]

	switch method {
	case http.MethodGet:
		return appConfigRoute{strategyID: strategyID, operation: opGetDeploymentStrategy}
	case http.MethodPatch:
		return appConfigRoute{strategyID: strategyID, operation: opUpdateDeploymentStrategy}
	case http.MethodDelete:
		return appConfigRoute{strategyID: strategyID, operation: opDeleteDeploymentStrategy}
	}

	return appConfigRoute{strategyID: strategyID, operation: opUnknown}
}

// parseApplicationRoute parses routes starting with /applications.
//

func parseApplicationRoute(method string, parts []string) appConfigRoute {
	if len(parts) == 1 {
		switch method {
		case http.MethodPost:
			return appConfigRoute{operation: opCreateApplication}
		case http.MethodGet:
			return appConfigRoute{operation: opListApplications}
		}

		return appConfigRoute{operation: opUnknown}
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

	return appConfigRoute{applicationID: appID, operation: opUnknown}
}

func parseAppIDRoute(method, appID string) appConfigRoute {
	switch method {
	case http.MethodGet:
		return appConfigRoute{applicationID: appID, operation: opGetApplication}
	case http.MethodPatch:
		return appConfigRoute{applicationID: appID, operation: opUpdateApplication}
	case http.MethodDelete:
		return appConfigRoute{applicationID: appID, operation: opDeleteApplication}
	}

	return appConfigRoute{applicationID: appID, operation: opUnknown}
}

func parseEnvironmentRoute(method, appID string, parts []string) appConfigRoute {
	if len(parts) == pathPartsSubLevel {
		switch method {
		case http.MethodPost:
			return appConfigRoute{applicationID: appID, operation: opCreateEnvironment}
		case http.MethodGet:
			return appConfigRoute{applicationID: appID, operation: opListEnvironments}
		}

		return appConfigRoute{applicationID: appID, operation: opUnknown}
	}

	envID := parts[3]

	if len(parts) == pathPartsSubItem {
		return parseEnvIDRoute(method, appID, envID)
	}

	if len(parts) >= pathPartsDeepLevel && parts[4] == "deployments" {
		return parseDeploymentRoute(method, appID, envID, parts)
	}

	if len(parts) == pathPartsDeepItem && parts[4] == "configurations" && method == http.MethodGet {
		return appConfigRoute{
			applicationID:   appID,
			environmentID:   envID,
			configurationID: parts[5],
			operation:       opGetConfiguration,
		}
	}

	return appConfigRoute{applicationID: appID, environmentID: envID, operation: opUnknown}
}

func parseEnvIDRoute(method, appID, envID string) appConfigRoute {
	switch method {
	case http.MethodGet:
		return appConfigRoute{
			applicationID: appID,
			environmentID: envID,
			operation:     opGetEnvironment,
		}
	case http.MethodPatch:
		return appConfigRoute{
			applicationID: appID,
			environmentID: envID,
			operation:     opUpdateEnvironment,
		}
	case http.MethodDelete:
		return appConfigRoute{
			applicationID: appID,
			environmentID: envID,
			operation:     opDeleteEnvironment,
		}
	}

	return appConfigRoute{applicationID: appID, environmentID: envID, operation: opUnknown}
}

// parseDeploymentRoute parses deployment routes under /environments/{envId}/deployments.
//
//nolint:dupl // similar structure to parseHostedVersionRoute by design; different resource fields
func parseDeploymentRoute(method, appID, envID string, parts []string) appConfigRoute {
	if len(parts) == pathPartsDeepLevel {
		switch method {
		case http.MethodPost:
			return appConfigRoute{
				applicationID: appID,
				environmentID: envID,
				operation:     opStartDeployment,
			}
		case http.MethodGet:
			return appConfigRoute{
				applicationID: appID,
				environmentID: envID,
				operation:     opListDeployments,
			}
		}

		return appConfigRoute{applicationID: appID, environmentID: envID, operation: opUnknown}
	}

	depNum, err := strconv.ParseInt(parts[5], 10, 32)
	if err != nil {
		return appConfigRoute{applicationID: appID, environmentID: envID, operation: opUnknown}
	}

	num := int32(depNum)

	switch method {
	case http.MethodGet:
		return appConfigRoute{
			applicationID: appID,
			environmentID: envID,
			deploymentNum: num,
			operation:     opGetDeployment,
		}
	case http.MethodDelete:
		return appConfigRoute{
			applicationID: appID,
			environmentID: envID,
			deploymentNum: num,
			operation:     opStopDeployment,
		}
	}

	return appConfigRoute{
		applicationID: appID,
		environmentID: envID,
		deploymentNum: num,
		operation:     opUnknown,
	}
}

// parseConfigProfileRoute parses configuration profile routes.
//

func parseConfigProfileRoute(method, appID string, parts []string) appConfigRoute {
	if len(parts) == pathPartsSubLevel {
		switch method {
		case http.MethodPost:
			return appConfigRoute{applicationID: appID, operation: opCreateConfigurationProfile}
		case http.MethodGet:
			return appConfigRoute{applicationID: appID, operation: opListConfigurationProfiles}
		}

		return appConfigRoute{applicationID: appID, operation: opUnknown}
	}

	profileID := parts[3]

	if len(parts) == pathPartsSubItem {
		return parseProfileIDRoute(method, appID, profileID)
	}

	if len(parts) >= pathPartsDeepLevel && parts[4] == "hostedconfigurationversions" {
		return parseHostedVersionRoute(method, appID, profileID, parts)
	}

	if len(parts) == pathPartsDeepLevel && parts[4] == "validators" && method == http.MethodPost {
		return appConfigRoute{
			applicationID: appID,
			profileID:     profileID,
			operation:     opValidateConfiguration,
		}
	}

	return appConfigRoute{applicationID: appID, profileID: profileID, operation: opUnknown}
}

func parseProfileIDRoute(method, appID, profileID string) appConfigRoute {
	switch method {
	case http.MethodGet:
		return appConfigRoute{
			applicationID: appID,
			profileID:     profileID,
			operation:     opGetConfigurationProfile,
		}
	case http.MethodPatch:
		return appConfigRoute{
			applicationID: appID,
			profileID:     profileID,
			operation:     opUpdateConfigurationProfile,
		}
	case http.MethodDelete:
		return appConfigRoute{
			applicationID: appID,
			profileID:     profileID,
			operation:     opDeleteConfigurationProfile,
		}
	}

	return appConfigRoute{applicationID: appID, profileID: profileID, operation: opUnknown}
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
				operation:     opCreateHostedConfigurationVersion,
			}
		case http.MethodGet:
			return appConfigRoute{
				applicationID: appID,
				profileID:     profileID,
				operation:     opListHostedConfigurationVersions,
			}
		}

		return appConfigRoute{applicationID: appID, profileID: profileID, operation: opUnknown}
	}

	verNum, err := strconv.ParseInt(parts[5], 10, 32)
	if err != nil {
		return appConfigRoute{applicationID: appID, profileID: profileID, operation: opUnknown}
	}

	num := int32(verNum)

	switch method {
	case http.MethodGet:
		return appConfigRoute{
			applicationID: appID,
			profileID:     profileID,
			versionNumber: num,
			operation:     opGetHostedConfigurationVersion,
		}
	case http.MethodDelete:
		return appConfigRoute{
			applicationID: appID,
			profileID:     profileID,
			versionNumber: num,
			operation:     opDeleteHostedConfigurationVersion,
		}
	}

	return appConfigRoute{
		applicationID: appID,
		profileID:     profileID,
		versionNumber: num,
		operation:     opUnknown,
	}
}

// appConfigDispatchFn is the function signature for AppConfig dispatch handlers.
type appConfigDispatchFn func(*Handler, *echo.Context, appConfigRoute) error

// appConfigDispatch maps operation names to their handler wrappers.
//
//nolint:gochecknoglobals // read-only dispatch table initialized once at startup
var appConfigDispatch = map[string]appConfigDispatchFn{
	opCreateApplication: func(h *Handler, c *echo.Context, _ appConfigRoute) error {
		return h.handleCreateApplication(c)
	},
	opGetApplication: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleGetApplication(c, r.applicationID)
	},
	opListApplications: func(h *Handler, c *echo.Context, _ appConfigRoute) error {
		return h.handleListApplications(c)
	},
	opUpdateApplication: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleUpdateApplication(c, r.applicationID)
	},
	opDeleteApplication: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleDeleteApplication(c, r.applicationID)
	},
	opCreateEnvironment: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleCreateEnvironment(c, r.applicationID)
	},
	opGetEnvironment: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleGetEnvironment(c, r.applicationID, r.environmentID)
	},
	opListEnvironments: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleListEnvironments(c, r.applicationID)
	},
	opUpdateEnvironment: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleUpdateEnvironment(c, r.applicationID, r.environmentID)
	},
	opDeleteEnvironment: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleDeleteEnvironment(c, r.applicationID, r.environmentID)
	},
	opCreateConfigurationProfile: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleCreateConfigurationProfile(c, r.applicationID)
	},
	opGetConfigurationProfile: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleGetConfigurationProfile(c, r.applicationID, r.profileID)
	},
	opListConfigurationProfiles: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleListConfigurationProfiles(c, r.applicationID)
	},
	opUpdateConfigurationProfile: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleUpdateConfigurationProfile(c, r.applicationID, r.profileID)
	},
	opDeleteConfigurationProfile: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleDeleteConfigurationProfile(c, r.applicationID, r.profileID)
	},
	opCreateHostedConfigurationVersion: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleCreateHostedConfigurationVersion(c, r.applicationID, r.profileID)
	},
	opGetHostedConfigurationVersion: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleGetHostedConfigurationVersion(c, r.applicationID, r.profileID, r.versionNumber)
	},
	opListHostedConfigurationVersions: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleListHostedConfigurationVersions(c, r.applicationID, r.profileID)
	},
	opDeleteHostedConfigurationVersion: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleDeleteHostedConfigurationVersion(c, r.applicationID, r.profileID, r.versionNumber)
	},
	opCreateDeploymentStrategy: func(h *Handler, c *echo.Context, _ appConfigRoute) error {
		return h.handleCreateDeploymentStrategy(c)
	},
	opGetDeploymentStrategy: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleGetDeploymentStrategy(c, r.strategyID)
	},
	opListDeploymentStrategies: func(h *Handler, c *echo.Context, _ appConfigRoute) error {
		return h.handleListDeploymentStrategies(c)
	},
	opUpdateDeploymentStrategy: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleUpdateDeploymentStrategy(c, r.strategyID)
	},
	opDeleteDeploymentStrategy: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleDeleteDeploymentStrategy(c, r.strategyID)
	},
	opStartDeployment: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleStartDeployment(c, r.applicationID, r.environmentID)
	},
	opGetDeployment: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleGetDeployment(c, r.applicationID, r.environmentID, r.deploymentNum)
	},
	opListDeployments: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleListDeployments(c, r.applicationID, r.environmentID)
	},
	opStopDeployment: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleStopDeployment(c, r.applicationID, r.environmentID, r.deploymentNum)
	},
	opListTagsForResource: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleListTagsForResource(c, r.resourceArn)
	},
	opTagResource: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleTagResource(c, r.resourceArn)
	},
	opUntagResource: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleUntagResource(c, r.resourceArn)
	},
	opCreateExtension: func(h *Handler, c *echo.Context, _ appConfigRoute) error {
		return h.handleCreateExtension(c)
	},
	opGetExtension: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleGetExtension(c, r.extensionID)
	},
	opListExtensions: func(h *Handler, c *echo.Context, _ appConfigRoute) error {
		return h.handleListExtensions(c)
	},
	opUpdateExtension: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleUpdateExtension(c, r.extensionID)
	},
	opDeleteExtension: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleDeleteExtension(c, r.extensionID)
	},
	opCreateExtensionAssociation: func(h *Handler, c *echo.Context, _ appConfigRoute) error {
		return h.handleCreateExtensionAssociation(c)
	},
	opGetExtensionAssociation: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleGetExtensionAssociation(c, r.extensionAssociationID)
	},
	opListExtensionAssociations: func(h *Handler, c *echo.Context, _ appConfigRoute) error {
		return h.handleListExtensionAssociations(c)
	},
	opUpdateExtensionAssociation: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleUpdateExtensionAssociation(c, r.extensionAssociationID)
	},
	opDeleteExtensionAssociation: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleDeleteExtensionAssociation(c, r.extensionAssociationID)
	},
	opGetAccountSettings: func(h *Handler, c *echo.Context, _ appConfigRoute) error {
		return h.handleGetAccountSettings(c)
	},
	opUpdateAccountSettings: func(h *Handler, c *echo.Context, _ appConfigRoute) error {
		return h.handleUpdateAccountSettings(c)
	},
	opGetConfiguration: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleGetConfiguration(c, r.applicationID, r.environmentID, r.configurationID)
	},
	opValidateConfiguration: func(h *Handler, c *echo.Context, r appConfigRoute) error {
		return h.handleValidateConfiguration(c, r.applicationID, r.profileID)
	},
}

// Handler returns the Echo handler function for AppConfig operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		log := logger.Load(c.Request().Context())
		route := parseAppConfigPath(c.Request().Method, c.Request().URL.Path)

		if fn, ok := appConfigDispatch[route.operation]; ok {
			return fn(h, c, route)
		}

		log.Warn(
			"appconfig: unmatched route",
			"method",
			c.Request().Method,
			"path",
			c.Request().URL.Path,
		)

		return c.JSON(http.StatusNotFound, map[string]string{keyMessageField: "not found"})
	}
}
func notFoundResponse(c *echo.Context, err error) error {
	return c.JSON(http.StatusNotFound, map[string]string{keyMessageField: err.Error()})
}

func badRequestResponse(c *echo.Context, err error) error {
	return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: err.Error()})
}

func conflictResponse(c *echo.Context, err error) error {
	return c.JSON(http.StatusConflict, map[string]string{keyMessageField: err.Error()})
}

func (h *Handler) handleCreateApplication(c *echo.Context) error {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	app, err := h.Backend.CreateApplication(req.Name, req.Description)
	if err != nil {
		if errors.Is(err, awserr.ErrInvalidParameter) {
			return badRequestResponse(c, err)
		}

		if errors.Is(err, awserr.ErrAlreadyExists) {
			return conflictResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusCreated, app)
}

func (h *Handler) handleGetApplication(c *echo.Context, applicationID string) error {
	app, err := h.Backend.GetApplication(applicationID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, app)
}

func (h *Handler) handleListApplications(c *echo.Context) error {
	nextToken, maxResults := appConfigPaginationParams(c)
	apps, outToken := h.Backend.ListApplications(nextToken, maxResults)

	resp := map[string]any{keyItems: apps}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateApplication(c *echo.Context, applicationID string) error {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	app, err := h.Backend.UpdateApplication(applicationID, req.Name, req.Description)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, app)
}

func (h *Handler) handleDeleteApplication(c *echo.Context, applicationID string) error {
	if err := h.Backend.DeleteApplication(applicationID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateEnvironment(c *echo.Context, applicationID string) error {
	var req struct {
		Name        string    `json:"Name"`
		Description string    `json:"Description"`
		Monitors    []Monitor `json:"Monitors"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	env, err := h.Backend.CreateEnvironment(applicationID, req.Name, req.Description, req.Monitors)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		if errors.Is(err, awserr.ErrAlreadyExists) {
			return conflictResponse(c, err)
		}

		if errors.Is(err, awserr.ErrInvalidParameter) {
			return badRequestResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusCreated, env)
}

func (h *Handler) handleGetEnvironment(c *echo.Context, applicationID, environmentID string) error {
	env, err := h.Backend.GetEnvironment(applicationID, environmentID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, env)
}

func (h *Handler) handleListEnvironments(c *echo.Context, applicationID string) error {
	nextToken, maxResults := appConfigPaginationParams(c)
	envs, outToken, err := h.Backend.ListEnvironments(applicationID, nextToken, maxResults)

	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	resp := map[string]any{keyItems: envs}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateEnvironment(
	c *echo.Context,
	applicationID, environmentID string,
) error {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	env, err := h.Backend.UpdateEnvironment(applicationID, environmentID, req.Name, req.Description)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, env)
}

func (h *Handler) handleDeleteEnvironment(
	c *echo.Context,
	applicationID, environmentID string,
) error {
	if err := h.Backend.DeleteEnvironment(applicationID, environmentID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateConfigurationProfile(c *echo.Context, applicationID string) error {
	var req struct {
		Name             string      `json:"Name"`
		Description      string      `json:"Description"`
		LocationURI      string      `json:"LocationUri"`
		Type             string      `json:"Type"`
		RetrievalRoleArn string      `json:"RetrievalRoleArn"`
		Validators       []Validator `json:"Validators"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	profile, err := h.Backend.CreateConfigurationProfile(
		applicationID,
		req.Name,
		req.Description,
		req.LocationURI,
		req.Type,
		req.RetrievalRoleArn,
		req.Validators,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		if errors.Is(err, awserr.ErrAlreadyExists) {
			return conflictResponse(c, err)
		}

		if errors.Is(err, awserr.ErrInvalidParameter) {
			return badRequestResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusCreated, profile)
}

func (h *Handler) handleGetConfigurationProfile(
	c *echo.Context,
	applicationID, profileID string,
) error {
	profile, err := h.Backend.GetConfigurationProfile(applicationID, profileID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, profile)
}

func (h *Handler) handleListConfigurationProfiles(c *echo.Context, applicationID string) error {
	nextToken, maxResults := appConfigPaginationParams(c)
	profiles, outToken, err := h.Backend.ListConfigurationProfiles(
		applicationID,
		nextToken,
		maxResults,
	)

	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	resp := map[string]any{keyItems: profiles}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateConfigurationProfile(
	c *echo.Context,
	applicationID, profileID string,
) error {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	profile, err := h.Backend.UpdateConfigurationProfile(
		applicationID,
		profileID,
		req.Name,
		req.Description,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, profile)
}

func (h *Handler) handleDeleteConfigurationProfile(
	c *echo.Context,
	applicationID, profileID string,
) error {
	if err := h.Backend.DeleteConfigurationProfile(applicationID, profileID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateHostedConfigurationVersion(
	c *echo.Context,
	applicationID, profileID string,
) error {
	contentType := c.Request().Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// AWS AppConfig accepts description and version label via custom request headers.
	description := c.Request().Header.Get("Description")
	versionLabel := c.Request().Header.Get("Versionlabel")

	content, err := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, maxHostedConfigurationVersionBytes))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			return c.JSON(
				http.StatusRequestEntityTooLarge,
				map[string]string{keyMessageField: "configuration content exceeds maximum size of 1 MiB"},
			)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: "failed to read request body"},
		)
	}

	v, err := h.Backend.CreateHostedConfigurationVersion(
		applicationID,
		profileID,
		contentType,
		description,
		versionLabel,
		content,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		if errors.Is(err, awserr.ErrAlreadyExists) {
			return conflictResponse(c, err)
		}

		if errors.Is(err, awserr.ErrInvalidParameter) {
			return badRequestResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
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

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	c.Response().Header().Set("Content-Type", v.ContentType)
	c.Response().Header().Set("Appconfig-Configuration-Version", strconv.Itoa(int(v.VersionNumber)))

	return c.Blob(http.StatusOK, v.ContentType, v.Content)
}

func (h *Handler) handleListHostedConfigurationVersions(
	c *echo.Context,
	applicationID, profileID string,
) error {
	nextToken, maxResults := appConfigPaginationParams(c)
	versionLabel := c.Request().URL.Query().Get("version_label")
	versions, outToken, err := h.Backend.ListHostedConfigurationVersions(
		applicationID,
		profileID,
		nextToken,
		versionLabel,
		maxResults,
	)

	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	resp := map[string]any{keyItems: versions}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
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

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
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
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	strategy, err := h.Backend.CreateDeploymentStrategy(
		req.Name, req.Description,
		req.DeploymentDurationInMinutes, req.FinalBakeTimeInMinutes,
		req.GrowthFactor, req.GrowthType, req.ReplicateTo,
	)
	if err != nil {
		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusCreated, strategy)
}

func (h *Handler) handleGetDeploymentStrategy(c *echo.Context, strategyID string) error {
	strategy, err := h.Backend.GetDeploymentStrategy(strategyID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, strategy)
}

func (h *Handler) handleListDeploymentStrategies(c *echo.Context) error {
	nextToken, maxResults := appConfigPaginationParams(c)
	strategies, outToken := h.Backend.ListDeploymentStrategies(nextToken, maxResults)

	resp := map[string]any{keyItems: strategies}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
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
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	// Fetch current values to use as defaults for omitted pointer fields.
	existing, err := h.Backend.GetDeploymentStrategy(strategyID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
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

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, strategy)
}

func (h *Handler) handleDeleteDeploymentStrategy(c *echo.Context, strategyID string) error {
	if err := h.Backend.DeleteDeploymentStrategy(strategyID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleStartDeployment(
	c *echo.Context,
	applicationID, environmentID string,
) error {
	var req struct {
		ConfigurationProfileID string `json:"ConfigurationProfileId"`
		DeploymentStrategyID   string `json:"DeploymentStrategyId"`
		ConfigurationVersion   string `json:"ConfigurationVersion"`
		Description            string `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	deployment, err := h.Backend.StartDeployment(
		applicationID, environmentID,
		req.ConfigurationProfileID, req.DeploymentStrategyID,
		req.ConfigurationVersion, req.Description,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		if errors.Is(err, awserr.ErrInvalidParameter) {
			return badRequestResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
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

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, deployment)
}

func (h *Handler) handleListDeployments(
	c *echo.Context,
	applicationID, environmentID string,
) error {
	nextToken, maxResults := appConfigPaginationParams(c)
	deployments, outToken, err := h.Backend.ListDeployments(
		applicationID,
		environmentID,
		nextToken,
		maxResults,
	)

	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	resp := map[string]any{keyItems: deployments}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
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

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceArn string) error {
	tags, err := h.Backend.ListTagsForResource(resourceArn)
	if err != nil {
		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, map[string]any{"Tags": tags})
}

func (h *Handler) handleTagResource(c *echo.Context, resourceArn string) error {
	var req struct {
		Tags map[string]string `json:"Tags"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	if err := h.Backend.TagResource(resourceArn, req.Tags); err != nil {
		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceArn string) error {
	keysToRemove := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceArn, keysToRemove); err != nil {
		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateExtension(c *echo.Context) error {
	var req struct {
		Actions     map[string][]ExtensionAction  `json:"Actions"`
		Parameters  map[string]ExtensionParameter `json:"Parameters"`
		Name        string                        `json:"Name"`
		Description string                        `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	ext, err := h.Backend.CreateExtension(req.Name, req.Description, req.Actions, req.Parameters)
	if err != nil {
		if errors.Is(err, awserr.ErrInvalidParameter) {
			return badRequestResponse(c, err)
		}

		if errors.Is(err, awserr.ErrAlreadyExists) {
			return conflictResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusCreated, ext)
}

func (h *Handler) handleGetExtension(c *echo.Context, extensionID string) error {
	ext, err := h.Backend.GetExtension(extensionID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, ext)
}

func (h *Handler) handleListExtensions(c *echo.Context) error {
	nextToken, maxResults := appConfigPaginationParams(c)
	q := c.Request().URL.Query()
	nameFilter := q.Get("name")
	var versionNumber int32
	if s := q.Get("extension_version_number"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			versionNumber = int32(n) //nolint:gosec // version number bounded by API constraints
		}
	}
	exts, outToken := h.Backend.ListExtensions(nextToken, maxResults, nameFilter, versionNumber)

	resp := map[string]any{keyItems: exts}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateExtension(c *echo.Context, extensionID string) error {
	var req struct {
		Actions     map[string][]ExtensionAction  `json:"Actions"`
		Parameters  map[string]ExtensionParameter `json:"Parameters"`
		Description string                        `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	ext, err := h.Backend.UpdateExtension(extensionID, req.Description, req.Actions, req.Parameters)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, ext)
}

func (h *Handler) handleDeleteExtension(c *echo.Context, extensionID string) error {
	if err := h.Backend.DeleteExtension(extensionID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateExtensionAssociation(c *echo.Context) error {
	var req struct {
		Parameters             map[string]string `json:"Parameters"`
		ExtensionVersionNumber *int32            `json:"ExtensionVersionNumber"`
		ExtensionIdentifier    string            `json:"ExtensionIdentifier"`
		ResourceIdentifier     string            `json:"ResourceIdentifier"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	assoc, err := h.Backend.CreateExtensionAssociation(
		req.ExtensionIdentifier,
		req.ResourceIdentifier,
		req.Parameters,
		req.ExtensionVersionNumber,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrInvalidParameter) {
			return badRequestResponse(c, err)
		}

		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusCreated, assoc)
}

func (h *Handler) handleGetExtensionAssociation(
	c *echo.Context,
	extensionAssociationID string,
) error {
	assoc, err := h.Backend.GetExtensionAssociation(extensionAssociationID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, assoc)
}

func (h *Handler) handleListExtensionAssociations(c *echo.Context) error {
	nextToken, maxResults := appConfigPaginationParams(c)
	q := c.Request().URL.Query()
	extIdentifier := q.Get("extension_identifier")
	resourceIdentifier := q.Get("resource_identifier")
	assocs, outToken := h.Backend.ListExtensionAssociations(
		nextToken,
		extIdentifier,
		resourceIdentifier,
		maxResults,
	)

	resp := map[string]any{keyItems: assocs}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateExtensionAssociation(
	c *echo.Context,
	extensionAssociationID string,
) error {
	var req struct {
		Parameters map[string]string `json:"Parameters"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	assoc, err := h.Backend.UpdateExtensionAssociation(extensionAssociationID, req.Parameters)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, assoc)
}

func (h *Handler) handleDeleteExtensionAssociation(
	c *echo.Context,
	extensionAssociationID string,
) error {
	if err := h.Backend.DeleteExtensionAssociation(extensionAssociationID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetAccountSettings(c *echo.Context) error {
	settings, err := h.Backend.GetAccountSettings()
	if err != nil {
		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, settings)
}

func (h *Handler) handleUpdateAccountSettings(c *echo.Context) error {
	var req struct {
		DeletionProtection *DeletionProtectionSettings `json:"DeletionProtection"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	settings, err := h.Backend.UpdateAccountSettings(req.DeletionProtection)
	if err != nil {
		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, settings)
}

func (h *Handler) handleGetConfiguration(
	c *echo.Context,
	application, environment, configuration string,
) error {
	configVersion, err := h.Backend.GetConfiguration(application, environment, configuration)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	if configVersion.VersionNumber > 0 {
		c.Response().
			Header().
			Set("Configuration-Version", strconv.Itoa(int(configVersion.VersionNumber)))
	}

	if len(configVersion.Content) == 0 {
		return c.NoContent(http.StatusNoContent)
	}

	contentType := configVersion.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	return c.Blob(http.StatusOK, contentType, configVersion.Content)
}

func (h *Handler) handleValidateConfiguration(
	c *echo.Context,
	applicationID, profileID string,
) error {
	configVersion := c.Request().URL.Query().Get("configuration_version")

	if err := h.Backend.ValidateConfiguration(applicationID, profileID, configVersion); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.NoContent(http.StatusNoContent)
}

// appConfigPaginationParams reads the next_token and max_results query parameters.
func appConfigPaginationParams(c *echo.Context) (string, int) {
	q := c.Request().URL.Query()
	nextToken := q.Get("next_token")

	maxResults := 0

	if s := q.Get("max_results"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			maxResults = n
		}
	}

	return nextToken, maxResults
}
