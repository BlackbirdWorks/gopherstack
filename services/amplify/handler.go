package amplify

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	amplifyAppsPrefix        = "/apps"
	amplifyTagsPrefix        = "/tags/"
	amplifyWebhooksPrefix    = "/webhooks/"
	amplifyArtifactsPrefix   = "/artifacts/"
	amplifyServiceIdentifier = ":amplify"

	// Path segment counts for Amplify routes.
	pathSegsApps      = 1 // ["apps"]
	pathSegsAppID     = 2 // ["apps", "{appId}"]
	pathSegsAppSub    = 3 // ["apps", "{appId}", "branches"]
	pathSegsAppBranch = 4 // ["apps", "{appId}", "branches", "{branchName}"]

	pathSegsAppBranchSub  = 5 // ["apps", "{appId}", "branches", "{branchName}", "jobs"]
	pathSegsAppBranchItem = 6 // ["apps", "{appId}", "branches", "{branchName}", "jobs", "{jobId}"]
	pathSegsJobAction     = 7 // ["apps", "{appId}", "branches", "{branchName}", "jobs", "{jobId}", "action"]

	opUnknown = "Unknown"

	// Path segment literals shared by the routing tree below.
	subWebhooks            = "webhooks"
	subBackendEnvironments = "backendenvironments"
	subDomains             = "domains"
	subJobs                = "jobs"
	subDeployments         = "deployments"
	subAccessLogs          = "accesslogs"
	subArtifactsRoot       = "artifacts"
	subStop                = "stop"
	subStart               = "start"
)

// Handler is the Echo HTTP handler for Amplify operations.
type Handler struct {
	Backend       StorageBackend
	janitor       *Janitor
	DefaultRegion string
	AccountID     string
}

// NewHandler creates a new Amplify handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// WithJanitor attaches a background janitor to the handler. backend must be
// the concrete *InMemoryBackend backing this Handler -- the janitor needs
// direct access to the backend's tables and lock, which the StorageBackend
// interface (used for h.Backend so the handler stays mockable) does not
// expose.
func (h *Handler) WithJanitor(
	backend *InMemoryBackend,
	interval time.Duration,
	taskTimeout ...time.Duration,
) *Handler {
	j := NewJanitor(backend, interval)
	if len(taskTimeout) > 0 {
		j.TaskTimeout = taskTimeout[0]
	}

	h.janitor = j

	return h
}

// StartWorker starts the background janitor if configured. It implements the
// worker-lifecycle hook of service.Registerable, so cli.go's generic startup
// dispatch starts it without any Amplify-specific wiring there.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

// Name returns the service name.
func (h *Handler) Name() string { return "Amplify" }

// GetSupportedOperations returns the list of supported Amplify operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateApp",
		"GetApp",
		"ListApps",
		"DeleteApp",
		"CreateBranch",
		"GetBranch",
		"ListBranches",
		"DeleteBranch",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
		"CreateBackendEnvironment",
		"CreateDeployment",
		"CreateDomainAssociation",
		"CreateWebhook",
		"DeleteBackendEnvironment",
		"DeleteDomainAssociation",
		"DeleteJob",
		"DeleteWebhook",
		"GenerateAccessLogs",
		"GetArtifactUrl",
		"GetBackendEnvironment",
		"GetDomainAssociation",
		"GetJob",
		"GetWebhook",
		"ListArtifacts",
		"ListBackendEnvironments",
		"ListDomainAssociations",
		"ListJobs",
		"ListWebhooks",
		"StartDeployment",
		"StartJob",
		"StopJob",
		"UpdateApp",
		"UpdateBranch",
		"UpdateDomainAssociation",
		"UpdateWebhook",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "amplify" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Amplify instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches Amplify API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		if strings.HasPrefix(path, amplifyAppsPrefix) {
			return true
		}

		// Only claim /tags/{arn} when the ARN belongs to Amplify.
		// Other services (e.g. FIS) also expose a /tags/{arn} endpoint at the
		// same path prefix; we must not steal their requests.
		if strings.HasPrefix(path, amplifyTagsPrefix) {
			arn := path[len(amplifyTagsPrefix):]

			return strings.Contains(arn, amplifyServiceIdentifier)
		}

		if strings.HasPrefix(path, amplifyWebhooksPrefix) || strings.HasPrefix(path, amplifyArtifactsPrefix) {
			return true
		}

		return false
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the Amplify operation from the request path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return parseAmplifyOperation(c.Request().Method, c.Request().URL.Path)
}

// ExtractResource extracts the app ID from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	segs := splitAmplifyPath(c.Request().URL.Path)
	// Path: /apps/{appId}/...
	const appIDIndex = 1
	if len(segs) > appIDIndex && segs[0] == arnResourceApps {
		return segs[appIDIndex]
	}

	return ""
}

// parseAmplifyOperation derives an operation name from the HTTP method and path.
func parseAmplifyOperation(method, path string) string {
	if strings.HasPrefix(path, amplifyTagsPrefix) {
		return parseTagsOperation(method)
	}

	segs := splitAmplifyPath(path)
	if len(segs) == 0 {
		return opUnknown
	}

	switch segs[0] {
	case subWebhooks:
		return parseWebhookIDOp(method)
	case subArtifactsRoot:
		if method == http.MethodGet {
			return "GetArtifactUrl"
		}

		return opUnknown
	case arnResourceApps:
		return parseAppsPathOp(method, segs)
	default:
		return opUnknown
	}
}

// parseAppsPathOp dispatches operation-name parsing for /apps/* paths by
// segment-count shape. This mirrors the runtime routing tree below
// (routeApps and friends) and must stay in lock-step with it.
func parseAppsPathOp(method string, segs []string) string {
	switch len(segs) {
	case pathSegsApps:
		return parseAppsOperation(method)
	case pathSegsAppID:
		return parseAppIDOperation(method)
	case pathSegsAppSub:
		return parseAppSubPathOp(method, segs[2])
	case pathSegsAppBranch:
		return parseAppItemPathOp(method, segs[2])
	case pathSegsAppBranchSub:
		if segs[2] != arnResourceBranches {
			return opUnknown
		}

		return parseAppBranchSubPathOp(method, segs[4])
	case pathSegsAppBranchItem:
		if segs[2] != arnResourceBranches {
			return opUnknown
		}

		return parseAppBranchItemPathOp(method, segs[4], segs[5])
	case pathSegsJobAction:
		if segs[2] != arnResourceBranches || segs[4] != subJobs {
			return opUnknown
		}

		return parseJobActionPathOp(method, segs[6])
	default:
		return opUnknown
	}
}

func parseAppSubPathOp(method, sub string) string {
	switch sub {
	case arnResourceBranches:
		return parseBranchesOperation(method)
	case subWebhooks:
		return parseListOrCreateOp(method, "ListWebhooks", "CreateWebhook")
	case subBackendEnvironments:
		return parseListOrCreateOp(method, "ListBackendEnvironments", "CreateBackendEnvironment")
	case subDomains:
		return parseListOrCreateOp(method, "ListDomainAssociations", "CreateDomainAssociation")
	case subAccessLogs:
		if method == http.MethodPost {
			return "GenerateAccessLogs"
		}

		return opUnknown
	default:
		return opUnknown
	}
}

// parseListOrCreateOp maps a list/create resource-collection path to its
// operation name based on HTTP method. Shared across the webhooks, backend
// environments, and domain associations sub-resource families.
func parseListOrCreateOp(method, listOp, createOp string) string {
	switch method {
	case http.MethodGet:
		return listOp
	case http.MethodPost:
		return createOp
	default:
		return opUnknown
	}
}

func parseAppItemPathOp(method, sub string) string {
	switch {
	case sub == arnResourceBranches:
		return parseBranchOperation(method)
	case sub == subBackendEnvironments && method == http.MethodGet:
		return "GetBackendEnvironment"
	case sub == subBackendEnvironments && method == http.MethodDelete:
		return "DeleteBackendEnvironment"
	case sub == subDomains && method == http.MethodGet:
		return "GetDomainAssociation"
	case sub == subDomains && method == http.MethodDelete:
		return "DeleteDomainAssociation"
	case sub == subDomains && method == http.MethodPost:
		return "UpdateDomainAssociation"
	default:
		return opUnknown
	}
}

func parseAppBranchSubPathOp(method, sub string) string {
	switch {
	case sub == subJobs && method == http.MethodPost:
		return "StartJob"
	case sub == subJobs && method == http.MethodGet:
		return "ListJobs"
	case sub == subDeployments && method == http.MethodPost:
		return "CreateDeployment"
	default:
		return opUnknown
	}
}

func parseAppBranchItemPathOp(method, sub, item string) string {
	switch {
	case sub == subJobs && method == http.MethodGet:
		return "GetJob"
	case sub == subJobs && method == http.MethodDelete:
		return "DeleteJob"
	case sub == subDeployments && item == subStart && method == http.MethodPost:
		return "StartDeployment"
	default:
		return opUnknown
	}
}

func parseJobActionPathOp(method, action string) string {
	switch {
	case action == subStop && method == http.MethodDelete:
		return "StopJob"
	case action == subArtifactsRoot && method == http.MethodGet:
		return "ListArtifacts"
	default:
		return opUnknown
	}
}

// splitAmplifyPath splits a URL path into non-empty segments.
func splitAmplifyPath(path string) []string {
	var segs []string

	for s := range strings.SplitSeq(path, "/") {
		if s != "" {
			segs = append(segs, s)
		}
	}

	return segs
}

// Handler returns the Echo handler function for Amplify requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		method := c.Request().Method
		path := c.Request().URL.Path
		log := logger.Load(ctx)

		log.DebugContext(ctx, "Amplify request", "method", method, "path", path)

		if strings.HasPrefix(path, amplifyTagsPrefix) {
			return h.handleTags(ctx, c)
		}

		segs := splitAmplifyPath(path)
		if len(segs) == 0 {
			return amplifyErrorJSON(c, http.StatusNotFound, "not found")
		}

		switch segs[0] {
		case arnResourceApps:
			return h.routeApps(ctx, c, segs)
		case subWebhooks:
			return h.routeWebhooks(ctx, c, segs)
		case subArtifactsRoot:
			return h.routeArtifacts(ctx, c, segs)
		default:
			return amplifyErrorJSON(c, http.StatusNotFound, "not found")
		}
	}
}

// routeApps dispatches /apps/* paths.
func (h *Handler) routeApps(ctx context.Context, c *echo.Context, segs []string) error {
	switch len(segs) {
	case pathSegsApps:
		return h.handleApps(ctx, c)
	case pathSegsAppID:
		return h.handleAppID(ctx, c, segs[1])
	case pathSegsAppSub:
		return h.routeAppSub(ctx, c, segs)
	case pathSegsAppBranch:
		return h.routeAppItem(ctx, c, segs)
	case pathSegsAppBranchSub:
		return h.routeAppBranchSub(ctx, c, segs)
	case pathSegsAppBranchItem:
		return h.routeAppBranchItem(ctx, c, segs)
	case pathSegsJobAction:
		return h.routeJobAction(ctx, c, segs)
	default:
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}
}

// routeAppSub dispatches /apps/{appId}/{sub}.
func (h *Handler) routeAppSub(ctx context.Context, c *echo.Context, segs []string) error {
	appID, sub := segs[1], segs[2]
	switch sub {
	case arnResourceBranches:
		return h.handleBranches(ctx, c, appID)
	case subWebhooks:
		return h.handleAppWebhooks(ctx, c, appID)
	case subBackendEnvironments:
		return h.handleBackendEnvironments(ctx, c, appID)
	case subDomains:
		return h.handleDomainAssociations(ctx, c, appID)
	case subAccessLogs:
		return h.generateAccessLogs(ctx, c, appID)
	default:
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}
}

// routeAppItem dispatches /apps/{appId}/{sub}/{item}.
func (h *Handler) routeAppItem(ctx context.Context, c *echo.Context, segs []string) error {
	appID, sub, item := segs[1], segs[2], segs[3]
	switch sub {
	case arnResourceBranches:
		return h.handleBranchName(ctx, c, appID, item)
	case subBackendEnvironments:
		return h.handleBackendEnvironmentName(ctx, c, appID, item)
	case subDomains:
		return h.handleDomainAssociationName(ctx, c, appID, item)
	default:
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}
}

// routeAppBranchSub dispatches /apps/{appId}/branches/{branchName}/{sub}.
func (h *Handler) routeAppBranchSub(ctx context.Context, c *echo.Context, segs []string) error {
	if segs[2] != arnResourceBranches {
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}

	appID, branchName, sub := segs[1], segs[3], segs[4]
	switch sub {
	case subJobs:
		return h.handleBranchJobs(ctx, c, appID, branchName)
	case subDeployments:
		return h.createDeployment(ctx, c, appID, branchName)
	default:
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}
}

// routeAppBranchItem dispatches /apps/{appId}/branches/{branchName}/{sub}/{item}.
func (h *Handler) routeAppBranchItem(ctx context.Context, c *echo.Context, segs []string) error {
	if segs[2] != arnResourceBranches {
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}

	appID, branchName, sub, item := segs[1], segs[3], segs[4], segs[5]
	switch sub {
	case subJobs:
		return h.handleBranchJobID(ctx, c, appID, branchName, item)
	case subDeployments:
		if item != subStart {
			return amplifyErrorJSON(c, http.StatusNotFound, "not found")
		}

		return h.startDeployment(ctx, c, appID, branchName)
	default:
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}
}

// routeJobAction dispatches /apps/{appId}/branches/{branchName}/jobs/{jobId}/{action}.
func (h *Handler) routeJobAction(ctx context.Context, c *echo.Context, segs []string) error {
	if segs[2] != arnResourceBranches || segs[4] != subJobs {
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}

	appID, branchName, jobID, action := segs[1], segs[3], segs[5], segs[6]
	switch action {
	case subStop:
		return h.stopJob(ctx, c, appID, branchName, jobID)
	case subArtifactsRoot:
		return h.listArtifacts(ctx, c, appID, branchName, jobID)
	default:
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}
}

// routeWebhooks dispatches /webhooks/{webhookId}.
func (h *Handler) routeWebhooks(ctx context.Context, c *echo.Context, segs []string) error {
	const webhookIDSegs = 2
	if len(segs) != webhookIDSegs {
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}

	return h.handleWebhookID(ctx, c, segs[1])
}

// routeArtifacts dispatches /artifacts/{artifactId}.
func (h *Handler) routeArtifacts(ctx context.Context, c *echo.Context, segs []string) error {
	const artifactIDSegs = 2
	if len(segs) != artifactIDSegs {
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}

	return h.getArtifactURL(ctx, c, segs[1])
}

// handleBackendError maps backend errors to appropriate HTTP responses.
func (h *Handler) handleBackendError(ctx context.Context, c *echo.Context, op string, err error) error {
	log := logger.Load(ctx)
	log.ErrorContext(ctx, "Amplify operation failed", "operation", op, "error", err)

	if errors.Is(err, awserr.ErrNotFound) {
		return amplifyErrorJSON(c, http.StatusNotFound, err.Error())
	}

	if errors.Is(err, awserr.ErrAlreadyExists) || errors.Is(err, awserr.ErrConflict) {
		return amplifyErrorJSON(c, http.StatusBadRequest, err.Error())
	}

	return amplifyErrorJSON(c, http.StatusInternalServerError, "internal error: "+err.Error())
}

// codeForStatus maps an HTTP status code to the Amplify restjson1 exception
// type name that would accompany it. aws-sdk-go-v2's generated deserializer
// (awsRestjson1_deserializeOpErrorGetApp and friends) selects a typed
// exception (types.NotFoundException, types.BadRequestException, ...) by
// reading the "X-Amzn-Errortype" response header or, failing that, a
// "code"/"__type" field in the JSON body -- never the HTTP status alone. A
// response with neither deserializes client-side as a generic UnknownError,
// so any caller that type-switches on a specific Amplify exception (a common
// pattern for e.g. treating NotFoundException as "already gone") breaks even
// though the HTTP status and message are otherwise correct.
func codeForStatus(status int) string {
	switch status {
	case http.StatusNotFound:
		return "NotFoundException"
	case http.StatusBadRequest, http.StatusMethodNotAllowed:
		return "BadRequestException"
	default:
		return "InternalFailureException"
	}
}

// amplifyErrorJSON writes an Amplify-shaped restjson1 error response: the
// HTTP status, the "X-Amzn-Errortype" header, and a JSON body carrying both
// "__type" and "message" so aws-sdk-go-v2 resolves the correct typed
// exception (see codeForStatus).
func amplifyErrorJSON(c *echo.Context, status int, message string) error {
	code := codeForStatus(status)
	c.Response().Header().Set("X-Amzn-Errortype", code)

	return c.JSON(status, map[string]any{"__type": code, "message": message})
}
