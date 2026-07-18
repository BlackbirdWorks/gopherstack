package serverlessrepo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyTypeField       = "__type"
	keyMessageField    = "message"
	keyApplicationID   = "applicationId"
	keySemanticVersion = "semanticVersion"
	keyCreationTime    = "creationTime"
	keyTemplateURL     = "templateUrl"
)

const (
	opCreateApplication             = "CreateApplication"
	opCreateApplicationVersion      = "CreateApplicationVersion"
	opCreateCloudFormationChangeSet = "CreateCloudFormationChangeSet"
	opCreateCloudFormationTemplate  = "CreateCloudFormationTemplate"
	opDeleteApplication             = "DeleteApplication"
	opGetApplication                = "GetApplication"
	opGetApplicationPolicy          = "GetApplicationPolicy"
	opGetCloudFormationTemplate     = "GetCloudFormationTemplate"
	opListApplicationDependencies   = "ListApplicationDependencies"
	opListApplicationVersions       = "ListApplicationVersions"
	opListApplications              = "ListApplications"
	opPutApplicationPolicy          = "PutApplicationPolicy"
	opUnshareApplication            = "UnshareApplication"
	opUpdateApplication             = "UpdateApplication"
)

const (
	serverlessrepoService       = "serverlessrepo"
	serverlessrepoMatchPriority = 87
	// pathSegmentsMax is used to split the URL path into at most 2 parts.
	pathSegmentsMax = 2
	// pathSplitParts is the number of path parts to split into when parsing sub-paths.
	pathSplitParts = 3
	// pathSplitTwoParts splits into two parts.
	pathSplitTwoParts = 2
	// pathIndexSeg is the index of the sub-path segment in split results.
	pathIndexSeg = 1
	// pathIndexExtra is the index of the extra segment (e.g. version or template ID) in split results.
	pathIndexExtra = 2

	// path segment constants.
	pathSegVersions     = "versions"
	pathSegTemplates    = "templates"
	pathSegChangesets   = "changesets"
	pathSegPolicy       = "policy"
	pathSegDependencies = "dependencies"
	pathSegUnshare      = "unshare"

	// templateStatusExpired is the status of an expired CloudFormation template.
	templateStatusExpired = "EXPIRED"

	// maxItemsDefault is the default maximum number of items returned in list operations.
	maxItemsDefault = 100
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
	// errHTTP201 is a sentinel returned by create handlers to signal HTTP 201 Created.
	// It is checked in Handler() before other error handling.
	errHTTP201 = errors.New("201 Created")
)

// Handler is the HTTP handler for the AWS Serverless Application Repository REST API.
type Handler struct {
	Backend   StorageBackend
	AccountID string
	Region    string
}

// NewHandler creates a new Serverless Application Repository handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.AccountID(),
		Region:    backend.Region(),
	}
}

// Reset clears the handler's backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "ServerlessRepo" }

// GetSupportedOperations returns the list of supported Serverless Application Repository operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateApplication,
		opCreateApplicationVersion,
		opCreateCloudFormationChangeSet,
		opCreateCloudFormationTemplate,
		opDeleteApplication,
		opGetApplication,
		opGetApplicationPolicy,
		opGetCloudFormationTemplate,
		opListApplicationDependencies,
		opListApplicationVersions,
		opListApplications,
		opPutApplicationPolicy,
		opUnshareApplication,
		opUpdateApplication,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return serverlessrepoService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a function that matches Serverless Application Repository API requests.
// All path-based matches are gated on the SigV4 service name to prevent routing conflicts.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if httputils.ExtractServiceFromRequest(c.Request()) != serverlessrepoService {
			return false
		}

		path := c.Request().URL.Path

		return path == "/applications" || strings.HasPrefix(path, "/applications/")
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return serverlessrepoMatchPriority }

// ExtractOperation extracts the operation name from the HTTP method and request path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	method := c.Request().Method
	// Use the raw (percent-encoded) path for routing to correctly handle ARN application IDs
	// that contain a literal '/' (encoded as %2F) in their path component.
	path := rawPathOrPath(c.Request())

	// /applications → list or create
	if path == "/applications" || path == "/applications/" {
		return extractRootOp(method)
	}

	// /applications/{applicationId}/...
	if after, ok := strings.CutPrefix(path, "/applications/"); ok {
		parts := strings.SplitN(after, "/", pathSplitParts)

		switch len(parts) {
		case pathSplitTwoParts - 1:
			return extractSingleSegOp(method)
		case pathSplitTwoParts:
			return extractTwoSegOp(method, parts[pathIndexSeg])
		case pathSplitParts:
			return extractThreeSegOp(method, parts[pathIndexSeg])
		}
	}

	return ""
}

// extractRootOp returns the operation for the /applications root path.
func extractRootOp(method string) string {
	switch method {
	case http.MethodGet:
		return opListApplications
	case http.MethodPost:
		return opCreateApplication
	}

	return ""
}

// extractSingleSegOp returns the operation for /applications/{applicationId}.
func extractSingleSegOp(method string) string {
	switch method {
	case http.MethodGet:
		return opGetApplication
	case http.MethodPatch:
		return opUpdateApplication
	case http.MethodDelete:
		return opDeleteApplication
	}

	return ""
}

// extractTwoSegOp returns the operation for /applications/{applicationId}/{segment}.
func extractTwoSegOp(method, seg string) string {
	switch seg {
	case pathSegPolicy:
		return extractPolicyOp(method)
	case pathSegChangesets:
		if method == http.MethodPost {
			return opCreateCloudFormationChangeSet
		}
	case pathSegTemplates:
		if method == http.MethodPost {
			return opCreateCloudFormationTemplate
		}
	case pathSegVersions:
		if method == http.MethodGet {
			return opListApplicationVersions
		}
	case pathSegDependencies:
		if method == http.MethodGet {
			return opListApplicationDependencies
		}
	case pathSegUnshare:
		if method == http.MethodPost {
			return opUnshareApplication
		}
	}

	return ""
}

// extractPolicyOp returns the operation for policy sub-paths.
func extractPolicyOp(method string) string {
	switch method {
	case http.MethodGet:
		return opGetApplicationPolicy
	case http.MethodPut:
		return opPutApplicationPolicy
	}

	return ""
}

// extractThreeSegOp returns the operation for /applications/{applicationId}/{segment}/{id}.
func extractThreeSegOp(method, seg string) string {
	switch {
	case seg == pathSegVersions && method == http.MethodPut:
		return opCreateApplicationVersion
	case seg == pathSegTemplates && method == http.MethodGet:
		return opGetCloudFormationTemplate
	}

	return ""
}

// rawPathOrPath returns URL.RawPath if non-empty (i.e. the path contains percent-encoded
// characters whose decoded form would change the path structure, such as %2F → '/').
// Otherwise it falls back to URL.Path. This is necessary for correctly routing requests where
// the application ID is an ARN that contains a '/' encoded as '%2F'.
func rawPathOrPath(req *http.Request) string {
	if req.URL.RawPath != "" {
		return req.URL.RawPath
	}

	return req.URL.Path
}

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return h.ExtractOperation(c)
}

// Handler returns the Echo handler function for Serverless Application Repository requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "serverlessrepo: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op := h.ExtractOperation(c)

		result, dispErr := h.dispatch(ctx, op, c.Request(), body)

		// errHTTP201 is a create-success sentinel; return 201 with body.
		if errors.Is(dispErr, errHTTP201) {
			return c.JSONBlob(http.StatusCreated, result)
		}

		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.NoContent(http.StatusNoContent)
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(ctx context.Context, op string, req *http.Request, body []byte) ([]byte, error) {
	if result, ok, err := h.dispatchAppOps(ctx, op, req, body); ok {
		return result, err
	}

	if result, ok, err := h.dispatchCFOps(ctx, op, req, body); ok {
		return result, err
	}

	if result, ok, err := h.dispatchPolicyAndMiscOps(ctx, op, req, body); ok {
		return result, err
	}

	return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
}

func (h *Handler) dispatchAppOps(ctx context.Context, op string, req *http.Request, body []byte) ([]byte, bool, error) {
	switch op {
	case opCreateApplication:
		result, err := h.handleCreateApplication(ctx, body)

		return result, true, err
	case opGetApplication:
		result, err := h.handleGetApplication(req)

		return result, true, err
	case opListApplications:
		result, err := h.handleListApplications(req)

		return result, true, err
	case opUpdateApplication:
		result, err := h.handleUpdateApplication(ctx, req, body)

		return result, true, err
	case opDeleteApplication:
		return nil, true, h.handleDeleteApplication(ctx, req)
	case opCreateApplicationVersion:
		result, err := h.handleCreateApplicationVersion(ctx, req, body)

		return result, true, err
	case opListApplicationVersions:
		result, err := h.handleListApplicationVersions(req)

		return result, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchCFOps(ctx context.Context, op string, req *http.Request, body []byte) ([]byte, bool, error) {
	switch op {
	case opCreateCloudFormationTemplate:
		result, err := h.handleCreateCloudFormationTemplate(ctx, req, body)

		return result, true, err
	case opGetCloudFormationTemplate:
		result, err := h.handleGetCloudFormationTemplate(req)

		return result, true, err
	case opCreateCloudFormationChangeSet:
		result, err := h.handleCreateCloudFormationChangeSet(ctx, req, body)

		return result, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchPolicyAndMiscOps(
	ctx context.Context,
	op string,
	req *http.Request,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case opGetApplicationPolicy:
		result, err := h.handleGetApplicationPolicy(req)

		return result, true, err
	case opPutApplicationPolicy:
		result, err := h.handlePutApplicationPolicy(ctx, req, body)

		return result, true, err
	case opListApplicationDependencies:
		result, err := h.handleListApplicationDependencies(req)

		return result, true, err
	case opUnshareApplication:
		return nil, true, h.handleUnshareApplication(ctx, req, body)
	}

	return nil, false, nil
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		// json.Marshal on map[string]string never returns an error; _ is intentional.
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "NotFoundException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusNotFound, payload)
	case errors.Is(err, awserr.ErrConflict):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "ConflictException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusConflict, payload)
	case errors.Is(err, awserr.ErrInvalidParameter),
		errors.Is(err, errInvalidRequest),
		errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr),
		errors.As(err, &typeErr):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "BadRequestException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	default:
		// The real AWS SAR service returns "InternalServerErrorException" as the
		// __type value (see types.InternalServerErrorException in
		// aws-sdk-go-v2/service/serverlessapplicationrepository); the aws-sdk-go-v2
		// restjson1 error deserializer matches on this exact string (case-insensitively)
		// to construct a typed error, so any other spelling falls through to a generic
		// smithy.GenericAPIError on the client side.
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "InternalServerErrorException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusInternalServerError, payload)
	}
}

// extractApplicationName extracts the application name from the URL path
// at /applications/{applicationId} (URL-encoded). If the segment is an ARN
// (e.g. arn:aws:serverlessrepo:us-east-1:123:applications/my-app), the name
// after the final "/" is extracted.
//
// Uses URL.RawPath (if set) to correctly handle ARN application IDs that contain
// a literal '/' encoded as '%2F' in the path; otherwise falls back to URL.Path.
func extractApplicationName(req *http.Request) (string, error) {
	path := rawPathOrPath(req)
	path = strings.TrimPrefix(path, "/applications/")
	path = strings.SplitN(path, "/", pathSegmentsMax)[0]

	name, err := url.PathUnescape(path)
	if err != nil {
		return "", fmt.Errorf("%w: invalid application id encoding", errInvalidRequest)
	}

	// Accept ARN-form application IDs (e.g. arn:aws:serverlessrepo:us-east-1:123456789:applications/my-app).
	// The SAR ARN format uses a colon before "applications", not a slash.
	if strings.HasPrefix(name, "arn:") {
		const arnAppResource = ":applications/"

		idx := strings.Index(name, arnAppResource)
		if idx < 0 {
			return "", fmt.Errorf("%w: ARN does not contain expected :applications/ resource path", errInvalidRequest)
		}

		name = strings.TrimSuffix(name[idx+len(arnAppResource):], "/")
		if name == "" {
			return "", fmt.Errorf("%w: ARN has empty application name", errInvalidRequest)
		}
	}

	if name == "" {
		return "", fmt.Errorf("%w: applicationId is required", errInvalidRequest)
	}

	return name, nil
}

// extractPathExtra extracts the application name and the trailing extra segment
// from a path of the form /applications/{appId}/{segment}/{extra}.
//
// Uses URL.RawPath (if set) to correctly handle ARN application IDs that contain
// a literal '/' encoded as '%2F' in the path; otherwise falls back to URL.Path.
func extractPathExtra(req *http.Request) (string, string, error) {
	path := strings.TrimPrefix(rawPathOrPath(req), "/applications/")
	parts := strings.SplitN(path, "/", pathSplitParts)

	appName, urlErr := url.PathUnescape(parts[0])
	if urlErr != nil {
		return "", "", fmt.Errorf("%w: invalid application id encoding", errInvalidRequest)
	}

	if strings.HasPrefix(appName, "arn:") {
		const arnAppResource = ":applications/"

		idx := strings.Index(appName, arnAppResource)
		if idx >= 0 {
			appName = strings.TrimSuffix(appName[idx+len(arnAppResource):], "/")
		}
	}

	var extra string

	if len(parts) > pathIndexExtra {
		extra, _ = url.PathUnescape(parts[pathIndexExtra])
	}

	return appName, extra, nil
}

// isoTimestamp converts a [time.Time] to an RFC3339 UTC string, matching the AWS SAR API shape.
func isoTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

// parseMaxItems parses the maxItems query parameter, returning defaultVal when the parameter
// is empty or invalid (non-positive). The returned value is always at least 1.
func parseMaxItems(raw string, defaultVal int) int {
	if raw == "" {
		return defaultVal
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return defaultVal
	}

	return n
}
