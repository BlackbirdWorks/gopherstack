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
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "InternalServerException",
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

// createApplicationRequest is the request body for CreateApplication.
type createApplicationRequest struct {
	Name                 string   `json:"name"`
	Description          string   `json:"description"`
	Author               string   `json:"author"`
	HomePageURL          string   `json:"homePageUrl"`
	LicenseURL           string   `json:"licenseUrl"`
	ReadmeURL            string   `json:"readmeUrl"`
	SpdxLicenseID        string   `json:"spdxLicenseId"`
	SourceCodeURL        string   `json:"sourceCodeUrl"`
	SourceCodeArchiveURL string   `json:"sourceCodeArchiveUrl"`
	TemplateURL          string   `json:"templateUrl"`
	SemanticVersion      string   `json:"semanticVersion"`
	Labels               []string `json:"labels"`
}

// versionResponse represents the SAR Version type in API responses.
// The botocore SAR model expects "version" to be a struct, not a plain string.
type versionResponse struct {
	ApplicationID        string                `json:"applicationId,omitempty"`
	CreationTime         string                `json:"creationTime,omitempty"`
	SemanticVersion      string                `json:"semanticVersion,omitempty"`
	TemplateURL          string                `json:"templateUrl,omitempty"`
	SourceCodeURL        string                `json:"sourceCodeUrl,omitempty"`
	SourceCodeArchiveURL string                `json:"sourceCodeArchiveUrl,omitempty"`
	ParameterDefinitions []ParameterDefinition `json:"parameterDefinitions"`
	RequiredCapabilities []string              `json:"requiredCapabilities"`
	ResourcesSupported   bool                  `json:"resourcesSupported"`
}

// applicationResponse represents the API response shape for a single application.
type applicationResponse struct {
	Version           *versionResponse `json:"version,omitempty"`
	HomePageURL       string           `json:"homePageUrl,omitempty"`
	ApplicationID     string           `json:"applicationId"`
	Name              string           `json:"name"`
	Description       string           `json:"description"`
	Author            string           `json:"author"`
	CreationTime      string           `json:"creationTime"`
	LicenseURL        string           `json:"licenseUrl,omitempty"`
	ReadmeURL         string           `json:"readmeUrl,omitempty"`
	SpdxLicenseID     string           `json:"spdxLicenseId,omitempty"`
	SourceCodeURL     string           `json:"sourceCodeUrl,omitempty"`
	VerifiedAuthorURL string           `json:"verifiedAuthorUrl,omitempty"`
	Labels            []string         `json:"labels,omitempty"`
	IsVerifiedAuthor  bool             `json:"isVerifiedAuthor"`
}

// applicationSummary is a summary used in list responses.
type applicationSummary struct {
	CreationTime  string   `json:"creationTime"`
	ApplicationID string   `json:"applicationId"`
	Name          string   `json:"name"`
	Description   string   `json:"description"`
	Author        string   `json:"author"`
	HomePageURL   string   `json:"homePageUrl,omitempty"`
	SpdxLicenseID string   `json:"spdxLicenseId,omitempty"`
	Labels        []string `json:"labels,omitempty"`
}

func toApplicationResponse(a *Application) applicationResponse {
	resp := applicationResponse{
		ApplicationID:     a.ApplicationID,
		Name:              a.Name,
		Description:       a.Description,
		Author:            a.Author,
		SourceCodeURL:     a.SourceCodeURL,
		HomePageURL:       a.HomePageURL,
		LicenseURL:        a.LicenseURL,
		ReadmeURL:         a.ReadmeURL,
		SpdxLicenseID:     a.SpdxLicenseID,
		CreationTime:      isoTimestamp(a.CreationTime),
		IsVerifiedAuthor:  a.IsVerifiedAuthor,
		VerifiedAuthorURL: a.VerifiedAuthorURL,
		Labels:            a.Labels,
	}

	if a.SemanticVersion != "" {
		resp.Version = &versionResponse{
			ApplicationID:        a.ApplicationID,
			CreationTime:         isoTimestamp(a.CreationTime),
			SemanticVersion:      a.SemanticVersion,
			ParameterDefinitions: []ParameterDefinition{},
			RequiredCapabilities: []string{},
			ResourcesSupported:   true,
		}
	}

	return resp
}

func toEmbeddedVersionResponse(v *ApplicationVersion) *versionResponse {
	return &versionResponse{
		ApplicationID:        v.ApplicationID,
		CreationTime:         isoTimestamp(v.CreationTime),
		SemanticVersion:      v.SemanticVersion,
		TemplateURL:          v.TemplateURL,
		SourceCodeURL:        v.SourceCodeURL,
		SourceCodeArchiveURL: v.SourceCodeArchiveURL,
		ParameterDefinitions: v.ParameterDefinitions,
		RequiredCapabilities: v.RequiredCapabilities,
		ResourcesSupported:   v.ResourcesSupported,
	}
}

func (h *Handler) handleCreateApplication(ctx context.Context, body []byte) ([]byte, error) {
	var req createApplicationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	a, err := h.Backend.CreateApplication(
		req.Name,
		req.Description,
		req.Author,
		req.SourceCodeURL,
		req.SemanticVersion,
		req.Labels,
		req.HomePageURL,
		req.LicenseURL,
		req.SpdxLicenseID,
	)
	if err != nil {
		return nil, err
	}

	if req.ReadmeURL != "" {
		a, err = h.Backend.SetApplicationReadmeURL(a.Name, req.ReadmeURL)
		if err != nil {
			return nil, err
		}
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "serverlessrepo: created application", "name", a.Name, "id", a.ApplicationID)

	resp := toApplicationResponse(a)
	if req.SemanticVersion != "" &&
		(req.SourceCodeURL != "" || req.SourceCodeArchiveURL != "" || req.TemplateURL != "") {
		v, versionErr := h.Backend.CreateApplicationVersionWithOptions(
			a.Name,
			req.SemanticVersion,
			CreateApplicationVersionOptions{
				SourceCodeURL:        req.SourceCodeURL,
				SourceCodeArchiveURL: req.SourceCodeArchiveURL,
				TemplateURL:          req.TemplateURL,
			},
		)
		if versionErr != nil {
			return nil, versionErr
		}

		resp.Version = toEmbeddedVersionResponse(v)
	}

	b, marshalErr := json.Marshal(resp)
	if marshalErr != nil {
		return nil, marshalErr
	}

	return b, errHTTP201
}

func (h *Handler) handleGetApplication(req *http.Request) ([]byte, error) {
	name, err := extractApplicationName(req)
	if err != nil {
		return nil, err
	}

	a, err := h.Backend.GetApplication(name)
	if err != nil {
		return nil, err
	}

	resp := toApplicationResponse(a)

	// A query parameter selects a version; otherwise GetApplication returns its current version.
	sv := req.URL.Query().Get(keySemanticVersion)
	explicitVersion := sv != ""
	if sv == "" {
		sv = a.SemanticVersion
	}

	if sv != "" {
		v, vErr := h.Backend.GetApplicationVersion(name, sv)
		if vErr != nil {
			if explicitVersion {
				return nil, vErr
			}
		} else {
			resp.Version = toEmbeddedVersionResponse(v)
		}
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListApplications(req *http.Request) ([]byte, error) {
	apps := h.Backend.ListApplications()

	// Apply pagination: nextToken is treated as the last-seen application name (exclusive cursor).
	nextToken := req.URL.Query().Get("nextToken")
	maxItems := parseMaxItems(req.URL.Query().Get("maxItems"), maxItemsDefault)

	start := 0

	if nextToken != "" {
		for i, a := range apps {
			if a.Name == nextToken {
				start = i + 1

				break
			}
		}
	}

	end := min(start+maxItems, len(apps))

	page := apps[start:end]

	summaries := make([]applicationSummary, 0, len(page))

	for _, a := range page {
		summaries = append(summaries, applicationSummary{
			ApplicationID: a.ApplicationID,
			Name:          a.Name,
			Description:   a.Description,
			Author:        a.Author,
			HomePageURL:   a.HomePageURL,
			SpdxLicenseID: a.SpdxLicenseID,
			Labels:        a.Labels,
			CreationTime:  isoTimestamp(a.CreationTime),
		})
	}

	resp := map[string]any{"applications": summaries}

	if end < len(apps) {
		resp["nextToken"] = apps[end-1].Name
	}

	return json.Marshal(resp)
}

// updateApplicationRequest is the request body for UpdateApplication.
type updateApplicationRequest struct {
	Description string   `json:"description"`
	Author      string   `json:"author"`
	HomePageURL string   `json:"homePageUrl"`
	ReadmeURL   string   `json:"readmeUrl"`
	Labels      []string `json:"labels"`
}

func (h *Handler) handleUpdateApplication(ctx context.Context, req *http.Request, body []byte) ([]byte, error) {
	name, nameErr := extractApplicationName(req)
	if nameErr != nil {
		return nil, nameErr
	}

	var updateReq updateApplicationRequest
	if err := json.Unmarshal(body, &updateReq); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	a, err := h.Backend.UpdateApplication(
		name,
		updateReq.Description,
		updateReq.Author,
		updateReq.HomePageURL,
		updateReq.ReadmeURL,
	)
	if err != nil {
		return nil, err
	}

	if updateReq.Labels != nil {
		a, err = h.Backend.UpdateApplicationLabels(name, updateReq.Labels)
		if err != nil {
			return nil, err
		}
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "serverlessrepo: updated application", "name", a.Name)

	resp := toApplicationResponse(a)

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteApplication(ctx context.Context, req *http.Request) error {
	name, nameErr := extractApplicationName(req)
	if nameErr != nil {
		return nameErr
	}

	if err := h.Backend.DeleteApplication(name); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "serverlessrepo: deleted application", "name", name)

	return nil
}

// createApplicationVersionRequest is the request body for CreateApplicationVersion.
type createApplicationVersionRequest struct {
	SourceCodeURL        string `json:"sourceCodeUrl"`
	SourceCodeArchiveURL string `json:"sourceCodeArchiveUrl"`
	TemplateURL          string `json:"templateUrl"`
}

func (h *Handler) handleCreateApplicationVersion(ctx context.Context, req *http.Request, body []byte) ([]byte, error) {
	appName, semanticVersion, err := extractPathExtra(req)
	if err != nil {
		return nil, err
	}

	if appName == "" {
		return nil, fmt.Errorf("%w: applicationId is required", errInvalidRequest)
	}

	if semanticVersion == "" {
		return nil, fmt.Errorf("%w: semanticVersion is required", errInvalidRequest)
	}

	var createReq createApplicationVersionRequest
	if jsonErr := json.Unmarshal(body, &createReq); jsonErr != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, jsonErr)
	}

	v, backendErr := h.Backend.CreateApplicationVersionWithOptions(
		appName,
		semanticVersion,
		CreateApplicationVersionOptions(createReq),
	)
	if backendErr != nil {
		return nil, backendErr
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "serverlessrepo: created application version",
		"app", appName, "version", v.SemanticVersion)

	b, marshalErr := json.Marshal(toVersionResponse(v))
	if marshalErr != nil {
		return nil, marshalErr
	}

	return b, errHTTP201
}

func (h *Handler) handleListApplicationVersions(req *http.Request) ([]byte, error) {
	appName, err := extractApplicationName(req)
	if err != nil {
		return nil, err
	}

	versions, err := h.Backend.ListApplicationVersions(appName)
	if err != nil {
		return nil, err
	}

	// Optional: filter by specific semantic version.
	if sv := req.URL.Query().Get(keySemanticVersion); sv != "" {
		filtered := versions[:0]

		for _, v := range versions {
			if v.SemanticVersion == sv {
				filtered = append(filtered, v)
			}
		}

		versions = filtered
	}

	// Apply pagination: nextToken is treated as the last-seen semantic version (exclusive cursor).
	nextToken := req.URL.Query().Get("nextToken")
	maxItems := parseMaxItems(req.URL.Query().Get("maxItems"), maxItemsDefault)

	start := 0

	if nextToken != "" {
		for i, v := range versions {
			if v.SemanticVersion == nextToken {
				start = i + 1

				break
			}
		}
	}

	end := min(start+maxItems, len(versions))

	page := versions[start:end]

	summaries := make([]map[string]any, 0, len(page))

	for _, v := range page {
		summaries = append(summaries, map[string]any{
			keyApplicationID:   v.ApplicationID,
			keySemanticVersion: v.SemanticVersion,
			"sourceCodeUrl":    v.SourceCodeURL,
			keyCreationTime:    isoTimestamp(v.CreationTime),
		})
	}

	resp := map[string]any{"versions": summaries}

	if end < len(versions) {
		resp["nextToken"] = versions[end-1].SemanticVersion
	}

	return json.Marshal(resp)
}

// toVersionResponse converts an ApplicationVersion to a map matching the AWS SAR Version shape.
func toVersionResponse(v *ApplicationVersion) map[string]any {
	return map[string]any{
		keyApplicationID:       v.ApplicationID,
		keySemanticVersion:     v.SemanticVersion,
		"sourceCodeUrl":        v.SourceCodeURL,
		"sourceCodeArchiveUrl": v.SourceCodeArchiveURL,
		keyTemplateURL:         v.TemplateURL,
		keyCreationTime:        isoTimestamp(v.CreationTime),
		"parameterDefinitions": v.ParameterDefinitions,
		"requiredCapabilities": v.RequiredCapabilities,
		"resourcesSupported":   v.ResourcesSupported,
	}
}

// createCFTemplateRequest is the request body for CreateCloudFormationTemplate.
type createCFTemplateRequest struct {
	SemanticVersion string `json:"semanticVersion"`
}

func (h *Handler) handleCreateCloudFormationTemplate(
	ctx context.Context,
	req *http.Request,
	body []byte,
) ([]byte, error) {
	appName, err := extractApplicationName(req)
	if err != nil {
		return nil, err
	}

	var createReq createCFTemplateRequest
	if jsonErr := json.Unmarshal(body, &createReq); jsonErr != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, jsonErr)
	}

	t, backendErr := h.Backend.CreateCloudFormationTemplate(appName, createReq.SemanticVersion)
	if backendErr != nil {
		return nil, backendErr
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "serverlessrepo: created CloudFormation template",
		"app", appName, "templateId", t.TemplateID)

	b, marshalErr := json.Marshal(map[string]any{
		keyApplicationID:   t.ApplicationID,
		"templateId":       t.TemplateID,
		keySemanticVersion: t.SemanticVersion,
		"status":           t.Status,
		keyCreationTime:    isoTimestamp(t.CreationTime),
		"expirationTime":   isoTimestamp(t.ExpirationTime),
		keyTemplateURL:     t.TemplateURL,
	})
	if marshalErr != nil {
		return nil, marshalErr
	}

	return b, errHTTP201
}

func (h *Handler) handleGetCloudFormationTemplate(req *http.Request) ([]byte, error) {
	appName, templateID, err := extractPathExtra(req)
	if err != nil {
		return nil, err
	}

	if appName == "" {
		return nil, fmt.Errorf("%w: applicationId is required", errInvalidRequest)
	}

	if templateID == "" {
		return nil, fmt.Errorf("%w: templateId is required", errInvalidRequest)
	}

	t, backendErr := h.Backend.GetCloudFormationTemplate(appName, templateID)
	if backendErr != nil {
		return nil, backendErr
	}

	// Dynamically compute the status: real SAR returns EXPIRED once past the expiration time.
	status := t.Status
	if status == templateStatusActive && time.Now().After(t.ExpirationTime) {
		status = templateStatusExpired
	}

	return json.Marshal(map[string]any{
		keyApplicationID:   t.ApplicationID,
		"templateId":       t.TemplateID,
		keySemanticVersion: t.SemanticVersion,
		"status":           status,
		keyCreationTime:    isoTimestamp(t.CreationTime),
		"expirationTime":   isoTimestamp(t.ExpirationTime),
		keyTemplateURL:     t.TemplateURL,
	})
}

// createCFChangeSetRequest is the request body for CreateCloudFormationChangeSet.
type createCFChangeSetRequest struct {
	StackName       string   `json:"stackName"`
	ChangeSetName   string   `json:"changeSetName"`
	SemanticVersion string   `json:"semanticVersion"`
	TemplateID      string   `json:"templateId"`
	Capabilities    []string `json:"capabilities"`
	Tags            []Tag    `json:"tags"`
}

func (h *Handler) handleCreateCloudFormationChangeSet(
	ctx context.Context,
	req *http.Request,
	body []byte,
) ([]byte, error) {
	appName, err := extractApplicationName(req)
	if err != nil {
		return nil, err
	}

	var createReq createCFChangeSetRequest
	if jsonErr := json.Unmarshal(body, &createReq); jsonErr != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, jsonErr)
	}

	if createReq.StackName == "" {
		return nil, fmt.Errorf("%w: stackName is required", errInvalidRequest)
	}

	cs, backendErr := h.Backend.CreateCloudFormationChangeSetWithOptions(
		appName,
		createReq.StackName,
		createReq.ChangeSetName,
		createReq.SemanticVersion,
		CreateCloudFormationChangeSetOptions{
			Capabilities: createReq.Capabilities,
			Tags:         createReq.Tags,
		},
	)
	if backendErr != nil {
		return nil, backendErr
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"serverlessrepo: created CloudFormation change set",
		"app",
		appName,
		"changeSetId",
		cs.ChangeSetID,
	)

	b, marshalErr := json.Marshal(map[string]any{
		keyApplicationID:   cs.ApplicationID,
		"changeSetId":      cs.ChangeSetID,
		keySemanticVersion: cs.SemanticVersion,
		"stackId":          cs.StackID,
	})
	if marshalErr != nil {
		return nil, marshalErr
	}

	return b, errHTTP201
}

// policyStatementRequest represents a policy statement in a PutApplicationPolicy request.
type policyStatementRequest struct {
	StatementID     string   `json:"statementId"`
	Actions         []string `json:"actions"`
	Principals      []string `json:"principals"`
	PrincipalOrgIDs []string `json:"principalOrgIDs"`
}

// putApplicationPolicyRequest is the request body for PutApplicationPolicy.
type putApplicationPolicyRequest struct {
	Statements []policyStatementRequest `json:"statements"`
}

func (h *Handler) handleGetApplicationPolicy(req *http.Request) ([]byte, error) {
	appName, err := extractApplicationName(req)
	if err != nil {
		return nil, err
	}

	stmts, backendErr := h.Backend.GetApplicationPolicy(appName)
	if backendErr != nil {
		return nil, backendErr
	}

	return json.Marshal(map[string]any{"statements": toPolicyStatementsResponse(stmts)})
}

func (h *Handler) handlePutApplicationPolicy(ctx context.Context, req *http.Request, body []byte) ([]byte, error) {
	appName, err := extractApplicationName(req)
	if err != nil {
		return nil, err
	}

	var putReq putApplicationPolicyRequest
	if jsonErr := json.Unmarshal(body, &putReq); jsonErr != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, jsonErr)
	}

	stmts := make([]*ApplicationPolicyStatement, 0, len(putReq.Statements))
	for _, s := range putReq.Statements {
		stmts = append(stmts, &ApplicationPolicyStatement{
			Actions:         s.Actions,
			Principals:      s.Principals,
			PrincipalOrgIDs: s.PrincipalOrgIDs,
			StatementID:     s.StatementID,
		})
	}

	result, backendErr := h.Backend.PutApplicationPolicy(appName, stmts)
	if backendErr != nil {
		return nil, backendErr
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "serverlessrepo: updated application policy", "app", appName)

	return json.Marshal(map[string]any{"statements": toPolicyStatementsResponse(result)})
}

func toPolicyStatementsResponse(stmts []*ApplicationPolicyStatement) []map[string]any {
	out := make([]map[string]any, 0, len(stmts))

	for _, s := range stmts {
		out = append(out, map[string]any{
			"actions":         nonNilStringSlice(s.Actions),
			"principals":      nonNilStringSlice(s.Principals),
			"principalOrgIDs": nonNilStringSlice(s.PrincipalOrgIDs),
			"statementId":     s.StatementID,
		})
	}

	return out
}

func (h *Handler) handleListApplicationDependencies(req *http.Request) ([]byte, error) {
	appName, err := extractApplicationName(req)
	if err != nil {
		return nil, err
	}

	semanticVersion := req.URL.Query().Get(keySemanticVersion)

	deps, backendErr := h.Backend.ListApplicationDependencies(appName, semanticVersion)
	if backendErr != nil {
		return nil, backendErr
	}

	depList := make([]map[string]any, 0, len(deps))
	for _, d := range deps {
		depList = append(depList, map[string]any{
			keyApplicationID:   d.ApplicationID,
			keySemanticVersion: d.SemanticVersion,
		})
	}

	return json.Marshal(map[string]any{"dependencies": depList})
}

// unshareApplicationRequest is the request body for UnshareApplication.
type unshareApplicationRequest struct {
	OrganizationID string `json:"organizationId"`
}

func (h *Handler) handleUnshareApplication(ctx context.Context, req *http.Request, body []byte) error {
	appName, err := extractApplicationName(req)
	if err != nil {
		return err
	}

	var unshareReq unshareApplicationRequest
	if jsonErr := json.Unmarshal(body, &unshareReq); jsonErr != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, jsonErr)
	}

	if unshareReq.OrganizationID == "" {
		return fmt.Errorf("%w: organizationId is required", errInvalidRequest)
	}

	if backendErr := h.Backend.UnshareApplication(appName, unshareReq.OrganizationID); backendErr != nil {
		return backendErr
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "serverlessrepo: unshared application",
		"app", appName, "orgId", unshareReq.OrganizationID)

	return nil
}
