package codeartifact

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	codeartifactMatchPriority = service.PriorityPathVersioned + 1

	pathV1Domain             = "/v1/domain"
	pathV1Domains            = "/v1/domains"
	pathV1DomainRepositories = "/v1/domain/repositories"
	pathV1DomainPermissions  = "/v1/domain/permissions/policy"
	pathV1Repository         = "/v1/repository"
	pathV1Repositories       = "/v1/repositories"
	pathV1RepositoryEndpoint = "/v1/repository/endpoint"
	pathV1Tags               = "/v1/tags"
	pathV1Tag                = "/v1/tag"
	pathV1Untag              = "/v1/untag"
	pathV1AuthToken          = "/v1/authorization-token" //nolint:gosec // not a credential
)

const (
	// stubTokenExpireHours is the expiry duration for stub authorization tokens.
	stubTokenExpireHours = 12
)

var (
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for AWS CodeArtifact operations (REST-JSON protocol).
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new CodeArtifact handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "CodeArtifact" }

// GetSupportedOperations returns the list of supported CodeArtifact operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateDomain",
		"DescribeDomain",
		"DeleteDomain",
		"ListDomains",
		"CreateRepository",
		"DescribeRepository",
		"DeleteRepository",
		"ListRepositoriesInDomain",
		"ListRepositories",
		"GetRepositoryEndpoint",
		"GetAuthorizationToken",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"GetDomainPermissionsPolicy",
		"PutDomainPermissionsPolicy",
		"DeleteDomainPermissionsPolicy",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "codeartifact" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this CodeArtifact instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches AWS CodeArtifact REST requests.
// CodeArtifact uses /v1/ paths that are distinct from Batch and AppSync.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return path == pathV1Domain ||
			strings.HasPrefix(path, pathV1Domain+"/") ||
			path == pathV1Domains ||
			path == pathV1Repository ||
			strings.HasPrefix(path, pathV1Repository+"/") ||
			path == pathV1Repositories ||
			path == pathV1Tags ||
			path == pathV1Tag ||
			path == pathV1Untag ||
			path == pathV1AuthToken
	}
}

// MatchPriority returns the routing priority (higher than Batch to avoid conflicts on /v1/).
func (h *Handler) MatchPriority() int { return codeartifactMatchPriority }

// codeartifactRoute holds the parsed information from a CodeArtifact REST request.
type codeartifactRoute struct {
	operation string
}

// parseCodeArtifactPath maps HTTP method + path to an operation name.
func parseCodeArtifactPath(method, path string) codeartifactRoute {
	switch path {
	case pathV1Domain:
		return parseDomainRoute(method)
	case pathV1Domains:
		return codeartifactRoute{operation: "ListDomains"}
	case pathV1DomainRepositories:
		return codeartifactRoute{operation: "ListRepositoriesInDomain"}
	case pathV1DomainPermissions:
		return parseDomainPermissionsRoute(method)
	case pathV1Repository:
		return parseRepositoryRoute(method)
	case pathV1RepositoryEndpoint:
		return codeartifactRoute{operation: "GetRepositoryEndpoint"}
	case pathV1Repositories:
		return codeartifactRoute{operation: "ListRepositories"}
	case pathV1Tags:
		return codeartifactRoute{operation: "ListTagsForResource"}
	case pathV1Tag:
		return codeartifactRoute{operation: "TagResource"}
	case pathV1Untag:
		return codeartifactRoute{operation: "UntagResource"}
	case pathV1AuthToken:
		return codeartifactRoute{operation: "GetAuthorizationToken"}
	}

	return codeartifactRoute{operation: "Unknown"}
}

func parseDomainRoute(method string) codeartifactRoute {
	switch method {
	case http.MethodPost:
		return codeartifactRoute{operation: "CreateDomain"}
	case http.MethodGet:
		return codeartifactRoute{operation: "DescribeDomain"}
	case http.MethodDelete:
		return codeartifactRoute{operation: "DeleteDomain"}
	}

	return codeartifactRoute{operation: "Unknown"}
}

func parseDomainPermissionsRoute(method string) codeartifactRoute {
	switch method {
	case http.MethodGet:
		return codeartifactRoute{operation: "GetDomainPermissionsPolicy"}
	case http.MethodPut:
		return codeartifactRoute{operation: "PutDomainPermissionsPolicy"}
	case http.MethodDelete:
		return codeartifactRoute{operation: "DeleteDomainPermissionsPolicy"}
	}

	return codeartifactRoute{operation: "Unknown"}
}

func parseRepositoryRoute(method string) codeartifactRoute {
	switch method {
	case http.MethodPost:
		return codeartifactRoute{operation: "CreateRepository"}
	case http.MethodGet:
		return codeartifactRoute{operation: "DescribeRepository"}
	case http.MethodDelete:
		return codeartifactRoute{operation: "DeleteRepository"}
	}

	return codeartifactRoute{operation: "Unknown"}
}

// ExtractOperation extracts the CodeArtifact operation name from the REST path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := parseCodeArtifactPath(c.Request().Method, c.Request().URL.Path)

	return r.operation
}

// ExtractResource extracts the primary resource identifier from the URL path or query params.
func (h *Handler) ExtractResource(c *echo.Context) string {
	q := c.Request().URL.Query()
	if domain := q.Get("domain"); domain != "" {
		if repo := q.Get("repository"); repo != "" {
			return domain + "/" + repo
		}

		return domain
	}

	return q.Get("resourceArn")
}

// Handler returns the Echo handler function for CodeArtifact requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		log := logger.Load(c.Request().Context())
		path := c.Request().URL.Path
		route := parseCodeArtifactPath(c.Request().Method, path)

		log.Debug("codeartifact request", "operation", route.operation, "path", path)

		var body []byte
		if c.Request().Body != nil {
			decoder := json.NewDecoder(c.Request().Body)
			var raw json.RawMessage
			if err := decoder.Decode(&raw); err == nil {
				body = raw
			}
		}

		return h.dispatch(c, route, body)
	}
}

//nolint:cyclop // dispatch table for 17 REST operations is inherently wide
func (h *Handler) dispatch(c *echo.Context, route codeartifactRoute, body []byte) error {
	q := c.Request().URL.Query()

	switch route.operation {
	case "CreateDomain":
		return h.handleCreateDomain(c, q.Get("domain"), body)
	case "DescribeDomain":
		return h.handleDescribeDomain(c, q.Get("domain"))
	case "DeleteDomain":
		return h.handleDeleteDomain(c, q.Get("domain"))
	case "ListDomains":
		return h.handleListDomains(c)
	case "CreateRepository":
		return h.handleCreateRepository(c, q.Get("domain"), q.Get("repository"), body)
	case "DescribeRepository":
		return h.handleDescribeRepository(c, q.Get("domain"), q.Get("repository"))
	case "DeleteRepository":
		return h.handleDeleteRepository(c, q.Get("domain"), q.Get("repository"))
	case "ListRepositoriesInDomain":
		return h.handleListRepositoriesInDomain(c, q.Get("domain"))
	case "ListRepositories":
		return h.handleListRepositories(c)
	case "GetRepositoryEndpoint":
		return h.handleGetRepositoryEndpoint(c, q.Get("domain"), q.Get("repository"), q.Get("format"))
	case "GetAuthorizationToken":
		return h.handleGetAuthorizationToken(c, q.Get("domain"))
	case "ListTagsForResource":
		return h.handleListTagsForResource(c, q.Get("resourceArn"))
	case "TagResource":
		return h.handleTagResource(c, q.Get("resourceArn"), body)
	case "UntagResource":
		return h.handleUntagResource(c, q.Get("resourceArn"), body)
	case "GetDomainPermissionsPolicy":
		return h.handleGetDomainPermissionsPolicy(c, q.Get("domain"))
	case "PutDomainPermissionsPolicy":
		return h.handlePutDomainPermissionsPolicy(c, q.Get("domain"))
	case "DeleteDomainPermissionsPolicy":
		return h.handleDeleteDomainPermissionsPolicy(c, q.Get("domain"))
	default:
		return c.JSON(http.StatusNotFound, errResp("ResourceNotFoundException", "unknown operation: "+route.operation))
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errResp("ResourceNotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errResp("ConflictException", err.Error()))
	case errors.Is(err, errInvalidRequest):
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errResp("InternalFailure", err.Error()))
	}
}

func errResp(code, msg string) map[string]string {
	return map[string]string{"code": code, "message": msg}
}

// epochSeconds returns the Unix epoch timestamp as a float64 for JSON serialization.
// The AWS CodeArtifact SDK deserializes timestamps as JSON numbers (epoch seconds).
func epochSeconds(ts time.Time) float64 {
	return float64(ts.Unix())
}

// --- Domain handlers ---

type createDomainBody struct {
	EncryptionKey string           `json:"encryptionKey"`
	Tags          []map[string]any `json:"tags"`
}

func tagsFromSlice(raw []map[string]any) map[string]string {
	out := make(map[string]string, len(raw))
	for _, entry := range raw {
		k, _ := entry["key"].(string)
		v, _ := entry["value"].(string)
		if k != "" {
			out[k] = v
		}
	}

	return out
}

func domainToMap(d *Domain) map[string]any {
	m := map[string]any{
		"arn":             d.ARN,
		"name":            d.Name,
		"owner":           d.Owner,
		"status":          d.Status,
		"createdTime":     epochSeconds(d.CreatedTime),
		"assetSizeBytes":  d.AssetSizeBytes,
		"repositoryCount": 0,
	}
	if d.EncryptionKey != "" {
		m["encryptionKey"] = d.EncryptionKey
	}
	if d.S3BucketARN != "" {
		m["s3BucketArn"] = d.S3BucketARN
	}

	return m
}

func (h *Handler) handleCreateDomain(c *echo.Context, name string, body []byte) error {
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain name is required"))
	}

	var in createDomainBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	d, err := h.Backend.CreateDomain(name, in.EncryptionKey, tagsFromSlice(in.Tags))
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"domain": domainToMap(d),
	})
}

func (h *Handler) handleDescribeDomain(c *echo.Context, name string) error {
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain name is required"))
	}

	d, err := h.Backend.DescribeDomain(name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"domain": domainToMap(d),
	})
}

func (h *Handler) handleListDomains(c *echo.Context) error {
	domains := h.Backend.ListDomains()
	items := make([]map[string]any, 0, len(domains))

	for _, d := range domains {
		items = append(items, map[string]any{
			"arn":    d.ARN,
			"name":   d.Name,
			"owner":  d.Owner,
			"status": d.Status,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"domains": items,
	})
}

func (h *Handler) handleDeleteDomain(c *echo.Context, name string) error {
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain name is required"))
	}

	d, err := h.Backend.DeleteDomain(name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"domain": domainToMap(d),
	})
}

// --- Repository handlers ---

type createRepositoryBody struct {
	Description string           `json:"description"`
	Tags        []map[string]any `json:"tags"`
}

func repoToMap(r *Repository) map[string]any {
	m := map[string]any{
		"arn":                  r.ARN,
		"name":                 r.Name,
		"domainName":           r.DomainName,
		"domainOwner":          r.DomainOwner,
		"administratorAccount": r.AdministratorAccount,
	}
	if r.Description != "" {
		m["description"] = r.Description
	}

	return m
}

func (h *Handler) handleCreateRepository(c *echo.Context, domainName, repoName string, body []byte) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}

	var in createRepositoryBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	r, err := h.Backend.CreateRepository(domainName, repoName, in.Description, tagsFromSlice(in.Tags))
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"repository": repoToMap(r),
	})
}

func (h *Handler) handleDescribeRepository(c *echo.Context, domainName, repoName string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}

	r, err := h.Backend.DescribeRepository(domainName, repoName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"repository": repoToMap(r),
	})
}

func (h *Handler) handleDeleteRepository(c *echo.Context, domainName, repoName string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}

	r, err := h.Backend.DeleteRepository(domainName, repoName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"repository": repoToMap(r),
	})
}

func (h *Handler) handleListRepositoriesInDomain(c *echo.Context, domainName string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}

	repos := h.Backend.ListRepositoriesInDomain(domainName)
	items := make([]map[string]any, 0, len(repos))

	for _, r := range repos {
		items = append(items, map[string]any{
			"arn":         r.ARN,
			"name":        r.Name,
			"domainName":  r.DomainName,
			"domainOwner": r.DomainOwner,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"repositories": items,
	})
}

func (h *Handler) handleListRepositories(c *echo.Context) error {
	repos := h.Backend.ListRepositories()
	items := make([]map[string]any, 0, len(repos))

	for _, r := range repos {
		items = append(items, map[string]any{
			"arn":         r.ARN,
			"name":        r.Name,
			"domainName":  r.DomainName,
			"domainOwner": r.DomainOwner,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"repositories": items,
	})
}

func (h *Handler) handleGetRepositoryEndpoint(c *echo.Context, domainName, repoName, format string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}
	if format == "" {
		format = "generic"
	}

	_, err := h.Backend.DescribeRepository(domainName, repoName)
	if err != nil {
		return h.handleError(c, err)
	}

	endpoint := fmt.Sprintf(
		"https://%s-%s.d.codeartifact.%s.amazonaws.com/%s/%s/",
		domainName, h.Backend.accountID, h.Backend.region, format, repoName,
	)

	return c.JSON(http.StatusOK, map[string]any{
		"repositoryEndpoint": endpoint,
	})
}

func (h *Handler) handleGetAuthorizationToken(c *echo.Context, domainName string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}

	_, err := h.Backend.DescribeDomain(domainName)
	if err != nil {
		return h.handleError(c, err)
	}

	// Return a plausible stub token.
	return c.JSON(http.StatusOK, map[string]any{
		"authorizationToken": "codeartifact-stub-token-" + domainName,
		"expiration":         epochSeconds(time.Now().Add(stubTokenExpireHours * time.Hour)),
	})
}

// --- Tag handlers ---

type tagResourceBody struct {
	Tags []map[string]any `json:"tags"`
}

type untagResourceBody struct {
	TagKeys []string `json:"tagKeys"`
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	if resourceARN == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "resourceArn is required"))
	}

	kv, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return h.handleError(c, err)
	}

	tagList := make([]map[string]string, 0, len(kv))
	for k, v := range kv {
		tagList = append(tagList, map[string]string{"key": k, "value": v})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"tags": tagList,
	})
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string, body []byte) error {
	if resourceARN == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "resourceArn is required"))
	}

	var in tagResourceBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if err := h.Backend.TagResource(resourceARN, tagsFromSlice(in.Tags)); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string, body []byte) error {
	if resourceARN == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "resourceArn is required"))
	}

	var in untagResourceBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if err := h.Backend.UntagResource(resourceARN, in.TagKeys); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Permissions policy handlers ---
// These are stub implementations that satisfy Terraform provider requirements.

func (h *Handler) handleGetDomainPermissionsPolicy(c *echo.Context, domainName string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}

	_, err := h.Backend.DescribeDomain(domainName)
	if err != nil {
		return h.handleError(c, err)
	}

	// Return empty policy - no policy has been set.
	return c.JSON(http.StatusOK, map[string]any{
		"policy": map[string]any{
			"document": `{"Version":"2012-10-17","Statement":[]}`,
			"revision": "1",
		},
	})
}

func (h *Handler) handlePutDomainPermissionsPolicy(c *echo.Context, domainName string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}

	_, err := h.Backend.DescribeDomain(domainName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"policy": map[string]any{
			"document": `{"Version":"2012-10-17","Statement":[]}`,
			"revision": "1",
		},
	})
}

func (h *Handler) handleDeleteDomainPermissionsPolicy(c *echo.Context, domainName string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}

	_, err := h.Backend.DescribeDomain(domainName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"policy": map[string]any{
			"document": `{"Version":"2012-10-17","Statement":[]}`,
			"revision": "1",
		},
	})
}
