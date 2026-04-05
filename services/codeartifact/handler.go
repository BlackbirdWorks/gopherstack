package codeartifact

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	codeartifactMatchPriority = service.PriorityPathVersioned + 1

	pathV1Domain                       = "/v1/domain"
	pathV1Domains                      = "/v1/domains"
	pathV1DomainRepositories           = "/v1/domain/repositories"
	pathV1DomainPermissions            = "/v1/domain/permissions/policy"
	pathV1Repository                   = "/v1/repository"
	pathV1Repositories                 = "/v1/repositories"
	pathV1RepositoryEndpoint           = "/v1/repository/endpoint"
	pathV1RepositoryExternalConnection = "/v1/repository/external-connection"
	pathV1RepositoryPermissions        = "/v1/repository/permissions/policy"
	pathV1Tags                         = "/v1/tags"
	pathV1Tag                          = "/v1/tag"
	pathV1Untag                        = "/v1/untag"
	pathV1AuthToken                    = "/v1/authorization-token" //nolint:gosec // not a credential
	pathV1PackageGroup                 = "/v1/package-group"
	pathV1Package                      = "/v1/package"
	pathV1PackageVersion               = "/v1/package/version"
	pathV1PackageVersionsCopy          = "/v1/package/versions/copy"
	pathV1PackageVersionsDelete        = "/v1/package/versions/delete"
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
	ops     map[string]func(*echo.Context, []byte) error
	Backend *InMemoryBackend
}

// NewHandler creates a new CodeArtifact handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "CodeArtifact" }

// Reset clears all backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns the list of supported CodeArtifact operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AssociateExternalConnection",
		"CopyPackageVersions",
		"CreateDomain",
		"CreatePackageGroup",
		"CreateRepository",
		"DeleteDomain",
		"DeletePackage",
		"DeletePackageGroup",
		"DeletePackageVersions",
		"DeleteRepository",
		"DeleteRepositoryPermissionsPolicy",
		"DescribeDomain",
		"DescribePackage",
		"DescribePackageGroup",
		"DescribePackageVersion",
		"DescribeRepository",
		"GetAuthorizationToken",
		"GetDomainPermissionsPolicy",
		"GetRepositoryEndpoint",
		"GetRepositoryPermissionsPolicy",
		"ListDomains",
		"ListRepositories",
		"ListRepositoriesInDomain",
		"ListTagsForResource",
		"PutDomainPermissionsPolicy",
		"DeleteDomainPermissionsPolicy",
		"PutRepositoryPermissionsPolicy",
		"TagResource",
		"UntagResource",
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
			path == pathV1AuthToken ||
			path == pathV1PackageGroup ||
			path == pathV1Package ||
			path == pathV1PackageVersion ||
			path == pathV1PackageVersionsCopy ||
			path == pathV1PackageVersionsDelete
	}
}

// MatchPriority returns the routing priority (higher than Batch to avoid conflicts on /v1/).
func (h *Handler) MatchPriority() int { return codeartifactMatchPriority }

// codeartifactRoute holds the parsed information from a CodeArtifact REST request.
type codeartifactRoute struct {
	operation string
}

// parseCodeArtifactPath maps HTTP method + path to an operation name.
// It delegates to sub-parsers to stay within cyclomatic complexity limits.
func parseCodeArtifactPath(method, path string) codeartifactRoute {
	if strings.HasPrefix(path, "/v1/package") {
		return parsePackageOpPath(method, path)
	}

	return parseDomainRepoPath(method, path)
}

// parseDomainRepoPath handles domain, repository, tag and auth token routes.
func parseDomainRepoPath(method, path string) codeartifactRoute {
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
	case pathV1RepositoryExternalConnection:
		return codeartifactRoute{operation: "AssociateExternalConnection"}
	case pathV1RepositoryPermissions:
		return parseRepositoryPermissionsRoute(method)
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

// parsePackageOpPath handles package, package-group, and package-version routes.
func parsePackageOpPath(method, path string) codeartifactRoute {
	switch path {
	case pathV1PackageGroup:
		return parsePackageGroupRoute(method)
	case pathV1Package:
		return parsePackageRoute(method)
	case pathV1PackageVersion:
		return codeartifactRoute{operation: "DescribePackageVersion"}
	case pathV1PackageVersionsCopy:
		return codeartifactRoute{operation: "CopyPackageVersions"}
	case pathV1PackageVersionsDelete:
		return codeartifactRoute{operation: "DeletePackageVersions"}
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

func parseRepositoryPermissionsRoute(method string) codeartifactRoute {
	switch method {
	case http.MethodGet:
		return codeartifactRoute{operation: "GetRepositoryPermissionsPolicy"}
	case http.MethodPut:
		return codeartifactRoute{operation: "PutRepositoryPermissionsPolicy"}
	case http.MethodDelete:
		return codeartifactRoute{operation: "DeleteRepositoryPermissionsPolicy"}
	}

	return codeartifactRoute{operation: "Unknown"}
}

func parsePackageGroupRoute(method string) codeartifactRoute {
	switch method {
	case http.MethodPost:
		return codeartifactRoute{operation: "CreatePackageGroup"}
	case http.MethodGet:
		return codeartifactRoute{operation: "DescribePackageGroup"}
	case http.MethodDelete:
		return codeartifactRoute{operation: "DeletePackageGroup"}
	}

	return codeartifactRoute{operation: "Unknown"}
}

func parsePackageRoute(method string) codeartifactRoute {
	switch method {
	case http.MethodGet:
		return codeartifactRoute{operation: "DescribePackage"}
	case http.MethodDelete:
		return codeartifactRoute{operation: "DeletePackage"}
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

func (h *Handler) dispatch(c *echo.Context, route codeartifactRoute, body []byte) error {
	fn, ok := h.ops[route.operation]
	if !ok {
		return c.JSON(http.StatusNotFound, errResp("ResourceNotFoundException", "unknown operation: "+route.operation))
	}

	return fn(c, body)
}

func (h *Handler) buildOps() map[string]func(*echo.Context, []byte) error {
	ops := h.buildDomainRepoOps()
	maps.Copy(ops, h.buildPackageOps())

	return ops
}

func (h *Handler) buildDomainRepoOps() map[string]func(*echo.Context, []byte) error {
	return map[string]func(*echo.Context, []byte) error{
		"CreateDomain": func(c *echo.Context, body []byte) error {
			return h.handleCreateDomain(c, c.Request().URL.Query().Get("domain"), body)
		},
		"DescribeDomain": func(c *echo.Context, _ []byte) error {
			return h.handleDescribeDomain(c, c.Request().URL.Query().Get("domain"))
		},
		"DeleteDomain": func(c *echo.Context, _ []byte) error {
			return h.handleDeleteDomain(c, c.Request().URL.Query().Get("domain"))
		},
		"ListDomains": func(c *echo.Context, _ []byte) error {
			return h.handleListDomains(c)
		},
		"CreateRepository": func(c *echo.Context, body []byte) error {
			q := c.Request().URL.Query()

			return h.handleCreateRepository(c, q.Get("domain"), q.Get("repository"), body)
		},
		"DescribeRepository": func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleDescribeRepository(c, q.Get("domain"), q.Get("repository"))
		},
		"DeleteRepository": func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleDeleteRepository(c, q.Get("domain"), q.Get("repository"))
		},
		"ListRepositoriesInDomain": func(c *echo.Context, _ []byte) error {
			return h.handleListRepositoriesInDomain(c, c.Request().URL.Query().Get("domain"))
		},
		"ListRepositories": func(c *echo.Context, _ []byte) error {
			return h.handleListRepositories(c)
		},
		"GetRepositoryEndpoint": func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleGetRepositoryEndpoint(c, q.Get("domain"), q.Get("repository"), q.Get("format"))
		},
		"GetAuthorizationToken": func(c *echo.Context, _ []byte) error {
			return h.handleGetAuthorizationToken(c, c.Request().URL.Query().Get("domain"))
		},
		"ListTagsForResource": func(c *echo.Context, _ []byte) error {
			return h.handleListTagsForResource(c, c.Request().URL.Query().Get("resourceArn"))
		},
		"TagResource": func(c *echo.Context, body []byte) error {
			return h.handleTagResource(c, c.Request().URL.Query().Get("resourceArn"), body)
		},
		"UntagResource": func(c *echo.Context, body []byte) error {
			return h.handleUntagResource(c, c.Request().URL.Query().Get("resourceArn"), body)
		},
		"GetDomainPermissionsPolicy": func(c *echo.Context, _ []byte) error {
			return h.handleGetDomainPermissionsPolicy(c, c.Request().URL.Query().Get("domain"))
		},
		"PutDomainPermissionsPolicy": func(c *echo.Context, body []byte) error {
			return h.handlePutDomainPermissionsPolicy(c, c.Request().URL.Query().Get("domain"), body)
		},
		"DeleteDomainPermissionsPolicy": func(c *echo.Context, _ []byte) error {
			return h.handleDeleteDomainPermissionsPolicy(c, c.Request().URL.Query().Get("domain"))
		},
		"AssociateExternalConnection": func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleAssociateExternalConnection(
				c,
				q.Get("domain"),
				q.Get("repository"),
				q.Get("externalConnection"),
			)
		},
		"GetRepositoryPermissionsPolicy": func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleGetRepositoryPermissionsPolicy(c, q.Get("domain"), q.Get("repository"))
		},
		"PutRepositoryPermissionsPolicy": func(c *echo.Context, body []byte) error {
			q := c.Request().URL.Query()

			return h.handlePutRepositoryPermissionsPolicy(c, q.Get("domain"), q.Get("repository"), body)
		},
		"DeleteRepositoryPermissionsPolicy": func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleDeleteRepositoryPermissionsPolicy(c, q.Get("domain"), q.Get("repository"))
		},
	}
}

func (h *Handler) buildPackageOps() map[string]func(*echo.Context, []byte) error {
	return map[string]func(*echo.Context, []byte) error{
		"CopyPackageVersions": func(c *echo.Context, body []byte) error {
			q := c.Request().URL.Query()

			return h.handleCopyPackageVersions(
				c, q.Get("domain"), q.Get("sourceRepository"), q.Get("destinationRepository"),
				q.Get("format"), q.Get("namespace"), q.Get("package"), body,
			)
		},
		"CreatePackageGroup": func(c *echo.Context, body []byte) error {
			return h.handleCreatePackageGroup(c, c.Request().URL.Query().Get("domain"), body)
		},
		"DeletePackage": func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleDeletePackage(
				c, q.Get("domain"), q.Get("repository"), q.Get("format"), q.Get("namespace"), q.Get("package"),
			)
		},
		"DeletePackageGroup": func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleDeletePackageGroup(c, q.Get("domain"), q.Get("packageGroup"))
		},
		"DeletePackageVersions": func(c *echo.Context, body []byte) error {
			q := c.Request().URL.Query()

			return h.handleDeletePackageVersions(
				c, q.Get("domain"), q.Get("repository"), q.Get("format"), q.Get("namespace"), q.Get("package"), body,
			)
		},
		"DescribePackage": func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleDescribePackage(
				c, q.Get("domain"), q.Get("repository"), q.Get("format"), q.Get("namespace"), q.Get("package"),
			)
		},
		"DescribePackageGroup": func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleDescribePackageGroup(c, q.Get("domain"), q.Get("packageGroup"))
		},
		"DescribePackageVersion": func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleDescribePackageVersion(
				c, q.Get("domain"), q.Get("repository"), q.Get("format"),
				q.Get("namespace"), q.Get("package"), q.Get("version"),
			)
		},
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errResp("ResourceNotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errResp("ConflictException", err.Error()))
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", err.Error()))
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

func domainToMap(d *Domain, repoCount int) map[string]any {
	m := map[string]any{
		"arn":             d.ARN,
		"name":            d.Name,
		"owner":           d.Owner,
		"status":          d.Status,
		"createdTime":     epochSeconds(d.CreatedTime),
		"assetSizeBytes":  d.AssetSizeBytes,
		"repositoryCount": repoCount,
	}
	if d.EncryptionKey != "" {
		m["encryptionKey"] = d.EncryptionKey
	}
	if d.S3BucketARN != "" {
		m["s3BucketArn"] = d.S3BucketARN
	}

	return m
}

func domainSummaryToMap(d *Domain) map[string]any {
	m := map[string]any{
		"arn":         d.ARN,
		"name":        d.Name,
		"owner":       d.Owner,
		"status":      d.Status,
		"createdTime": epochSeconds(d.CreatedTime),
	}
	if d.EncryptionKey != "" {
		m["encryptionKey"] = d.EncryptionKey
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
		"domain": domainToMap(d, 0),
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

	repoCount := h.Backend.CountRepositoriesInDomain(name)

	return c.JSON(http.StatusOK, map[string]any{
		"domain": domainToMap(d, repoCount),
	})
}

func (h *Handler) handleListDomains(c *echo.Context) error {
	domains := h.Backend.ListDomains()
	items := make([]map[string]any, 0, len(domains))

	for _, d := range domains {
		items = append(items, domainSummaryToMap(d))
	}

	return c.JSON(http.StatusOK, map[string]any{
		"domains": items,
	})
}

func (h *Handler) handleDeleteDomain(c *echo.Context, name string) error {
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain name is required"))
	}

	repoCount := h.Backend.CountRepositoriesInDomain(name)

	d, err := h.Backend.DeleteDomain(name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"domain": domainToMap(d, repoCount),
	})
}

// --- Repository handlers ---

type createRepositoryBody struct {
	Description string           `json:"description"`
	Tags        []map[string]any `json:"tags"`
}

func repoToMap(r *Repository, connections []ExternalConnection) map[string]any {
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

	extConns := make([]map[string]any, 0, len(connections))
	for _, ec := range connections {
		extConns = append(extConns, map[string]any{
			"externalConnectionName": ec.ExternalConnectionName,
			"packageFormat":          ec.PackageFormat,
			"status":                 ec.Status,
		})
	}
	m["externalConnections"] = extConns

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
		"repository": repoToMap(r, h.Backend.GetExternalConnections(domainName, repoName)),
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
		"repository": repoToMap(r, h.Backend.GetExternalConnections(domainName, repoName)),
	})
}

func (h *Handler) handleDeleteRepository(c *echo.Context, domainName, repoName string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}

	conns := h.Backend.GetExternalConnections(domainName, repoName)

	r, err := h.Backend.DeleteRepository(domainName, repoName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"repository": repoToMap(r, conns),
	})
}

func (h *Handler) handleListRepositoriesInDomain(c *echo.Context, domainName string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}

	repos, err := h.Backend.ListRepositoriesInDomain(domainName)
	if err != nil {
		return h.handleError(c, err)
	}

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
	slices.SortFunc(tagList, func(a, b map[string]string) int {
		return strings.Compare(a["key"], b["key"])
	})

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

func (h *Handler) handleGetDomainPermissionsPolicy(c *echo.Context, domainName string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}

	pol, err := h.Backend.GetDomainPermissionsPolicy(domainName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"policy": map[string]any{
			"document":    pol.Document,
			"revision":    pol.Revision,
			"resourceArn": pol.ResourceARN,
		},
	})
}

type putDomainPermissionsPolicyBody struct {
	PolicyDocument string `json:"policyDocument"`
}

func (h *Handler) handlePutDomainPermissionsPolicy(c *echo.Context, domainName string, body []byte) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}

	var in putDomainPermissionsPolicyBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	if in.PolicyDocument == "" {
		in.PolicyDocument = `{"Version":"2012-10-17","Statement":[]}`
	}

	pol, err := h.Backend.PutDomainPermissionsPolicy(domainName, in.PolicyDocument)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"policy": map[string]any{
			"document":    pol.Document,
			"revision":    pol.Revision,
			"resourceArn": pol.ResourceARN,
		},
	})
}

func (h *Handler) handleDeleteDomainPermissionsPolicy(c *echo.Context, domainName string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}

	pol, err := h.Backend.DeleteDomainPermissionsPolicy(domainName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"policy": map[string]any{
			"document":    pol.Document,
			"revision":    pol.Revision,
			"resourceArn": pol.ResourceARN,
		},
	})
}

// --- Package group handlers ---

type createPackageGroupBody struct {
	Pattern     string           `json:"pattern"`
	Description string           `json:"description"`
	ContactInfo string           `json:"contactInfo"`
	Tags        []map[string]any `json:"tags"`
}

func packageGroupToMap(pg *PackageGroup) map[string]any {
	m := map[string]any{
		"arn":         pg.ARN,
		"domainName":  pg.DomainName,
		"domainOwner": pg.DomainOwner,
		"pattern":     pg.Pattern,
	}
	if pg.Description != "" {
		m["description"] = pg.Description
	}
	if pg.ContactInfo != "" {
		m["contactInfo"] = pg.ContactInfo
	}

	return m
}

func (h *Handler) handleCreatePackageGroup(c *echo.Context, domainName string, body []byte) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}

	var in createPackageGroupBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	pattern := in.Pattern
	if pattern == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "pattern is required"))
	}

	pg, err := h.Backend.CreatePackageGroup(domainName, pattern, in.Description, in.ContactInfo, tagsFromSlice(in.Tags))
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"packageGroup": packageGroupToMap(pg),
	})
}

func (h *Handler) handleDescribePackageGroup(c *echo.Context, domainName, pattern string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if pattern == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "packageGroup is required"))
	}

	pg, err := h.Backend.DescribePackageGroup(domainName, pattern)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"packageGroup": packageGroupToMap(pg),
	})
}

func (h *Handler) handleDeletePackageGroup(c *echo.Context, domainName, pattern string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if pattern == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "packageGroup is required"))
	}

	pg, err := h.Backend.DeletePackageGroup(domainName, pattern)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"packageGroup": packageGroupToMap(pg),
	})
}

// --- Package handlers ---

func packageToMap(pkg *Package) map[string]any {
	m := map[string]any{
		"format":      pkg.Format,
		"name":        pkg.Name,
		"domainName":  pkg.DomainName,
		"domainOwner": pkg.DomainOwner,
		"repository":  pkg.Repository,
	}
	if pkg.Namespace != "" {
		m["namespace"] = pkg.Namespace
	}

	return m
}

func (h *Handler) handleDescribePackage(c *echo.Context, domainName, repoName, format, namespace, name string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}

	pkg, err := h.Backend.DescribePackage(domainName, repoName, format, namespace, name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"package": packageToMap(pkg),
	})
}

func (h *Handler) handleDeletePackage(c *echo.Context, domainName, repoName, format, namespace, name string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}

	pkg, err := h.Backend.DeletePackage(domainName, repoName, format, namespace, name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"deletedPackage": packageToMap(pkg),
	})
}

// --- Package version handlers ---

func packageVersionToMap(pv *PackageVersion) map[string]any {
	m := map[string]any{
		"version":     pv.Version,
		"status":      pv.Status,
		"format":      pv.Format,
		"publishedAt": epochSeconds(pv.PublishedAt),
		"revision":    pv.Revision,
	}
	if pv.Namespace != "" {
		m["namespace"] = pv.Namespace
	}

	return m
}

func (h *Handler) handleDescribePackageVersion(
	c *echo.Context,
	domainName, repoName, format, namespace, name, version string,
) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}
	if version == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "version is required"))
	}

	pv, err := h.Backend.DescribePackageVersion(domainName, repoName, format, namespace, name, version)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"packageVersion": packageVersionToMap(pv),
	})
}

type deletePackageVersionsBody struct {
	Versions []string `json:"versions"`
}

func (h *Handler) handleDeletePackageVersions(
	c *echo.Context,
	domainName, repoName, format, namespace, name string,
	body []byte,
) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}

	var in deletePackageVersionsBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	failed, err := h.Backend.DeletePackageVersions(domainName, repoName, format, namespace, name, in.Versions)
	if err != nil {
		return h.handleError(c, err)
	}

	failedList := make([]map[string]string, 0, len(failed))
	for v, code := range failed {
		failedList = append(failedList, map[string]string{"version": v, "errorCode": code})
	}

	successList := make([]map[string]string, 0)
	for _, v := range in.Versions {
		if _, ok := failed[v]; !ok {
			successList = append(successList, map[string]string{"version": v, "status": "Deleted"})
		}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"failedVersions":     failedList,
		"successfulVersions": successList,
	})
}

type copyPackageVersionsBody struct {
	Versions []string `json:"versions"`
}

func (h *Handler) handleCopyPackageVersions(
	c *echo.Context,
	domainName, srcRepo, dstRepo, format, namespace, name string,
	body []byte,
) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if srcRepo == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "sourceRepository is required"))
	}
	if dstRepo == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "destinationRepository is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}

	var in copyPackageVersionsBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	failed, err := h.Backend.CopyPackageVersions(domainName, srcRepo, dstRepo, format, namespace, name, in.Versions)
	if err != nil {
		return h.handleError(c, err)
	}

	failedList := make([]map[string]string, 0, len(failed))
	for v, code := range failed {
		failedList = append(failedList, map[string]string{"version": v, "errorCode": code})
	}

	successList := make([]map[string]string, 0)
	for _, v := range in.Versions {
		if _, ok := failed[v]; !ok {
			successList = append(successList, map[string]string{"version": v, "status": "Copied"})
		}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"failedVersions":     failedList,
		"successfulVersions": successList,
	})
}

// --- External connection handler ---

func (h *Handler) handleAssociateExternalConnection(
	c *echo.Context,
	domainName, repoName, connectionName string,
) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}
	if connectionName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "externalConnection is required"))
	}

	r, err := h.Backend.AssociateExternalConnection(domainName, repoName, connectionName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"repository": repoToMap(r, h.Backend.GetExternalConnections(domainName, repoName)),
	})
}

// --- Repository permissions policy handlers (new) ---

func (h *Handler) handleGetRepositoryPermissionsPolicy(c *echo.Context, domainName, repoName string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}

	pol, err := h.Backend.GetRepositoryPermissionsPolicy(domainName, repoName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"policy": map[string]any{
			"document":    pol.Document,
			"revision":    pol.Revision,
			"resourceArn": pol.ResourceARN,
		},
	})
}

type putRepositoryPermissionsPolicyBody struct {
	PolicyDocument string `json:"policyDocument"`
}

func (h *Handler) handlePutRepositoryPermissionsPolicy(
	c *echo.Context,
	domainName, repoName string,
	body []byte,
) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}

	var in putRepositoryPermissionsPolicyBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	if in.PolicyDocument == "" {
		in.PolicyDocument = `{"Version":"2012-10-17","Statement":[]}`
	}

	pol, err := h.Backend.PutRepositoryPermissionsPolicy(domainName, repoName, in.PolicyDocument)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"policy": map[string]any{
			"document":    pol.Document,
			"revision":    pol.Revision,
			"resourceArn": pol.ResourceARN,
		},
	})
}

func (h *Handler) handleDeleteRepositoryPermissionsPolicy(c *echo.Context, domainName, repoName string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}

	pol, err := h.Backend.DeleteRepositoryPermissionsPolicy(domainName, repoName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"policy": map[string]any{
			"document":    pol.Document,
			"revision":    pol.Revision,
			"resourceArn": pol.ResourceARN,
		},
	})
}
