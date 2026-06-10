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
	keyName               = "name"
	keyStatusField        = "status"
	keyDomainOwner        = "domainOwner"
	keyRepository         = "repository"
	keyPolicy             = "policy"
	keyRevision           = "revision"
	keyFormat             = "format"
	keyPackageKey         = "package"
	keyFailedVersions     = "failedVersions"
	keySuccessfulVersions = "successfulVersions"
	keyResourceArn        = "resourceArn"
	keyPackageGroup       = "packageGroup"
	keyVersion            = "version"
)

const (
	opUnknown     = "Unknown"
	keyArn        = "arn"
	keyDomain     = "domain"
	keyDomainName = "domainName"
	keyDocument   = "document"
)

const (
	opAssociateExternalConnection           = "AssociateExternalConnection"
	opCopyPackageVersions                   = "CopyPackageVersions"
	opCreateDomain                          = "CreateDomain"
	opCreatePackageGroup                    = "CreatePackageGroup"
	opCreateRepository                      = "CreateRepository"
	opDeleteDomain                          = "DeleteDomain"
	opDeleteDomainPermissionsPolicy         = "DeleteDomainPermissionsPolicy"
	opDeletePackage                         = "DeletePackage"
	opDeletePackageGroup                    = "DeletePackageGroup"
	opDeletePackageVersions                 = "DeletePackageVersions"
	opDeleteRepository                      = "DeleteRepository"
	opDeleteRepositoryPermissionsPolicy     = "DeleteRepositoryPermissionsPolicy"
	opDescribeDomain                        = "DescribeDomain"
	opDescribePackage                       = "DescribePackage"
	opDescribePackageGroup                  = "DescribePackageGroup"
	opDescribePackageVersion                = "DescribePackageVersion"
	opDescribeRepository                    = "DescribeRepository"
	opGetAuthorizationToken                 = "GetAuthorizationToken"
	opGetDomainPermissionsPolicy            = "GetDomainPermissionsPolicy"
	opGetRepositoryEndpoint                 = "GetRepositoryEndpoint"
	opGetRepositoryPermissionsPolicy        = "GetRepositoryPermissionsPolicy"
	opListDomains                           = "ListDomains"
	opListRepositories                      = "ListRepositories"
	opListRepositoriesInDomain              = "ListRepositoriesInDomain"
	opListTagsForResource                   = "ListTagsForResource"
	opPutDomainPermissionsPolicy            = "PutDomainPermissionsPolicy"
	opPutRepositoryPermissionsPolicy        = "PutRepositoryPermissionsPolicy"
	opTagResource                           = "TagResource"
	opUntagResource                         = "UntagResource"
	opDisassociateExternalConnection        = "DisassociateExternalConnection"
	opDisposePackageVersions                = "DisposePackageVersions"
	opGetAssociatedPackageGroup             = "GetAssociatedPackageGroup"
	opGetPackageVersionAsset                = "GetPackageVersionAsset"
	opGetPackageVersionReadme               = "GetPackageVersionReadme"
	opListAllowedRepositoriesForGroup       = "ListAllowedRepositoriesForGroup"
	opListAssociatedPackages                = "ListAssociatedPackages"
	opListPackageGroups                     = "ListPackageGroups"
	opListPackageVersionAssets              = "ListPackageVersionAssets"
	opListPackageVersionDependencies        = "ListPackageVersionDependencies"
	opListPackageVersions                   = "ListPackageVersions"
	opListPackages                          = "ListPackages"
	opListSubPackageGroups                  = "ListSubPackageGroups"
	opPublishPackageVersion                 = "PublishPackageVersion"
	opPutPackageOriginConfiguration         = "PutPackageOriginConfiguration"
	opUpdatePackageGroup                    = "UpdatePackageGroup"
	opUpdatePackageGroupOriginConfiguration = "UpdatePackageGroupOriginConfiguration"
	opUpdatePackageVersionsStatus           = "UpdatePackageVersionsStatus"
	opUpdateRepository                      = "UpdateRepository"
)

const (
	codeartifactMatchPriority = service.PriorityPathVersioned + 1

	pathV1Domain                          = "/v1/domain"
	pathV1Domains                         = "/v1/domains"
	pathV1DomainRepositories              = "/v1/domain/repositories"
	pathV1DomainPermissions               = "/v1/domain/permissions/policy"
	pathV1Repository                      = "/v1/repository"
	pathV1Repositories                    = "/v1/repositories"
	pathV1RepositoryEndpoint              = "/v1/repository/endpoint"
	pathV1RepositoryExternalConnection    = "/v1/repository/external-connection"
	pathV1RepositoryPermissions           = "/v1/repository/permissions/policy"
	pathV1Tags                            = "/v1/tags"
	pathV1Tag                             = "/v1/tag"
	pathV1Untag                           = "/v1/untag"
	pathV1AuthToken                       = "/v1/authorization-token" //nolint:gosec // not a credential
	pathV1PackageGroup                    = "/v1/package-group"
	pathV1Package                         = "/v1/package"
	pathV1PackageVersion                  = "/v1/package/version"
	pathV1PackageVersionsCopy             = "/v1/package/versions/copy"
	pathV1PackageVersionsDelete           = "/v1/package/versions/delete"
	pathV1PackageVersionsDispose          = "/v1/package/versions/dispose"
	pathV1PackageVersionsUpdateStatus     = "/v1/package/versions/update_status"
	pathV1PackageVersionAsset             = "/v1/package/version/asset"
	pathV1PackageVersionReadme            = "/v1/package/version/readme"
	pathV1PackageVersionAssets            = "/v1/package/version/assets"
	pathV1PackageVersionDependencies      = "/v1/package/version/dependencies"
	pathV1PackageVersionsPublish          = "/v1/package/versions/publish"
	pathV1PackageOriginConfiguration      = "/v1/package/origin-configuration"
	pathV1PackageGroups                   = "/v1/package-groups"
	pathV1PackageGroupAssociatedPackages  = "/v1/package-group-associated-packages" //nolint:gosec // not a credential
	pathV1PackageGroupAllowedRepos        = "/v1/package-group-allowed-repositories"
	pathV1PackageGroupOriginConfiguration = "/v1/package-group-origin-configuration"
	pathV1SubPackageGroups                = "/v1/sub-package-groups"
	pathV1AssociatedPackageGroup          = "/v1/associated-package-group"
	pathV1Packages                        = "/v1/packages"
	pathV1PackageVersions                 = "/v1/package/versions"
)

const (
	// stubTokenExpireHours is the expiry duration for stub authorization tokens.
	stubTokenExpireHours = 12
)

var errInvalidRequest = errors.New("invalid request")

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
		opAssociateExternalConnection,
		opCopyPackageVersions,
		opCreateDomain,
		opCreatePackageGroup,
		opCreateRepository,
		opDeleteDomain,
		opDeletePackage,
		opDeletePackageGroup,
		opDeletePackageVersions,
		opDeleteRepository,
		opDeleteRepositoryPermissionsPolicy,
		opDescribeDomain,
		opDescribePackage,
		opDescribePackageGroup,
		opDescribePackageVersion,
		opDescribeRepository,
		opGetAuthorizationToken,
		opGetDomainPermissionsPolicy,
		opGetRepositoryEndpoint,
		opGetRepositoryPermissionsPolicy,
		opListDomains,
		opListRepositories,
		opListRepositoriesInDomain,
		opListTagsForResource,
		opPutDomainPermissionsPolicy,
		opDeleteDomainPermissionsPolicy,
		opPutRepositoryPermissionsPolicy,
		opTagResource,
		opUntagResource,
		opDisassociateExternalConnection,
		opDisposePackageVersions,
		opGetAssociatedPackageGroup,
		opGetPackageVersionAsset,
		opGetPackageVersionReadme,
		opListAllowedRepositoriesForGroup,
		opListAssociatedPackages,
		opListPackageGroups,
		opListPackageVersionAssets,
		opListPackageVersionDependencies,
		opListPackageVersions,
		opListPackages,
		opListSubPackageGroups,
		opPublishPackageVersion,
		opPutPackageOriginConfiguration,
		opUpdatePackageGroup,
		opUpdatePackageGroupOriginConfiguration,
		opUpdatePackageVersionsStatus,
		opUpdateRepository,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "codeartifact" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this CodeArtifact instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// isDomainRepoPath returns true if the path is a known domain/repo/tag/auth path.
func isDomainRepoPath(path string) bool {
	return path == pathV1Domain || strings.HasPrefix(path, pathV1Domain+"/") ||
		path == pathV1Domains || path == pathV1Repository ||
		strings.HasPrefix(path, pathV1Repository+"/") ||
		path == pathV1Repositories || path == pathV1Tags ||
		path == pathV1Tag || path == pathV1Untag || path == pathV1AuthToken
}

// isPackageCoreGroupPath returns true for core package and package-group paths.
func isPackageCoreGroupPath(path string) bool {
	return path == pathV1PackageGroup || path == pathV1PackageGroups ||
		path == pathV1Package || path == pathV1PackageVersion ||
		path == pathV1PackageVersionsCopy || path == pathV1PackageVersionsDelete
}

// isPackageExtendedPath returns true for extended package-version and group operation paths.
func isPackageExtendedPath(path string) bool {
	return path == pathV1PackageVersionsDispose || path == pathV1PackageVersionsUpdateStatus ||
		path == pathV1PackageVersionAsset || path == pathV1PackageVersionReadme ||
		path == pathV1PackageVersionAssets || path == pathV1PackageVersionDependencies ||
		path == pathV1PackageVersionsPublish || path == pathV1PackageOriginConfiguration ||
		path == pathV1PackageGroupAssociatedPackages || path == pathV1PackageGroupAllowedRepos ||
		path == pathV1PackageGroupOriginConfiguration || path == pathV1SubPackageGroups ||
		path == pathV1AssociatedPackageGroup || path == pathV1Packages || path == pathV1PackageVersions
}

// isPackagePath returns true if the path is a known package/package-group path.
func isPackagePath(path string) bool {
	return isPackageCoreGroupPath(path) || isPackageExtendedPath(path)
}

// isCodeArtifactPath returns true if the given path is a known CodeArtifact REST path.
func isCodeArtifactPath(path string) bool {
	return isDomainRepoPath(path) || isPackagePath(path)
}

// RouteMatcher returns a function that matches AWS CodeArtifact REST requests.
// CodeArtifact uses /v1/ paths that are distinct from Batch and AppSync.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return isCodeArtifactPath(path)
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
	if op, ok := domainRepoStaticRoutes[path]; ok {
		return codeartifactRoute{operation: op}
	}

	switch path {
	case pathV1Domain:
		return parseDomainRoute(method)
	case pathV1DomainPermissions:
		return parseDomainPermissionsRoute(method)
	case pathV1Repository:
		return parseRepositoryRoute(method)
	case pathV1RepositoryExternalConnection:
		return parseRepositoryExternalConnectionRoute(method)
	case pathV1RepositoryPermissions:
		return parseRepositoryPermissionsRoute(method)
	}

	return codeartifactRoute{operation: opUnknown}
}

// domainRepoStaticRoutes maps domain/repo/tag/auth paths that need no method dispatch.
//
//nolint:gochecknoglobals // read-only dispatch table initialized once at startup
var domainRepoStaticRoutes = map[string]string{
	pathV1Domains:                opListDomains,
	pathV1DomainRepositories:     opListRepositoriesInDomain,
	pathV1RepositoryEndpoint:     opGetRepositoryEndpoint,
	pathV1Repositories:           opListRepositories,
	pathV1Tags:                   opListTagsForResource,
	pathV1Tag:                    opTagResource,
	pathV1Untag:                  opUntagResource,
	pathV1AuthToken:              opGetAuthorizationToken,
	pathV1SubPackageGroups:       opListSubPackageGroups,
	pathV1AssociatedPackageGroup: opGetAssociatedPackageGroup,
}

// packageOpStaticRoutes maps static paths (no method dispatch) to their operations.
//
//nolint:gochecknoglobals // read-only dispatch table initialized once at startup
var packageOpStaticRoutes = map[string]string{
	pathV1PackageGroups:                   opListPackageGroups,
	pathV1Packages:                        opListPackages,
	pathV1PackageVersion:                  opDescribePackageVersion,
	pathV1PackageVersions:                 opListPackageVersions,
	pathV1PackageVersionsCopy:             opCopyPackageVersions,
	pathV1PackageVersionsDelete:           opDeletePackageVersions,
	pathV1PackageVersionsDispose:          opDisposePackageVersions,
	pathV1PackageVersionsUpdateStatus:     opUpdatePackageVersionsStatus,
	pathV1PackageVersionsPublish:          opPublishPackageVersion,
	pathV1PackageVersionAsset:             opGetPackageVersionAsset,
	pathV1PackageVersionReadme:            opGetPackageVersionReadme,
	pathV1PackageVersionAssets:            opListPackageVersionAssets,
	pathV1PackageVersionDependencies:      opListPackageVersionDependencies,
	pathV1PackageOriginConfiguration:      opPutPackageOriginConfiguration,
	pathV1PackageGroupAssociatedPackages:  opListAssociatedPackages,
	pathV1PackageGroupAllowedRepos:        opListAllowedRepositoriesForGroup,
	pathV1PackageGroupOriginConfiguration: opUpdatePackageGroupOriginConfiguration,
	pathV1SubPackageGroups:                opListSubPackageGroups,
	pathV1AssociatedPackageGroup:          opGetAssociatedPackageGroup,
}

// parsePackageOpPath handles package, package-group, and package-version routes.
func parsePackageOpPath(method, path string) codeartifactRoute {
	// Method-dispatched paths first.
	switch path {
	case pathV1PackageGroup:
		return parsePackageGroupRoute(method)
	case pathV1Package:
		return parsePackageRoute(method)
	}

	// Static path → operation mapping.
	if op, ok := packageOpStaticRoutes[path]; ok {
		return codeartifactRoute{operation: op}
	}

	return codeartifactRoute{operation: opUnknown}
}

func parseDomainRoute(method string) codeartifactRoute {
	switch method {
	case http.MethodPost:
		return codeartifactRoute{operation: opCreateDomain}
	case http.MethodGet:
		return codeartifactRoute{operation: opDescribeDomain}
	case http.MethodDelete:
		return codeartifactRoute{operation: opDeleteDomain}
	}

	return codeartifactRoute{operation: opUnknown}
}

func parseDomainPermissionsRoute(method string) codeartifactRoute {
	switch method {
	case http.MethodGet:
		return codeartifactRoute{operation: opGetDomainPermissionsPolicy}
	case http.MethodPut:
		return codeartifactRoute{operation: opPutDomainPermissionsPolicy}
	case http.MethodDelete:
		return codeartifactRoute{operation: opDeleteDomainPermissionsPolicy}
	}

	return codeartifactRoute{operation: opUnknown}
}

func parseRepositoryRoute(method string) codeartifactRoute {
	switch method {
	case http.MethodPost:
		return codeartifactRoute{operation: opCreateRepository}
	case http.MethodGet:
		return codeartifactRoute{operation: opDescribeRepository}
	case http.MethodDelete:
		return codeartifactRoute{operation: opDeleteRepository}
	case http.MethodPut:
		return codeartifactRoute{operation: opUpdateRepository}
	}

	return codeartifactRoute{operation: opUnknown}
}

func parseRepositoryPermissionsRoute(method string) codeartifactRoute {
	switch method {
	case http.MethodGet:
		return codeartifactRoute{operation: opGetRepositoryPermissionsPolicy}
	case http.MethodPut:
		return codeartifactRoute{operation: opPutRepositoryPermissionsPolicy}
	case http.MethodDelete:
		return codeartifactRoute{operation: opDeleteRepositoryPermissionsPolicy}
	}

	return codeartifactRoute{operation: opUnknown}
}

func parsePackageGroupRoute(method string) codeartifactRoute {
	switch method {
	case http.MethodPost:
		return codeartifactRoute{operation: opCreatePackageGroup}
	case http.MethodGet:
		return codeartifactRoute{operation: opDescribePackageGroup}
	case http.MethodDelete:
		return codeartifactRoute{operation: opDeletePackageGroup}
	case http.MethodPut:
		return codeartifactRoute{operation: opUpdatePackageGroup}
	}

	return codeartifactRoute{operation: opUnknown}
}

func parseRepositoryExternalConnectionRoute(method string) codeartifactRoute {
	switch method {
	case http.MethodPost:
		return codeartifactRoute{operation: opAssociateExternalConnection}
	case http.MethodDelete:
		return codeartifactRoute{operation: opDisassociateExternalConnection}
	}

	return codeartifactRoute{operation: opUnknown}
}

func parsePackageRoute(method string) codeartifactRoute {
	switch method {
	case http.MethodGet:
		return codeartifactRoute{operation: opDescribePackage}
	case http.MethodDelete:
		return codeartifactRoute{operation: opDeletePackage}
	}

	return codeartifactRoute{operation: opUnknown}
}

// ExtractOperation extracts the CodeArtifact operation name from the REST path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := parseCodeArtifactPath(c.Request().Method, c.Request().URL.Path)

	return r.operation
}

// ExtractResource extracts the primary resource identifier from the URL path or query params.
func (h *Handler) ExtractResource(c *echo.Context) string {
	q := c.Request().URL.Query()
	if domain := q.Get(keyDomain); domain != "" {
		if repo := q.Get(keyRepository); repo != "" {
			return domain + "/" + repo
		}

		return domain
	}

	return q.Get(keyResourceArn)
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
		opCreateDomain: func(c *echo.Context, body []byte) error {
			return h.handleCreateDomain(c, c.Request().URL.Query().Get(keyDomain), body)
		},
		opDescribeDomain: func(c *echo.Context, _ []byte) error {
			return h.handleDescribeDomain(c, c.Request().URL.Query().Get(keyDomain))
		},
		opDeleteDomain: func(c *echo.Context, _ []byte) error {
			return h.handleDeleteDomain(c, c.Request().URL.Query().Get(keyDomain))
		},
		opListDomains: func(c *echo.Context, _ []byte) error {
			return h.handleListDomains(c)
		},
		opCreateRepository: func(c *echo.Context, body []byte) error {
			q := c.Request().URL.Query()

			return h.handleCreateRepository(c, q.Get(keyDomain), q.Get(keyRepository), body)
		},
		opDescribeRepository: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleDescribeRepository(c, q.Get(keyDomain), q.Get(keyRepository))
		},
		opDeleteRepository: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleDeleteRepository(c, q.Get(keyDomain), q.Get(keyRepository))
		},
		opListRepositoriesInDomain: func(c *echo.Context, _ []byte) error {
			return h.handleListRepositoriesInDomain(c, c.Request().URL.Query().Get(keyDomain))
		},
		opListRepositories: func(c *echo.Context, _ []byte) error {
			return h.handleListRepositories(c)
		},
		opGetRepositoryEndpoint: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleGetRepositoryEndpoint(c, q.Get(keyDomain), q.Get(keyRepository), q.Get("format"))
		},
		opGetAuthorizationToken: func(c *echo.Context, _ []byte) error {
			return h.handleGetAuthorizationToken(c, c.Request().URL.Query().Get(keyDomain))
		},
		opListTagsForResource: func(c *echo.Context, _ []byte) error {
			return h.handleListTagsForResource(c, c.Request().URL.Query().Get(keyResourceArn))
		},
		opTagResource: func(c *echo.Context, body []byte) error {
			return h.handleTagResource(c, c.Request().URL.Query().Get(keyResourceArn), body)
		},
		opUntagResource: func(c *echo.Context, body []byte) error {
			return h.handleUntagResource(c, c.Request().URL.Query().Get(keyResourceArn), body)
		},
		opGetDomainPermissionsPolicy: func(c *echo.Context, _ []byte) error {
			return h.handleGetDomainPermissionsPolicy(c, c.Request().URL.Query().Get(keyDomain))
		},
		opPutDomainPermissionsPolicy: func(c *echo.Context, body []byte) error {
			return h.handlePutDomainPermissionsPolicy(c, c.Request().URL.Query().Get(keyDomain), body)
		},
		opDeleteDomainPermissionsPolicy: func(c *echo.Context, _ []byte) error {
			return h.handleDeleteDomainPermissionsPolicy(c, c.Request().URL.Query().Get(keyDomain))
		},
		opAssociateExternalConnection: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleAssociateExternalConnection(
				c,
				q.Get(keyDomain),
				q.Get(keyRepository),
				q.Get("externalConnection"),
			)
		},
		opGetRepositoryPermissionsPolicy: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleGetRepositoryPermissionsPolicy(c, q.Get(keyDomain), q.Get(keyRepository))
		},
		opPutRepositoryPermissionsPolicy: func(c *echo.Context, body []byte) error {
			q := c.Request().URL.Query()

			return h.handlePutRepositoryPermissionsPolicy(c, q.Get(keyDomain), q.Get(keyRepository), body)
		},
		opDeleteRepositoryPermissionsPolicy: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleDeleteRepositoryPermissionsPolicy(c, q.Get(keyDomain), q.Get(keyRepository))
		},
	}
}

func (h *Handler) buildPackageOps() map[string]func(*echo.Context, []byte) error { //nolint:funlen
	return map[string]func(*echo.Context, []byte) error{
		opCopyPackageVersions: func(c *echo.Context, body []byte) error {
			q := c.Request().URL.Query()

			return h.handleCopyPackageVersions(
				c, q.Get(keyDomain), q.Get("sourceRepository"), q.Get("destinationRepository"),
				q.Get("format"), q.Get("namespace"), q.Get("package"), body,
			)
		},
		opCreatePackageGroup: func(c *echo.Context, body []byte) error {
			return h.handleCreatePackageGroup(c, c.Request().URL.Query().Get(keyDomain), body)
		},
		opDeletePackage: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleDeletePackage(
				c, q.Get(keyDomain), q.Get(keyRepository), q.Get("format"), q.Get("namespace"), q.Get("package"),
			)
		},
		opDeletePackageGroup: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleDeletePackageGroup(c, q.Get(keyDomain), q.Get(keyPackageGroup))
		},
		opDeletePackageVersions: func(c *echo.Context, body []byte) error {
			q := c.Request().URL.Query()

			return h.handleDeletePackageVersions(
				c, q.Get(keyDomain), q.Get(keyRepository), q.Get("format"), q.Get("namespace"), q.Get("package"), body,
			)
		},
		opDescribePackage: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleDescribePackage(
				c, q.Get(keyDomain), q.Get(keyRepository), q.Get("format"), q.Get("namespace"), q.Get("package"),
			)
		},
		opDescribePackageGroup: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleDescribePackageGroup(c, q.Get(keyDomain), q.Get(keyPackageGroup))
		},
		opDescribePackageVersion: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleDescribePackageVersion(
				c, q.Get(keyDomain), q.Get(keyRepository), q.Get("format"),
				q.Get("namespace"), q.Get("package"), q.Get(keyVersion),
			)
		},
		opDisassociateExternalConnection: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleDisassociateExternalConnection(
				c, q.Get(keyDomain), q.Get(keyRepository), q.Get("externalConnection"),
			)
		},
		opDisposePackageVersions: func(c *echo.Context, body []byte) error {
			q := c.Request().URL.Query()

			return h.handleDisposePackageVersions(
				c, q.Get(keyDomain), q.Get(keyRepository), q.Get("format"), q.Get("namespace"), q.Get("package"), body,
			)
		},
		opGetAssociatedPackageGroup: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleGetAssociatedPackageGroup(
				c, q.Get(keyDomain), q.Get("format"), q.Get("namespace"), q.Get("package"),
			)
		},
		opGetPackageVersionAsset: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleGetPackageVersionAsset(
				c, q.Get(keyDomain), q.Get(keyRepository), q.Get("format"),
				q.Get("namespace"), q.Get("package"), q.Get(keyVersion), q.Get("asset"),
			)
		},
		opGetPackageVersionReadme: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleGetPackageVersionReadme(
				c, q.Get(keyDomain), q.Get(keyRepository), q.Get("format"),
				q.Get("namespace"), q.Get("package"), q.Get(keyVersion),
			)
		},
		opListAllowedRepositoriesForGroup: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleListAllowedRepositoriesForGroup(c, q.Get(keyDomain), q.Get(keyPackageGroup))
		},
		opListAssociatedPackages: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleListAssociatedPackages(c, q.Get(keyDomain), q.Get(keyPackageGroup))
		},
		opListPackageGroups: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleListPackageGroups(c, q.Get(keyDomain), q.Get("prefix"))
		},
		opListPackageVersionAssets: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleListPackageVersionAssets(
				c, q.Get(keyDomain), q.Get(keyRepository), q.Get("format"),
				q.Get("namespace"), q.Get("package"), q.Get(keyVersion),
			)
		},
		opListPackageVersionDependencies: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleListPackageVersionDependencies(
				c, q.Get(keyDomain), q.Get(keyRepository), q.Get("format"),
				q.Get("namespace"), q.Get("package"), q.Get(keyVersion),
			)
		},
		opListPackageVersions: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleListPackageVersions(
				c, q.Get(keyDomain), q.Get(keyRepository), q.Get("format"),
				q.Get("namespace"), q.Get("package"),
			)
		},
		opListPackages: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleListPackages(c, q.Get(keyDomain), q.Get(keyRepository), q.Get("format"), q.Get("namespace"))
		},
		opListSubPackageGroups: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleListSubPackageGroups(c, q.Get(keyDomain), q.Get(keyPackageGroup))
		},
		opPublishPackageVersion: func(c *echo.Context, body []byte) error {
			q := c.Request().URL.Query()

			return h.handlePublishPackageVersion(
				c, q.Get(keyDomain), q.Get(keyRepository), q.Get("format"),
				q.Get("namespace"), q.Get("package"), q.Get(keyVersion), body,
			)
		},
		opPutPackageOriginConfiguration: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handlePutPackageOriginConfiguration(
				c, q.Get(keyDomain), q.Get(keyRepository), q.Get("format"),
				q.Get("namespace"), q.Get("package"),
			)
		},
		opUpdatePackageGroup: func(c *echo.Context, body []byte) error {
			return h.handleUpdatePackageGroup(c, c.Request().URL.Query().Get(keyDomain), body)
		},
		opUpdatePackageGroupOriginConfiguration: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleUpdatePackageGroupOriginConfiguration(c, q.Get(keyDomain), q.Get(keyPackageGroup))
		},
		opUpdatePackageVersionsStatus: func(c *echo.Context, body []byte) error {
			q := c.Request().URL.Query()

			return h.handleUpdatePackageVersionsStatus(
				c, q.Get(keyDomain), q.Get(keyRepository), q.Get("format"),
				q.Get("namespace"), q.Get("package"), body,
			)
		},
		opUpdateRepository: func(c *echo.Context, body []byte) error {
			q := c.Request().URL.Query()

			return h.handleUpdateRepository(c, q.Get(keyDomain), q.Get(keyRepository), body)
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
		keyArn:            d.ARN,
		keyName:           d.Name,
		"owner":           d.Owner,
		keyStatusField:    d.Status,
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
		keyArn:         d.ARN,
		keyName:        d.Name,
		"owner":        d.Owner,
		keyStatusField: d.Status,
		"createdTime":  epochSeconds(d.CreatedTime),
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
		keyDomain: domainToMap(d, 0),
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
		keyDomain: domainToMap(d, repoCount),
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
		keyDomain: domainToMap(d, repoCount),
	})
}

// --- Repository handlers ---

type createRepositoryBody struct {
	Description string           `json:"description"`
	Tags        []map[string]any `json:"tags"`
}

func repoToMap(r *Repository, connections []ExternalConnection) map[string]any {
	m := map[string]any{
		keyArn:                 r.ARN,
		keyName:                r.Name,
		keyDomainName:          r.DomainName,
		keyDomainOwner:         r.DomainOwner,
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
			keyStatusField:           ec.Status,
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
		keyRepository: repoToMap(r, h.Backend.GetExternalConnections(domainName, repoName)),
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
		keyRepository: repoToMap(r, h.Backend.GetExternalConnections(domainName, repoName)),
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
		keyRepository: repoToMap(r, conns),
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
			keyArn:         r.ARN,
			keyName:        r.Name,
			keyDomainName:  r.DomainName,
			keyDomainOwner: r.DomainOwner,
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
			keyArn:         r.ARN,
			keyName:        r.Name,
			keyDomainName:  r.DomainName,
			keyDomainOwner: r.DomainOwner,
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
		keyPolicy: map[string]any{
			keyDocument:    pol.Document,
			keyRevision:    pol.Revision,
			keyResourceArn: pol.ResourceARN,
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
		keyPolicy: map[string]any{
			keyDocument:    pol.Document,
			keyRevision:    pol.Revision,
			keyResourceArn: pol.ResourceARN,
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
		keyPolicy: map[string]any{
			keyDocument:    pol.Document,
			keyRevision:    pol.Revision,
			keyResourceArn: pol.ResourceARN,
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
		keyArn:         pg.ARN,
		keyDomainName:  pg.DomainName,
		keyDomainOwner: pg.DomainOwner,
		"pattern":      pg.Pattern,
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
		keyPackageGroup: packageGroupToMap(pg),
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
		keyPackageGroup: packageGroupToMap(pg),
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
		keyPackageGroup: packageGroupToMap(pg),
	})
}

// --- Package handlers ---

func packageToMap(pkg *Package) map[string]any {
	m := map[string]any{
		keyFormat:      pkg.Format,
		keyName:        pkg.Name,
		keyDomainName:  pkg.DomainName,
		keyDomainOwner: pkg.DomainOwner,
		keyRepository:  pkg.Repository,
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
		keyPackageKey: packageToMap(pkg),
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
		keyVersion:     pv.Version,
		keyStatusField: pv.Status,
		"format":       pv.Format,
		"publishedAt":  epochSeconds(pv.PublishedAt),
		keyRevision:    pv.Revision,
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
		failedList = append(failedList, map[string]string{keyVersion: v, "errorCode": code})
	}

	successList := make([]map[string]string, 0, len(in.Versions))
	for _, v := range in.Versions {
		if _, ok := failed[v]; !ok {
			successList = append(successList, map[string]string{keyVersion: v, keyStatusField: "Deleted"})
		}
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyFailedVersions:     failedList,
		keySuccessfulVersions: successList,
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
		failedList = append(failedList, map[string]string{keyVersion: v, "errorCode": code})
	}

	successList := make([]map[string]string, 0, len(in.Versions))
	for _, v := range in.Versions {
		if _, ok := failed[v]; !ok {
			successList = append(successList, map[string]string{keyVersion: v, keyStatusField: "Copied"})
		}
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyFailedVersions:     failedList,
		keySuccessfulVersions: successList,
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
		keyRepository: repoToMap(r, h.Backend.GetExternalConnections(domainName, repoName)),
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
		keyPolicy: map[string]any{
			keyDocument:    pol.Document,
			keyRevision:    pol.Revision,
			keyResourceArn: pol.ResourceARN,
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
		keyPolicy: map[string]any{
			keyDocument:    pol.Document,
			keyRevision:    pol.Revision,
			keyResourceArn: pol.ResourceARN,
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
		keyPolicy: map[string]any{
			keyDocument:    pol.Document,
			keyRevision:    pol.Revision,
			keyResourceArn: pol.ResourceARN,
		},
	})
}

// --- New handler implementations ---

func (h *Handler) handleDisassociateExternalConnection(
	c *echo.Context, domainName, repoName, connectionName string,
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

	r, err := h.Backend.DisassociateExternalConnection(domainName, repoName, connectionName)
	if err != nil {
		return h.handleError(c, err)
	}

	extConns := h.Backend.GetExternalConnections(domainName, repoName)

	return c.JSON(http.StatusOK, map[string]any{keyRepository: repoToMap(r, extConns)})
}

type disposeVersionsBody struct {
	Versions []string `json:"versions"`
}

func (h *Handler) handleDisposePackageVersions(
	c *echo.Context, domainName, repoName, format, namespace, name string, body []byte,
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

	var in disposeVersionsBody
	if len(body) > 0 {
		_ = json.Unmarshal(body, &in)
	}

	results, err := h.Backend.DisposePackageVersions(domainName, repoName, format, namespace, name, in.Versions)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keySuccessfulVersions: results, keyFailedVersions: map[string]any{}})
}

func (h *Handler) handleGetAssociatedPackageGroup(c *echo.Context, domainName, format, namespace, name string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}

	pg, err := h.Backend.GetAssociatedPackageGroup(domainName, format, namespace, name)
	if err != nil {
		return h.handleError(c, err)
	}

	if pg == nil {
		return c.JSON(http.StatusOK, map[string]any{keyPackageGroup: nil})
	}

	return c.JSON(http.StatusOK, map[string]any{keyPackageGroup: packageGroupToMap(pg)})
}

func (h *Handler) handleGetPackageVersionAsset(
	c *echo.Context, domainName, repoName, format, namespace, name, version, asset string,
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
	if asset == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "asset is required"))
	}

	data, err := h.Backend.GetPackageVersionAsset(domainName, repoName, format, namespace, name, version, asset)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.Blob(http.StatusOK, "application/octet-stream", data)
}

func (h *Handler) validatePackageVersionParams(
	c *echo.Context,
	domainName, repoName, format, name, version string,
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

	return nil
}

func (h *Handler) handleGetPackageVersionReadme(
	c *echo.Context, domainName, repoName, format, namespace, name, version string,
) error {
	if err := h.validatePackageVersionParams(c, domainName, repoName, format, name, version); err != nil {
		return err
	}

	readme, err := h.Backend.GetPackageVersionReadme(domainName, repoName, format, namespace, name, version)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"readme": readme})
}

func (h *Handler) handleListAllowedRepositoriesForGroup(c *echo.Context, domainName, pattern string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if pattern == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "packageGroup is required"))
	}

	repos, err := h.Backend.ListAllowedRepositoriesForGroup(domainName, pattern)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"allowedRepositories": repos})
}

func (h *Handler) handleListAssociatedPackages(c *echo.Context, domainName, pattern string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if pattern == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "packageGroup is required"))
	}

	pkgs, err := h.Backend.ListAssociatedPackages(domainName, pattern)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(pkgs))
	for _, pkg := range pkgs {
		items = append(items, packageToMap(pkg))
	}

	return c.JSON(http.StatusOK, map[string]any{"packages": items})
}

func (h *Handler) handleListPackageGroups(c *echo.Context, domainName, prefix string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}

	groups, err := h.Backend.ListPackageGroups(domainName, prefix)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(groups))
	for _, pg := range groups {
		items = append(items, packageGroupToMap(pg))
	}

	return c.JSON(http.StatusOK, map[string]any{"packageGroups": items})
}

func (h *Handler) handleListPackageVersionAssets(
	c *echo.Context, domainName, repoName, format, namespace, name, version string,
) error {
	if err := h.validatePackageVersionParams(c, domainName, repoName, format, name, version); err != nil {
		return err
	}

	assets, err := h.Backend.ListPackageVersionAssets(domainName, repoName, format, namespace, name, version)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"assets": assets})
}

func (h *Handler) handleListPackageVersionDependencies(
	c *echo.Context, domainName, repoName, format, namespace, name, version string,
) error {
	if err := h.validatePackageVersionParams(c, domainName, repoName, format, name, version); err != nil {
		return err
	}

	deps, err := h.Backend.ListPackageVersionDependencies(domainName, repoName, format, namespace, name, version)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"dependencies": deps})
}

func (h *Handler) handleListPackageVersions(
	c *echo.Context, domainName, repoName, format, namespace, name string,
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

	versions, err := h.Backend.ListPackageVersions(domainName, repoName, format, namespace, name)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(versions))
	for _, pv := range versions {
		items = append(items, packageVersionToMap(pv))
	}

	return c.JSON(http.StatusOK, map[string]any{"versions": items, "package": name, "format": format})
}

func (h *Handler) handleListPackages(c *echo.Context, domainName, repoName, format, namespace string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}

	pkgs, err := h.Backend.ListPackages(domainName, repoName, format, namespace)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(pkgs))
	for _, pkg := range pkgs {
		items = append(items, packageToMap(pkg))
	}

	return c.JSON(http.StatusOK, map[string]any{"packages": items})
}

func (h *Handler) handleListSubPackageGroups(c *echo.Context, domainName, pattern string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if pattern == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "packageGroup is required"))
	}

	groups, err := h.Backend.ListSubPackageGroups(domainName, pattern)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(groups))
	for _, pg := range groups {
		items = append(items, packageGroupToMap(pg))
	}

	return c.JSON(http.StatusOK, map[string]any{"packageGroups": items})
}

func (h *Handler) handlePublishPackageVersion(
	c *echo.Context, domainName, repoName, format, namespace, name, version string, _ []byte,
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

	pv, err := h.Backend.PublishPackageVersion(domainName, repoName, format, namespace, name, version)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, packageVersionToMap(pv))
}

func (h *Handler) handlePutPackageOriginConfiguration(
	c *echo.Context, domainName, repoName, format, namespace, name string,
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

	pkg, err := h.Backend.PutPackageOriginConfiguration(domainName, repoName, format, namespace, name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"package": packageToMap(pkg)})
}

type updatePackageGroupBody struct {
	Description  string `json:"description"`
	ContactInfo  string `json:"contactInfo"`
	PackageGroup string `json:"packageGroup"`
}

func (h *Handler) handleUpdatePackageGroup(c *echo.Context, domainName string, body []byte) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}

	var in updatePackageGroupBody
	if len(body) > 0 {
		_ = json.Unmarshal(body, &in)
	}

	pattern := c.Request().URL.Query().Get(keyPackageGroup)
	if pattern == "" {
		pattern = in.PackageGroup
	}

	pg, err := h.Backend.UpdatePackageGroup(domainName, pattern, in.Description, in.ContactInfo)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"packageGroup": packageGroupToMap(pg)})
}

func (h *Handler) handleUpdatePackageGroupOriginConfiguration(c *echo.Context, domainName, pattern string) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if pattern == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "packageGroup is required"))
	}

	pg, err := h.Backend.UpdatePackageGroupOriginConfiguration(domainName, pattern)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"packageGroup": packageGroupToMap(pg)})
}

type updateVersionsStatusBody struct {
	TargetStatus string   `json:"targetStatus"`
	Versions     []string `json:"versions"`
}

func (h *Handler) handleUpdatePackageVersionsStatus(
	c *echo.Context, domainName, repoName, format, namespace, name string, body []byte,
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

	var in updateVersionsStatusBody
	if len(body) > 0 {
		_ = json.Unmarshal(body, &in)
	}

	if in.TargetStatus == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "targetStatus is required"))
	}

	results, err := h.Backend.UpdatePackageVersionsStatus(
		domainName, repoName, format, namespace, name, in.TargetStatus, in.Versions,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keySuccessfulVersions: results, keyFailedVersions: map[string]any{}})
}

type updateRepositoryBody struct {
	Description string `json:"description"`
}

func (h *Handler) handleUpdateRepository(c *echo.Context, domainName, repoName string, body []byte) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}

	var in updateRepositoryBody
	if len(body) > 0 {
		_ = json.Unmarshal(body, &in)
	}

	r, err := h.Backend.UpdateRepository(domainName, repoName, in.Description)
	if err != nil {
		return h.handleError(c, err)
	}

	extConns := h.Backend.GetExternalConnections(domainName, repoName)

	return c.JSON(http.StatusOK, map[string]any{keyRepository: repoToMap(r, extConns)})
}
