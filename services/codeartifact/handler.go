package codeartifact

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
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
	// keyPackageGroup is the request-BODY field name used by UpdatePackageGroup
	// (json:"packageGroup"). For the QUERY-STRING form used by every other
	// package-group op (DescribePackageGroup, DeletePackageGroup,
	// ListSubPackageGroups, ListAllowedRepositoriesForGroup,
	// ListAssociatedPackages, UpdatePackageGroupOriginConfiguration), see
	// keyPackageGroupQuery below -- verified against aws-sdk-go-v2 serializers.go,
	// real AWS uses the kebab-case "package-group" query param, not "packageGroup".
	keyPackageGroup = "packageGroup"
	// keyPackageGroupQuery is the real query-string parameter name ("package-group").
	keyPackageGroupQuery = "package-group"
	// keyExternalConnectionQuery is the real query-string parameter name for
	// AssociateExternalConnection/DisassociateExternalConnection ("external-connection").
	keyExternalConnectionQuery = "external-connection"
	// keySourceRepositoryQuery and keyDestinationRepositoryQuery are CopyPackageVersions'
	// real query-string parameter names ("source-repository"/"destination-repository").
	keySourceRepositoryQuery      = "source-repository"
	keyDestinationRepositoryQuery = "destination-repository"
	keyVersion                    = "version"
	keyCreatedTime                = "createdTime"
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

	pathV1Domain                       = "/v1/domain"
	pathV1Domains                      = "/v1/domains"
	pathV1DomainRepositories           = "/v1/domain/repositories"
	pathV1DomainPermissions            = "/v1/domain/permissions/policy"
	pathV1Repository                   = "/v1/repository"
	pathV1Repositories                 = "/v1/repositories"
	pathV1RepositoryEndpoint           = "/v1/repository/endpoint"
	pathV1RepositoryExternalConnection = "/v1/repository/external-connection"
	pathV1RepositoryPermissions        = "/v1/repository/permissions/policy"
	// pathV1RepositoryPermissionsPolicies is DeleteRepositoryPermissionsPolicy's own path
	// (plural "policies") -- unlike Get/Put, real AWS does NOT serve delete on the singular
	// "/v1/repository/permissions/policy" path. Verified against aws-sdk-go-v2 serializers.go.
	pathV1RepositoryPermissionsPolicies = "/v1/repository/permissions/policies"
	pathV1Tags                          = "/v1/tags"
	pathV1Tag                           = "/v1/tag"
	pathV1Untag                         = "/v1/untag"
	pathV1AuthToken                     = "/v1/authorization-token" //nolint:gosec // not a credential
	pathV1PackageGroup                  = "/v1/package-group"
	pathV1Package                       = "/v1/package"
	pathV1PackageVersion                = "/v1/package/version"
	pathV1PackageVersionsCopy           = "/v1/package/versions/copy"
	pathV1PackageVersionsDelete         = "/v1/package/versions/delete"
	pathV1PackageVersionsDispose        = "/v1/package/versions/dispose"
	pathV1PackageVersionsUpdateStatus   = "/v1/package/versions/update_status"
	pathV1PackageVersionAsset           = "/v1/package/version/asset"
	pathV1PackageVersionReadme          = "/v1/package/version/readme"
	pathV1PackageVersionAssets          = "/v1/package/version/assets"
	pathV1PackageVersionDependencies    = "/v1/package/version/dependencies"
	// pathV1PackageVersionPublish is PublishPackageVersion's real path --
	// verified against aws-sdk-go-v2's generated serializers.go
	// (httpbinding.SplitURI("/v1/package/version/publish")). Uses singular
	// "version" like the other single-package-version ops (asset/readme/
	// assets/dependencies), unlike the plural "versions" batch ops
	// (copy/delete/dispose/update_status) -- a prior version of this constant
	// used the plural form, which a real aws-sdk-go-v2 client's
	// PublishPackageVersion call would never have matched (404
	// UnknownOperationException), caught by
	// test/integration/codeartifact_test.go's SDK-driven coverage.
	pathV1PackageVersionPublish = "/v1/package/version/publish"
	// PutPackageOriginConfiguration has no path of its own in the real API: it shares
	// "/v1/package" (POST) with DescribePackage (GET) and DeletePackage (DELETE) -- see
	// parsePackageRoute. There is no separate "/v1/package/origin-configuration" path.
	pathV1PackageGroups            = "/v1/package-groups"
	pathV1PackageGroupAllowedRepos = "/v1/package-group-allowed-repositories"
	// pathV1ListAssociatedPackages is ListAssociatedPackages' real path. It does NOT
	// live under "/v1/package-group-*" like the sibling package-group ops.
	pathV1ListAssociatedPackages          = "/v1/list-associated-packages"
	pathV1PackageGroupOriginConfiguration = "/v1/package-group-origin-configuration"
	// pathV1SubPackageGroups is ListSubPackageGroups' real path, nested under
	// "/v1/package-groups" (it happens to also satisfy the "/v1/package" prefix test).
	pathV1SubPackageGroups = "/v1/package-groups/sub-groups"
	// pathV1AssociatedPackageGroup is GetAssociatedPackageGroup's real path. It does NOT
	// live under "/v1/package" and is not the same path as ListAssociatedPackages above.
	pathV1AssociatedPackageGroup = "/v1/get-associated-package-group"
	pathV1Packages               = "/v1/packages"
	pathV1PackageVersions        = "/v1/package/versions"
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
		path == pathV1Tag || path == pathV1Untag || path == pathV1AuthToken ||
		path == pathV1AssociatedPackageGroup || path == pathV1ListAssociatedPackages
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
		path == pathV1PackageVersionPublish || path == pathV1PackageGroupAllowedRepos ||
		path == pathV1PackageGroupOriginConfiguration || path == pathV1SubPackageGroups ||
		path == pathV1Packages || path == pathV1PackageVersions
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
	pathV1Domains:                       opListDomains,
	pathV1DomainRepositories:            opListRepositoriesInDomain,
	pathV1RepositoryEndpoint:            opGetRepositoryEndpoint,
	pathV1Repositories:                  opListRepositories,
	pathV1RepositoryPermissionsPolicies: opDeleteRepositoryPermissionsPolicy,
	pathV1Tags:                          opListTagsForResource,
	pathV1Tag:                           opTagResource,
	pathV1Untag:                         opUntagResource,
	pathV1AuthToken:                     opGetAuthorizationToken,
	pathV1AssociatedPackageGroup:        opGetAssociatedPackageGroup,
	pathV1ListAssociatedPackages:        opListAssociatedPackages,
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
	pathV1PackageVersionPublish:           opPublishPackageVersion,
	pathV1PackageVersionAsset:             opGetPackageVersionAsset,
	pathV1PackageVersionReadme:            opGetPackageVersionReadme,
	pathV1PackageVersionAssets:            opListPackageVersionAssets,
	pathV1PackageVersionDependencies:      opListPackageVersionDependencies,
	pathV1PackageGroupAllowedRepos:        opListAllowedRepositoriesForGroup,
	pathV1PackageGroupOriginConfiguration: opUpdatePackageGroupOriginConfiguration,
	pathV1SubPackageGroups:                opListSubPackageGroups,
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

// parseRepositoryPermissionsRoute handles the singular "/v1/repository/permissions/policy"
// path. Unlike Get/Put, DeleteRepositoryPermissionsPolicy does NOT live here -- it has its
// own plural "/v1/repository/permissions/policies" path (see domainRepoStaticRoutes),
// verified against aws-sdk-go-v2 serializers.go.

func parseRepositoryPermissionsRoute(method string) codeartifactRoute {
	switch method {
	case http.MethodGet:
		return codeartifactRoute{operation: opGetRepositoryPermissionsPolicy}
	case http.MethodPut:
		return codeartifactRoute{operation: opPutRepositoryPermissionsPolicy}
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

// parsePackageRoute handles the shared "/v1/package" path. PutPackageOriginConfiguration
// has no path of its own in the real API -- it is POST on this same path (verified against
// aws-sdk-go-v2 serializers.go), alongside GET DescribePackage and DELETE DeletePackage.

func parsePackageRoute(method string) codeartifactRoute {
	switch method {
	case http.MethodGet:
		return codeartifactRoute{operation: opDescribePackage}
	case http.MethodDelete:
		return codeartifactRoute{operation: opDeletePackage}
	case http.MethodPost:
		return codeartifactRoute{operation: opPutPackageOriginConfiguration}
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
		// Attach the resolved region to the request context so backend operations
		// are routed to the correct region.
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
		ctx := context.WithValue(c.Request().Context(), regionContextKey{}, region)
		c.SetRequest(c.Request().WithContext(ctx))

		log := logger.Load(ctx)
		path := c.Request().URL.Path
		route := parseCodeArtifactPath(c.Request().Method, path)

		log.Debug("codeartifact request", "operation", route.operation, "path", path)

		return h.dispatch(c, route, readRequestBody(c, route.operation))
	}
}

// readRequestBody extracts the request body appropriately for op's wire shape.
// PublishPackageVersion's httpPayload is the raw asset content (application/octet-stream),
// not a JSON document -- attempting a JSON decode there would silently discard every
// published asset's bytes. Every other op's httpPayload-less body is a JSON document.

func readRequestBody(c *echo.Context, op string) []byte {
	if c.Request().Body == nil {
		return nil
	}

	if op == opPublishPackageVersion {
		raw, err := io.ReadAll(c.Request().Body)
		if err != nil {
			return nil
		}

		return raw
	}

	decoder := json.NewDecoder(c.Request().Body)
	var raw json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return nil
	}

	return raw
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
		opListDomains: func(c *echo.Context, body []byte) error {
			return h.handleListDomains(c, body)
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
				q.Get(keyExternalConnectionQuery),
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

func (h *Handler) buildPackageOps() map[string]func(*echo.Context, []byte) error {
	ops := h.buildPackageCoreOps()
	maps.Copy(ops, h.buildPackageVersionOps())

	return ops
}

func (h *Handler) buildPackageCoreOps() map[string]func(*echo.Context, []byte) error {
	return map[string]func(*echo.Context, []byte) error{
		opCopyPackageVersions: func(c *echo.Context, body []byte) error {
			q := c.Request().URL.Query()

			return h.handleCopyPackageVersions(
				c, q.Get(keyDomain), q.Get(keySourceRepositoryQuery), q.Get(keyDestinationRepositoryQuery),
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

			return h.handleDeletePackageGroup(c, q.Get(keyDomain), q.Get(keyPackageGroupQuery))
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

			return h.handleDescribePackageGroup(c, q.Get(keyDomain), q.Get(keyPackageGroupQuery))
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
				c, q.Get(keyDomain), q.Get(keyRepository), q.Get(keyExternalConnectionQuery),
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
	}
}

func (h *Handler) buildPackageVersionOps() map[string]func(*echo.Context, []byte) error {
	return map[string]func(*echo.Context, []byte) error{
		opListAllowedRepositoriesForGroup: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleListAllowedRepositoriesForGroup(c, q.Get(keyDomain), q.Get(keyPackageGroupQuery))
		},
		opListAssociatedPackages: func(c *echo.Context, _ []byte) error {
			q := c.Request().URL.Query()

			return h.handleListAssociatedPackages(c, q.Get(keyDomain), q.Get(keyPackageGroupQuery))
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

			return h.handleListSubPackageGroups(c, q.Get(keyDomain), q.Get(keyPackageGroupQuery))
		},
		opPublishPackageVersion: func(c *echo.Context, body []byte) error {
			q := c.Request().URL.Query()

			return h.handlePublishPackageVersion(
				c, q.Get(keyDomain), q.Get(keyRepository), q.Get("format"),
				q.Get("namespace"), q.Get("package"), q.Get(keyVersion), q.Get("asset"),
				c.Request().Header.Get("X-Amz-Content-Sha256"), body,
			)
		},
		opPutPackageOriginConfiguration: func(c *echo.Context, body []byte) error {
			q := c.Request().URL.Query()

			return h.handlePutPackageOriginConfiguration(
				c, q.Get(keyDomain), q.Get(keyRepository), q.Get("format"),
				q.Get("namespace"), q.Get("package"), body,
			)
		},
		opUpdatePackageGroup: func(c *echo.Context, body []byte) error {
			return h.handleUpdatePackageGroup(c, c.Request().URL.Query().Get(keyDomain), body)
		},
		opUpdatePackageGroupOriginConfiguration: func(c *echo.Context, body []byte) error {
			q := c.Request().URL.Query()

			return h.handleUpdatePackageGroupOriginConfiguration(c, q.Get(keyDomain), q.Get(keyPackageGroupQuery), body)
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

// parseMaxResults parses an integer from a query-param string; returns 0 on empty/invalid.

func parseMaxResults(s string) int {
	if s == "" {
		return 0
	}
	n, _ := strconv.Atoi(s)

	return n
}

// paginateSlice applies cursor-based pagination to a pre-sorted slice.
// keyFn must return the same value used for sorting. nextToken is the key of the
// first item on the next page (opaque to callers). Returns (page, nextToken).

func paginateSlice[T any](list []T, maxResults int, nextToken string, keyFn func(T) string) ([]T, string) {
	const defaultMax = 100
	limit := maxResults
	if limit <= 0 || limit > defaultMax {
		limit = defaultMax
	}

	start := 0
	if nextToken != "" {
		for i := range list {
			if keyFn(list[i]) >= nextToken {
				start = i

				break
			}
			start = i + 1
		}
	}

	end := min(start+limit, len(list))
	page := list[start:end]
	next := ""
	if end < len(list) {
		next = keyFn(list[end])
	}

	return page, next
}

// epochSeconds returns the Unix epoch timestamp as a float64 for JSON serialization.
// The AWS CodeArtifact SDK deserializes timestamps as JSON numbers (epoch seconds).

func epochSeconds(ts time.Time) float64 {
	return float64(ts.Unix())
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
