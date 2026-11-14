package dashboard

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/labstack/echo/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/blackbirdworks/gopherstack/services/gcpartifactregistry"
)

const (
	gcpArtifactRegistryProxyPrefix = "/dashboard/api/v1/gcp/artifactregistry"

	arLocationSegs  = 4
	arReposSegs     = 5
	arRepoSegs      = 6
	arRepoSubSegs   = 7
	arPkgSegs       = 8
	arPkgSubSegs    = 9
	arVersionSegs   = 10
	arLocationsIdx  = 2
	arReposIdx      = 4
	arSubIdx        = 6
	arPkgSubIdx     = 8
	arReposKind     = "repositories"
	arPackagesKind  = "packages"
	arFilesKind     = "files"
	arImagesKind    = "dockerImages"
	arVersionsKind  = "versions"
	arTagsKind      = "tags"
	arVerbCleanup   = "cleanupPreview"
	arLocationsKind = "locations"

	arDownloadMaxBytes = 32 << 20
	arFileSegs         = 8
)

// registerGCPArtifactRegistryRoutes exposes repositories, packages, versions, tags and files to the SPA.
func (h *DashboardHandler) registerGCPArtifactRegistryRoutes() {
	var route gcpRESTRoute

	api := (*gcpArtifactRegistryAPI)(nil)

	if ops := h.config.GCPArtifactRegistryOps; ops != nil {
		api = &gcpArtifactRegistryAPI{ar: ops.API()}
		route = api.route
	}

	h.registerGCPRESTProxy(gcpArtifactRegistryProxyPrefix, route)
	h.SubRouter.Add(http.MethodGet, gcpArtifactRegistryProxyPrefix+"/download", func(c *echo.Context) error {
		if api == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{keyError: errServiceUnavailable})
		}

		return api.download(c)
	})
}

// arFileNameOK accepts only projects/P/locations/L/repositories/R/files/F names, with F still path-escaped.
func arFileNameOK(name string) bool {
	segs := strings.Split(name, "/")
	if len(segs) != arFileSegs || segs[0] != gcpRESTProjectsSeg || segs[arLocationsIdx] != arLocationsKind ||
		segs[arReposIdx] != arReposKind || segs[arSubIdx] != arFilesKind {
		return false
	}

	for _, s := range segs {
		if s == "" || s == "." || s == ".." {
			return false
		}
	}

	return true
}

// download streams one repository file as an attachment, size-capped and never rendered by the browser.
func (a *gcpArtifactRegistryAPI) download(c *echo.Context) error {
	name := c.QueryParam("name")
	if !arFileNameOK(name) {
		return writeGCPRESTError(c, status.Error(codes.InvalidArgument, "invalid file name"))
	}

	data, err := a.ar.FileContent(c.Request().Context(), name, arDownloadMaxBytes)
	if err != nil {
		return writeGCPRESTError(c, err)
	}

	leaf, _ := url.PathUnescape(name[strings.LastIndex(name, "/")+1:])
	leaf = strings.NewReplacer("\"", "_", "\r", "_", "\n", "_", "/", "_").Replace(leaf)

	h := c.Response().Header()
	h.Set("Content-Type", "application/octet-stream")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Disposition", "attachment; filename="+strconv.Quote(leaf))

	return c.Blob(http.StatusOK, "application/octet-stream", data)
}

// arName joins the first n segments, re-escaping package, version and file IDs
// (which contain %2F for slashes) so they match the backend's resource names.
func arName(r gcpRESTCall, n int) string {
	parts := make([]string, n)

	for i, s := range r.segs[:n] {
		if i > arRepoSegs {
			s = url.PathEscape(s)
		}

		parts[i] = s
	}

	return strings.Join(parts, "/")
}

type gcpArtifactRegistryAPI struct {
	ar *gcpartifactregistry.API
}

func (a *gcpArtifactRegistryAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if n < arReposSegs || n > arVersionSegs || r.segs[0] != gcpRESTProjectsSeg ||
		r.segs[arLocationsIdx] != arLocationsKind || r.segs[arReposIdx] != arReposKind ||
		(r.verb != "" && (r.verb != arVerbCleanup || n != arRepoSegs)) {
		return nil, errGCPUnsupported()
	}

	switch n {
	case arReposSegs:
		return a.repositories(ctx, r)
	case arRepoSegs:
		return a.repository(ctx, r)
	case arRepoSubSegs:
		return a.repoChildren(ctx, r)
	case arPkgSegs:
		return a.pkg(ctx, r)
	case arPkgSubSegs:
		return a.pkgChildren(ctx, r)
	}

	return a.version(ctx, r)
}

func (a *gcpArtifactRegistryAPI) repositories(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if r.method != http.MethodGet {
		return nil, errGCPUnsupported()
	}

	return a.ar.ListRepositories(ctx, &artifactregistrypb.ListRepositoriesRequest{
		Parent: r.name(arLocationSegs), PageSize: r.pageSize(), PageToken: r.query.Get("pageToken"),
	})
}

func (a *gcpArtifactRegistryAPI) repository(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := r.name(arRepoSegs)

	if r.verb == arVerbCleanup {
		return a.cleanupPreview(ctx, r, name)
	}

	switch r.method {
	case http.MethodGet:
		return a.ar.GetRepository(ctx, &artifactregistrypb.GetRepositoryRequest{Name: name})
	case http.MethodDelete:
		return a.ar.DeleteRepository(ctx, &artifactregistrypb.DeleteRepositoryRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}

// cleanupPreview reports what the repository's cleanup policies would delete now; nothing is deleted.
func (a *gcpArtifactRegistryAPI) cleanupPreview(
	ctx context.Context, r gcpRESTCall, name string,
) (proto.Message, error) {
	if r.method != http.MethodGet {
		return nil, errGCPUnsupported()
	}

	repo, err := a.ar.GetRepository(ctx, &artifactregistrypb.GetRepositoryRequest{Name: name})
	if err != nil {
		return nil, err //nolint:wrapcheck // backend status passes through
	}

	names, err := a.ar.PreviewCleanup(ctx, name)
	if err != nil {
		return nil, err //nolint:wrapcheck // backend status passes through
	}

	list := make([]any, 0, len(names))
	for _, n := range names {
		list = append(list, n)
	}

	out, sErr := structpb.NewStruct(map[string]any{
		"dryRun":       repo.GetCleanupPolicyDryRun(),
		"policies":     float64(len(repo.GetCleanupPolicies())),
		arVersionsKind: list,
	})
	if sErr != nil {
		return nil, status.Error(codes.Internal, sErr.Error())
	}

	return out, nil
}

func (a *gcpArtifactRegistryAPI) repoChildren(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if r.method != http.MethodGet {
		return nil, errGCPUnsupported()
	}

	parent := r.name(arRepoSegs)
	size, token := r.pageSize(), r.query.Get("pageToken")

	switch r.segs[arSubIdx] {
	case arPackagesKind:
		return a.ar.ListPackages(ctx, &artifactregistrypb.ListPackagesRequest{
			Parent: parent, PageSize: size, PageToken: token,
		})
	case arFilesKind:
		return a.ar.ListFiles(ctx, &artifactregistrypb.ListFilesRequest{
			Parent: parent, PageSize: size, PageToken: token, Filter: r.query.Get("filter"),
		})
	case arImagesKind:
		return a.ar.ListDockerImages(ctx, &artifactregistrypb.ListDockerImagesRequest{
			Parent: parent, PageSize: size, PageToken: token,
		})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpArtifactRegistryAPI) pkg(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	name := arName(r, arPkgSegs)

	switch {
	case r.segs[arSubIdx] == arFilesKind && r.method == http.MethodGet:
		return a.ar.GetFile(ctx, &artifactregistrypb.GetFileRequest{Name: name})
	case r.segs[arSubIdx] == arPackagesKind && r.method == http.MethodGet:
		return a.ar.GetPackage(ctx, &artifactregistrypb.GetPackageRequest{Name: name})
	case r.segs[arSubIdx] == arPackagesKind && r.method == http.MethodDelete:
		return a.ar.DeletePackage(ctx, &artifactregistrypb.DeletePackageRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpArtifactRegistryAPI) pkgChildren(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if r.method != http.MethodGet || r.segs[arSubIdx] != arPackagesKind {
		return nil, errGCPUnsupported()
	}

	parent := arName(r, arPkgSegs)
	size, token := r.pageSize(), r.query.Get("pageToken")

	switch r.segs[arPkgSubIdx] {
	case arVersionsKind:
		view := artifactregistrypb.VersionView(artifactregistrypb.VersionView_value[r.query.Get("view")])

		return a.ar.ListVersions(ctx, &artifactregistrypb.ListVersionsRequest{
			Parent: parent, PageSize: size, PageToken: token, View: view,
		})
	case arTagsKind:
		return a.ar.ListTags(ctx, &artifactregistrypb.ListTagsRequest{
			Parent: parent, PageSize: size, PageToken: token, Filter: r.query.Get("filter"),
		})
	}

	return nil, errGCPUnsupported()
}

func (a *gcpArtifactRegistryAPI) version(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	if r.segs[arSubIdx] != arPackagesKind {
		return nil, errGCPUnsupported()
	}

	name := arName(r, arVersionSegs)

	if r.segs[arPkgSubIdx] == arTagsKind {
		switch r.method {
		case http.MethodGet:
			return a.ar.GetTag(ctx, &artifactregistrypb.GetTagRequest{Name: name})
		case http.MethodDelete:
			return a.ar.DeleteTag(ctx, &artifactregistrypb.DeleteTagRequest{Name: name})
		}

		return nil, errGCPUnsupported()
	}

	if r.segs[arPkgSubIdx] != arVersionsKind {
		return nil, errGCPUnsupported()
	}

	switch r.method {
	case http.MethodGet:
		view := artifactregistrypb.VersionView(artifactregistrypb.VersionView_value[r.query.Get("view")])

		return a.ar.GetVersion(ctx, &artifactregistrypb.GetVersionRequest{Name: name, View: view})
	case http.MethodDelete:
		return a.ar.DeleteVersion(ctx, &artifactregistrypb.DeleteVersionRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}
