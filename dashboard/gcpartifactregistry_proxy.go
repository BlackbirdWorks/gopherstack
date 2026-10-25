package dashboard

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"google.golang.org/protobuf/proto"
)

const (
	gcpArtifactRegistryProxyPrefix = "/dashboard/api/v1/gcp/artifactregistry"

	arLocationSegs = 4
	arReposSegs    = 5
	arRepoSegs     = 6
	arRepoSubSegs  = 7
	arPkgSegs      = 8
	arPkgSubSegs   = 9
	arVersionSegs  = 10
	arLocationsIdx = 2
	arReposIdx     = 4
	arSubIdx       = 6
	arPkgSubIdx    = 8
	arReposKind    = "repositories"
	arPackagesKind = "packages"
	arFilesKind    = "files"
	arImagesKind   = "dockerImages"
	arVersionsKind = "versions"
	arTagsKind     = "tags"
)

// registerGCPArtifactRegistryRoutes exposes repositories, packages, versions, tags and files to the SPA.
func (h *DashboardHandler) registerGCPArtifactRegistryRoutes() {
	var route gcpRESTRoute

	if ops := h.config.GCPArtifactRegistryOps; ops != nil {
		route = (&gcpArtifactRegistryAPI{ar: ops.API()}).route
	}

	h.registerGCPRESTProxy(gcpArtifactRegistryProxyPrefix, route)
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
	ar artifactregistrypb.ArtifactRegistryServer
}

func (a *gcpArtifactRegistryAPI) route(ctx context.Context, r gcpRESTCall) (proto.Message, error) {
	n := len(r.segs)
	if n < arReposSegs || n > arVersionSegs || r.segs[0] != gcpRESTProjectsSeg ||
		r.segs[arLocationsIdx] != "locations" || r.segs[arReposIdx] != arReposKind || r.verb != "" {
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

	switch r.method {
	case http.MethodGet:
		return a.ar.GetRepository(ctx, &artifactregistrypb.GetRepositoryRequest{Name: name})
	case http.MethodDelete:
		return a.ar.DeleteRepository(ctx, &artifactregistrypb.DeleteRepositoryRequest{Name: name})
	}

	return nil, errGCPUnsupported()
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
	if r.segs[arSubIdx] != arPackagesKind || r.segs[arPkgSubIdx] != arVersionsKind {
		return nil, errGCPUnsupported()
	}

	name := arName(r, arVersionSegs)

	switch r.method {
	case http.MethodGet:
		view := artifactregistrypb.VersionView(artifactregistrypb.VersionView_value[r.query.Get("view")])

		return a.ar.GetVersion(ctx, &artifactregistrypb.GetVersionRequest{Name: name, View: view})
	case http.MethodDelete:
		return a.ar.DeleteVersion(ctx, &artifactregistrypb.DeleteVersionRequest{Name: name})
	}

	return nil, errGCPUnsupported()
}
