package ociartifacts

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Operation names, matching the OCI SDK operation names.
const (
	opCreateContainerRepo   = "CreateContainerRepository"
	opGetContainerRepo      = "GetContainerRepository"
	opListContainerRepos    = "ListContainerRepositories"
	opUpdateContainerRepo   = "UpdateContainerRepository"
	opDeleteContainerRepo   = "DeleteContainerRepository"
	opChangeContainerRepoC  = "ChangeContainerRepositoryCompartment"
	opListContainerImages   = "ListContainerImages"
	opGetContainerImage     = "GetContainerImage"
	opDeleteContainerImage  = "DeleteContainerImage"
	opUpdateContainerImage  = "UpdateContainerImage"
	opRestoreContainerImage = "RestoreContainerImage"
	opRemoveContainerVer    = "RemoveContainerVersion"
	opLookupImageByURI      = "LookupContainerImageByUri"
	opCreateImageSignature  = "CreateContainerImageSignature"
	opGetImageSignature     = "GetContainerImageSignature"
	opListImageSignatures   = "ListContainerImageSignatures"
	opUpdateImageSignature  = "UpdateContainerImageSignature"
	opDeleteImageSignature  = "DeleteContainerImageSignature"
	opGetContainerConfig    = "GetContainerConfiguration"
	opUpdateContainerConfig = "UpdateContainerConfiguration"
	opCreateRepository      = "CreateRepository"
	opGetRepository         = "GetRepository"
	opListRepositories      = "ListRepositories"
	opUpdateRepository      = "UpdateRepository"
	opDeleteRepository      = "DeleteRepository"
	opChangeRepositoryComp  = "ChangeRepositoryCompartment"
	opGetGenericArtifact    = "GetGenericArtifact"
	opGetGenericByPath      = "GetGenericArtifactByPath"
	opListGenericArtifacts  = "ListGenericArtifacts"
	opUpdateGenericArtifact = "UpdateGenericArtifact"
	opUpdateGenericByPath   = "UpdateGenericArtifactByPath"
	opDeleteGenericArtifact = "DeleteGenericArtifact"
	opDeleteGenericByPath   = "DeleteGenericArtifactByPath"
	opPutGenericContent     = "PutGenericArtifactContentByPath"
	opGetGenericContent     = "GetGenericArtifactContent"
	opGetGenericContentPath = "GetGenericArtifactContentByPath"
	opGetAccessToken        = "GetAccessToken"
	opRegistry              = "RegistryV2"
	opAdmin                 = "Admin"
	opUnknown               = "Unknown"
	adminPrefix             = "/_gopherstack/"
	registryPrefix          = "/v2"
	base                    = "/20160918"
	tokenPath               = "/20180419/docker/token"
	containerRepoPath       = base + "/container/repositories/{repositoryId}"
	containerImagePath      = base + "/container/images/{imageId}"
	signaturePath           = base + "/container/imageSignatures/{imageSignatureId}"
	repositoryPath          = base + "/repositories/{repositoryId}"
	genericArtifactPath     = base + "/generic/artifacts/{artifactId}"
	genericRepoPrefix       = base + "/generic/repositories/"
	artifactPathsSegment    = "/artifactPaths/"
	versionsSegment         = "/versions/"
	contentSuffix           = "/content"
)

//nolint:gochecknoglobals // immutable route table
var router = restroute.New([]restroute.Route{
	{Method: http.MethodPost, Path: base + "/container/repositories", Op: opCreateContainerRepo},
	{Method: http.MethodGet, Path: base + "/container/repositories", Op: opListContainerRepos},
	{Method: http.MethodGet, Path: containerRepoPath, Op: opGetContainerRepo},
	{Method: http.MethodPut, Path: containerRepoPath, Op: opUpdateContainerRepo},
	{Method: http.MethodDelete, Path: containerRepoPath, Op: opDeleteContainerRepo},
	{Method: http.MethodPost, Path: containerRepoPath + "/actions/changeCompartment", Op: opChangeContainerRepoC},
	{Method: http.MethodGet, Path: base + "/container/images", Op: opListContainerImages},
	{Method: http.MethodPost, Path: base + "/container/images/actions/lookupImageByUri", Op: opLookupImageByURI},
	{Method: http.MethodGet, Path: containerImagePath, Op: opGetContainerImage},
	{Method: http.MethodPut, Path: containerImagePath, Op: opUpdateContainerImage},
	{Method: http.MethodDelete, Path: containerImagePath, Op: opDeleteContainerImage},
	{Method: http.MethodPost, Path: containerImagePath + "/actions/restore", Op: opRestoreContainerImage},
	{Method: http.MethodPost, Path: containerImagePath + "/actions/removeVersion", Op: opRemoveContainerVer},
	{Method: http.MethodPost, Path: base + "/container/imageSignatures", Op: opCreateImageSignature},
	{Method: http.MethodGet, Path: base + "/container/imageSignatures", Op: opListImageSignatures},
	{Method: http.MethodGet, Path: signaturePath, Op: opGetImageSignature},
	{Method: http.MethodPut, Path: signaturePath, Op: opUpdateImageSignature},
	{Method: http.MethodDelete, Path: signaturePath, Op: opDeleteImageSignature},
	{Method: http.MethodGet, Path: base + "/container/configuration", Op: opGetContainerConfig},
	{Method: http.MethodPut, Path: base + "/container/configuration", Op: opUpdateContainerConfig},
	{Method: http.MethodPost, Path: base + "/repositories", Op: opCreateRepository},
	{Method: http.MethodGet, Path: base + "/repositories", Op: opListRepositories},
	{Method: http.MethodGet, Path: repositoryPath, Op: opGetRepository},
	{Method: http.MethodPut, Path: repositoryPath, Op: opUpdateRepository},
	{Method: http.MethodDelete, Path: repositoryPath, Op: opDeleteRepository},
	{Method: http.MethodPost, Path: repositoryPath + "/actions/changeCompartment", Op: opChangeRepositoryComp},
	{Method: http.MethodGet, Path: base + "/generic/artifacts", Op: opListGenericArtifacts},
	{Method: http.MethodGet, Path: genericArtifactPath, Op: opGetGenericArtifact},
	{Method: http.MethodPut, Path: genericArtifactPath, Op: opUpdateGenericArtifact},
	{Method: http.MethodDelete, Path: genericArtifactPath, Op: opDeleteGenericArtifact},
	{Method: http.MethodGet, Path: genericArtifactPath + "/content", Op: opGetGenericContent},
	{Method: http.MethodGet, Path: tokenPath, Op: opGetAccessToken},
})

type route struct {
	params map[string]string
	op     string
}

// parseRoute maps method and path to an operation.
func parseRoute(r *http.Request) route {
	path := r.URL.EscapedPath()

	switch {
	case strings.HasPrefix(path, adminPrefix):
		return route{op: opAdmin}
	case path == registryPrefix || strings.HasPrefix(path, registryPrefix+"/"):
		return route{op: opRegistry}
	}

	if rt, ok := parseGenericByPath(r.Method, path); ok {
		return rt
	}

	op, params := router.Match(r.Method, path)
	if op == "" {
		op = opUnknown
	}

	return route{op: op, params: params}
}

// parseGenericByPath matches the by-path generic artifact routes. The SDK does not escape "/" in artifactPath,
// so the path is everything between /artifactPaths/ and the last /versions/.
func parseGenericByPath(method, escaped string) (route, bool) {
	rest, ok := strings.CutPrefix(escaped, genericRepoPrefix)
	if !ok {
		return route{}, false
	}

	repoID, rest, ok := strings.Cut(rest, artifactPathsSegment)
	if !ok {
		return route{}, false
	}

	isContent := false

	if trimmed, found := strings.CutSuffix(rest, contentSuffix); found {
		rest, isContent = trimmed, true
	}

	i := strings.LastIndex(rest, versionsSegment)
	if i < 0 {
		return route{}, false
	}

	params := map[string]string{}

	parts := map[string]string{
		"repositoryId": repoID, "artifactPath": rest[:i], "version": rest[i+len(versionsSegment):],
	}

	for k, v := range parts {
		dec, err := url.PathUnescape(v)
		if err != nil || dec == "" {
			return route{}, false
		}

		params[k] = dec
	}

	ops := map[string]string{
		http.MethodGet:    opGetGenericByPath,
		http.MethodPut:    opUpdateGenericByPath,
		http.MethodDelete: opDeleteGenericByPath,
	}
	if isContent {
		ops = map[string]string{http.MethodGet: opGetGenericContentPath, http.MethodPut: opPutGenericContent}
	}

	op, found := ops[method]
	if !found {
		return route{}, false
	}

	return route{op: op, params: params}, true
}
