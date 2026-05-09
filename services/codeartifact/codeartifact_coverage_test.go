package codeartifact_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/codeartifact"
)

// ---- Provider tests ----

func TestCAProvider_Name(t *testing.T) {
	t.Parallel()

	p := &codeartifact.Provider{}
	assert.Equal(t, "CodeArtifact", p.Name())
}

func TestCAProvider_Init_NilCtx(t *testing.T) {
	t.Parallel()

	p := &codeartifact.Provider{}
	_, err := p.Init(nil)
	// CodeArtifact requires a non-nil context
	require.Error(t, err)
}

func TestCAProvider_Init_WithCtx(t *testing.T) {
	t.Parallel()

	p := &codeartifact.Provider{}
	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, reg)
}

// setupDomain creates a domain and returns its name.
func setupDomain(t *testing.T, h *codeartifact.Handler, name string) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/v1/domain?domain="+name, nil)
	require.Equal(t, http.StatusOK, rec.Code)
}

// setupRepo creates a repository in a domain and returns its name.
func setupRepo(t *testing.T, h *codeartifact.Handler, domain, repo string) {
	t.Helper()

	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/v1/repository?domain="+domain+"&repository="+repo,
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)
}

// ---- DisassociateExternalConnection tests ----

func TestHandler_DisassociateExternalConnection(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "my-domain")
	setupRepo(t, h, "my-domain", "my-repo")

	// Disassociate (no connection exists - should succeed or return not found)
	rec := doRequest(
		t,
		h,
		http.MethodDelete,
		"/v1/repository/external-connection?domain=my-domain&repository=my-repo&externalConnection=public:npmjs",
		nil,
	)
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)

	// Missing domain
	rec = doRequest(t, h, http.MethodDelete,
		"/v1/repository/external-connection?repository=my-repo&externalConnection=public:npmjs",
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- DisposePackageVersions tests ----

func TestHandler_DisposePackageVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "dp-domain")
	setupRepo(t, h, "dp-domain", "dp-repo")

	// Dispose package versions
	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/v1/package/versions/dispose?domain=dp-domain&repository=dp-repo&format=npm&package=lodash",
		map[string]any{"versions": []string{"1.0.0", "1.0.1"}},
	)
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)

	// Missing domain
	rec = doRequest(t, h, http.MethodPost,
		"/v1/package/versions/dispose?repository=dp-repo&format=npm&package=lodash",
		map[string]any{"versions": []string{"1.0.0"}},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- GetAssociatedPackageGroup tests (backend-level) ----

func TestBackend_GetAssociatedPackageGroup(t *testing.T) {
	t.Parallel()

	b := codeartifact.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	_, err := b.CreateDomain("apg-domain", "", nil)
	require.NoError(t, err)

	// Get associated package group (returns nil, nil when no group matches)
	pg, err := b.GetAssociatedPackageGroup("apg-domain", "npm", "", "lodash")
	require.NoError(t, err)
	assert.Nil(t, pg)

	// Domain not found
	_, err = b.GetAssociatedPackageGroup("nonexistent", "npm", "", "lodash")
	require.Error(t, err)
}

// ---- GetPackageVersionAsset tests ----

func TestHandler_GetPackageVersionAsset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "pva-domain")
	setupRepo(t, h, "pva-domain", "pva-repo")

	// Get package version asset (no real asset)
	const assetPath = "/v1/package/version/asset" +
		"?domain=pva-domain&repository=pva-repo&format=npm" +
		"&package=lodash&version=1.0.0&asset=lodash-1.0.0.tgz"
	rec := doRequest(t, h, http.MethodGet, assetPath, nil)
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)

	// Missing domain
	rec = doRequest(
		t,
		h,
		http.MethodGet,
		"/v1/package/version/asset?repository=pva-repo&format=npm&package=lodash&version=1.0.0&asset=x.tgz",
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- GetPackageVersionReadme tests ----

func TestHandler_GetPackageVersionReadme(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "pvr-domain")
	setupRepo(t, h, "pvr-domain", "pvr-repo")

	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v1/package/version/readme?domain=pvr-domain&repository=pvr-repo&format=npm&package=lodash&version=1.0.0",
		nil,
	)
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)

	// Missing domain
	rec = doRequest(t, h, http.MethodGet,
		"/v1/package/version/readme?repository=pvr-repo&format=npm&package=lodash&version=1.0.0",
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- ListAllowedRepositoriesForGroup tests ----

func TestHandler_ListAllowedRepositoriesForGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "larg-domain")

	rec := doRequest(t, h, http.MethodGet,
		"/v1/package-group-allowed-repositories?domain=larg-domain&package-group=/",
		nil,
	)
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)

	// Missing domain
	rec = doRequest(t, h, http.MethodGet,
		"/v1/package-group-allowed-repositories?package-group=/",
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- ListAssociatedPackages tests ----

func TestHandler_ListAssociatedPackages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "lap-domain")

	rec := doRequest(t, h, http.MethodGet,
		"/v1/package-group-associated-packages?domain=lap-domain&package-group=/",
		nil,
	)
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)

	// Missing domain
	rec = doRequest(t, h, http.MethodGet,
		"/v1/package-group-associated-packages?package-group=/",
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- ListPackageGroups tests ----

func TestHandler_ListPackageGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "lpg-domain")

	rec := doRequest(t, h, http.MethodGet,
		"/v1/package-groups?domain=lpg-domain",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Missing domain
	rec = doRequest(t, h, http.MethodGet,
		"/v1/package-groups",
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- ListPackageVersionAssets tests ----

func TestHandler_ListPackageVersionAssets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "lpva-domain")
	setupRepo(t, h, "lpva-domain", "lpva-repo")

	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v1/package/version/assets?domain=lpva-domain&repository=lpva-repo&format=npm&package=lodash&version=1.0.0",
		nil,
	)
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)

	// Missing domain
	rec = doRequest(t, h, http.MethodGet,
		"/v1/package/version/assets?repository=lpva-repo&format=npm&package=lodash&version=1.0.0",
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- ListPackageVersionDependencies tests ----

func TestHandler_ListPackageVersionDependencies(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "lpvd-domain")
	setupRepo(t, h, "lpvd-domain", "lpvd-repo")

	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v1/package/version/dependencies?domain=lpvd-domain&repository=lpvd-repo&format=npm&package=lodash&version=1.0.0",
		nil,
	)
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)

	// Missing domain
	rec = doRequest(
		t,
		h,
		http.MethodGet,
		"/v1/package/version/dependencies?repository=lpvd-repo&format=npm&package=lodash&version=1.0.0",
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- ListPackageVersions tests ----

func TestHandler_ListPackageVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "lpv-domain")
	setupRepo(t, h, "lpv-domain", "lpv-repo")

	rec := doRequest(t, h, http.MethodGet,
		"/v1/package/versions?domain=lpv-domain&repository=lpv-repo&format=npm&package=lodash",
		nil,
	)
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)

	// Missing domain
	rec = doRequest(t, h, http.MethodGet,
		"/v1/package/versions?repository=lpv-repo&format=npm&package=lodash",
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- ListPackages tests ----

func TestHandler_ListPackages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "lp-domain")
	setupRepo(t, h, "lp-domain", "lp-repo")

	rec := doRequest(t, h, http.MethodGet,
		"/v1/packages?domain=lp-domain&repository=lp-repo",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Missing domain
	rec = doRequest(t, h, http.MethodGet,
		"/v1/packages?repository=lp-repo",
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- ListSubPackageGroups tests (backend-level) ----

func TestBackend_ListSubPackageGroups(t *testing.T) {
	t.Parallel()

	b := codeartifact.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	_, err := b.CreateDomain("lspg-domain", "", nil)
	require.NoError(t, err)

	groups, err := b.ListSubPackageGroups("lspg-domain", "/")
	require.NoError(t, err)
	assert.NotNil(t, groups)

	// Domain not found
	_, err = b.ListSubPackageGroups("nonexistent", "/")
	require.Error(t, err)
}

// ---- PublishPackageVersion tests ----

func TestHandler_PublishPackageVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "ppv-domain")
	setupRepo(t, h, "ppv-domain", "ppv-repo")

	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/v1/package/versions/publish?domain=ppv-domain&repository=ppv-repo&format=generic&package=mylib&version=1.0.0",
		map[string]any{"assetName": "mylib-1.0.0.zip", "assetSHA256": "abc123"},
	)
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)

	// Missing domain
	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/v1/package/versions/publish?repository=ppv-repo&format=generic&package=mylib&version=1.0.0",
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- PutPackageOriginConfiguration tests ----

func TestHandler_PutPackageOriginConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "ppoc-domain")
	setupRepo(t, h, "ppoc-domain", "ppoc-repo")

	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/v1/package/origin-configuration?domain=ppoc-domain&repository=ppoc-repo&format=npm&package=lodash",
		map[string]any{"restrictions": map[string]any{"publish": "ALLOW", "upstream": "ALLOW"}},
	)
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)

	// Missing domain
	rec = doRequest(t, h, http.MethodPost,
		"/v1/package/origin-configuration?repository=ppoc-repo&format=npm&package=lodash",
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- UpdatePackageGroup tests ----

func TestHandler_UpdatePackageGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "upg-domain")

	// Create a package group first
	doRequest(t, h, http.MethodPost,
		"/v1/package-group?domain=upg-domain",
		map[string]any{"packageGroup": "/test/"},
	)

	rec := doRequest(t, h, http.MethodPut,
		"/v1/package-group?domain=upg-domain",
		map[string]any{"packageGroup": "/test/", "description": "updated"},
	)
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)

	// Missing domain
	rec = doRequest(t, h, http.MethodPut,
		"/v1/package-group",
		map[string]any{"packageGroup": "/test/"},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- UpdatePackageGroupOriginConfiguration tests ----

func TestHandler_UpdatePackageGroupOriginConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "upgoc-domain")

	rec := doRequest(
		t,
		h,
		http.MethodPut,
		"/v1/package-group-origin-configuration?domain=upgoc-domain&package-group=/",
		map[string]any{
			"restrictions": map[string]any{"publish": map[string]any{"restrictionMode": "ALLOW"}},
		},
	)
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)

	// Missing domain
	rec = doRequest(t, h, http.MethodPut,
		"/v1/package-group-origin-configuration?package-group=/",
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- UpdatePackageVersionsStatus tests ----

func TestHandler_UpdatePackageVersionsStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "upvs-domain")
	setupRepo(t, h, "upvs-domain", "upvs-repo")

	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/v1/package/versions/update_status?domain=upvs-domain&repository=upvs-repo&format=npm&package=lodash",
		map[string]any{"targetStatus": "Archived", "versions": []string{"1.0.0"}},
	)
	assert.NotEqual(t, http.StatusInternalServerError, rec.Code)

	// Missing domain
	rec = doRequest(t, h, http.MethodPost,
		"/v1/package/versions/update_status?repository=upvs-repo&format=npm&package=lodash",
		nil,
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- UpdateRepository tests ----

func TestHandler_UpdateRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "ur-domain")
	setupRepo(t, h, "ur-domain", "ur-repo")

	rec := doRequest(t, h, http.MethodPut,
		"/v1/repository?domain=ur-domain&repository=ur-repo",
		map[string]any{"description": "updated repo description"},
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Missing domain
	rec = doRequest(t, h, http.MethodPut,
		"/v1/repository?repository=ur-repo",
		map[string]any{"description": "updated"},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---- Persistence coverage ----

func TestCABackend_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := codeartifact.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	_, err := b.CreateDomain("snap-domain", "", nil)
	require.NoError(t, err)

	_, err = b.CreateRepository("snap-domain", "snap-repo", "", nil)
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := codeartifact.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	err = b2.Restore(snap)
	require.NoError(t, err)

	doms := b2.ListDomains()
	require.Len(t, doms, 1)
}
