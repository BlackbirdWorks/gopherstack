package dashboard_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/ociartifacts"
)

const (
	artComp = "ocid1.compartment.oc1..art"
	artBase = "/dashboard/api/v1/oci/artifacts"
	artAPI  = artBase + "/api"
	artMsg  = "TOP-SECRET-SIGNED-MESSAGE"
	artSig  = "TOP-SECRET-SIGNATURE-VALUE"
)

type artFixture struct {
	h       *dashboard.DashboardHandler
	ops     *ociartifacts.Handler
	srvURL  string
	imageID string
	digest  string
}

func artDo(t *testing.T, h *dashboard.DashboardHandler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	return lbDo(t, h, method, path, body)
}

func (f artFixture) direct(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	e.Any("/*", f.ops.Trusted())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	e.ServeHTTP(rec, req)

	return rec
}

func newArtDashboard(t *testing.T) artFixture {
	t.Helper()

	be := ociartifacts.NewInMemoryBackend()
	be.SetContext(t.Context())

	ops := ociartifacts.NewHandler(be)

	e := echo.New()
	e.Any("/*", ops.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	login := base64.StdEncoding.EncodeToString([]byte(ociartifacts.DefaultNamespace + "/alice:a-registry-auth-token"))
	reg := ocitest.Client{T: t, Base: srv.URL, HTTP: testclient.New(t), Prepare: func(r *http.Request) {
		r.Header.Set("Authorization", "Basic "+login)
	}}
	img := reg.PushImage(ociartifacts.DefaultNamespace+"/web/api", "v1")

	f := artFixture{
		h: dashboard.NewHandler(dashboard.Config{OCIArtifactsOps: ops}), ops: ops, srvURL: srv.URL,
		digest: ocitest.Digest(img.Body),
	}

	rec := artDo(t, f.h, http.MethodGet, artAPI+"/containerImages?compartmentId="+artTenancy(t, f), "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var list struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &list))
	require.Len(t, list.Items, 1)
	f.imageID = list.Items[0].ID

	return f
}

func artTenancy(t *testing.T, f artFixture) string {
	t.Helper()

	rec := artDo(t, f.h, http.MethodGet, artBase+"/compartments", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out struct {
		Compartments []string `json:"compartments"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.Compartments)

	return out.Compartments[0]
}

func TestOCIArtifactsProxyRead(t *testing.T) {
	t.Parallel()

	f := newArtDashboard(t)
	tenancy := artTenancy(t, f)

	runGCPSteps(t, f.h, []gcpStep{
		{http.MethodGet, artBase + "/compartments", "", tenancy, 200},
		{
			http.MethodGet,
			artAPI + "/containerRepositories?compartmentId=" + tenancy,
			"",
			`"displayName":"web/api"`,
			200,
		},
		{http.MethodGet, artAPI + "/containerImages?compartmentId=" + tenancy, "", f.digest, 200},
		{http.MethodGet, artAPI + "/containerImages/" + f.imageID, "", `"layers"`, 200},
		{
			http.MethodGet,
			artAPI + "/containerConfiguration?compartmentId=" + tenancy,
			"",
			`"namespace":"gopherstack"`,
			200,
		},
		{http.MethodGet, artAPI + "/genericRepositories?compartmentId=" + tenancy, "", "items", 200},
		{http.MethodGet, artAPI + "/containerImages/missing", "", "NotAuthorizedOrNotFound", 404},
	})
}

func TestOCIArtifactsProxyRepositoryActions(t *testing.T) {
	t.Parallel()

	f := newArtDashboard(t)

	create := artDo(t, f.h, http.MethodPost, artAPI+"/containerRepositories",
		`{"compartmentId":"`+artComp+`","displayName":"team/svc","isPublic":true,"readme":{"content":"# hi"},`+
			`"freeformTags":{"forged":"yes"},"createdBy":"forged","id":"forged"}`)
	require.Equal(t, http.StatusOK, create.Code, create.Body.String())

	var repo struct {
		Tags   map[string]string `json:"freeformTags"`
		Readme struct {
			Format string `json:"format"`
		} `json:"readme"`
		ID        string `json:"id"`
		CreatedBy string `json:"createdBy"`
		Public    bool   `json:"isPublic"`
		Immutable bool   `json:"isImmutable"`
	}

	require.NoError(t, json.Unmarshal(create.Body.Bytes(), &repo))
	assert.NotEqual(t, "forged", repo.ID)
	assert.NotEqual(t, "forged", repo.CreatedBy)
	assert.Empty(t, repo.Tags)
	assert.True(t, repo.Public)
	assert.False(t, repo.Immutable)
	assert.Equal(t, "TEXT_MARKDOWN", repo.Readme.Format)

	upd := artDo(t, f.h, http.MethodPut, artAPI+"/containerRepositories/"+repo.ID,
		`{"isImmutable":true,"isPublic":false,"freeformTags":{"forged":"yes"},"compartmentId":"x"}`)
	require.Equal(t, http.StatusOK, upd.Code, upd.Body.String())
	require.NoError(t, json.Unmarshal(upd.Body.Bytes(), &repo))
	assert.True(t, repo.Immutable)
	assert.False(t, repo.Public)
	assert.Empty(t, repo.Tags)

	assert.Equal(t, http.StatusNoContent,
		artDo(t, f.h, http.MethodDelete, artAPI+"/containerRepositories/"+repo.ID, "").Code)
	assert.Equal(
		t,
		http.StatusNotFound,
		artDo(t, f.h, http.MethodGet, artAPI+"/containerRepositories/"+repo.ID, "").Code,
	)
}

func TestOCIArtifactsProxyImageDeleteRestoreAndConfig(t *testing.T) {
	t.Parallel()

	f := newArtDashboard(t)
	tenancy := artTenancy(t, f)
	img := artAPI + "/containerImages/" + f.imageID

	assert.Equal(t, http.StatusNoContent, artDo(t, f.h, http.MethodDelete, img, "").Code)
	assert.NotContains(t,
		artDo(t, f.h, http.MethodGet, artAPI+"/containerImages?compartmentId="+tenancy, "").Body.String(), f.digest)
	assert.Contains(t,
		artDo(t, f.h, http.MethodGet, artAPI+"/containerImages?compartmentId="+tenancy+"&lifecycleState=DELETED", "").
			Body.String(), f.digest)

	restored := artDo(t, f.h, http.MethodPost, img+"/restore", "")
	require.Equal(t, http.StatusOK, restored.Code, restored.Body.String())
	assert.Contains(t, restored.Body.String(), `"lifecycleState":"AVAILABLE"`)

	cfg := artDo(t, f.h, http.MethodPut, artAPI+"/containerConfiguration?compartmentId="+tenancy,
		`{"isRepositoryCreatedOnFirstPush":false,"namespace":"forged"}`)
	require.Equal(t, http.StatusOK, cfg.Code, cfg.Body.String())
	assert.Contains(t, cfg.Body.String(), `"isRepositoryCreatedOnFirstPush":false`)
	assert.Contains(t, cfg.Body.String(), `"namespace":"gopherstack"`)
}

func TestOCIArtifactsProxySignaturesAreScrubbed(t *testing.T) {
	t.Parallel()

	f := newArtDashboard(t)
	tenancy := artTenancy(t, f)
	enc := base64.StdEncoding.EncodeToString

	body, err := json.Marshal(map[string]string{
		"compartmentId": tenancy, "imageId": f.imageID, "kmsKeyId": "ocid1.key.oc1..kmskey0001",
		"kmsKeyVersionId": "ocid1.keyversion.oc1..ver", "message": enc([]byte(artMsg)),
		"signature": enc([]byte(artSig)), "signingAlgorithm": "SHA_256_RSA_PKCS_PSS",
	})
	require.NoError(t, err)

	made := f.direct(t, http.MethodPost, "/20160918/container/imageSignatures", string(body))
	require.Equal(t, http.StatusOK, made.Code, made.Body.String())
	require.Contains(t, made.Body.String(), enc([]byte(artSig)), "control plane keeps the blob")

	for _, q := range []string{"", "&imageId=" + f.imageID} {
		rec := artDo(t, f.h, http.MethodGet, artAPI+"/containerImageSignatures?compartmentId="+tenancy+q, "")
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), "ocid1.key.oc1..kmskey0001")
		assert.Contains(t, rec.Body.String(), "SHA_256_RSA_PKCS_PSS")
		assert.NotContains(t, rec.Body.String(), enc([]byte(artSig)))
		assert.NotContains(t, rec.Body.String(), enc([]byte(artMsg)))
		assert.NotContains(t, rec.Body.String(), `"signature"`)
		assert.NotContains(t, rec.Body.String(), `"message"`)
	}
}

func TestOCIArtifactsProxyGenericLifecycleAndDownload(t *testing.T) {
	t.Parallel()

	f := newArtDashboard(t)

	made := artDo(t, f.h, http.MethodPost, artAPI+"/genericRepositories",
		`{"compartmentId":"`+artComp+`","displayName":"files","description":"d","isImmutable":false,"id":"forged"}`)
	require.Equal(t, http.StatusOK, made.Code, made.Body.String())

	var repo struct {
		ID string `json:"id"`
	}

	require.NoError(t, json.Unmarshal(made.Body.Bytes(), &repo))
	assert.NotEqual(t, "forged", repo.ID)

	put := f.direct(t, http.MethodPut,
		"/20160918/generic/repositories/"+repo.ID+"/artifactPaths/dir/bad%22name%20x.html/versions/1.0/content",
		"<script>alert(1)</script>")
	require.Equal(t, http.StatusOK, put.Code, put.Body.String())

	list := artDo(t, f.h, http.MethodGet,
		artAPI+"/genericArtifacts?compartmentId="+artComp+"&repositoryId="+repo.ID, "")
	require.Equal(t, http.StatusOK, list.Code, list.Body.String())

	var arts struct {
		Items []struct {
			ID     string `json:"id"`
			Sha256 string `json:"sha256"`
		} `json:"items"`
	}

	require.NoError(t, json.Unmarshal(list.Body.Bytes(), &arts))
	require.Len(t, arts.Items, 1)
	assert.NotEmpty(t, arts.Items[0].Sha256)

	dl := artDo(t, f.h, http.MethodGet, artAPI+"/genericArtifacts/"+arts.Items[0].ID+"/content", "")
	require.Equal(t, http.StatusOK, dl.Code, dl.Body.String())
	assert.Equal(t, "<script>alert(1)</script>", dl.Body.String())
	assert.Equal(t, "application/octet-stream", dl.Header().Get("Content-Type"))
	assert.Equal(t, `attachment; filename="bad_name_x.html"`, dl.Header().Get("Content-Disposition"))
	assert.Equal(t, "nosniff", dl.Header().Get("X-Content-Type-Options"))
	assert.Contains(t, dl.Header().Get("Content-Security-Policy"), "sandbox")

	missing := artDo(t, f.h, http.MethodGet, artAPI+"/genericArtifacts/nope/content", "")
	assert.Equal(t, http.StatusNotFound, missing.Code)

	assert.Equal(t, http.StatusNoContent,
		artDo(t, f.h, http.MethodDelete, artAPI+"/genericArtifacts/"+arts.Items[0].ID, "").Code)
	assert.Equal(
		t,
		http.StatusNoContent,
		artDo(t, f.h, http.MethodDelete, artAPI+"/genericRepositories/"+repo.ID, "").Code,
	)
}

func TestOCIArtifactsProxyRejects(t *testing.T) {
	t.Parallel()

	f := newArtDashboard(t)
	img := artAPI + "/containerImages/" + f.imageID
	repos := artAPI + "/containerRepositories"

	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"registry data plane", http.MethodGet, artAPI + "/v2/gopherstack/web/api/manifests/v1", ""},
		{"token endpoint", http.MethodGet, artAPI + "/docker/token", ""},
		{"admin keys", http.MethodPost, artAPI + "/_gopherstack/oci/keys", `{}`},
		{"unknown collection", http.MethodGet, artAPI + "/shapes", ""},
		{"update image", http.MethodPut, img, `{}`},
		{"create image", http.MethodPost, artAPI + "/containerImages", `{}`},
		{"remove version", http.MethodPost, img + "/removeVersion", `{"version":"v1"}`},
		{"restore wrong verb", http.MethodGet, img + "/restore", ""},
		{"delete signature", http.MethodDelete, artAPI + "/containerImageSignatures/x", ""},
		{"create signature", http.MethodPost, artAPI + "/containerImageSignatures", `{}`},
		{"change compartment", http.MethodPost, repos + "/x/actions/changeCompartment", `{}`},
		{"update generic artifact", http.MethodPut, artAPI + "/genericArtifacts/x", `{}`},
		{"upload content", http.MethodPut, artAPI + "/genericArtifacts/x/content", "data"},
		{"delete content", http.MethodDelete, artAPI + "/genericArtifacts/x/content", ""},
		{"too many segments", http.MethodGet, repos + "/x/y/z", ""},
		{"dot segments", http.MethodGet, repos + "/%2e%2e", ""},
		{"encoded slash", http.MethodGet, repos + "/a%2fb", ""},
		{"bad id chars", http.MethodGet, repos + "/a%20b", ""},
		{"delete collection", http.MethodDelete, repos, ""},
		{"bad create body", http.MethodPost, repos, `nope`},
		{"create without name", http.MethodPost, repos, `{"compartmentId":"c"}`},
		{
			"create bad readme format",
			http.MethodPost,
			repos,
			`{"compartmentId":"c","displayName":"d","readme":{"content":"x","format":"HTML"}}`,
		},
		{"empty update", http.MethodPut, repos + "/x", `{"freeformTags":{"a":"b"}}`},
		{"config without flag", http.MethodPut, artAPI + "/containerConfiguration?compartmentId=c", `{}`},
		{"generic create without name", http.MethodPost, artAPI + "/genericRepositories", `{"compartmentId":"c"}`},
		{"restore bad body", http.MethodPost, img + "/restore", `nope`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := artDo(t, f.h, tt.method, tt.path, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}
}

func TestOCIArtifactsProxyDropsAuthorizationAndUnknownQuery(t *testing.T) {
	t.Parallel()

	f := newArtDashboard(t)
	tenancy := artTenancy(t, f)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodGet,
		artAPI+"/containerRepositories?compartmentId="+tenancy+"&limit=1&x=y",
		nil,
	)
	req.Header.Set("Authorization", "Basic ZmFrZTpmYWtl")
	require.NoError(t, f.h.Handler()(echo.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func TestOCIArtifactsProxyUnavailable(t *testing.T) {
	t.Parallel()

	runGCPSteps(t, dashboard.NewHandler(dashboard.Config{}), []gcpStep{
		{http.MethodGet, artAPI + "/containerRepositories?compartmentId=c", "", "", http.StatusServiceUnavailable},
		{http.MethodGet, artBase + "/compartments", "", "", http.StatusServiceUnavailable},
		{http.MethodGet, artAPI + "/genericArtifacts/x/content", "", "", http.StatusServiceUnavailable},
	})
}
