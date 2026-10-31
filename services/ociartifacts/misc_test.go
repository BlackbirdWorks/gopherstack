package ociartifacts_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/oracle/oci-go-sdk/v65/artifacts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ociartifacts"
)

type fakeConfig struct{ s ociartifacts.Settings }

func (f fakeConfig) GetOCIArtifactsSettings() ociartifacts.Settings { return f.s }

func TestProviderInit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		ctx      *service.AppContext
		name     string
		wantPort int
		wantAuth bool
	}{
		{name: "nil context", wantErr: ociartifacts.ErrNilAppContext},
		{name: "defaults", ctx: &service.AppContext{}, wantPort: ociartifacts.DefaultPort},
		{
			name:     "configured",
			ctx:      &service.AppContext{Config: fakeConfig{ociartifacts.Settings{Port: 9999, EnforceAuth: true}}},
			wantPort: 9999,
			wantAuth: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &ociartifacts.Provider{}
			assert.Equal(t, "OCIArtifacts", p.Name())

			svc, err := p.Init(tt.ctx)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			h, ok := svc.(*ociartifacts.Handler)
			require.True(t, ok)
			assert.Equal(t, tt.wantPort, h.Port)
			assert.Equal(t, tt.wantAuth, h.EnforceAuth)
			assert.Equal(t, "OCIArtifacts", h.Name())
			assert.Len(t, h.GetSupportedOperations(), 36)
			assert.Zero(t, h.MatchPriority())
			h.Shutdown(t.Context())
		})
	}
}

func TestRoutingMetadata(t *testing.T) {
	t.Parallel()

	h := ociartifacts.NewHandler(ociartifacts.NewInMemoryBackend())
	e := echo.New()

	tests := []struct {
		name     string
		method   string
		path     string
		wantOp   string
		wantRes  string
		wantMyRM bool
	}{
		{
			"get repo",
			http.MethodGet,
			"/20160918/container/repositories/ocid1.r",
			"GetContainerRepository",
			"ocid1.r",
			false,
		},
		{
			"image restore",
			http.MethodPost,
			"/20160918/container/images/ocid1.i/actions/restore",
			"RestoreContainerImage",
			"ocid1.i",
			false,
		},
		{
			"by path",
			http.MethodGet,
			"/20160918/generic/repositories/ocid1.g/artifactPaths/a/b/versions/1",
			"GetGenericArtifactByPath",
			"ocid1.g",
			false,
		},
		{
			"by path escaped",
			http.MethodGet,
			"/20160918/generic/repositories/ocid1.g/artifactPaths/a%2Fb/versions/1",
			"GetGenericArtifactByPath",
			"ocid1.g",
			false,
		},
		{
			"put content",
			http.MethodPut,
			"/20160918/generic/repositories/ocid1.g/artifactPaths/a/versions/1/content",
			"PutGenericArtifactContentByPath",
			"ocid1.g",
			false,
		},
		{
			"by path wrong method",
			http.MethodPatch,
			"/20160918/generic/repositories/ocid1.g/artifactPaths/a/versions/1",
			"Unknown",
			"",
			false,
		},
		{"registry", http.MethodGet, "/v2/gopherstack/x/manifests/v1", "RegistryV2", "", false},
		{"ping", http.MethodGet, "/v2/", "RegistryV2", "", false},
		{"token", http.MethodGet, "/20180419/docker/token", "GetAccessToken", "", false},
		{"admin", http.MethodGet, "/_gopherstack/oci/keys", "Admin", "", false},
		{"unknown", http.MethodGet, "/nope", "Unknown", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := e.NewContext(httptest.NewRequest(tt.method, tt.path, nil), httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
			assert.Equal(t, tt.wantRes, h.ExtractResource(c))
			assert.False(t, h.RouteMatcher()(c), "the shared router must never match")
		})
	}
}

func TestAdminKeys(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true})

	// An unregistered key is refused until it is registered through the admin endpoint.
	_, err := e.client.GetContainerConfiguration(
		t.Context(),
		artifacts.GetContainerConfigurationRequest{CompartmentId: new(testCompartment)},
	)
	assert.Equal(t, http.StatusUnauthorized, svcErr(t, err).GetHTTPStatusCode())

	kp := newKeyPair(t)
	status, body := rawDo(t, http.MethodPost, e.base+"/_gopherstack/oci/keys",
		jsonBody(t, map[string]string{"tenancy": testTenancy, "user": testUser, "publicKey": kp.pubPEM}))
	require.Equal(t, http.StatusCreated, status, body)
	assert.Contains(t, body, kp.fprint)

	status, body = rawDo(t, http.MethodGet, e.base+"/_gopherstack/oci/keys", "")
	assert.Equal(t, http.StatusOK, status)
	assert.Contains(t, body, testUser)
	assert.NotContains(t, body, "PRIVATE")

	status, _ = rawDo(t, http.MethodPost, e.base+"/_gopherstack/oci/keys", `{"tenancy":"t"}`)
	assert.Equal(t, http.StatusBadRequest, status)

	status, _ = rawDo(t, http.MethodPost, e.base+"/_gopherstack/oci/keys",
		jsonBody(t, map[string]string{"tenancy": testTenancy, "user": testUser, "publicKey": "not a key"}))
	assert.Equal(t, http.StatusBadRequest, status)

	status, _ = rawDo(t, http.MethodDelete, e.base+"/_gopherstack/oci/keys?keyId=missing", "")
	assert.Equal(t, http.StatusNotFound, status)

	status, _ = rawDo(t, http.MethodPatch, e.base+"/_gopherstack/oci/keys", "")
	assert.Equal(t, http.StatusNotFound, status)

	keyID := testTenancy + "/" + testUser + "/" + kp.fprint
	status, _ = rawDo(t, http.MethodDelete, e.base+"/_gopherstack/oci/keys?keyId="+keyID, "")
	assert.Equal(t, http.StatusNoContent, status)
}

func TestCompartmentChecks(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	known := map[string]bool{testCompartment: true, otherCompart: true, "ocid1.compartment.oc1..child": true}
	parent := map[string]string{"ocid1.compartment.oc1..child": testCompartment}

	e.backend.SetCompartmentChecker(func(id string) bool { return known[id] })
	e.backend.SetCompartmentTree(func(id, ancestor string) bool { return parent[id] == ancestor })

	tests := []struct {
		name    string
		compart string
		want    int
	}{
		{"known", testCompartment, http.StatusOK},
		{"unknown", "ocid1.compartment.oc1..ghost", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.client.CreateContainerRepository(t.Context(), artifacts.CreateContainerRepositoryRequest{
				CompartmentId: new(tt.compart), DisplayName: new("c-" + tt.name)})
			if tt.want == http.StatusOK {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.want, svcErr(t, err).GetHTTPStatusCode())
		})
	}

	child := e.createRepo(t, "in-child", func(d *artifacts.CreateContainerRepositoryDetails) {
		d.CompartmentId = new("ocid1.compartment.oc1..child")
	})

	flat, err := e.client.ListContainerRepositories(
		t.Context(),
		artifacts.ListContainerRepositoriesRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)

	for _, it := range flat.Items {
		assert.NotEqual(t, *child.Id, *it.Id)
	}

	deep, err := e.client.ListContainerRepositories(t.Context(), artifacts.ListContainerRepositoriesRequest{
		CompartmentId: new(testCompartment), CompartmentIdInSubtree: new(true)})
	require.NoError(t, err)

	found := false

	for _, it := range deep.Items {
		found = found || *it.Id == *child.Id
	}

	assert.True(t, found, "subtree listing includes child compartments")

	_, err = e.client.ChangeContainerRepositoryCompartment(
		t.Context(),
		artifacts.ChangeContainerRepositoryCompartmentRequest{
			RepositoryId:  child.Id,
			CompartmentId: new("ocid1.compartment.oc1..ghost"),
		},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.GetContainerConfiguration(t.Context(), artifacts.GetContainerConfigurationRequest{
		CompartmentId: new("ocid1.compartment.oc1..ghost")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestListValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	q := "?compartmentId=" + testCompartment

	tests := []struct {
		name string
		path string
	}{
		{"bad sortBy", "/20160918/container/repositories" + q + "&sortBy=SIZE"},
		{"bad sortOrder", "/20160918/container/images" + q + "&sortOrder=UP"},
		{"limit too big", "/20160918/container/imageSignatures" + q + "&limit=5000"},
		{"negative limit", "/20160918/repositories" + q + "&limit=-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, body := rawDo(t, http.MethodGet, e.base+tt.path, "")
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Contains(t, body, "InvalidParameter")
		})
	}
}
