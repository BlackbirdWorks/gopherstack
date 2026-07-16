package apigateway_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *apigateway.InMemoryBackend) string
		verify func(t *testing.T, b *apigateway.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *apigateway.InMemoryBackend) string {
				api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "test-api", Description: "test"})
				if err != nil {
					return ""
				}

				return api.ID
			},
			verify: func(t *testing.T, b *apigateway.InMemoryBackend, id string) {
				t.Helper()

				api, err := b.GetRestAPI(id)
				require.NoError(t, err)
				assert.Equal(t, "test-api", api.Name)
				assert.Equal(t, id, api.ID)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *apigateway.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *apigateway.InMemoryBackend, _ string) {
				t.Helper()

				apis, _, err := b.GetRestAPIs(0, "")
				require.NoError(t, err)
				assert.Empty(t, apis)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := apigateway.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := apigateway.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises every resource
// family touched by the Phase 3.3 pkgs/store conversion, in particular the
// ones whose composite key (restAPIID#childID / usagePlanID#keyID) depends on
// a field tagged json:"-" on the live type (Resource, Deployment, Stage,
// Authorizer, RequestValidator, DocumentationPart, DocumentationVersion,
// Model, UsagePlanKey -- see store_setup.go and persistence.go). If the
// DTO-registry snapshot/restore path in persistence.go ever dropped that
// field, every one of these lookups would fail post-restore even though the
// resource "exists" in the restored table under the wrong (unprefixed) key.
// This is a single sequential lifecycle (create everything once, snapshot,
// restore, verify everything), not a table of independent cases, so it does
// not need t.Run subtests.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()

	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "full-api", Description: "full"})
	require.NoError(t, err)

	resource, err := b.CreateResource(api.ID, api.RootResourceID, "widgets")
	require.NoError(t, err)

	deployment, err := b.CreateDeployment(api.ID, "prod", "initial")
	require.NoError(t, err)

	stage, err := b.GetStage(api.ID, "prod")
	require.NoError(t, err)
	assert.Equal(t, deployment.ID, stage.DeploymentID)

	authorizer, err := b.CreateAuthorizer(api.ID, apigateway.CreateAuthorizerInput{
		Name: "auth1", Type: "TOKEN", AuthorizerURI: "arn:aws:lambda:us-east-1:1:function:f/invocations",
	})
	require.NoError(t, err)

	validator, err := b.CreateRequestValidator(api.ID, apigateway.CreateRequestValidatorInput{
		Name: "validator1", ValidateRequestBody: true,
	})
	require.NoError(t, err)

	docPart, err := b.CreateDocumentationPart(apigateway.CreateDocumentationPartInput{
		RestAPIID:  api.ID,
		Location:   apigateway.DocumentationLocation{Type: "API"},
		Properties: `{"description":"d"}`,
	})
	require.NoError(t, err)

	docVersion, err := b.CreateDocumentationVersion(apigateway.CreateDocumentationVersionInput{
		RestAPIID: api.ID, Version: "v1",
	})
	require.NoError(t, err)

	model, err := b.CreateModel(apigateway.CreateModelInput{
		RestAPIID: api.ID, Name: "Widget", ContentType: "application/json", Schema: `{"type":"object"}`,
	})
	require.NoError(t, err)

	apiKey, err := b.CreateAPIKey(apigateway.CreateAPIKeyInput{Name: "key1", Enabled: true})
	require.NoError(t, err)

	domainName, err := b.CreateDomainName(apigateway.CreateDomainNameInput{DomainName: "api.example.com"})
	require.NoError(t, err)

	basePathMapping, err := b.CreateBasePathMapping(apigateway.CreateBasePathMappingInput{
		DomainName: domainName.DomainNameValue, BasePath: "v1", RestAPIID: api.ID, Stage: "prod",
	})
	require.NoError(t, err)

	domainAssoc, err := b.CreateDomainNameAccessAssociation(apigateway.CreateDomainNameAccessAssociationInput{
		DomainNameARN:               "arn:aws:apigateway:us-east-1::/domainnames/api.example.com",
		AccessAssociationSource:     "vpce-123",
		AccessAssociationSourceType: "VPCE",
	})
	require.NoError(t, err)

	usagePlan, err := b.CreateUsagePlan(apigateway.CreateUsagePlanInput{Name: "plan1"})
	require.NoError(t, err)

	usagePlanKey, err := b.CreateUsagePlanKey(apigateway.CreateUsagePlanKeyInput{
		UsagePlanID: usagePlan.ID, KeyID: apiKey.ID, KeyType: "API_KEY",
	})
	require.NoError(t, err)

	gatewayResponse, err := b.PutGatewayResponse(apigateway.PutGatewayResponseInput{
		RestAPIID: api.ID, ResponseType: "UNAUTHORIZED", StatusCode: "401",
	})
	require.NoError(t, err)

	clientCert, err := b.GenerateClientCertificate(apigateway.GenerateClientCertificateInput{Description: "cert1"})
	require.NoError(t, err)

	vpcLink, err := b.CreateVpcLink(apigateway.CreateVpcLinkInput{Name: "link1"})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := apigateway.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	gotAPI, err := fresh.GetRestAPI(api.ID)
	require.NoError(t, err)
	assert.Equal(t, api.Name, gotAPI.Name)

	gotResource, err := fresh.GetResource(api.ID, resource.ID)
	require.NoError(t, err)
	assert.Equal(t, "widgets", gotResource.PathPart)

	gotDeployment, err := fresh.GetDeployment(api.ID, deployment.ID)
	require.NoError(t, err)
	assert.Equal(t, deployment.Description, gotDeployment.Description)

	gotStage, err := fresh.GetStage(api.ID, "prod")
	require.NoError(t, err)
	assert.Equal(t, stage.DeploymentID, gotStage.DeploymentID)

	gotAuthorizer, err := fresh.GetAuthorizer(api.ID, authorizer.ID)
	require.NoError(t, err)
	assert.Equal(t, authorizer.Name, gotAuthorizer.Name)

	gotValidator, err := fresh.GetRequestValidator(api.ID, validator.ID)
	require.NoError(t, err)
	assert.Equal(t, validator.Name, gotValidator.Name)

	gotDocPart, err := fresh.GetDocumentationPart(api.ID, docPart.ID)
	require.NoError(t, err)
	assert.Equal(t, docPart.Properties, gotDocPart.Properties)

	gotDocVersion, err := fresh.GetDocumentationVersion(api.ID, docVersion.Version)
	require.NoError(t, err)
	assert.Equal(t, docVersion.Version, gotDocVersion.Version)

	gotModel, err := fresh.GetModel(api.ID, model.Name)
	require.NoError(t, err)
	assert.Equal(t, model.Schema, gotModel.Schema)

	gotAPIKey, err := fresh.GetAPIKey(apiKey.ID)
	require.NoError(t, err)
	assert.Equal(t, apiKey.Value, gotAPIKey.Value)

	// apiKeysByValue is a pre-existing gap carried over unchanged from before the
	// Phase 3.3 pkgs/store conversion: it was never part of the old backendSnapshot
	// either (compare git show HEAD:services/apigateway/persistence.go), so it was
	// already not rebuilt across a Restore. GetAPIKeyByValue is expected to miss
	// here -- this documents the existing limitation rather than "fixing" it as
	// part of a storage-layer-only swap.
	_, err = fresh.GetAPIKeyByValue(apiKey.Value)
	require.Error(t, err)

	gotDomainName, err := fresh.GetDomainName(domainName.DomainNameValue)
	require.NoError(t, err)
	assert.Equal(t, domainName.DomainNameValue, gotDomainName.DomainNameValue)

	gotBasePathMapping, err := fresh.GetBasePathMapping(basePathMapping.DomainName, basePathMapping.BasePath)
	require.NoError(t, err)
	assert.Equal(t, api.ID, gotBasePathMapping.RestAPIID)

	gotDomainAssocs, err := fresh.GetDomainNameAccessAssociations("SELF")
	require.NoError(t, err)
	if assert.Len(t, gotDomainAssocs, 1) {
		assert.Equal(t, domainAssoc.DomainNameAccessAssociationARN, gotDomainAssocs[0].DomainNameAccessAssociationARN)
	}

	gotUsagePlan, err := fresh.GetUsagePlan(usagePlan.ID)
	require.NoError(t, err)
	assert.Equal(t, usagePlan.Name, gotUsagePlan.Name)

	gotUsagePlanKey, err := fresh.GetUsagePlanKey(usagePlan.ID, usagePlanKey.ID)
	require.NoError(t, err)
	assert.Equal(t, usagePlanKey.Value, gotUsagePlanKey.Value)

	gotGatewayResponse, err := fresh.GetGatewayResponse(api.ID, "UNAUTHORIZED")
	require.NoError(t, err)
	assert.Equal(t, gatewayResponse.StatusCode, gotGatewayResponse.StatusCode)
	assert.False(t, gotGatewayResponse.DefaultResponse)

	gotClientCert, err := fresh.GetClientCertificate(clientCert.ClientCertificateID)
	require.NoError(t, err)
	assert.Equal(t, clientCert.Description, gotClientCert.Description)

	gotVpcLink, err := fresh.GetVpcLink(vpcLink.ID)
	require.NoError(t, err)
	assert.Equal(t, vpcLink.Name, gotVpcLink.Name)
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestAPIGatewayHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	_, err := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "snap-api", Description: "test"})
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := apigateway.NewInMemoryBackend()
	freshH := apigateway.NewHandler(fresh)
	require.NoError(t, freshH.Restore(t.Context(), snap))

	apis, _, err := fresh.GetRestAPIs(0, "")
	require.NoError(t, err)
	assert.Len(t, apis, 1)
}

func TestAPIGatewayHandler_Routing(t *testing.T) {
	t.Parallel()

	h := apigateway.NewHandler(apigateway.NewInMemoryBackend())

	assert.Equal(t, "APIGateway", h.Name())
	assert.Positive(t, h.MatchPriority())

	e := echo.New()

	tests := []struct {
		name      string
		path      string
		target    string
		wantMatch bool
	}{
		{"target match", "/", "APIGateway.GetRestApis", true},
		{"rest path match", "/restapis", "", true},
		{"no match", "/other", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}

	// Test ExtractOperation
	req := httptest.NewRequest(http.MethodGet, "/restapis", nil)
	req.Header.Set("X-Amz-Target", "APIGateway.GetRestApis")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "GetRestApis", h.ExtractOperation(c))
}

func TestAPIGatewayBackend_DeploymentOperations(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()

	// Create a REST API first
	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "deploy-api", Description: "test"})
	require.NoError(t, err)

	// Create deployment without a stage so it can be deleted directly.
	dep, err := b.CreateDeployment(api.ID, "", "initial deployment")
	require.NoError(t, err)
	require.NotEmpty(t, dep.ID)

	// Get deployment
	got, err := b.GetDeployment(api.ID, dep.ID)
	require.NoError(t, err)
	assert.Equal(t, dep.ID, got.ID)

	// Delete deployment
	err = b.DeleteDeployment(api.ID, dep.ID)
	require.NoError(t, err)

	// Get should fail after delete
	_, err = b.GetDeployment(api.ID, dep.ID)
	require.Error(t, err)
}

func TestAPIGatewayBackend_GetDeployment_NotFound(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "test-api"})
	require.NoError(t, err)

	_, err = b.GetDeployment(api.ID, "nonexistent")
	require.Error(t, err)
}

func TestAPIGatewayHandler_RESTPath(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	// GET /restapis → GetRestApis
	req := httptest.NewRequest(http.MethodGet, "/restapis", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusOK, rec.Code)

	// POST /restapis → CreateRestApi
	body := `{"name":"rest-api","description":"test"}`
	req2 := httptest.NewRequest(http.MethodPost, "/restapis", strings.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	c2 := e.NewContext(req2, rec2)
	require.NoError(t, h.Handler()(c2))
	assert.Contains(t, []int{http.StatusOK, http.StatusCreated}, rec2.Code)
}

// TestInMemoryBackend_RestoreWithNilMaps ensures that a snapshot whose "dirty"
// tables (resources/deployments/stages -- see store_setup.go's
// registerAllTables doc) are explicitly null or entirely absent restores
// cleanly to empty tables rather than panicking. Pre-Phase-3.3 this exercised
// hand-rolled nil-map init logic in Restore; that logic is now handled
// generically by store.Registry.RestoreAll/store.Table.Restore (both treat
// nil/absent data as "reset to empty" -- see pkgs/store's docs), so this
// covers the same edge case against the current Tables-based snapshot shape.
func TestInMemoryBackend_RestoreWithNilMaps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		snapshot string
	}{
		{
			name: "null_resources_deployments_stages",
			snapshot: `{"version":1,"tables":{` +
				`"restApis":[{"id":"api1","name":"n","createdDate":0}],` +
				`"resources":null,"deployments":null,"stages":null}}`,
		},
		{
			name: "missing_inner_tables",
			snapshot: `{"version":1,"tables":{` +
				`"restApis":[{"id":"api2","name":"m","createdDate":0}]}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			err := b.Restore(t.Context(), []byte(tt.snapshot))
			require.NoError(t, err)

			// The Restore should have initialised the empty tables – calling
			// GetResources should succeed (the REST API itself was restored) without
			// a nil-pointer panic, and report no resources.
			apiID := "api1"
			if tt.name == "missing_inner_tables" {
				apiID = "api2"
			}

			resources, _, err := b.GetResources(apiID, "", 0)
			require.NoError(t, err)
			assert.Empty(t, resources)
		})
	}
}
