package apigateway_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// errNoopNotImplemented is returned by noopBackend for methods that are not expected
// to be called in the fallback-persistence tests.
var errNoopNotImplemented = errors.New("not implemented")

// noopBackend implements StorageBackend without Snapshot/Restore so we can test
// the persistence fallback branches in Handler.Snapshot and Handler.Restore.
type noopBackend struct{}

func (n *noopBackend) CreateRestAPI(_ apigateway.CreateRestAPIInput) (*apigateway.RestAPI, error) {
	return &apigateway.RestAPI{ID: "x", Name: "x"}, nil
}

func (n *noopBackend) DeleteRestAPI(_ string) error { return nil }

func (n *noopBackend) GetRestAPI(_ string) (*apigateway.RestAPI, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetRestAPIs(_ int, _ string) ([]apigateway.RestAPI, string, error) {
	return nil, "", nil
}

func (n *noopBackend) GetResources(_ string, _ string, _ int) ([]apigateway.Resource, string, error) {
	return nil, "", nil
}

func (n *noopBackend) ResourcesForRouting(_ string) ([]apigateway.Resource, uint64, error) {
	return nil, 0, nil
}

func (n *noopBackend) GetResource(_ string, _ string) (*apigateway.Resource, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) CreateResource(_ string, _ string, _ string) (*apigateway.Resource, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteResource(_ string, _ string) error { return nil }

func (n *noopBackend) PutMethod(_ apigateway.PutMethodInput) (*apigateway.Method, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetMethod(_ string, _ string, _ string) (*apigateway.Method, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteMethod(_ string, _ string, _ string) error { return nil }

func (n *noopBackend) PutMethodResponse(
	_ string, _ string, _ string, _ string, _ apigateway.PutMethodResponseInput,
) (*apigateway.MethodResponse, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetMethodResponse(_ string, _ string, _ string, _ string) (*apigateway.MethodResponse, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteMethodResponse(_ string, _ string, _ string, _ string) error { return nil }

func (n *noopBackend) PutIntegration(
	_ string, _ string, _ string, _ apigateway.PutIntegrationInput,
) (*apigateway.Integration, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetIntegration(_ string, _ string, _ string) (*apigateway.Integration, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteIntegration(_ string, _ string, _ string) error { return nil }

func (n *noopBackend) PutIntegrationResponse(
	_ string, _ string, _ string, _ string, _ apigateway.PutIntegrationResponseInput,
) (*apigateway.IntegrationResponse, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetIntegrationResponse(
	_ string,
	_ string,
	_ string,
	_ string,
) (*apigateway.IntegrationResponse, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteIntegrationResponse(_ string, _ string, _ string, _ string) error {
	return nil
}

func (n *noopBackend) CreateDeployment(_ string, _ string, _ string) (*apigateway.Deployment, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetDeployment(_ string, _ string) (*apigateway.Deployment, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetDeployments(_ string) ([]apigateway.Deployment, error) {
	return nil, nil
}

func (n *noopBackend) DeleteDeployment(_ string, _ string) error { return nil }

func (n *noopBackend) GetStages(_ string) ([]apigateway.Stage, error) { return nil, nil }

func (n *noopBackend) GetStage(_ string, _ string) (*apigateway.Stage, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteStage(_ string, _ string) error { return nil }

func (n *noopBackend) CreateAuthorizer(_ string, _ apigateway.CreateAuthorizerInput) (*apigateway.Authorizer, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetAuthorizer(_ string, _ string) (*apigateway.Authorizer, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetAuthorizers(_ string) ([]apigateway.Authorizer, error) { return nil, nil }

func (n *noopBackend) UpdateAuthorizer(
	_ string,
	_ string,
	_ apigateway.UpdateAuthorizerInput,
) (*apigateway.Authorizer, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteAuthorizer(_ string, _ string) error { return nil }

func (n *noopBackend) CreateRequestValidator(
	_ string,
	_ apigateway.CreateRequestValidatorInput,
) (*apigateway.RequestValidator, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetRequestValidator(_ string, _ string) (*apigateway.RequestValidator, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetRequestValidators(_ string) ([]apigateway.RequestValidator, error) {
	return nil, nil
}

func (n *noopBackend) UpdateRequestValidator(
	_ string,
	_ string,
	_ apigateway.UpdateRequestValidatorInput,
) (*apigateway.RequestValidator, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteRequestValidator(_ string, _ string) error { return nil }

func (n *noopBackend) CreateAPIKey(_ apigateway.CreateAPIKeyInput) (*apigateway.APIKey, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) CreateBasePathMapping(
	_ apigateway.CreateBasePathMappingInput,
) (*apigateway.BasePathMapping, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) CreateDocumentationPart(
	_ apigateway.CreateDocumentationPartInput,
) (*apigateway.DocumentationPart, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) CreateDocumentationVersion(
	_ apigateway.CreateDocumentationVersionInput,
) (*apigateway.DocumentationVersion, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) CreateDomainName(_ apigateway.CreateDomainNameInput) (*apigateway.DomainName, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) CreateDomainNameAccessAssociation(
	_ apigateway.CreateDomainNameAccessAssociationInput,
) (*apigateway.DomainNameAccessAssociation, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) CreateModel(_ apigateway.CreateModelInput) (*apigateway.Model, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) CreateStage(_ apigateway.CreateStageInput) (*apigateway.Stage, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) CreateUsagePlan(_ apigateway.CreateUsagePlanInput) (*apigateway.UsagePlan, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) CreateUsagePlanKey(_ apigateway.CreateUsagePlanKeyInput) (*apigateway.UsagePlanKey, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetAPIKey(_ string) (*apigateway.APIKey, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetAPIKeyByValue(_ string) (*apigateway.APIKey, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetAPIKeys() ([]apigateway.APIKey, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteAPIKey(_ string) error { return errNoopNotImplemented }

func (n *noopBackend) UpdateAPIKey(_ string, _ apigateway.UpdateAPIKeyInput) (*apigateway.APIKey, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetDomainName(_ string) (*apigateway.DomainName, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetDomainNames() ([]apigateway.DomainName, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteDomainName(_ string) error { return errNoopNotImplemented }

func (n *noopBackend) GetBasePathMapping(_ string, _ string) (*apigateway.BasePathMapping, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetBasePathMappings(_ string) ([]apigateway.BasePathMapping, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteBasePathMapping(_ string, _ string) error { return errNoopNotImplemented }

func (n *noopBackend) GetModel(_ string, _ string) (*apigateway.Model, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetModels(_ string) ([]apigateway.Model, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteModel(_ string, _ string) error { return errNoopNotImplemented }

func (n *noopBackend) UpdateModel(_ string, _ string, _ apigateway.UpdateModelInput) (*apigateway.Model, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) UpdateStage(_ string, _ string, _ apigateway.UpdateStageInput) (*apigateway.Stage, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetUsagePlan(_ string) (*apigateway.UsagePlan, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetUsagePlans() ([]apigateway.UsagePlan, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteUsagePlan(_ string) error { return errNoopNotImplemented }

func (n *noopBackend) GetUsagePlanKey(_ string, _ string) (*apigateway.UsagePlanKey, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetUsagePlanKeys(_ string) ([]apigateway.UsagePlanKey, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteUsagePlanKey(_ string, _ string) error { return errNoopNotImplemented }

func (n *noopBackend) GetDocumentationPart(_ string, _ string) (*apigateway.DocumentationPart, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetDocumentationParts(_ string) ([]apigateway.DocumentationPart, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteDocumentationPart(_ string, _ string) error {
	return errNoopNotImplemented
}

func (n *noopBackend) GetDocumentationVersion(_ string, _ string) (*apigateway.DocumentationVersion, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetDocumentationVersions(_ string) ([]apigateway.DocumentationVersion, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteDocumentationVersion(_ string, _ string) error {
	return errNoopNotImplemented
}

func (n *noopBackend) UpdateRestAPI(_ string, _ apigateway.UpdateRestAPIInput) (*apigateway.RestAPI, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) UpdateResource(
	_ string,
	_ string,
	_ apigateway.UpdateResourceInput,
) (*apigateway.Resource, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) UpdateDeployment(
	_ string,
	_ string,
	_ apigateway.UpdateDeploymentInput,
) (*apigateway.Deployment, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetAccount() (*apigateway.Account, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetResourceTags(_ string) (map[string]string, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) TagResource(_ string, _ map[string]string) error {
	return errNoopNotImplemented
}

func (n *noopBackend) UntagResource(_ string, _ []string) error {
	return errNoopNotImplemented
}

func (n *noopBackend) TestInvokeMethod(_ apigateway.TestInvokeMethodInput) (*apigateway.TestInvokeMethodOutput, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetAPIKeysPage(_ int, _ string) ([]apigateway.APIKey, string, error) {
	return nil, "", errNoopNotImplemented
}

func (n *noopBackend) GetDomainNamesPage(_ int, _ string) ([]apigateway.DomainName, string, error) {
	return nil, "", errNoopNotImplemented
}

func (n *noopBackend) GetUsagePlansPage(_ int, _ string) ([]apigateway.UsagePlan, string, error) {
	return nil, "", errNoopNotImplemented
}

func (n *noopBackend) UpdateUsagePlan(_ apigateway.UpdateUsagePlanInput) (*apigateway.UsagePlan, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) UpdateDomainName(_ apigateway.UpdateDomainNameInput) (*apigateway.DomainName, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) UpdateBasePathMapping(
	_ apigateway.UpdateBasePathMappingInput,
) (*apigateway.BasePathMapping, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) UpdateDocumentationPart(
	_ apigateway.UpdateDocumentationPartInput,
) (*apigateway.DocumentationPart, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) UpdateDocumentationVersion(
	_ apigateway.UpdateDocumentationVersionInput,
) (*apigateway.DocumentationVersion, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) UpdateMethod(_ apigateway.UpdateMethodInput) (*apigateway.Method, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) UpdateIntegration(_ apigateway.UpdateIntegrationInput) (*apigateway.Integration, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) UpdateIntegrationResponse(
	_ apigateway.UpdateIntegrationResponseInput,
) (*apigateway.IntegrationResponse, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) UpdateMethodResponse(_ apigateway.UpdateMethodResponseInput) (*apigateway.MethodResponse, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) UpdateAccount(_ apigateway.UpdateAccountInput) (*apigateway.Account, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) TestInvokeAuthorizer(
	_ apigateway.TestInvokeAuthorizerInput,
) (*apigateway.TestInvokeAuthorizerOutput, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetModelTemplate(_ string, _ string) (string, error) {
	return "", errNoopNotImplemented
}

func (n *noopBackend) GetGatewayResponse(_ string, _ string) (*apigateway.GatewayResponse, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetGatewayResponses(_ string) ([]apigateway.GatewayResponse, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) PutGatewayResponse(_ apigateway.PutGatewayResponseInput) (*apigateway.GatewayResponse, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) UpdateGatewayResponse(_ apigateway.PutGatewayResponseInput) (*apigateway.GatewayResponse, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteGatewayResponse(_ string, _ string) error { return errNoopNotImplemented }

func (n *noopBackend) GenerateClientCertificate(
	_ apigateway.GenerateClientCertificateInput,
) (*apigateway.ClientCertificate, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetClientCertificate(_ string) (*apigateway.ClientCertificate, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetClientCertificates() ([]apigateway.ClientCertificate, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteClientCertificate(_ string) error { return errNoopNotImplemented }

func (n *noopBackend) GetUsage(_ apigateway.GetUsageInput) (*apigateway.UsageData, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) EnforceUsagePlan(_, _, _ string) error {
	return nil
}

func (n *noopBackend) CreateVpcLink(_ apigateway.CreateVpcLinkInput) (*apigateway.VpcLink, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetVpcLink(_ string) (*apigateway.VpcLink, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetVpcLinks() ([]apigateway.VpcLink, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteVpcLink(_ string) error { return errNoopNotImplemented }

func (n *noopBackend) UpdateVpcLink(_ apigateway.UpdateVpcLinkInput) (*apigateway.VpcLink, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) UpdateClientCertificate(
	_ apigateway.UpdateClientCertificateInput,
) (*apigateway.ClientCertificate, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetExport(_ string, _ string, _ string) (map[string]any, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetDomainNameAccessAssociations(
	_ string,
) ([]apigateway.DomainNameAccessAssociation, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) DeleteDomainNameAccessAssociation(_ string) error {
	return errNoopNotImplemented
}

func (n *noopBackend) RejectDomainNameAccessAssociation(_, _ string) error {
	return errNoopNotImplemented
}

func (n *noopBackend) GetSdkTypes() []apigateway.SdkType { return nil }

func (n *noopBackend) GetSdkType(_ string) (*apigateway.SdkType, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) GetSdk(_, _, _ string) (*apigateway.SdkExport, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) ImportAPIKeys(_ []byte, _ string, _ bool) ([]string, []string, error) {
	return nil, nil, errNoopNotImplemented
}

func (n *noopBackend) ImportDocumentationParts(
	_ string, _ []byte, _ string, _ bool,
) ([]string, []string, error) {
	return nil, nil, errNoopNotImplemented
}

func (n *noopBackend) UpdateUsage(_, _ string, _ map[string]string) (*apigateway.UsageData, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) ImportRestAPI(_ apigateway.ImportRestAPIInput) (*apigateway.RestAPI, error) {
	return nil, errNoopNotImplemented
}

func (n *noopBackend) PutRestAPI(_ apigateway.PutRestAPIInput) (*apigateway.RestAPI, error) {
	return nil, errNoopNotImplemented
}

// restRequest sends a REST-style request (no X-Amz-Target header) to the handler.
func restRequest(t *testing.T, handler *apigateway.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	err := handler.Handler()(c)
	require.NoError(t, err)

	return rec
}

// TestHandlerPersistence_NoopBackend covers the fallback branches in Handler.Snapshot
// and Handler.Restore when the backend does not implement those interfaces.
func TestHandlerPersistence_NoopBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantNilSnap bool
		wantNoErr   bool
	}{
		{
			name:        "Snapshot_returns_nil_for_non_snapshotter",
			wantNilSnap: true,
		},
		{
			name:      "Restore_returns_nil_for_non_restorer",
			wantNoErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := apigateway.NewHandler(&noopBackend{})

			if tt.wantNilSnap {
				snap := h.Snapshot(t.Context())
				assert.Nil(t, snap)

				return
			}

			err := h.Restore(t.Context(), []byte(`{"apis":{}}`))
			require.NoError(t, err)
		})
	}
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

// TestHandleRESTAPI_Branches covers the branches inside handleRESTAPI that are not
// hit by the existing REST-path test: unknown path → 404, dispatch error → 4xx,
// and successful DELETE that returns 204.
func TestHandleRESTAPI_Branches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *apigateway.InMemoryBackend) string
		name     string
		method   string
		path     string
		body     string
		wantCode int
	}{
		{
			name:     "unknown_rest_path_returns_404",
			method:   http.MethodGet,
			path:     "/restapis/abc/unknownsegment",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "dispatch_error_nonexistent_api",
			method:   http.MethodGet,
			path:     "/restapis/nonexistent",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete_resource_returns_204",
			method:   http.MethodDelete,
			wantCode: http.StatusNoContent,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				resources, _, _ := b.GetResources(api.ID, "", 0)
				rootID := resources[0].ID
				child, _ := b.CreateResource(api.ID, rootID, "items")

				return fmt.Sprintf("/restapis/%s/resources/%s", api.ID, child.ID)
			},
		},
		{
			name:     "delete_stage_returns_204",
			method:   http.MethodDelete,
			wantCode: http.StatusNoContent,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				_, _ = b.CreateDeployment(api.ID, "prod", "")

				return fmt.Sprintf("/restapis/%s/stages/prod", api.ID)
			},
		},
		{
			name:     "delete_method_returns_204",
			method:   http.MethodDelete,
			wantCode: http.StatusNoContent,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				resources, _, _ := b.GetResources(api.ID, "", 0)
				rootID := resources[0].ID
				_, _ = b.PutMethod(
					apigateway.PutMethodInput{
						RestAPIID:         api.ID,
						ResourceID:        rootID,
						HTTPMethod:        "GET",
						AuthorizationType: "NONE",
					},
				)

				return fmt.Sprintf("/restapis/%s/resources/%s/methods/GET", api.ID, rootID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(backend)

			path := tt.path
			if tt.setup != nil {
				path = tt.setup(backend)
			}

			rec := restRequest(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestBackend_DeleteResource_NotFound ensures the "resource not found" error branch
// in DeleteResource is covered (API exists but resource ID is absent).
func TestBackend_DeleteResource_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "resource_not_found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
			require.NoError(t, err)

			err = b.DeleteResource(api.ID, "nonexistent-resource")
			require.Error(t, err)
		})
	}
}

// TestBackend_DeleteMethod_NotFound covers the "resource not found" and
// "method not found" error branches in DeleteMethod.
func TestBackend_DeleteMethod_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resourceID string
		httpMethod string
		wantErr    bool
	}{
		{
			name:       "resource_not_found",
			resourceID: "nonexistent",
			httpMethod: "GET",
			wantErr:    true,
		},
		{
			name:       "method_not_found",
			resourceID: "", // filled in by setup
			httpMethod: "DELETE",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
			require.NoError(t, err)

			resources, _, err := b.GetResources(api.ID, "", 0)
			require.NoError(t, err)
			rootID := resources[0].ID

			resID := tt.resourceID
			if resID == "" {
				resID = rootID
			}

			err = b.DeleteMethod(api.ID, resID, tt.httpMethod)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestBackend_PutIntegration_NotFound covers the "resource not found" and
// "method not found" error branches in PutIntegration.
func TestBackend_PutIntegration_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		resourceID string
		httpMethod string
	}{
		{
			name:       "resource_not_found",
			resourceID: "nonexistent",
			httpMethod: "GET",
		},
		{
			name:       "method_not_found",
			resourceID: "", // uses root ID (method not set)
			httpMethod: "PATCH",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
			require.NoError(t, err)

			resources, _, err := b.GetResources(api.ID, "", 0)
			require.NoError(t, err)
			rootID := resources[0].ID

			resID := tt.resourceID
			if resID == "" {
				resID = rootID
			}

			_, err = b.PutIntegration(api.ID, resID, tt.httpMethod, apigateway.PutIntegrationInput{Type: "MOCK"})
			require.Error(t, err)
		})
	}
}

// TestBackend_DeleteStage_NotFound covers the "stage not found" error branch in DeleteStage.
func TestBackend_DeleteStage_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "stage_not_found"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
			require.NoError(t, err)

			err = b.DeleteStage(api.ID, "nonexistent-stage")
			require.Error(t, err)
		})
	}
}

// TestComputePath_NonRootParent covers the computePath branch where parentPath != "/".
// This is exercised indirectly by creating a nested (grandchild) resource.
func TestComputePath_NonRootParent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		parts     []string
		wantPaths []string
	}{
		{
			name:      "two_level_nesting",
			parts:     []string{"users", "profile"},
			wantPaths: []string{"/users", "/users/profile"},
		},
		{
			name:      "three_level_nesting",
			parts:     []string{"v1", "pets", "{petId}"},
			wantPaths: []string{"/v1", "/v1/pets", "/v1/pets/{petId}"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
			require.NoError(t, err)

			resources, _, err := b.GetResources(api.ID, "", 0)
			require.NoError(t, err)
			parentID := resources[0].ID

			for i, part := range tt.parts {
				child, cerr := b.CreateResource(api.ID, parentID, part)
				require.NoError(t, cerr)
				assert.Equal(t, tt.wantPaths[i], child.Path)
				parentID = child.ID
			}
		})
	}
}

// TestOpaquePagination_EdgeCases verifies that GetRestAPIs treats malformed/legacy
// (numeric) position tokens as "start from the beginning" — the opaque cursor is not a
// numeric offset — and that a real cursor round-trips to the next page.
func TestOpaquePagination_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		position string
		wantLen  int
	}{
		{
			name:     "invalid_position_string_treated_as_start",
			position: "not-a-number",
			wantLen:  2,
		},
		{
			name:     "legacy_numeric_position_treated_as_start",
			position: "1",
			wantLen:  2,
		},
		{
			name:     "empty_position_returns_all",
			position: "",
			wantLen:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			_, _ = b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api-a"})
			_, _ = b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api-b"})

			apis, _, err := b.GetRestAPIs(0, tt.position)
			require.NoError(t, err)
			assert.Len(t, apis, tt.wantLen)
		})
	}
}

// TestOpaquePagination_RoundTrip verifies that the opaque cursor returned by a limited
// page resumes at the correct item on the next call and is not a numeric offset.
func TestOpaquePagination_RoundTrip(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	for _, name := range []string{"api-a", "api-b", "api-c"} {
		_, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: name})
		require.NoError(t, err)
	}

	first, token, err := b.GetRestAPIs(1, "")
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.NotEmpty(t, token, "cursor must be present when more pages remain")
	assert.NotEqual(t, "1", token, "cursor must be opaque, not a numeric offset")

	// Walk the remaining pages using the opaque cursor.
	seen := map[string]bool{first[0].ID: true}
	for token != "" {
		var page []apigateway.RestAPI
		page, token, err = b.GetRestAPIs(1, token)
		require.NoError(t, err)
		for _, api := range page {
			assert.False(t, seen[api.ID], "cursor must not repeat an item")
			seen[api.ID] = true
		}
	}
	assert.Len(t, seen, 3, "cursor pagination must cover every item exactly once")
}

// TestExtractResource_AdditionalBranches covers the "name" key fallback and the
// non-string-value branch in ExtractResource.
func TestExtractResource_AdditionalBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantResource string
	}{
		{
			name:         "name_key_fallback",
			body:         `{"name":"my-api"}`,
			wantResource: "my-api",
		},
		{
			name:         "non_string_restApiId_falls_through_to_name",
			body:         `{"restApiId":42,"name":"fallback-api"}`,
			wantResource: "fallback-api",
		},
		{
			name:         "invalid_json_returns_empty",
			body:         `not-json`,
			wantResource: "",
		},
		{
			name:         "no_matching_keys",
			body:         `{"other":"value"}`,
			wantResource: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := apigateway.NewHandler(apigateway.NewInMemoryBackend())
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			got := h.ExtractResource(e.NewContext(req, httptest.NewRecorder()))

			assert.Equal(t, tt.wantResource, got)
		})
	}
}

// TestRestAPIActions_RESTPathCoverage exercises the restAPIActions closures via REST
// path requests (GET/DELETE /restapis/...) to cover branches not reached by the
// X-Amz-Target path.
func TestRestAPIActions_RESTPathCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *apigateway.InMemoryBackend) string
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "GET_restapis_returns_200",
			method:   http.MethodGet,
			path:     "/restapis",
			wantCode: http.StatusOK,
		},
		{
			name:   "GET_restapis_by_id_returns_200",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "test-api"})

				return "/restapis/" + api.ID
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DELETE_restapi_returns_202",
			method: http.MethodDelete,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "del-api"})

				return "/restapis/" + api.ID
			},
			wantCode: http.StatusAccepted,
		},
		{
			name:     "POST_restapis_returns_201",
			method:   http.MethodPost,
			path:     "/restapis",
			wantCode: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(backend)

			path := tt.path
			if tt.setup != nil {
				path = tt.setup(backend)
			}

			body := ""
			if tt.method == http.MethodPost {
				body = `{"name":"rest-created-api"}`
			}

			rec := restRequest(t, h, tt.method, path, body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestResourceActions_RESTPathCoverage exercises resourceActions closures via REST paths.
func TestResourceActions_RESTPathCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *apigateway.InMemoryBackend) (apiID, path string)
		name     string
		method   string
		body     string
		wantCode int
	}{
		{
			name:   "GET_resources_returns_200",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) (string, string) {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})

				return api.ID, "/restapis/" + api.ID + "/resources"
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "GET_resource_by_id_returns_200",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) (string, string) {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				resources, _, _ := b.GetResources(api.ID, "", 0)

				return api.ID, fmt.Sprintf("/restapis/%s/resources/%s", api.ID, resources[0].ID)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "POST_resource_creates_child",
			method: http.MethodPost,
			setup: func(b *apigateway.InMemoryBackend) (string, string) {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				resources, _, _ := b.GetResources(api.ID, "", 0)

				return api.ID, fmt.Sprintf("/restapis/%s/resources/%s", api.ID, resources[0].ID)
			},
			body:     `{"pathPart":"widgets"}`,
			wantCode: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(backend)

			_, path := tt.setup(backend)

			rec := restRequest(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestMethodActions_RESTPathCoverage exercises methodActions closures via REST paths.
func TestMethodActions_RESTPathCoverage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *apigateway.InMemoryBackend) string
		name     string
		method   string
		body     string
		wantCode int
	}{
		{
			name:   "PUT_method_via_REST",
			method: http.MethodPut,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				resources, _, _ := b.GetResources(api.ID, "", 0)

				return fmt.Sprintf("/restapis/%s/resources/%s/methods/GET", api.ID, resources[0].ID)
			},
			body:     `{"authorizationType":"NONE"}`,
			wantCode: http.StatusCreated,
		},
		{
			name:   "GET_method_via_REST",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				resources, _, _ := b.GetResources(api.ID, "", 0)
				_, _ = b.PutMethod(
					apigateway.PutMethodInput{
						RestAPIID:         api.ID,
						ResourceID:        resources[0].ID,
						HTTPMethod:        "POST",
						AuthorizationType: "NONE",
					},
				)

				return fmt.Sprintf("/restapis/%s/resources/%s/methods/POST", api.ID, resources[0].ID)
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(backend)

			path := tt.setup(backend)
			rec := restRequest(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestGetRestAPIs_Pagination exercises GetRestAPIs with a limit that triggers
// the pagination position output.
func TestGetRestAPIs_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		limit        int
		wantLen      int
		wantPosition bool
	}{
		{
			name:         "limit_1_returns_position",
			limit:        1,
			wantLen:      1,
			wantPosition: true,
		},
		{
			name:    "limit_0_returns_all",
			limit:   0,
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			for i := range 3 {
				_, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: fmt.Sprintf("api-%d", i)})
				require.NoError(t, err)
			}

			apis, pos, err := b.GetRestAPIs(tt.limit, "")
			require.NoError(t, err)
			assert.Len(t, apis, tt.wantLen)

			if tt.wantPosition {
				assert.NotEmpty(t, pos)
			} else {
				assert.Empty(t, pos)
			}
		})
	}
}

// TestGetRestAPIs_RESTPath_WithLimit exercises GetRestApis via REST path with
// limit and position query parameters (covers the restAPIActions GetRestApis closure
// when limit/position are passed via body from REST path merging).
func TestGetRestAPIs_RESTPath_WithLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "get_rest_apis_via_REST_path",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			_, _ = backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api-x"})

			h := apigateway.NewHandler(backend)
			rec := restRequest(t, h, http.MethodGet, "/restapis", "")

			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp["item"])
		})
	}
}

// TestHandler_GetAndDeleteDeployment exercises the GetDeployment and DeleteDeployment
// action closures in deploymentActions which are not hit by other tests.
func TestHandler_GetAndDeleteDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "GetDeployment_returns_200",
			action:   "GetDeployment",
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteDeployment_returns_204",
			action:   "DeleteDeployment",
			wantCode: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, e := sharedSetup()

			createRec := postWithHandler(t, h, e, "CreateRestApi", `{"name":"api"}`)
			var created map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
			apiID := created["id"].(string)

			deplRec := postWithHandler(t, h, e, "CreateDeployment",
				fmt.Sprintf(`{"restApiId":%q,"stageName":"prod","description":""}`, apiID))
			var depl map[string]any
			require.NoError(t, json.Unmarshal(deplRec.Body.Bytes(), &depl))
			deplID := depl["id"].(string)

			if tt.action == "DeleteDeployment" {
				// Delete the referencing stage first so the deployment can be removed.
				stageRec := restRequest(t, h, http.MethodDelete,
					fmt.Sprintf("/restapis/%s/stages/prod", apiID), "")
				require.Equal(t, http.StatusNoContent, stageRec.Code)
			}

			rec := postWithHandler(t, h, e, tt.action,
				fmt.Sprintf(`{"restApiId":%q,"deploymentId":%q}`, apiID, deplID))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestParseAPIGWMethodPath_EdgeCases covers the branches in parseAPIGWMethodPath
// that are unreachable via normal REST calls:
//   - path ending at "methods" with no httpMethod segment → returns false
//   - integration segment with an unsupported HTTP method → returns false
func TestParseAPIGWMethodPath_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "methods_segment_without_httpMethod_returns_404",
			method:   http.MethodGet,
			path:     "/restapis/abc123/resources/resxyz/methods",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "integration_with_POST_method_returns_404",
			method:   http.MethodPost,
			path:     "/restapis/abc123/resources/resxyz/methods/GET/integration",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := apigateway.NewHandler(apigateway.NewInMemoryBackend())
			rec := restRequest(t, h, tt.method, tt.path, "")
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_RESTPath_Deployments exercises all deployment REST-path branches in
// parseAPIGWRESTPath that are not covered by the X-Amz-Target tests.
func TestHandler_RESTPath_Deployments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *apigateway.InMemoryBackend) string
		name     string
		method   string
		body     string
		wantCode int
	}{
		{
			name:   "POST_deployments_creates_deployment",
			method: http.MethodPost,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})

				return fmt.Sprintf("/restapis/%s/deployments", api.ID)
			},
			body:     `{"stageName":"v1","description":""}`,
			wantCode: http.StatusCreated,
		},
		{
			name:   "GET_deployments_lists",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				_, _ = b.CreateDeployment(api.ID, "prod", "")

				return fmt.Sprintf("/restapis/%s/deployments", api.ID)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "GET_deployment_by_id",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				dep, _ := b.CreateDeployment(api.ID, "prod", "")

				return fmt.Sprintf("/restapis/%s/deployments/%s", api.ID, dep.ID)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DELETE_deployment_returns_204",
			method: http.MethodDelete,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				dep, _ := b.CreateDeployment(api.ID, "", "")

				return fmt.Sprintf("/restapis/%s/deployments/%s", api.ID, dep.ID)
			},
			wantCode: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(backend)

			path := tt.setup(backend)
			rec := restRequest(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_RESTPath_Stages exercises the GET stages REST-path branches in
// parseAPIGWRESTPath that are not covered by existing tests.
func TestHandler_RESTPath_Stages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *apigateway.InMemoryBackend) string
		name     string
		method   string
		wantCode int
	}{
		{
			name:   "GET_stages_lists",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				_, _ = b.CreateDeployment(api.ID, "staging", "")

				return fmt.Sprintf("/restapis/%s/stages", api.ID)
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "GET_stage_by_name",
			method: http.MethodGet,
			setup: func(b *apigateway.InMemoryBackend) string {
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				_, _ = b.CreateDeployment(api.ID, "prod", "")

				return fmt.Sprintf("/restapis/%s/stages/prod", api.ID)
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(backend)

			path := tt.setup(backend)
			rec := restRequest(t, h, tt.method, path, "")
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_RESTPath_Integration exercises the PUT/GET/DELETE integration REST-path
// branches in parseAPIGWMethodPath.
func TestHandler_RESTPath_Integration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		body     string
		wantCode int
	}{
		{
			name:     "PUT_integration_via_REST",
			method:   http.MethodPut,
			body:     `{"type":"MOCK"}`,
			wantCode: http.StatusCreated,
		},
		{
			name:     "GET_integration_via_REST",
			method:   http.MethodGet,
			wantCode: http.StatusOK,
		},
		{
			name:     "DELETE_integration_via_REST",
			method:   http.MethodDelete,
			wantCode: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			api, err := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
			require.NoError(t, err)

			resources, _, err := backend.GetResources(api.ID, "", 0)
			require.NoError(t, err)
			rootID := resources[0].ID

			_, err = backend.PutMethod(
				apigateway.PutMethodInput{
					RestAPIID:         api.ID,
					ResourceID:        rootID,
					HTTPMethod:        "GET",
					AuthorizationType: "NONE",
				},
			)
			require.NoError(t, err)

			// Ensure integration exists for GET and DELETE operations.
			if tt.method != http.MethodPut {
				_, err = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
				require.NoError(t, err)
			}

			h := apigateway.NewHandler(backend)
			path := fmt.Sprintf("/restapis/%s/resources/%s/methods/GET/integration", api.ID, rootID)

			rec := restRequest(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestHandler_GetSupportedOperations covers the `GET /` handler branch that returns
// the list of supported operations.
func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "GET_root_returns_operations",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h := apigateway.NewHandler(apigateway.NewInMemoryBackend())

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			err := h.Handler()(e.NewContext(req, rec))
			require.NoError(t, err)

			assert.Equal(t, tt.wantCode, rec.Code)

			var ops []string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ops))
			assert.Contains(t, ops, "CreateRestApi")
		})
	}
}

// TestHandler_InvalidTarget covers the branch that rejects an X-Amz-Target header
// that does not contain exactly one dot (e.g. "NoDotsHere").
func TestHandler_InvalidTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		target   string
		wantCode int
	}{
		{
			name:     "target_without_dot_returns_400",
			target:   "NoDotInTarget",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h := apigateway.NewHandler(apigateway.NewInMemoryBackend())

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			err := h.Handler()(e.NewContext(req, rec))
			require.NoError(t, err)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestVTL_DefaultJSONType covers the default branch in jsonValueToString (objects/arrays).
func TestVTL_DefaultJSONType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		tmpl         string
		ctx          apigateway.VTLContext
		wantContains string
	}{
		{
			name:         "input_path_array_returns_json_encoded",
			tmpl:         `$input.path('$.items')`,
			ctx:          apigateway.VTLContext{Body: `{"items":[1,2,3]}`},
			wantContains: "1",
		},
		{
			name:         "input_path_object_returns_json_encoded",
			tmpl:         `$input.path('$.obj')`,
			ctx:          apigateway.VTLContext{Body: `{"obj":{"key":"val"}}`},
			wantContains: "key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := apigateway.RenderTemplate(tt.tmpl, tt.ctx)
			assert.Contains(t, out, tt.wantContains)
		})
	}
}

// TestGetResources_SortWithMultipleItems ensures the sort closure in GetResources is
// exercised by requesting all resources when at least two exist.
func TestGetResources_SortWithMultipleItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		childParts  []string
		wantAtLeast int
	}{
		{
			name:        "two_resources_triggers_sort",
			childParts:  []string{"orders"},
			wantAtLeast: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
			require.NoError(t, err)

			resources, _, err := b.GetResources(api.ID, "", 0)
			require.NoError(t, err)
			rootID := resources[0].ID

			for _, part := range tt.childParts {
				_, err = b.CreateResource(api.ID, rootID, part)
				require.NoError(t, err)
			}

			all, _, err := b.GetResources(api.ID, "", 0)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(all), tt.wantAtLeast)
		})
	}
}

// TestGetStages_SortWithMultipleItems ensures the sort closure in GetStages is exercised
// by creating two deployments with different stage names and then listing stages.
func TestGetStages_SortWithMultipleItems(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stageNames []string
	}{
		{
			name:       "two_stages_triggers_sort",
			stageNames: []string{"prod", "staging"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
			require.NoError(t, err)

			for _, s := range tt.stageNames {
				_, err = b.CreateDeployment(api.ID, s, "")
				require.NoError(t, err)
			}

			stages, err := b.GetStages(api.ID)
			require.NoError(t, err)
			assert.Len(t, stages, len(tt.stageNames))
		})
	}
}

// TestVTL_AdditionalBranches covers the false-bool, fractional-float, and
// remaining escapeJavaScript character branches in vtl.go.
func TestVTL_AdditionalBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tmpl      string
		ctx       apigateway.VTLContext
		wantEqual string
	}{
		{
			name:      "input_path_bool_false",
			tmpl:      `$input.path('$.active')`,
			ctx:       apigateway.VTLContext{Body: `{"active":false}`},
			wantEqual: "false",
		},
		{
			name:      "input_path_float_with_fractional",
			tmpl:      `$input.path('$.ratio')`,
			ctx:       apigateway.VTLContext{Body: `{"ratio":3.14}`},
			wantEqual: "3.14",
		},
		{
			name:      "escape_javascript_single_quote",
			tmpl:      `$util.escapeJavaScript("it's here")`,
			ctx:       apigateway.VTLContext{},
			wantEqual: `it\'s here`,
		},
		{
			name:      "escape_javascript_tab",
			tmpl:      "$util.escapeJavaScript('col1\tcol2')",
			ctx:       apigateway.VTLContext{},
			wantEqual: `col1\tcol2`,
		},
		{
			name:      "escape_javascript_carriage_return",
			tmpl:      "$util.escapeJavaScript('line\r\n')",
			ctx:       apigateway.VTLContext{},
			wantEqual: `line\r\n`,
		},
		{
			name:      "escape_javascript_backslash",
			tmpl:      `$util.escapeJavaScript('path\file')`,
			ctx:       apigateway.VTLContext{},
			wantEqual: `path\\file`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := apigateway.RenderTemplate(tt.tmpl, tt.ctx)
			assert.Equal(t, tt.wantEqual, out)
		})
	}
}
