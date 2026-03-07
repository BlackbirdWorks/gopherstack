package apigateway_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

func TestBackend_RestAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_and_get",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, err := b.CreateRestAPI(
					"my-api",
					"desc",
					tags.FromMap("test.apigw", map[string]string{"env": "test"}),
				)
				require.NoError(t, err)
				assert.NotEmpty(t, api.ID)
				assert.Equal(t, "my-api", api.Name)

				got, err := b.GetRestAPI(api.ID)
				require.NoError(t, err)
				assert.Equal(t, api.ID, got.ID)
			},
		},
		{
			name: "get_nonexistent_returns_error",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				_, err := b.GetRestAPI("nonexistent")
				require.Error(t, err)
			},
		},
		{
			name: "list_all",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				_, _ = b.CreateRestAPI("a", "", nil)
				_, _ = b.CreateRestAPI("b", "", nil)

				apis, pos, err := b.GetRestAPIs(0, "")
				require.NoError(t, err)
				assert.Len(t, apis, 2)
				assert.Empty(t, pos)
			},
		},
		{
			name: "delete_existing",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, _ := b.CreateRestAPI("to-del", "", nil)

				err := b.DeleteRestAPI(api.ID)
				require.NoError(t, err)

				_, err = b.GetRestAPI(api.ID)
				require.Error(t, err)
			},
		},
		{
			name: "delete_nonexistent_returns_error",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				err := b.DeleteRestAPI("nonexistent")
				require.Error(t, err)
			},
		},
		{
			name: "create_with_empty_name_returns_error",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				_, err := b.CreateRestAPI("", "", nil)
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestBackend_Resource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "root_resource_created_on_api_creation",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, _ := b.CreateRestAPI("api", "", nil)

				resources, _, err := b.GetResources(api.ID, "", 0)
				require.NoError(t, err)
				assert.Len(t, resources, 1)
				assert.Equal(t, "/", resources[0].Path)
				assert.Empty(t, resources[0].PathPart)
			},
		},
		{
			name: "create_and_get_child",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, _ := b.CreateRestAPI("api", "", nil)

				resources, _, _ := b.GetResources(api.ID, "", 0)
				rootID := resources[0].ID

				child, err := b.CreateResource(api.ID, rootID, "users")
				require.NoError(t, err)
				assert.Equal(t, "/users", child.Path)
				assert.Equal(t, rootID, child.ParentID)

				got, err := b.GetResource(api.ID, child.ID)
				require.NoError(t, err)
				assert.Equal(t, child.ID, got.ID)
			},
		},
		{
			name: "delete_existing",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, _ := b.CreateRestAPI("api", "", nil)
				resources, _, _ := b.GetResources(api.ID, "", 0)
				rootID := resources[0].ID

				child, _ := b.CreateResource(api.ID, rootID, "items")

				err := b.DeleteResource(api.ID, child.ID)
				require.NoError(t, err)

				_, err = b.GetResource(api.ID, child.ID)
				require.Error(t, err)
			},
		},
		{
			name: "create_with_empty_path_part_returns_error",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, _ := b.CreateRestAPI("api", "", nil)
				resources, _, _ := b.GetResources(api.ID, "", 0)
				rootID := resources[0].ID

				_, err := b.CreateResource(api.ID, rootID, "")
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestBackend_Method(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "put_get_delete",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, _ := b.CreateRestAPI("api", "", nil)
				resources, _, _ := b.GetResources(api.ID, "", 0)
				rootID := resources[0].ID

				m, err := b.PutMethod(api.ID, rootID, "GET", "NONE", "", "", false)
				require.NoError(t, err)
				assert.Equal(t, "GET", m.HTTPMethod)

				got, err := b.GetMethod(api.ID, rootID, "GET")
				require.NoError(t, err)
				assert.Equal(t, "NONE", got.AuthorizationType)

				err = b.DeleteMethod(api.ID, rootID, "GET")
				require.NoError(t, err)

				_, err = b.GetMethod(api.ID, rootID, "GET")
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestBackend_Integration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "put_get_delete",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, _ := b.CreateRestAPI("api", "", nil)
				resources, _, _ := b.GetResources(api.ID, "", 0)
				rootID := resources[0].ID

				_, _ = b.PutMethod(api.ID, rootID, "POST", "NONE", "", "", false)

				input := apigateway.PutIntegrationInput{Type: "MOCK"}
				integ, err := b.PutIntegration(api.ID, rootID, "POST", input)
				require.NoError(t, err)
				assert.Equal(t, "MOCK", integ.Type)

				got, err := b.GetIntegration(api.ID, rootID, "POST")
				require.NoError(t, err)
				assert.Equal(t, "MOCK", got.Type)

				err = b.DeleteIntegration(api.ID, rootID, "POST")
				require.NoError(t, err)

				_, err = b.GetIntegration(api.ID, rootID, "POST")
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestBackend_DeploymentAndStage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_deployment_and_stage",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, _ := b.CreateRestAPI("api", "", nil)

				depl, err := b.CreateDeployment(api.ID, "prod", "initial")
				require.NoError(t, err)
				assert.NotEmpty(t, depl.ID)

				depls, err := b.GetDeployments(api.ID)
				require.NoError(t, err)
				assert.Len(t, depls, 1)

				stages, err := b.GetStages(api.ID)
				require.NoError(t, err)
				assert.Len(t, stages, 1)
				assert.Equal(t, "prod", stages[0].StageName)
			},
		},
		{
			name: "get_and_delete_stage",
			run: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, _ := b.CreateRestAPI("api", "", nil)
				_, _ = b.CreateDeployment(api.ID, "v1", "")

				stage, err := b.GetStage(api.ID, "v1")
				require.NoError(t, err)
				assert.Equal(t, "v1", stage.StageName)

				err = b.DeleteStage(api.ID, "v1")
				require.NoError(t, err)

				_, err = b.GetStage(api.ID, "v1")
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

func TestBackend_Authorizer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		testFunc func(t *testing.T)
		name     string
	}{
		{
			name: "create_authorizer",
			testFunc: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, err := b.CreateRestAPI("api", "", nil)
				require.NoError(t, err)

				auth, err := b.CreateAuthorizer(api.ID, apigateway.CreateAuthorizerInput{
					Name:           "my-auth",
					Type:           "TOKEN",
					IdentitySource: "method.request.header.Authorization",
				})
				require.NoError(t, err)
				assert.NotEmpty(t, auth.ID)
				assert.Equal(t, "my-auth", auth.Name)
				assert.Equal(t, "TOKEN", auth.Type)
				assert.Equal(t, "method.request.header.Authorization", auth.IdentitySource)
			},
		},
		{
			name: "get_authorizer",
			testFunc: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, err := b.CreateRestAPI("api", "", nil)
				require.NoError(t, err)

				created, err := b.CreateAuthorizer(api.ID, apigateway.CreateAuthorizerInput{
					Name: "my-auth",
					Type: "TOKEN",
				})
				require.NoError(t, err)

				got, err := b.GetAuthorizer(api.ID, created.ID)
				require.NoError(t, err)
				assert.Equal(t, created.ID, got.ID)
				assert.Equal(t, created.Name, got.Name)
				assert.Equal(t, created.Type, got.Type)
			},
		},
		{
			name: "get_nonexistent_authorizer_returns_error",
			testFunc: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, err := b.CreateRestAPI("api", "", nil)
				require.NoError(t, err)

				_, err = b.GetAuthorizer(api.ID, "nonexistent")
				require.Error(t, err)
			},
		},
		{
			name: "list_authorizers",
			testFunc: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, err := b.CreateRestAPI("api", "", nil)
				require.NoError(t, err)

				_, err = b.CreateAuthorizer(api.ID, apigateway.CreateAuthorizerInput{
					Name: "my-auth",
					Type: "TOKEN",
				})
				require.NoError(t, err)

				auths, err := b.GetAuthorizers(api.ID)
				require.NoError(t, err)
				assert.Len(t, auths, 1)
				assert.Equal(t, "my-auth", auths[0].Name)
			},
		},
		{
			name: "update_authorizer",
			testFunc: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, err := b.CreateRestAPI("api", "", nil)
				require.NoError(t, err)

				created, err := b.CreateAuthorizer(api.ID, apigateway.CreateAuthorizerInput{
					Name: "my-auth",
					Type: "TOKEN",
				})
				require.NoError(t, err)

				updated, err := b.UpdateAuthorizer(api.ID, created.ID, apigateway.UpdateAuthorizerInput{
					Name: "updated-auth",
				})
				require.NoError(t, err)
				assert.Equal(t, created.ID, updated.ID)
				assert.Equal(t, "updated-auth", updated.Name)
				assert.Equal(t, created.Type, updated.Type)
			},
		},
		{
			name: "update_nonexistent_authorizer_returns_error",
			testFunc: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, err := b.CreateRestAPI("api", "", nil)
				require.NoError(t, err)

				_, err = b.UpdateAuthorizer(api.ID, "nonexistent", apigateway.UpdateAuthorizerInput{Name: "x"})
				require.Error(t, err)
			},
		},
		{
			name: "delete_authorizer",
			testFunc: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, err := b.CreateRestAPI("api", "", nil)
				require.NoError(t, err)

				created, err := b.CreateAuthorizer(api.ID, apigateway.CreateAuthorizerInput{
					Name: "my-auth",
					Type: "TOKEN",
				})
				require.NoError(t, err)

				err = b.DeleteAuthorizer(api.ID, created.ID)
				require.NoError(t, err)

				_, err = b.GetAuthorizer(api.ID, created.ID)
				require.Error(t, err)
			},
		},
		{
			name: "delete_nonexistent_authorizer_returns_error",
			testFunc: func(t *testing.T) {
				t.Helper()

				b := apigateway.NewInMemoryBackend()
				api, err := b.CreateRestAPI("api", "", nil)
				require.NoError(t, err)

				err = b.DeleteAuthorizer(api.ID, "nonexistent")
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.testFunc(t)
		})
	}
}

func TestBackend_Authorizer_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   apigateway.CreateAuthorizerInput
		wantErr bool
	}{
		{
			name:    "create_missing_name_returns_error",
			input:   apigateway.CreateAuthorizerInput{Type: "TOKEN"},
			wantErr: true,
		},
		{
			name:    "create_missing_type_returns_error",
			input:   apigateway.CreateAuthorizerInput{Name: "auth"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, _ := b.CreateRestAPI("api", "", nil)

			_, err := b.CreateAuthorizer(api.ID, tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBackend_RequestValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want apigateway.RequestValidator
	}{
		{
			name: "create_get_update_delete",
			want: apigateway.RequestValidator{
				Name:                      "my-validator",
				ValidateRequestBody:       true,
				ValidateRequestParameters: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, err := b.CreateRestAPI("api", "", nil)
			require.NoError(t, err)

			rv, err := b.CreateRequestValidator(api.ID, apigateway.CreateRequestValidatorInput{
				Name:                tt.want.Name,
				ValidateRequestBody: tt.want.ValidateRequestBody,
			})
			require.NoError(t, err)
			assert.NotEmpty(t, rv.ID)
			assert.Equal(t, tt.want.Name, rv.Name)
			assert.Equal(t, tt.want.ValidateRequestBody, rv.ValidateRequestBody)

			got, err := b.GetRequestValidator(api.ID, rv.ID)
			require.NoError(t, err)
			assert.Equal(t, rv.ID, got.ID)

			rvs, err := b.GetRequestValidators(api.ID)
			require.NoError(t, err)
			assert.Len(t, rvs, 1)

			validateBody := false
			validateParams := true
			updated, err := b.UpdateRequestValidator(api.ID, rv.ID, apigateway.UpdateRequestValidatorInput{
				Name:                      "updated-validator",
				ValidateRequestBody:       &validateBody,
				ValidateRequestParameters: &validateParams,
			})
			require.NoError(t, err)
			assert.Equal(t, "updated-validator", updated.Name)
			assert.True(t, updated.ValidateRequestParameters)
			assert.False(t, updated.ValidateRequestBody)

			err = b.DeleteRequestValidator(api.ID, rv.ID)
			require.NoError(t, err)

			_, err = b.GetRequestValidator(api.ID, rv.ID)
			require.Error(t, err)
		})
	}
}

func TestBackend_MethodResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "put_get_delete"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, _ := b.CreateRestAPI("api", "", nil)
			resources, _, _ := b.GetResources(api.ID, "", 0)
			rootID := resources[0].ID

			_, _ = b.PutMethod(api.ID, rootID, "GET", "NONE", "", "", false)

			mr, err := b.PutMethodResponse(api.ID, rootID, "GET", "200", apigateway.PutMethodResponseInput{
				ResponseModels: map[string]string{"application/json": "Empty"},
			})
			require.NoError(t, err)
			assert.Equal(t, "200", mr.StatusCode)

			got, err := b.GetMethodResponse(api.ID, rootID, "GET", "200")
			require.NoError(t, err)
			assert.Equal(t, "200", got.StatusCode)

			err = b.DeleteMethodResponse(api.ID, rootID, "GET", "200")
			require.NoError(t, err)

			_, err = b.GetMethodResponse(api.ID, rootID, "GET", "200")
			require.Error(t, err)
		})
	}
}

func TestBackend_IntegrationResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "put_get_delete"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, _ := b.CreateRestAPI("api", "", nil)
			resources, _, _ := b.GetResources(api.ID, "", 0)
			rootID := resources[0].ID

			_, _ = b.PutMethod(api.ID, rootID, "GET", "NONE", "", "", false)
			_, _ = b.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})

			ir, err := b.PutIntegrationResponse(api.ID, rootID, "GET", "200", apigateway.PutIntegrationResponseInput{
				ResponseTemplates: map[string]string{"application/json": `{"status": "ok"}`},
			})
			require.NoError(t, err)
			assert.Equal(t, "200", ir.StatusCode)

			got, err := b.GetIntegrationResponse(api.ID, rootID, "GET", "200")
			require.NoError(t, err)
			assert.Equal(t, "200", got.StatusCode)

			err = b.DeleteIntegrationResponse(api.ID, rootID, "GET", "200")
			require.NoError(t, err)

			_, err = b.GetIntegrationResponse(api.ID, rootID, "GET", "200")
			require.Error(t, err)
		})
	}
}
