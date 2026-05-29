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
				api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{
					Name:        "my-api",
					Description: "desc",
					Tags:        tags.FromMap("test.apigw", map[string]string{"env": "test"}),
				})
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
				_, _ = b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "a"})
				_, _ = b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "b"})

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
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "to-del"})

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
				_, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{})
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
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})

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
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})

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
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
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
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
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
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				resources, _, _ := b.GetResources(api.ID, "", 0)
				rootID := resources[0].ID

				m, err := b.PutMethod(
					apigateway.PutMethodInput{
						RestAPIID:         api.ID,
						ResourceID:        rootID,
						HTTPMethod:        "GET",
						AuthorizationType: "NONE",
					},
				)
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
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
				resources, _, _ := b.GetResources(api.ID, "", 0)
				rootID := resources[0].ID

				_, _ = b.PutMethod(
					apigateway.PutMethodInput{
						RestAPIID:         api.ID,
						ResourceID:        rootID,
						HTTPMethod:        "POST",
						AuthorizationType: "NONE",
					},
				)

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
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})

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
				api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
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
				api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
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
				api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
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
				api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
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
				api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
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
				api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
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
				api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
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
				api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
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
				api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
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
			api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})

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
			api, err := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "api"})
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

func TestBackend_Integration_NewFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input apigateway.PutIntegrationInput
		check func(t *testing.T, got *apigateway.Integration)
	}{
		{
			name: "connection_type_vpc_link",
			input: apigateway.PutIntegrationInput{
				Type:           "HTTP",
				HTTPMethod:     "GET",
				URI:            "https://internal.example.com/api",
				ConnectionType: "VPC_LINK",
				ConnectionId:   "abc123",
			},
			check: func(t *testing.T, got *apigateway.Integration) {
				t.Helper()
				assert.Equal(t, "VPC_LINK", got.ConnectionType)
				assert.Equal(t, "abc123", got.ConnectionId)
			},
		},
		{
			name: "connection_type_internet",
			input: apigateway.PutIntegrationInput{
				Type:           "HTTP_PROXY",
				HTTPMethod:     "POST",
				URI:            "https://api.example.com/v1",
				ConnectionType: "INTERNET",
			},
			check: func(t *testing.T, got *apigateway.Integration) {
				t.Helper()
				assert.Equal(t, "INTERNET", got.ConnectionType)
			},
		},
		{
			name: "content_handling_convert_to_text",
			input: apigateway.PutIntegrationInput{
				Type:            "AWS",
				HTTPMethod:      "POST",
				URI:             "arn:aws:lambda:::function:fn",
				ContentHandling: "CONVERT_TO_TEXT",
			},
			check: func(t *testing.T, got *apigateway.Integration) {
				t.Helper()
				assert.Equal(t, "CONVERT_TO_TEXT", got.ContentHandling)
			},
		},
		{
			name: "credentials_and_cache",
			input: apigateway.PutIntegrationInput{
				Type:               "AWS",
				HTTPMethod:         "POST",
				URI:                "arn:aws:lambda:::function:fn",
				Credentials:        "arn:aws:iam::123456789012:role/MyRole",
				CacheNamespace:     "mynamespace",
				CacheKeyParameters: []string{"method.request.header.X-User-Id"},
			},
			check: func(t *testing.T, got *apigateway.Integration) {
				t.Helper()
				assert.Equal(t, "arn:aws:iam::123456789012:role/MyRole", got.Credentials)
				assert.Equal(t, "mynamespace", got.CacheNamespace)
				assert.Equal(t, []string{"method.request.header.X-User-Id"}, got.CacheKeyParameters)
			},
		},
		{
			name: "request_parameters",
			input: apigateway.PutIntegrationInput{
				Type:       "AWS_PROXY",
				HTTPMethod: "POST",
				URI:        "arn:aws:lambda:::function:fn",
				RequestParameters: map[string]string{
					"integration.request.header.X-User-Id": "method.request.header.Authorization",
				},
			},
			check: func(t *testing.T, got *apigateway.Integration) {
				t.Helper()
				require.Contains(t, got.RequestParameters, "integration.request.header.X-User-Id")
				assert.Equal(t, "method.request.header.Authorization",
					got.RequestParameters["integration.request.header.X-User-Id"])
			},
		},
		{
			name: "timeout_in_millis",
			input: apigateway.PutIntegrationInput{
				Type:            "AWS",
				HTTPMethod:      "POST",
				URI:             "arn:aws:lambda:::function:fn",
				TimeoutInMillis: 15000,
			},
			check: func(t *testing.T, got *apigateway.Integration) {
				t.Helper()
				assert.Equal(t, 15000, got.TimeoutInMillis)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "integ-" + tt.name})
			resources, _, _ := b.GetResources(api.ID, "", 0)
			rootID := resources[0].ID

			_, _ = b.PutMethod(apigateway.PutMethodInput{
				RestAPIID:         api.ID,
				ResourceID:        rootID,
				HTTPMethod:        "GET",
				AuthorizationType: "NONE",
			})

			integ, err := b.PutIntegration(api.ID, rootID, "GET", tt.input)
			require.NoError(t, err)
			tt.check(t, integ)

			got, err := b.GetIntegration(api.ID, rootID, "GET")
			require.NoError(t, err)
			tt.check(t, got)
		})
	}
}

func TestBackend_UpdateIntegration_NewFields(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "upd-integ-api"})
	resources, _, _ := b.GetResources(api.ID, "", 0)
	rootID := resources[0].ID

	_, _ = b.PutMethod(apigateway.PutMethodInput{
		RestAPIID:         api.ID,
		ResourceID:        rootID,
		HTTPMethod:        "GET",
		AuthorizationType: "NONE",
	})
	_, _ = b.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{
		Type:       "HTTP",
		HTTPMethod: "GET",
		URI:        "https://example.com/api",
	})

	updated, err := b.UpdateIntegration(apigateway.UpdateIntegrationInput{
		RestAPIID:          api.ID,
		ResourceID:         rootID,
		HTTPMethod:         "GET",
		ConnectionType:     "INTERNET",
		ContentHandling:    "CONVERT_TO_BINARY",
		Credentials:        "arn:aws:iam::123456789012:role/TestRole",
		CacheNamespace:     "ns1",
		CacheKeyParameters: []string{"method.request.querystring.key"},
		RequestParameters: map[string]string{
			"integration.request.header.X-Forwarded-For": "method.request.header.X-Forwarded-For",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "INTERNET", updated.ConnectionType)
	assert.Equal(t, "CONVERT_TO_BINARY", updated.ContentHandling)
	assert.Equal(t, "arn:aws:iam::123456789012:role/TestRole", updated.Credentials)
	assert.Equal(t, "ns1", updated.CacheNamespace)
	assert.Equal(t, []string{"method.request.querystring.key"}, updated.CacheKeyParameters)
	require.Contains(t, updated.RequestParameters, "integration.request.header.X-Forwarded-For")
}

func TestBackend_GetAPIKeyByValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		findValue string
		wantFound bool
	}{
		{name: "found_by_value", findValue: "auto", wantFound: true},
		{name: "not_found", findValue: "nonexistent-value-xyz", wantFound: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			key, err := b.CreateAPIKey(apigateway.CreateAPIKeyInput{
				Name:    "test-key",
				Enabled: true,
			})
			require.NoError(t, err)
			require.NotEmpty(t, key.Value)

			lookupValue := key.Value
			if tt.findValue != "auto" {
				lookupValue = tt.findValue
			}

			got, err := b.GetAPIKeyByValue(lookupValue)
			if tt.wantFound {
				require.NoError(t, err)
				assert.Equal(t, key.ID, got.ID)
				assert.Equal(t, key.Value, got.Value)
			} else {
				require.Error(t, err)
				assert.Nil(t, got)
			}
		})
	}
}

func TestBackend_GetAPIKeyByValue_MultipleKeys(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	k1, _ := b.CreateAPIKey(apigateway.CreateAPIKeyInput{Name: "key1", Enabled: true})
	k2, _ := b.CreateAPIKey(apigateway.CreateAPIKeyInput{Name: "key2", Enabled: true})

	got1, err := b.GetAPIKeyByValue(k1.Value)
	require.NoError(t, err)
	assert.Equal(t, k1.ID, got1.ID)

	got2, err := b.GetAPIKeyByValue(k2.Value)
	require.NoError(t, err)
	assert.Equal(t, k2.ID, got2.ID)
}

func TestBackend_UsagePlan_ApiStages_CRUD(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()

	api1, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "plan-api-1"})
	_, _ = b.CreateDeployment(api1.ID, "prod", "v1")
	api2, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "plan-api-2"})
	_, _ = b.CreateDeployment(api2.ID, "staging", "v1")

	tests := []struct {
		name  string
		input apigateway.CreateUsagePlanInput
		check func(t *testing.T, plan *apigateway.UsagePlan)
	}{
		{
			name: "no_api_stages",
			input: apigateway.CreateUsagePlanInput{
				Name: "plain-plan",
			},
			check: func(t *testing.T, plan *apigateway.UsagePlan) {
				t.Helper()
				assert.Empty(t, plan.ApiStages)
			},
		},
		{
			name: "single_api_stage",
			input: apigateway.CreateUsagePlanInput{
				Name: "single-stage-plan",
				ApiStages: []apigateway.ApiStageAssociation{
					{RestApiId: api1.ID, Stage: "prod"},
				},
			},
			check: func(t *testing.T, plan *apigateway.UsagePlan) {
				t.Helper()
				require.Len(t, plan.ApiStages, 1)
				assert.Equal(t, api1.ID, plan.ApiStages[0].RestApiId)
				assert.Equal(t, "prod", plan.ApiStages[0].Stage)
			},
		},
		{
			name: "multiple_api_stages",
			input: apigateway.CreateUsagePlanInput{
				Name: "multi-stage-plan",
				ApiStages: []apigateway.ApiStageAssociation{
					{RestApiId: api1.ID, Stage: "prod"},
					{RestApiId: api2.ID, Stage: "staging"},
				},
			},
			check: func(t *testing.T, plan *apigateway.UsagePlan) {
				t.Helper()
				assert.Len(t, plan.ApiStages, 2)
			},
		},
		{
			name: "api_stage_with_per_method_throttle",
			input: apigateway.CreateUsagePlanInput{
				Name: "throttle-method-plan",
				ApiStages: []apigateway.ApiStageAssociation{
					{
						RestApiId: api1.ID,
						Stage:     "prod",
						Throttle: map[string]*apigateway.ThrottleSettings{
							"GET /items":  {RateLimit: 100, BurstLimit: 50},
							"POST /items": {RateLimit: 50, BurstLimit: 25},
						},
					},
				},
			},
			check: func(t *testing.T, plan *apigateway.UsagePlan) {
				t.Helper()
				require.Len(t, plan.ApiStages, 1)
				require.NotNil(t, plan.ApiStages[0].Throttle)
				assert.Contains(t, plan.ApiStages[0].Throttle, "GET /items")
				assert.Contains(t, plan.ApiStages[0].Throttle, "POST /items")
				assert.InDelta(t, 100.0, plan.ApiStages[0].Throttle["GET /items"].RateLimit, 0.001)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			plan, err := b.CreateUsagePlan(tt.input)
			require.NoError(t, err)
			tt.check(t, plan)

			got, err := b.GetUsagePlan(plan.ID)
			require.NoError(t, err)
			tt.check(t, got)
		})
	}
}

func TestBackend_DomainName_NewFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input apigateway.CreateDomainNameInput
		check func(t *testing.T, dn *apigateway.DomainName)
	}{
		{
			name: "default_security_policy",
			input: apigateway.CreateDomainNameInput{
				DomainName:     "test1.example.com",
				CertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/abc",
			},
			check: func(t *testing.T, dn *apigateway.DomainName) {
				t.Helper()
				assert.Equal(t, "TLS_1_2", dn.SecurityPolicy)
				assert.Equal(t, "AVAILABLE", dn.DomainNameStatus)
				assert.NotEmpty(t, dn.RegionalDomainName)
				assert.NotEmpty(t, dn.DistributionDomainName)
				assert.NotEmpty(t, dn.RegionalHostedZoneId)
				assert.NotEmpty(t, dn.DistributionHostedZoneId)
			},
		},
		{
			name: "custom_security_policy_tls10",
			input: apigateway.CreateDomainNameInput{
				DomainName:     "test2.example.com",
				CertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/tls10",
				SecurityPolicy: "TLS_1_0",
			},
			check: func(t *testing.T, dn *apigateway.DomainName) {
				t.Helper()
				assert.Equal(t, "TLS_1_0", dn.SecurityPolicy)
			},
		},
		{
			name: "regional_endpoint",
			input: apigateway.CreateDomainNameInput{
				DomainName:             "regional.example.com",
				RegionalCertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/regional",
				EndpointConfiguration: &apigateway.EndpointConfiguration{
					Types: []string{"REGIONAL"},
				},
			},
			check: func(t *testing.T, dn *apigateway.DomainName) {
				t.Helper()
				require.NotNil(t, dn.EndpointConfiguration)
				assert.Contains(t, dn.EndpointConfiguration.Types, "REGIONAL")
				assert.Equal(t, "arn:aws:acm:us-east-1:123456789012:certificate/regional", dn.RegionalCertificateARN)
			},
		},
		{
			name: "edge_endpoint",
			input: apigateway.CreateDomainNameInput{
				DomainName:     "edge.example.com",
				CertificateARN: "arn:aws:acm:us-east-1:123456789012:certificate/edge",
				EndpointConfiguration: &apigateway.EndpointConfiguration{
					Types: []string{"EDGE"},
				},
			},
			check: func(t *testing.T, dn *apigateway.DomainName) {
				t.Helper()
				require.NotNil(t, dn.EndpointConfiguration)
				assert.Contains(t, dn.EndpointConfiguration.Types, "EDGE")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigateway.NewInMemoryBackend()
			dn, err := b.CreateDomainName(tt.input)
			require.NoError(t, err)
			tt.check(t, dn)

			got, err := b.GetDomainName(tt.input.DomainName)
			require.NoError(t, err)
			tt.check(t, got)
		})
	}
}

func TestBackend_Stage_ClientCertificateId(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, _ := b.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "cert-stage-api"})

	cert, err := b.GenerateClientCertificate(apigateway.GenerateClientCertificateInput{
		Description: "test cert",
	})
	require.NoError(t, err)

	depl, _ := b.CreateDeployment(api.ID, "", "v1")

	tests := []struct {
		name  string
		input apigateway.CreateStageInput
		check func(t *testing.T, stage *apigateway.Stage)
	}{
		{
			name: "with_cert",
			input: apigateway.CreateStageInput{
				RestAPIID:           api.ID,
				StageName:           "prod",
				DeploymentID:        depl.ID,
				ClientCertificateId: cert.ClientCertificateID,
			},
			check: func(t *testing.T, stage *apigateway.Stage) {
				t.Helper()
				assert.Equal(t, cert.ClientCertificateID, stage.ClientCertificateId)
			},
		},
		{
			name: "without_cert",
			input: apigateway.CreateStageInput{
				RestAPIID:    api.ID,
				StageName:    "dev",
				DeploymentID: depl.ID,
			},
			check: func(t *testing.T, stage *apigateway.Stage) {
				t.Helper()
				assert.Empty(t, stage.ClientCertificateId)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stage, err := b.CreateStage(tt.input)
			require.NoError(t, err)
			tt.check(t, stage)

			got, err := b.GetStage(api.ID, tt.input.StageName)
			require.NoError(t, err)
			tt.check(t, got)
		})
	}
}
