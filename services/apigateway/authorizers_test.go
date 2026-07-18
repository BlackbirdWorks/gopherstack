package apigateway_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// TestTestInvokeAuthorizer tests TestInvokeAuthorizer.
func TestTestInvokeAuthorizer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantCode     int
		useValidAPI  bool
		useValidAuth bool
	}{
		{
			name:         "invoke_ok",
			wantCode:     http.StatusOK,
			useValidAPI:  true,
			useValidAuth: true,
		},
		{
			name:         "authorizer_not_found",
			wantCode:     http.StatusNotFound,
			useValidAPI:  true,
			useValidAuth: false,
		},
		{
			name:        "api_not_found",
			wantCode:    http.StatusNotFound,
			useValidAPI: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			authID := boostAuthorizer(t, handler, e, apiID)

			lookupAPIID := apiID
			lookupAuthID := authID
			if !tt.useValidAPI {
				lookupAPIID = "notexist"
				lookupAuthID = "notexist"
			} else if !tt.useValidAuth {
				lookupAuthID = "notexist"
			}

			rec := postWithHandler(t, handler, e, "TestInvokeAuthorizer",
				fmt.Sprintf(`{"restApiId":%q,"authorizerId":%q}`, lookupAPIID, lookupAuthID))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "test-principal", resp["principalId"])
			}
		})
	}
}

// TestUpdateAuthorizer tests UpdateAuthorizer.
func TestUpdateAuthorizer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newName  string
		wantCode int
		useValid bool
	}{
		{
			name:     "update_name",
			newName:  "new-auth-name",
			wantCode: http.StatusOK,
			useValid: true,
		},
		{
			name:     "authorizer_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)
			authID := boostAuthorizer(t, handler, e, apiID)

			lookupID := authID
			if !tt.useValid {
				lookupID = "notexist"
			}

			rec := postWithHandler(t, handler, e, "UpdateAuthorizer",
				fmt.Sprintf(`{"restApiId":%q,"authorizerId":%q,"name":%q}`,
					apiID, lookupID, tt.newName))
			assert.Equal(t, tt.wantCode, rec.Code)
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
