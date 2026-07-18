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

// TestUpdateRequestValidator tests UpdateRequestValidator.
func TestUpdateRequestValidator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newName  string
		wantCode int
		useValid bool
	}{
		{
			name:     "update_name",
			newName:  "new-validator-name",
			wantCode: http.StatusOK,
			useValid: true,
		},
		{
			name:     "validator_not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()
			apiID := boostAPI(t, handler, e)

			createRec := postWithHandler(t, handler, e, "CreateRequestValidator",
				fmt.Sprintf(`{"restApiId":%q,"name":"orig-validator"}`, apiID))
			require.Equal(t, http.StatusCreated, createRec.Code)
			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			validatorID := createResp["id"].(string)

			lookupID := validatorID
			if !tt.useValid {
				lookupID = "notexist"
			}

			rec := postWithHandler(t, handler, e, "UpdateRequestValidator",
				fmt.Sprintf(`{"restApiId":%q,"requestValidatorId":%q,"name":%q}`,
					apiID, lookupID, tt.newName))
			assert.Equal(t, tt.wantCode, rec.Code)
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
