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

func TestAPIGateway_UpdateClientCertificate_RESTRoute(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	cert, err := backend.GenerateClientCertificate(apigateway.GenerateClientCertificateInput{})
	require.NoError(t, err)

	rec := restCall(t, h, http.MethodPatch, "/clientcertificates/"+cert.ClientCertificateID, "application/json",
		`[{"op":"replace","path":"/description","value":"updated desc"}]`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "updated desc", resp["description"])
}

// TestClientCertificates tests GenerateClientCertificate, GetClientCertificate, GetClientCertificates,
// DeleteClientCertificate, UpdateClientCertificate.
func TestClientCertificates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		description     string
		updateDesc      string
		wantCreateCode  int
		wantGetCode     int
		wantListMinLen  int
		wantDeleteCode  int
		wantUpdateCode  int
		doDelete        bool
		doUpdate        bool
		useInvalidGetID bool
	}{
		{
			name:           "full_lifecycle",
			description:    "test cert",
			updateDesc:     "updated description",
			wantCreateCode: http.StatusCreated,
			wantGetCode:    http.StatusOK,
			wantListMinLen: 1,
			doDelete:       true,
			wantDeleteCode: http.StatusNoContent,
			doUpdate:       true,
			wantUpdateCode: http.StatusOK,
		},
		{
			name:            "get_not_found",
			description:     "cert2",
			wantCreateCode:  http.StatusCreated,
			useInvalidGetID: true,
			wantGetCode:     http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()

			// Create
			createRec := postWithHandler(t, handler, e, "GenerateClientCertificate",
				fmt.Sprintf(`{"description":%q}`, tt.description))
			assert.Equal(t, tt.wantCreateCode, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			certID := createResp["clientCertificateId"].(string)

			// Get
			lookupID := certID
			if tt.useInvalidGetID {
				lookupID = "notexist"
			}
			getRec := postWithHandler(t, handler, e, "GetClientCertificate",
				fmt.Sprintf(`{"clientCertificateId":%q}`, lookupID))
			assert.Equal(t, tt.wantGetCode, getRec.Code)

			// List
			if tt.wantListMinLen > 0 {
				listRec := postWithHandler(t, handler, e, "GetClientCertificates", `{}`)
				assert.Equal(t, http.StatusOK, listRec.Code)
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				assert.GreaterOrEqual(t, len(listResp["item"].([]any)), tt.wantListMinLen)
			}

			// Update
			if tt.doUpdate {
				updateRec := postWithHandler(t, handler, e, "UpdateClientCertificate",
					fmt.Sprintf(`{"clientCertificateId":%q,"description":%q}`, certID, tt.updateDesc))
				assert.Equal(t, tt.wantUpdateCode, updateRec.Code)
			}

			// Delete
			if tt.doDelete {
				delRec := postWithHandler(t, handler, e, "DeleteClientCertificate",
					fmt.Sprintf(`{"clientCertificateId":%q}`, certID))
				assert.Equal(t, tt.wantDeleteCode, delRec.Code)

				getRec2 := postWithHandler(t, handler, e, "GetClientCertificate",
					fmt.Sprintf(`{"clientCertificateId":%q}`, certID))
				assert.Equal(t, http.StatusNotFound, getRec2.Code)
			}
		})
	}
}
