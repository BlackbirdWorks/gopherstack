package apigatewayv2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

func TestCreateAPICustomIDTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tags       map[string]string
		wantID     string
		wantStatus int
	}{
		{
			name:       "localstack tag",
			tags:       map[string]string{"_custom_id_": "myapi", "env": "dev"},
			wantID:     "myapi",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "native tag",
			tags:       map[string]string{"gopherstack:id": "native1"},
			wantID:     "native1",
			wantStatus: http.StatusCreated,
		},
		{
			name:       "invalid",
			tags:       map[string]string{"_custom_id_": "bad id!"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			body := map[string]any{"name": "a", "protocolType": "HTTP", "tags": tt.tags}

			rr := doRequest(t, h, http.MethodPost, "/v2/apis", body)
			require.Equal(t, tt.wantStatus, rr.Code, rr.Body.String())

			if tt.wantID == "" {
				return
			}

			var api apigatewayv2.API
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &api))
			assert.Equal(t, tt.wantID, api.APIID)
			assert.NotContains(t, api.Tags, "_custom_id_")
			assert.NotContains(t, api.Tags, "gopherstack:id")

			dup := doRequest(t, h, http.MethodPost, "/v2/apis", body)
			assert.Equal(t, http.StatusConflict, dup.Code)

			tag := doRequest(t, h, http.MethodPost, "/v2/tags/arn:aws:apigateway:us-east-1::/apis/"+tt.wantID,
				map[string]any{"tags": map[string]string{"_custom_id_": "x"}})
			assert.Equal(t, http.StatusBadRequest, tag.Code)
		})
	}
}
