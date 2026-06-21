package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_CreateEndpointConfigRequiresProductionVariants verifies that
// CreateEndpointConfig rejects requests with an empty ProductionVariants list.
// Real AWS returns ValidationException for this case; the emulator previously
// accepted empty variants and created a corrupt endpoint config.
func TestParity_CreateEndpointConfigRequiresProductionVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "empty_variants_rejected",
			body: map[string]any{
				"EndpointConfigName": "bad-config",
				"ProductionVariants": []map[string]any{},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "null_variants_rejected",
			body: map[string]any{
				"EndpointConfigName": "null-config",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "single_variant_accepted",
			body: map[string]any{
				"EndpointConfigName": "good-config",
				"ProductionVariants": []map[string]any{
					{
						"VariantName":          "AllTraffic",
						"ModelName":            "my-model",
						"InstanceType":         "ml.t2.medium",
						"InitialInstanceCount": 1,
					},
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "multiple_variants_accepted",
			body: map[string]any{
				"EndpointConfigName": "multi-config",
				"ProductionVariants": []map[string]any{
					{
						"VariantName":          "Variant1",
						"ModelName":            "model-a",
						"InstanceType":         "ml.t2.medium",
						"InitialInstanceCount": 1,
					},
					{
						"VariantName":          "Variant2",
						"ModelName":            "model-b",
						"InstanceType":         "ml.t2.large",
						"InitialInstanceCount": 2,
					},
				},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "CreateEndpointConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, "body=%v", tt.body)

			if tt.wantCode == http.StatusBadRequest {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				msg, _ := errResp["message"].(string)
				assert.Contains(t, msg, "ProductionVariant",
					"error message must mention ProductionVariant")
			}
		})
	}
}
