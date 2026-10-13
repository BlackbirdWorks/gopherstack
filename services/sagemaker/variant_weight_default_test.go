package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func TestEndpointConfig_VariantWeightDefaultsToOne(t *testing.T) {
	t.Parallel()

	tests := []struct {
		variant map[string]any
		name    string
		want    float64
	}{
		{
			name:    "unspecified",
			variant: map[string]any{"VariantName": "v", "ModelName": "m", "InitialInstanceCount": 1},
			want:    1,
		},
		{
			name: "explicit",
			variant: map[string]any{
				"VariantName":          "v",
				"ModelName":            "m",
				"InitialInstanceCount": 1,
				"InitialVariantWeight": 2.5,
			},
			want: 2.5,
		},
		{
			name: "explicit zero",
			variant: map[string]any{
				"VariantName":          "v",
				"ModelName":            "m",
				"InitialInstanceCount": 1,
				"InitialVariantWeight": 0,
			},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := sagemaker.NewHandler(sagemaker.NewInMemoryBackend("000000000000", "us-east-1"))

			rr := doSageMakerRequest(t, h, "CreateEndpointConfig", map[string]any{
				"EndpointConfigName": "cfg", "ProductionVariants": []any{tt.variant},
			})
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

			rr = doSageMakerRequest(
				t,
				h,
				"CreateEndpoint",
				map[string]any{"EndpointName": "ep", "EndpointConfigName": "cfg"},
			)
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

			rr = doSageMakerRequest(t, h, "DescribeEndpoint", map[string]any{"EndpointName": "ep"})
			require.Equal(t, http.StatusOK, rr.Code)

			var ep struct {
				ProductionVariants []struct {
					DesiredWeight float64 `json:"DesiredWeight"`
				} `json:"ProductionVariants"`
			}
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &ep))
			require.Len(t, ep.ProductionVariants, 1)
			assert.InDelta(t, tt.want, ep.ProductionVariants[0].DesiredWeight, 0.0001)

			rr = doSageMakerRequest(t, h, "DescribeEndpointConfig", map[string]any{"EndpointConfigName": "cfg"})
			require.Equal(t, http.StatusOK, rr.Code)

			var cfg struct {
				ProductionVariants []struct {
					InitialVariantWeight float64 `json:"InitialVariantWeight"`
				} `json:"ProductionVariants"`
			}
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &cfg))
			require.Len(t, cfg.ProductionVariants, 1)
			assert.InDelta(t, tt.want, cfg.ProductionVariants[0].InitialVariantWeight, 0.0001)
		})
	}
}
