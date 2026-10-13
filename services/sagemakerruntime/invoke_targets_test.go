package sagemakerruntime_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

type targetsLookup struct{ fakeEndpointLookup }

func (targetsLookup) DescribeInferenceComponent(_ context.Context, name string) (*sagemaker.InferenceComponent, error) {
	switch name {
	case "ic-ok":
		return &sagemaker.InferenceComponent{
			EndpointName: "ep", VariantName: "VariantX", InferenceComponentStatus: "InService",
		}, nil
	case "ic-other-ep":
		return &sagemaker.InferenceComponent{
			EndpointName: "other", VariantName: "AllTraffic", InferenceComponentStatus: "InService",
		}, nil
	case "ic-creating":
		return &sagemaker.InferenceComponent{
			EndpointName: "ep", VariantName: "AllTraffic", InferenceComponentStatus: "Creating",
		}, nil
	}

	return nil, sagemaker.ErrInferenceComponentNotFound
}

func (targetsLookup) DescribeEndpointConfig(context.Context, string) (*sagemaker.EndpointConfig, error) {
	return &sagemaker.EndpointConfig{ProductionVariants: []sagemaker.ProductionVariant{
		{VariantName: "AllTraffic", ModelName: "m"},
	}}, nil
}

func (targetsLookup) DescribeModel(context.Context, string) (*sagemaker.Model, error) {
	return &sagemaker.Model{Containers: []sagemaker.ContainerDefinition{{ContainerHostname: "c1"}}}, nil
}

func TestHandler_InvokeTargetValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		headers     map[string]string
		wantVariant string
		wantStatus  int
	}{
		{name: "no_targets", wantStatus: http.StatusOK, wantVariant: "AllTraffic"},
		{
			name:    "component_ok",
			headers: map[string]string{"X-Amzn-Sagemaker-Inference-Component": "ic-ok"}, wantStatus: http.StatusOK,
			wantVariant: "VariantX",
		},
		{
			name:       "component_unknown",
			headers:    map[string]string{"X-Amzn-Sagemaker-Inference-Component": "nope"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "component_other_endpoint",
			headers:    map[string]string{"X-Amzn-Sagemaker-Inference-Component": "ic-other-ep"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "component_not_in_service",
			headers:    map[string]string{"X-Amzn-Sagemaker-Inference-Component": "ic-creating"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:    "container_ok",
			headers: map[string]string{"X-Amzn-Sagemaker-Target-Container-Hostname": "c1"}, wantStatus: http.StatusOK,
			wantVariant: "AllTraffic",
		},
		{
			name:       "container_unknown",
			headers:    map[string]string{"X-Amzn-Sagemaker-Target-Container-Hostname": "c9"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		for _, suffix := range []string{"/invocations", "/invocations-response-stream"} {
			t.Run(tt.name+suffix, func(t *testing.T) {
				t.Parallel()

				h := newTestHandler(t)
				h.Backend.SetEndpointLookup(targetsLookup{fakeEndpointLookup{endpoints: map[string]*sagemaker.Endpoint{
					"ep": {EndpointName: "ep", EndpointStatus: "InService"},
				}}})

				rec := doRequestWithHeaders(
					t, h, http.MethodPost, "/endpoints/ep"+suffix, map[string]any{"data": "x"}, tt.headers,
				)
				require.Equal(t, tt.wantStatus, rec.Code)

				if tt.wantStatus == http.StatusOK && suffix == "/invocations" {
					assert.Equal(t, tt.wantVariant, rec.Header().Get("X-Amzn-Invoked-Production-Variant"))
				}
			})
		}
	}
}
