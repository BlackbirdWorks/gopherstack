package sagemaker_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func waitComponent(t *testing.T, b *sagemaker.InMemoryBackend, name string) *sagemaker.InferenceComponent {
	t.Helper()

	b.WaitIdle()

	c, err := b.DescribeInferenceComponent(t.Context(), name)
	require.NoError(t, err)

	return c
}

func TestEngineInferenceComponent_Invoke(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		wantBody   string
		spec       sagemaker.InferenceComponentSpecification
	}{
		{
			name: "container", wantStatus: "InService", wantBody: "ic-img|x|",
			spec: sagemaker.InferenceComponentSpecification{
				Container: &sagemaker.InferenceComponentContainerSpec{Image: "ic-img"},
			},
		},
		{
			name: "model", wantStatus: "InService", wantBody: "model-img|x|",
			spec: sagemaker.InferenceComponentSpecification{ModelName: "m1"},
		},
		{
			name: "empty spec", wantStatus: "Failed",
			spec: sagemaker.InferenceComponentSpecification{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rig := newEngineRig(t, sagemaker.EngineConfig{})
			rig.rt.serve = (&modelServer{}).handler
			ctx := t.Context()

			_, err := rig.b.CreateModel(ctx, "m1", "arn:aws:iam::000000000000:role/r",
				&sagemaker.ContainerDefinition{Image: "model-img"}, nil, nil)
			require.NoError(t, err)

			_, err = rig.b.CreateEndpointConfig(ctx, "ep-cfg", []sagemaker.ProductionVariant{
				{
					VariantName:          "AllTraffic",
					InstanceType:         "ml.m5.large",
					InitialInstanceCount: 1,
					InitialVariantWeight: 1,
				},
			}, nil)
			require.NoError(t, err)

			_, err = rig.b.CreateEndpointFSM(
				ctx,
				sagemaker.CreateEndpointOptions{Name: "ep", EndpointConfigName: "ep-cfg"},
			)
			require.NoError(t, err)
			require.Equal(t, "InService", waitEndpoint(t, rig.b, "ep").EndpointStatus)

			_, err = rig.b.CreateInferenceComponent(ctx, sagemaker.CreateInferenceComponentOptions{
				InferenceComponentName: "ic", EndpointName: "ep", VariantName: "AllTraffic",
				Specification: &tt.spec, CopyCount: 1,
			})
			require.NoError(t, err)

			c := waitComponent(t, rig.b, "ic")
			require.Equal(t, tt.wantStatus, c.InferenceComponentStatus, c.FailureReason)

			if tt.wantStatus != "InService" {
				assert.NotEmpty(t, c.FailureReason)

				return
			}

			assert.EqualValues(t, 1, c.CurrentCopyCount)

			res, err := rig.b.InvokeEndpoint(ctx, sagemaker.InvokeRequest{
				EndpointName: "ep", InferenceComponentName: "ic", Body: []byte("x"),
			})
			require.NoError(t, err)
			assert.Contains(t, string(res.Body), tt.wantBody)
			assert.Equal(t, "AllTraffic", res.InvokedVariant)

			require.NoError(t, rig.b.DeleteInferenceComponent(ctx, "ic"))
			rig.b.WaitIdle()

			_, err = rig.b.InvokeEndpoint(ctx, sagemaker.InvokeRequest{
				EndpointName: "ep", InferenceComponentName: "ic", Body: []byte("x"),
			})
			require.Error(t, err)
		})
	}
}
