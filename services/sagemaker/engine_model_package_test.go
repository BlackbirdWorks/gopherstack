package sagemaker_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func TestEngineEndpoint_ModelPackageContainer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		spec       string
		container  sagemaker.ContainerDefinition
		wantStatus string
		wantBody   string
	}{
		{
			name: "default spec",
			spec: `{"Containers":[{"Image":"pkg-img","Environment":{"A":"1"}}]}`,
			container: sagemaker.ContainerDefinition{
				ModelPackageName: "pkg", Environment: map[string]string{"B": "2"},
			},
			wantStatus: "InService", wantBody: "pkg-img|x|",
		},
		{
			name:       "no container",
			spec:       `{"Containers":[]}`,
			container:  sagemaker.ContainerDefinition{ModelPackageName: "pkg"},
			wantStatus: "Failed",
		},
		{
			name:       "missing package",
			container:  sagemaker.ContainerDefinition{ModelPackageName: "absent"},
			wantStatus: "Failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rig := newEngineRig(t, sagemaker.EngineConfig{})
			rig.rt.serve = (&modelServer{}).handler

			if tt.spec != "" {
				_, err := rig.b.CreateModelPackage(t.Context(), sagemaker.CreateModelPackageOptions{
					Name: "pkg", InferenceSpecification: []byte(tt.spec),
				})
				require.NoError(t, err)
			}

			endpointFixture{
				variants: singleVariant("m1"),
				models:   map[string]sagemaker.ContainerDefinition{"m1": tt.container},
			}.create(t, rig)

			ep := waitEndpoint(t, rig.b, "ep")
			require.Equal(t, tt.wantStatus, ep.EndpointStatus, ep.FailureReason)

			if tt.wantStatus != "InService" {
				return
			}

			spec, ok := rig.rt.lastSpec("pkg-img")
			require.True(t, ok)
			assert.Equal(t, "1", envValue(spec, "A"))
			assert.Equal(t, "2", envValue(spec, "B"))

			res, err := rig.b.InvokeEndpoint(
				t.Context(),
				sagemaker.InvokeRequest{EndpointName: "ep", Body: []byte("x")},
			)
			require.NoError(t, err)
			assert.Contains(t, string(res.Body), tt.wantBody)
		})
	}
}
