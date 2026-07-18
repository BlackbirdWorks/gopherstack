package apigatewayv2_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_Deployments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		desc string
	}{
		{
			name: "basic_deployment",
			desc: "initial",
		},
		{
			name: "empty_desc",
			desc: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			api, err := b.CreateAPI(
				context.Background(),
				apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"},
			)
			require.NoError(t, err)

			deployment, err := b.CreateDeployment(api.APIID, apigatewayv2.CreateDeploymentInput{Description: tt.desc})
			require.NoError(t, err)
			assert.NotEmpty(t, deployment.DeploymentID)
			assert.Equal(t, "DEPLOYED", deployment.DeploymentStatus)

			got, err := b.GetDeployment(api.APIID, deployment.DeploymentID)
			require.NoError(t, err)
			assert.Equal(t, deployment.DeploymentID, got.DeploymentID)

			deployments, err := b.GetDeployments(api.APIID)
			require.NoError(t, err)
			assert.Len(t, deployments, 1)

			err = b.DeleteDeployment(api.APIID, deployment.DeploymentID)
			require.NoError(t, err)

			_, err = b.GetDeployment(api.APIID, deployment.DeploymentID)
			require.ErrorIs(t, err, apigatewayv2.ErrDeploymentNotFound)
		})
	}
}

func TestInMemoryBackend_CreateDeployment_ApiNotFound(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	_, err := b.CreateDeployment("bad-api", apigatewayv2.CreateDeploymentInput{})
	require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
}
