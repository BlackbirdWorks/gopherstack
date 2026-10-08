package apigateway_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigwsdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetResource_EmbedMethods(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		embed      []string
		wantMethod bool
	}{
		{name: "no_embed"},
		{name: "embed_methods", embed: []string{"methods"}, wantMethod: true},
		{name: "unrelated_embed", embed: []string{"other"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, apiID, rootID := setupSDKMethod(t, nil)

			one, err := client.GetResource(t.Context(), &apigwsdk.GetResourceInput{
				RestApiId: aws.String(apiID), ResourceId: aws.String(rootID), Embed: tt.embed,
			})
			require.NoError(t, err)
			require.Contains(t, one.ResourceMethods, "GET")
			assert.Equal(t, tt.wantMethod, one.ResourceMethods["GET"].HttpMethod != nil)

			list, err := client.GetResources(t.Context(), &apigwsdk.GetResourcesInput{
				RestApiId: aws.String(apiID), Embed: tt.embed,
			})
			require.NoError(t, err)
			require.NotEmpty(t, list.Items)
			assert.Equal(t, tt.wantMethod, list.Items[0].ResourceMethods["GET"].HttpMethod != nil)
		})
	}
}

func TestGetDeployment_EmbedAPISummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		embed       []string
		wantSummary bool
	}{
		{name: "no_embed"},
		{name: "embed_apisummary", embed: []string{"apisummary"}, wantSummary: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, apiID, _ := setupSDKMethod(t, nil)

			dep, err := client.CreateDeployment(
				t.Context(),
				&apigwsdk.CreateDeploymentInput{RestApiId: aws.String(apiID)},
			)
			require.NoError(t, err)

			got, err := client.GetDeployment(t.Context(), &apigwsdk.GetDeploymentInput{
				RestApiId: aws.String(apiID), DeploymentId: dep.Id, Embed: tt.embed,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantSummary, len(got.ApiSummary) > 0)

			list, err := client.GetDeployments(t.Context(), &apigwsdk.GetDeploymentsInput{RestApiId: aws.String(apiID)})
			require.NoError(t, err)
			require.NotEmpty(t, list.Items)
			assert.Empty(t, list.Items[0].ApiSummary)
		})
	}
}
