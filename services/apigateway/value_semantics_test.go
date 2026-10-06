package apigateway_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigwsdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutIntegration_CacheNamespaceDefaultsToResourceID(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		namespace *string
		want      string
	}{
		{name: "omitted uses resource id", namespace: nil},
		{name: "explicit kept", namespace: aws.String("shared"), want: "shared"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newRealClient(t)
			api, err := c.CreateRestApi(t.Context(), &apigwsdk.CreateRestApiInput{Name: aws.String("a")})
			require.NoError(t, err)

			res, err := c.CreateResource(t.Context(), &apigwsdk.CreateResourceInput{
				RestApiId: api.Id, ParentId: api.RootResourceId, PathPart: aws.String("p"),
			})
			require.NoError(t, err)

			_, err = c.PutMethod(t.Context(), &apigwsdk.PutMethodInput{
				RestApiId: api.Id, ResourceId: res.Id, HttpMethod: aws.String("GET"),
				AuthorizationType: aws.String("NONE"),
			})
			require.NoError(t, err)

			_, err = c.PutIntegration(t.Context(), &apigwsdk.PutIntegrationInput{
				RestApiId: api.Id, ResourceId: res.Id, HttpMethod: aws.String("GET"),
				Type: types.IntegrationTypeMock, CacheNamespace: tc.namespace,
			})
			require.NoError(t, err)

			got, err := c.GetIntegration(t.Context(), &apigwsdk.GetIntegrationInput{
				RestApiId: api.Id, ResourceId: res.Id, HttpMethod: aws.String("GET"),
			})
			require.NoError(t, err)

			want := tc.want
			if want == "" {
				want = aws.ToString(res.Id)
			}

			assert.Equal(t, want, aws.ToString(got.CacheNamespace))
		})
	}
}
