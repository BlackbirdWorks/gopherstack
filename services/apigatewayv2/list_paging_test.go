package apigatewayv2_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apigatewayv2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// TestRealClient_GetOpsPageAndRejectBadTokens covers maxResults/nextToken
// query members (serializers.go:4218, apigatewayv2@v1.37.4) on the Get* collections.
func TestRealClient_GetOpsPageAndRejectBadTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fetch func(t *testing.T, c *apigatewayv2sdk.Client, apiID string, token, size *string) (int, *string)
		name  string
	}{
		{
			name: "routes",
			fetch: func(t *testing.T, c *apigatewayv2sdk.Client, id string, tok, sz *string) (int, *string) {
				t.Helper()

				out, err := c.GetRoutes(
					t.Context(),
					&apigatewayv2sdk.GetRoutesInput{ApiId: &id, NextToken: tok, MaxResults: sz},
				)
				require.NoError(t, err)

				return len(out.Items), out.NextToken
			},
		},
		{
			name: "models",
			fetch: func(t *testing.T, c *apigatewayv2sdk.Client, id string, tok, sz *string) (int, *string) {
				t.Helper()

				out, err := c.GetModels(
					t.Context(),
					&apigatewayv2sdk.GetModelsInput{ApiId: &id, NextToken: tok, MaxResults: sz},
				)
				require.NoError(t, err)

				return len(out.Items), out.NextToken
			},
		},
		{
			name: "authorizers",
			fetch: func(t *testing.T, c *apigatewayv2sdk.Client, id string, tok, sz *string) (int, *string) {
				t.Helper()

				out, err := c.GetAuthorizers(
					t.Context(),
					&apigatewayv2sdk.GetAuthorizersInput{ApiId: &id, NextToken: tok, MaxResults: sz},
				)
				require.NoError(t, err)

				return len(out.Items), out.NextToken
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()))
			api, err := c.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
				Name: aws.String("paged"), ProtocolType: apigatewayv2types.ProtocolTypeHttp,
			})
			require.NoError(t, err)

			for i := range 3 {
				_, err = c.CreateRoute(t.Context(), &apigatewayv2sdk.CreateRouteInput{
					ApiId: api.ApiId, RouteKey: aws.String(fmt.Sprintf("GET /r%d", i)),
				})
				require.NoError(t, err)
				_, err = c.CreateModel(t.Context(), &apigatewayv2sdk.CreateModelInput{
					ApiId: api.ApiId, Name: aws.String(fmt.Sprintf("m%d", i)), Schema: aws.String("{}"),
				})
				require.NoError(t, err)
				_, err = c.CreateAuthorizer(t.Context(), &apigatewayv2sdk.CreateAuthorizerInput{
					ApiId: api.ApiId, Name: aws.String(fmt.Sprintf("a%d", i)),
					AuthorizerType: apigatewayv2types.AuthorizerTypeJwt,
					IdentitySource: []string{"$request.header.Authorization"},
					JwtConfiguration: &apigatewayv2types.JWTConfiguration{
						Audience: []string{"aud"}, Issuer: aws.String("https://issuer.example.com"),
					},
				})
				require.NoError(t, err)
			}

			var token *string

			got := 0

			for range 4 {
				n, next := tt.fetch(t, c, aws.ToString(api.ApiId), token, aws.String("2"))
				got += n

				if next == nil {
					break
				}

				token = next
			}

			require.Equal(t, 3, got)

			var apiErr smithy.APIError

			_, err = c.GetRoutes(t.Context(), &apigatewayv2sdk.GetRoutesInput{
				ApiId: api.ApiId, NextToken: aws.String("%%%"),
			})
			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, "BadRequestException", apiErr.ErrorCode())
		})
	}
}
