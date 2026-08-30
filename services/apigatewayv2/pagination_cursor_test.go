package apigatewayv2_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apigatewayv2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// TestGetDomainNames_Limit asserts GetDomainNamesInput.MaxResults is
// honoured, and NextToken is returned when more results remain, instead of
// GetDomainNamesOutput always returning every domain name in one page.
func TestGetDomainNames_Limit(t *testing.T) {
	t.Parallel()

	h := apigatewayv2.NewInMemoryBackend()
	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(h))

	for _, name := range []string{"a.example.com", "b.example.com", "c.example.com"} {
		_, err := client.CreateDomainName(t.Context(), &apigatewayv2sdk.CreateDomainNameInput{
			DomainName: aws.String(name),
		})
		require.NoError(t, err)
	}

	out, err := client.GetDomainNames(t.Context(), &apigatewayv2sdk.GetDomainNamesInput{
		MaxResults: aws.String("1"),
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 1)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}

// TestGetApiMappings_Limit asserts GetApiMappingsInput.MaxResults is
// honoured, and NextToken is returned when more results remain, instead of
// GetApiMappingsOutput always returning every mapping in one page.
func TestGetApiMappings_Limit(t *testing.T) {
	t.Parallel()

	h := apigatewayv2.NewInMemoryBackend()
	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(h))

	_, err := client.CreateDomainName(t.Context(), &apigatewayv2sdk.CreateDomainNameInput{
		DomainName: aws.String("mapped.example.com"),
	})
	require.NoError(t, err)

	for i := range 3 {
		api, apiErr := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
			Name:         aws.String(fmt.Sprintf("api-%d", i)),
			ProtocolType: apigatewayv2types.ProtocolTypeHttp,
		})
		require.NoError(t, apiErr)

		_, err = client.CreateStage(t.Context(), &apigatewayv2sdk.CreateStageInput{
			ApiId:     api.ApiId,
			StageName: aws.String("$default"),
		})
		require.NoError(t, err)

		_, err = client.CreateApiMapping(t.Context(), &apigatewayv2sdk.CreateApiMappingInput{
			ApiId:         api.ApiId,
			DomainName:    aws.String("mapped.example.com"),
			Stage:         aws.String("$default"),
			ApiMappingKey: aws.String(fmt.Sprintf("k%d", i)),
		})
		require.NoError(t, err)
	}

	out, err := client.GetApiMappings(t.Context(), &apigatewayv2sdk.GetApiMappingsInput{
		DomainName: aws.String("mapped.example.com"),
		MaxResults: aws.String("1"),
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 1)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}

// TestGetIntegrationResponses_Limit asserts GetIntegrationResponsesInput.
// MaxResults is honoured, and NextToken is returned when more results
// remain, instead of GetIntegrationResponsesOutput always returning every
// response in one page.
func TestGetIntegrationResponses_Limit(t *testing.T) {
	t.Parallel()

	h := apigatewayv2.NewInMemoryBackend()
	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(h))

	api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name:         aws.String("api"),
		ProtocolType: apigatewayv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)

	integ, err := client.CreateIntegration(t.Context(), &apigatewayv2sdk.CreateIntegrationInput{
		ApiId:           api.ApiId,
		IntegrationType: apigatewayv2types.IntegrationTypeHttpProxy,
	})
	require.NoError(t, err)

	for _, key := range []string{"/200/", "/400/", "/500/"} {
		_, err = client.CreateIntegrationResponse(t.Context(), &apigatewayv2sdk.CreateIntegrationResponseInput{
			ApiId:                  api.ApiId,
			IntegrationId:          integ.IntegrationId,
			IntegrationResponseKey: aws.String(key),
		})
		require.NoError(t, err)
	}

	out, err := client.GetIntegrationResponses(t.Context(), &apigatewayv2sdk.GetIntegrationResponsesInput{
		ApiId:         api.ApiId,
		IntegrationId: integ.IntegrationId,
		MaxResults:    aws.String("1"),
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 1)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}

// TestGetRouteResponses_Limit asserts GetRouteResponsesInput.MaxResults is
// honoured, and NextToken is returned when more results remain, instead of
// GetRouteResponsesOutput always returning every response in one page.
func TestGetRouteResponses_Limit(t *testing.T) {
	t.Parallel()

	h := apigatewayv2.NewInMemoryBackend()
	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(h))

	api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
		Name:         aws.String("api"),
		ProtocolType: apigatewayv2types.ProtocolTypeHttp,
	})
	require.NoError(t, err)

	route, err := client.CreateRoute(t.Context(), &apigatewayv2sdk.CreateRouteInput{
		ApiId:    api.ApiId,
		RouteKey: aws.String("GET /test"),
	})
	require.NoError(t, err)

	for _, key := range []string{"$default", "200", "400"} {
		_, err = client.CreateRouteResponse(t.Context(), &apigatewayv2sdk.CreateRouteResponseInput{
			ApiId:            api.ApiId,
			RouteId:          route.RouteId,
			RouteResponseKey: aws.String(key),
		})
		require.NoError(t, err)
	}

	out, err := client.GetRouteResponses(t.Context(), &apigatewayv2sdk.GetRouteResponsesInput{
		ApiId:      api.ApiId,
		RouteId:    route.RouteId,
		MaxResults: aws.String("1"),
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 1)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}

// TestGetVpcLinks_Limit asserts GetVpcLinksInput.MaxResults is honoured, and
// NextToken is returned when more results remain, instead of
// GetVpcLinksOutput always returning every VPC link in one page.
func TestGetVpcLinks_Limit(t *testing.T) {
	t.Parallel()

	h := apigatewayv2.NewInMemoryBackend()
	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(h))

	for _, name := range []string{"link-a", "link-b", "link-c"} {
		_, err := client.CreateVpcLink(t.Context(), &apigatewayv2sdk.CreateVpcLinkInput{
			Name:      aws.String(name),
			SubnetIds: []string{"subnet-1234"},
		})
		require.NoError(t, err)
	}

	out, err := client.GetVpcLinks(t.Context(), &apigatewayv2sdk.GetVpcLinksInput{MaxResults: aws.String("1")})
	require.NoError(t, err)
	require.Len(t, out.Items, 1)
	require.NotEmpty(t, aws.ToString(out.NextToken))
}
