package apigatewayv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apigatewayv2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

func TestValidationAndNotFoundMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(c *apigatewayv2sdk.Client, apiID string) error
		name     string
		wantCode string
		wantMsg  string
	}{
		{
			name: "stage_name_chars",
			call: func(c *apigatewayv2sdk.Client, apiID string) error {
				_, err := c.CreateStage(t.Context(), &apigatewayv2sdk.CreateStageInput{
					ApiId: aws.String(apiID), StageName: aws.String("bad stage!"),
				})

				return err
			},
			wantCode: "BadRequestException", wantMsg: "Stage names can contain only alphanumeric characters",
		},
		{
			name: "payload_format_version",
			call: func(c *apigatewayv2sdk.Client, apiID string) error {
				_, err := c.CreateIntegration(t.Context(), &apigatewayv2sdk.CreateIntegrationInput{
					ApiId: aws.String(apiID), IntegrationType: apigatewayv2types.IntegrationTypeAwsProxy,
					IntegrationUri:       aws.String("arn:aws:lambda:us-east-1:123456789012:function:f"),
					PayloadFormatVersion: aws.String("3.0"),
				})

				return err
			},
			wantCode: "BadRequestException", wantMsg: "payloadFormatVersion must be 1.0 or 2.0",
		},
		{
			name: "route_key_bad",
			call: func(c *apigatewayv2sdk.Client, apiID string) error {
				_, err := c.CreateRoute(t.Context(), &apigatewayv2sdk.CreateRouteInput{
					ApiId: aws.String(apiID), RouteKey: aws.String("BAD"),
				})

				return err
			},
			wantCode: "BadRequestException", wantMsg: "routeKey must be",
		},
		{
			name: "api_not_found",
			call: func(c *apigatewayv2sdk.Client, _ string) error {
				_, err := c.GetApi(t.Context(), &apigatewayv2sdk.GetApiInput{ApiId: aws.String("nope")})

				return err
			},
			wantCode: "NotFoundException", wantMsg: "Invalid API identifier specified",
		},
		{
			name: "route_not_found",
			call: func(c *apigatewayv2sdk.Client, apiID string) error {
				_, err := c.GetRoute(t.Context(), &apigatewayv2sdk.GetRouteInput{
					ApiId: aws.String(apiID), RouteId: aws.String("nope"),
				})

				return err
			},
			wantCode: "NotFoundException", wantMsg: "Invalid Route identifier specified nope",
		},
		{
			name: "duplicate_stage",
			call: func(c *apigatewayv2sdk.Client, apiID string) error {
				for range 2 {
					_, err := c.CreateStage(t.Context(), &apigatewayv2sdk.CreateStageInput{
						ApiId: aws.String(apiID), StageName: aws.String("prod"),
					})
					if err != nil {
						return err
					}
				}

				return nil
			},
			wantCode: "ConflictException", wantMsg: `stage "prod" already exists`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()))
			api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
				Name: aws.String("a"), ProtocolType: apigatewayv2types.ProtocolTypeHttp,
			})
			require.NoError(t, err)

			err = tt.call(client, aws.ToString(api.ApiId))
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.Contains(t, apiErr.ErrorMessage(), tt.wantMsg)
			assert.NotContains(t, apiErr.ErrorMessage(), "Exception:")
		})
	}
}
