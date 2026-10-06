package apigatewayv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	apigatewayv2types "github.com/aws/aws-sdk-go-v2/service/apigatewayv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

func TestUpdateIntegration_TimeoutOmittedVsZero(t *testing.T) {
	t.Parallel()

	tests := []struct {
		timeout     *int32
		name        string
		wantTimeout int32
		wantErr     bool
	}{
		{name: "omitted_keeps", timeout: nil, wantTimeout: 4000},
		{name: "explicit_value_applies", timeout: aws.Int32(7000), wantTimeout: 7000},
		{name: "explicit_zero_rejected", timeout: aws.Int32(0), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(apigatewayv2.NewInMemoryBackend()))

			api, err := client.CreateApi(t.Context(), &apigatewayv2sdk.CreateApiInput{
				Name: aws.String("timeout-api"), ProtocolType: apigatewayv2types.ProtocolTypeHttp,
			})
			require.NoError(t, err)

			intg, err := client.CreateIntegration(t.Context(), &apigatewayv2sdk.CreateIntegrationInput{
				ApiId:                api.ApiId,
				IntegrationType:      apigatewayv2types.IntegrationTypeHttpProxy,
				IntegrationUri:       aws.String("https://example.com"),
				IntegrationMethod:    aws.String("GET"),
				PayloadFormatVersion: aws.String("1.0"),
				TimeoutInMillis:      aws.Int32(4000),
			})
			require.NoError(t, err)

			out, err := client.UpdateIntegration(t.Context(), &apigatewayv2sdk.UpdateIntegrationInput{
				ApiId:           api.ApiId,
				IntegrationId:   intg.IntegrationId,
				Description:     aws.String("d"),
				TimeoutInMillis: tt.timeout,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantTimeout, aws.ToInt32(out.TimeoutInMillis))
		})
	}
}
