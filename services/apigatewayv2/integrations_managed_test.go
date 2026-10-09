package apigatewayv2_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

func TestDeleteIntegration_QuickCreateManaged(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantError error
		name      string
		quick     bool
	}{
		{name: "managed rejected", quick: true, wantError: apigatewayv2.ErrBadRequest},
		{name: "regular deleted"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()
			in := apigatewayv2.CreateAPIInput{Name: "api", ProtocolType: "HTTP"}

			if tt.quick {
				in.RouteKey = "GET /"
				in.Target = "https://example.com/backend"
			}

			api, err := b.CreateAPI(context.Background(), in)
			require.NoError(t, err)

			if !tt.quick {
				_, err = b.CreateIntegration(api.APIID, apigatewayv2.CreateIntegrationInput{
					IntegrationType: "HTTP_PROXY", IntegrationURI: "https://example.com", IntegrationMethod: "GET",
					PayloadFormatVersion: "1.0",
				})
				require.NoError(t, err)
			}

			ints, err := b.GetIntegrations(api.APIID)
			require.NoError(t, err)
			require.Len(t, ints, 1)

			err = b.DeleteIntegration(api.APIID, ints[0].IntegrationID)
			if tt.wantError != nil {
				require.ErrorIs(t, err, tt.wantError)

				return
			}

			require.NoError(t, err)
		})
	}
}
