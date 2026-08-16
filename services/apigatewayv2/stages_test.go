package apigatewayv2_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

func TestInMemoryBackend_Stages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		stageName string
	}{
		{
			name:      "create_and_get",
			stageName: "prod",
		},
		{
			name:      "get_not_found",
			stageName: "nonexistent",
			wantErr:   apigatewayv2.ErrStageNotFound,
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

			if tt.wantErr == nil {
				stage, createErr := b.CreateStage(api.APIID, apigatewayv2.CreateStageInput{StageName: tt.stageName})
				require.NoError(t, createErr)
				assert.Equal(t, tt.stageName, stage.StageName)

				got, getErr := b.GetStage(api.APIID, tt.stageName)
				require.NoError(t, getErr)
				assert.Equal(t, tt.stageName, got.StageName)
			} else {
				_, getErr := b.GetStage(api.APIID, tt.stageName)
				require.ErrorIs(t, getErr, tt.wantErr)
			}
		})
	}
}

func TestInMemoryBackend_UpdateStage_AllFields(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
	require.NoError(t, err)

	_, err = b.CreateStage(api.APIID, apigatewayv2.CreateStageInput{StageName: "dev"})
	require.NoError(t, err)

	autoDeploy := true
	updated, err := b.UpdateStage(api.APIID, "dev", apigatewayv2.UpdateStageInput{
		DeploymentID:   "deploy-1",
		Description:    "new desc",
		AutoDeploy:     &autoDeploy,
		StageVariables: map[string]string{"key": "val"},
	})
	require.NoError(t, err)
	assert.Equal(t, "deploy-1", updated.DeploymentID)
	assert.Equal(t, "new desc", updated.Description)
	assert.True(t, updated.AutoDeploy)
}

func TestInMemoryBackend_UpdateStage_ManagedStageImmutable(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
		Name: "test", ProtocolType: "HTTP", RouteKey: "GET /foo", Target: "http://example.com",
	})
	require.NoError(t, err)

	stages, err := b.GetStages(api.APIID)
	require.NoError(t, err)
	require.Len(t, stages, 1)
	require.True(t, stages[0].APIGatewayManaged)

	_, err = b.UpdateStage(api.APIID, stages[0].StageName, apigatewayv2.UpdateStageInput{Description: "new desc"})
	require.ErrorIs(t, err, apigatewayv2.ErrBadRequest)

	got, err := b.GetStage(api.APIID, stages[0].StageName)
	require.NoError(t, err)
	assert.Empty(t, got.Description)
}

func TestInMemoryBackend_CreateStage_ApiNotFound(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	_, err := b.CreateStage("bad-api", apigatewayv2.CreateStageInput{StageName: "prod"})
	require.ErrorIs(t, err, apigatewayv2.ErrAPINotFound)
}

func TestInMemoryBackend_RouteSettings(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
	require.NoError(t, err)

	stage, err := b.CreateStage(api.APIID, apigatewayv2.CreateStageInput{StageName: "prod"})
	require.NoError(t, err)
	assert.Nil(t, stage.RouteSettings)

	// UpdateStage with RouteSettings persists them.
	routeKey := "GET /items"
	updated, err := b.UpdateStage(api.APIID, stage.StageName, apigatewayv2.UpdateStageInput{
		RouteSettings: map[string]apigatewayv2.RouteSettings{
			routeKey: {ThrottlingRateLimit: 100, ThrottlingBurstLimit: 50},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.RouteSettings)
	assert.InDelta(t, float64(100), updated.RouteSettings[routeKey].ThrottlingRateLimit, 0)

	// GetStage returns RouteSettings.
	got, err := b.GetStage(api.APIID, stage.StageName)
	require.NoError(t, err)
	require.NotNil(t, got.RouteSettings)
	assert.Equal(t, int32(50), got.RouteSettings[routeKey].ThrottlingBurstLimit)

	// DeleteRouteSettings removes the key.
	err = b.DeleteRouteSettings(api.APIID, stage.StageName, routeKey)
	require.NoError(t, err)

	got2, err := b.GetStage(api.APIID, stage.StageName)
	require.NoError(t, err)
	_, exists := got2.RouteSettings[routeKey]
	assert.False(t, exists)
}

func TestInMemoryBackend_StageAccessLogSettings(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{Name: "test", ProtocolType: "HTTP"})
	require.NoError(t, err)

	_, err = b.CreateStage(api.APIID, apigatewayv2.CreateStageInput{StageName: "prod"})
	require.NoError(t, err)

	destARN := "arn:aws:logs:us-east-1:123456789012:log-group:/aws/apigateway/prod"
	updated, err := b.UpdateStage(api.APIID, "prod", apigatewayv2.UpdateStageInput{
		AccessLogSettings: &apigatewayv2.AccessLogSettings{
			DestinationArn: destARN,
			Format:         "$context.requestId",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.AccessLogSettings)
	assert.Equal(t, destARN, updated.AccessLogSettings.DestinationArn)

	got, err := b.GetStage(api.APIID, "prod")
	require.NoError(t, err)
	require.NotNil(t, got.AccessLogSettings)
	assert.Equal(t, destARN, got.AccessLogSettings.DestinationArn)

	// DeleteAccessLogSettings clears it.
	err = b.DeleteAccessLogSettings(api.APIID, "prod")
	require.NoError(t, err)

	got2, err := b.GetStage(api.APIID, "prod")
	require.NoError(t, err)
	assert.Nil(t, got2.AccessLogSettings)
}

// Test_Stage_ClientCertificateID proves the clientCertificateId field
// (AWS-modeled for WebSocket API stages) round-trips through CreateStage,
// GetStage, and UpdateStage. Before this fix the Stage model had no
// ClientCertificateID field, so it was silently dropped.
func Test_Stage_ClientCertificateID(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name            string
		createCertID    string
		updateCertID    string
		wantAfterUpdate string
	}{
		{name: "set_on_create_no_update", createCertID: "cert-abc", wantAfterUpdate: "cert-abc"},
		{
			name: "set_on_create_then_replaced", createCertID: "cert-abc",
			updateCertID: "cert-xyz", wantAfterUpdate: "cert-xyz",
		},
		{name: "unset_stays_empty", createCertID: "", wantAfterUpdate: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			api, err := b.CreateAPI(context.Background(), apigatewayv2.CreateAPIInput{
				Name:         "api",
				ProtocolType: "WEBSOCKET",
			})
			require.NoError(t, err)

			stage, err := b.CreateStage(api.APIID, apigatewayv2.CreateStageInput{
				StageName:           "prod",
				ClientCertificateID: tc.createCertID,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.createCertID, stage.ClientCertificateID)

			if tc.updateCertID != "" {
				_, err = b.UpdateStage(api.APIID, "prod", apigatewayv2.UpdateStageInput{
					ClientCertificateID: tc.updateCertID,
				})
				require.NoError(t, err)
			}

			got, err := b.GetStage(api.APIID, "prod")
			require.NoError(t, err)
			assert.Equal(t, tc.wantAfterUpdate, got.ClientCertificateID)
		})
	}
}
