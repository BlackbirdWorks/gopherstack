package appconfigdata_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	appconfigdatasdk "github.com/aws/aws-sdk-go-v2/service/appconfigdata"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/appconfigdata"
)

// newSDKClient starts an Echo server backed by the given handler and returns an
// AppConfigData SDK client pointed at it. The server is shut down via t.Cleanup.
func newSDKClient(t *testing.T, h *appconfigdata.Handler) *appconfigdatasdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		context.Background(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return appconfigdatasdk.NewFromConfig(cfg, func(o *appconfigdatasdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestSDKClient_FullSessionFlow exercises the complete AppConfigData retrieval flow via
// the real AWS SDK v2 client: StartConfigurationSession → GetLatestConfiguration (200) →
// GetLatestConfiguration (204 unchanged) → update config → GetLatestConfiguration (200).
func TestSDKClient_FullSessionFlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		content        string
		updatedContent string
		contentType    string
	}{
		{
			name:           "json_config",
			content:        `{"featureFlag":true,"limit":100}`,
			updatedContent: `{"featureFlag":false,"limit":200}`,
			contentType:    "application/json",
		},
		{
			name:           "plain_text_config",
			content:        "feature.enabled=true",
			updatedContent: "feature.enabled=false",
			contentType:    "text/plain",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.NoError(t, h.Backend.SetConfiguration("my-app", "prod", "flags", tt.content, tt.contentType))
			client := newSDKClient(t, h)
			ctx := context.Background()

			startOut, err := client.StartConfigurationSession(ctx, &appconfigdatasdk.StartConfigurationSessionInput{
				ApplicationIdentifier:          aws.String("my-app"),
				EnvironmentIdentifier:          aws.String("prod"),
				ConfigurationProfileIdentifier: aws.String("flags"),
			})
			require.NoError(t, err)
			require.NotNil(t, startOut.InitialConfigurationToken)
			assert.NotEmpty(t, *startOut.InitialConfigurationToken)

			// First poll → must return content.
			getOut1, err := client.GetLatestConfiguration(ctx, &appconfigdatasdk.GetLatestConfigurationInput{
				ConfigurationToken: startOut.InitialConfigurationToken,
			})
			require.NoError(t, err)
			assert.NotEmpty(t, string(getOut1.Configuration), "first poll must return configuration content")
			assert.Equal(t, tt.content, string(getOut1.Configuration))
			assert.Positive(t, getOut1.NextPollIntervalInSeconds)
			require.NotNil(t, getOut1.NextPollConfigurationToken)

			// Second poll (unchanged) → empty body.
			getOut2, err := client.GetLatestConfiguration(ctx, &appconfigdatasdk.GetLatestConfigurationInput{
				ConfigurationToken: getOut1.NextPollConfigurationToken,
			})
			require.NoError(t, err)
			assert.Empty(t, getOut2.Configuration, "second poll with unchanged config must return empty")
			require.NotNil(t, getOut2.NextPollConfigurationToken)

			// Update then poll → must detect change.
			require.NoError(t, h.Backend.SetConfiguration("my-app", "prod", "flags", tt.updatedContent, tt.contentType))

			getOut3, err := client.GetLatestConfiguration(ctx, &appconfigdatasdk.GetLatestConfigurationInput{
				ConfigurationToken: getOut2.NextPollConfigurationToken,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.updatedContent, string(getOut3.Configuration))
		})
	}
}

// TestSDKClient_StartSession_NoDeployment verifies that starting a session for a
// profile with no active deployment returns an error via the SDK.
func TestSDKClient_StartSession_NoDeployment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newSDKClient(t, h)

	_, err := client.StartConfigurationSession(context.Background(), &appconfigdatasdk.StartConfigurationSessionInput{
		ApplicationIdentifier:          aws.String("nonexistent-app"),
		EnvironmentIdentifier:          aws.String("prod"),
		ConfigurationProfileIdentifier: aws.String("flags"),
	})
	require.Error(t, err)
}

// TestSDKClient_StartSession_PollInterval verifies poll interval validation via the SDK.
func TestSDKClient_StartSession_PollInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		interval   int32
		wantErrNil bool
	}{
		{name: "zero_accepted", interval: 0, wantErrNil: true},
		{name: "minimum_15s_accepted", interval: 15, wantErrNil: true},
		{name: "60s_accepted", interval: 60, wantErrNil: true},
		{name: "too_low_rejected", interval: 5, wantErrNil: false},
		{name: "above_max_rejected", interval: 86401, wantErrNil: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.NoError(t, h.Backend.SetConfiguration("app", "env", "p", `{}`, "application/json"))
			client := newSDKClient(t, h)

			_, err := client.StartConfigurationSession(
				context.Background(),
				&appconfigdatasdk.StartConfigurationSessionInput{
					ApplicationIdentifier:                aws.String("app"),
					EnvironmentIdentifier:                aws.String("env"),
					ConfigurationProfileIdentifier:       aws.String("p"),
					RequiredMinimumPollIntervalInSeconds: aws.Int32(tt.interval),
				},
			)
			if tt.wantErrNil {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
