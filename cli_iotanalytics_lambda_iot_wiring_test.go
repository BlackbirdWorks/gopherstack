package main

import (
	"encoding/json"
	"log/slog"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	iotanalyticssdk "github.com/aws/aws-sdk-go-v2/service/iotanalytics"
	iotanalyticstypes "github.com/aws/aws-sdk-go-v2/service/iotanalytics/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
	iotanalyticsbackend "github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

// TestInitializeServices_IoTAnalyticsLambdaIoTWiring drives the actual composition root
// (initializeServices, the function cli.go's Run() calls) rather than invoking
// wireIoTAnalyticsCrossService directly, so that deleting the wiring call from
// wireStorageAndSecretsIntegrations -- not just breaking the helper function itself -- is
// what this test is sensitive to. See cli_sagemaker_s3_pipeline_wiring_test.go for the same
// shape. deviceRegistryEnrich/deviceShadowEnrich are exercised end to end (pure state
// lookups, no container needed); the lambda activity is exercised via its not-found error
// path, since actually invoking a function needs a container runtime this test environment
// doesn't have -- an unwired backend would pass the unknown function through silently
// instead of failing, so the error path alone is wiring-sensitive.
func TestInitializeServices_IoTAnalyticsLambdaIoTWiring(t *testing.T) {
	t.Parallel()

	cli := &CLI{AccountID: "000000000000"}
	appCtx := &service.AppContext{
		Logger:     slog.Default(),
		Config:     cli,
		JanitorCtx: t.Context(),
	}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(appCtx)
	require.NoError(t, err)

	byName := serviceByName(services)

	iotaH, ok := byName["IoTAnalytics"].(*iotanalyticsbackend.Handler)
	require.True(t, ok, "IoTAnalytics handler must be registered")

	iotH, ok := byName["IoT"].(*iotbackend.Handler)
	require.True(t, ok, "IoT handler must be registered")

	iotBk, ok := iotH.Backend.(*iotbackend.InMemoryBackend)
	require.True(t, ok, "IoT backend must be an InMemoryBackend")

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(iotaH))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	ctx := t.Context()

	cfg, err := awscfg.LoadDefaultConfig(
		ctx,
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	client := iotanalyticssdk.NewFromConfig(cfg, func(o *iotanalyticssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	t.Run("device_registry_enrich_wired", func(t *testing.T) {
		t.Parallel()

		_, createErr := iotBk.CreateThing(&iotbackend.CreateThingInput{
			ThingName: "wiring-test-thing",
			AttributePayload: &iotbackend.AttributePayload{
				Attributes: map[string]string{"model": "sensor-9000"},
			},
		})
		require.NoError(t, createErr)

		out, runErr := client.RunPipelineActivity(ctx, &iotanalyticssdk.RunPipelineActivityInput{
			Payloads: [][]byte{[]byte(`{"temp":72}`)},
			PipelineActivity: &iotanalyticstypes.PipelineActivity{
				DeviceRegistryEnrich: &iotanalyticstypes.DeviceRegistryEnrichActivity{
					Name:      aws.String("enrich"),
					Attribute: aws.String("registry"),
					ThingName: aws.String("wiring-test-thing"),
					RoleArn:   aws.String("arn:aws:iam::000000000000:role/r"),
				},
			},
		})
		require.NoError(t, runErr,
			"RunPipelineActivity must look up the thing through the actual cli.go "+
				"composition root's IoT wiring, not just the wiring helper called directly")
		require.Len(t, out.Payloads, 1)

		var decoded map[string]any
		require.NoError(t, json.Unmarshal(out.Payloads[0], &decoded))

		registryData, isMap := decoded["registry"].(map[string]any)
		require.True(t, isMap, "registry attribute must be populated from the real IoT backend")
		require.Equal(t, "wiring-test-thing", registryData["thingName"])

		attrs, isMap := registryData["attributes"].(map[string]any)
		require.True(t, isMap)
		require.Equal(t, "sensor-9000", attrs["model"])
	})

	t.Run("device_shadow_enrich_wired", func(t *testing.T) {
		t.Parallel()

		_, createErr := iotBk.CreateThing(&iotbackend.CreateThingInput{ThingName: "wiring-test-shadow-thing"})
		require.NoError(t, createErr)

		wantState := map[string]any{"reported": map[string]any{"on": true}}
		_, shadowErr := iotBk.UpdateThingShadow("wiring-test-shadow-thing", "", wantState)
		require.NoError(t, shadowErr)

		out, runErr := client.RunPipelineActivity(ctx, &iotanalyticssdk.RunPipelineActivityInput{
			Payloads: [][]byte{[]byte(`{"temp":72}`)},
			PipelineActivity: &iotanalyticstypes.PipelineActivity{
				DeviceShadowEnrich: &iotanalyticstypes.DeviceShadowEnrichActivity{
					Name:      aws.String("enrich"),
					Attribute: aws.String("shadow"),
					ThingName: aws.String("wiring-test-shadow-thing"),
					RoleArn:   aws.String("arn:aws:iam::000000000000:role/r"),
				},
			},
		})
		require.NoError(t, runErr,
			"RunPipelineActivity must look up the shadow through the actual cli.go "+
				"composition root's IoT wiring, not just the wiring helper called directly")
		require.Len(t, out.Payloads, 1)

		var decoded map[string]any
		require.NoError(t, json.Unmarshal(out.Payloads[0], &decoded))

		shadowData, isMap := decoded["shadow"].(map[string]any)
		require.True(t, isMap, "shadow attribute must be populated from the real IoT backend")

		state, isMap := shadowData["state"].(map[string]any)
		require.True(t, isMap)

		reported, isMap := state["reported"].(map[string]any)
		require.True(t, isMap)
		require.Equal(t, true, reported["on"])
	})

	t.Run("lambda_wired", func(t *testing.T) {
		t.Parallel()

		_, runErr := client.RunPipelineActivity(ctx, &iotanalyticssdk.RunPipelineActivityInput{
			Payloads: [][]byte{[]byte(`{"temp":72}`)},
			PipelineActivity: &iotanalyticstypes.PipelineActivity{
				Lambda: &iotanalyticstypes.LambdaActivity{
					Name:       aws.String("run-lambda"),
					LambdaName: aws.String("does-not-exist"),
					BatchSize:  aws.Int32(1),
				},
			},
		})
		require.Error(t, runErr,
			"a wired backend must actually invoke Lambda and surface its not-found error, "+
				"not silently pass the payload through unchanged")
	})
}
