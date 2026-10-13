package main

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	amplifybackend "github.com/blackbirdworks/gopherstack/services/amplify"
	apigwv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	databrewbackend "github.com/blackbirdworks/gopherstack/services/databrew"
	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
	iotanalyticsbackend "github.com/blackbirdworks/gopherstack/services/iotanalytics"
	kafkabackend "github.com/blackbirdworks/gopherstack/services/kafka"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
	textractbackend "github.com/blackbirdworks/gopherstack/services/textract"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func TestInitializeServices_RGTAPIExtraServicesWiring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(ctx context.Context, t *testing.T, byName map[string]service.Registerable) string
		name   string
	}{
		{
			name: "amplify",
			create: func(_ context.Context, t *testing.T, byName map[string]service.Registerable) string {
				t.Helper()
				h, ok := byName["Amplify"].(*amplifybackend.Handler)
				require.True(t, ok)
				app, err := h.Backend.CreateApp("wire-app", "", "", "WEB", nil)
				require.NoError(t, err)

				return app.ARN
			},
		},
		{
			name: "apigatewayv2",
			create: func(ctx context.Context, t *testing.T, byName map[string]service.Registerable) string {
				t.Helper()
				h, ok := byName["APIGatewayV2"].(*apigwv2backend.Handler)
				require.True(t, ok)
				api, err := h.Backend.CreateAPI(ctx, apigwv2backend.CreateAPIInput{
					Name: "wire-api", ProtocolType: "HTTP",
				})
				require.NoError(t, err)

				return "arn:aws:apigateway:us-east-1::/apis/" + api.APIID
			},
		},
		{
			name: "databrew",
			create: func(ctx context.Context, t *testing.T, byName map[string]service.Registerable) string {
				t.Helper()
				h, ok := byName["DataBrew"].(*databrewbackend.Handler)
				require.True(t, ok)
				ds, err := h.Backend.CreateDataset(ctx, "wire-ds", "CSV", databrewbackend.DatasetInput{
					S3InputDefinition: &databrewbackend.S3Location{Bucket: "b", Key: "k.csv"},
				}, databrewbackend.DatasetFormatOptions{}, nil, nil)
				require.NoError(t, err)

				return ds.Arn
			},
		},
		{
			name: "iot",
			create: func(_ context.Context, t *testing.T, byName map[string]service.Registerable) string {
				t.Helper()
				h, ok := byName["IoT"].(*iotbackend.Handler)
				require.True(t, ok)
				out, err := h.Backend.CreateThing(&iotbackend.CreateThingInput{ThingName: "wire-thing"})
				require.NoError(t, err)

				return out.ThingARN
			},
		},
		{
			name: "iotanalytics",
			create: func(ctx context.Context, t *testing.T, byName map[string]service.Registerable) string {
				t.Helper()
				h, ok := byName["IoTAnalytics"].(*iotanalyticsbackend.Handler)
				require.True(t, ok)
				ch, err := h.Backend.CreateChannel(ctx, "wire_channel", nil, nil, nil)
				require.NoError(t, err)

				return ch.ARN
			},
		},
		{
			name: "kafka",
			create: func(ctx context.Context, t *testing.T, byName map[string]service.Registerable) string {
				t.Helper()
				h, ok := byName["Kafka"].(*kafkabackend.Handler)
				require.True(t, ok)
				cl, err := h.Backend.CreateCluster(ctx, "wire-cluster", "3.6.0", 2,
					kafkabackend.BrokerNodeGroupInfo{
						InstanceType:  "kafka.m5.large",
						ClientSubnets: []string{"subnet-1", "subnet-2"},
					}, nil, nil)
				require.NoError(t, err)

				return cl.ClusterArn
			},
		},
		{
			name: "textract",
			create: func(ctx context.Context, t *testing.T, byName map[string]service.Registerable) string {
				t.Helper()
				h, ok := byName["Textract"].(*textractbackend.Handler)
				require.True(t, ok)
				ad, err := h.Backend.CreateAdapter(ctx, "wire-adapter", "", "ENABLED", []string{"QUERIES"}, nil)
				require.NoError(t, err)

				return h.Backend.(*textractbackend.InMemoryBackend).BuildAdapterARN(ad.AdapterID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cli := &CLI{AccountID: "000000000000", Region: "us-east-1"}
			appCtx := &service.AppContext{Logger: slog.Default(), Config: cli, JanitorCtx: t.Context()}
			cli.faultStore = chaos.NewFaultStore()

			services, err := initializeServices(appCtx)
			require.NoError(t, err)

			byName := serviceByName(services)
			rgtH, ok := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler)
			require.True(t, ok)

			ctx := t.Context()
			resourceARN := tt.create(ctx, t, byName)
			require.NotEmpty(t, resourceARN)

			tagOut, err := rgtH.Backend.TagResources(ctx, &rgtapibackend.TagResourcesInput{
				ResourceARNList: []string{resourceARN},
				Tags:            map[string]string{"Team": "emu"},
			})
			require.NoError(t, err)
			require.Empty(t, tagOut.FailedResourcesMap)

			found := func() bool {
				out, gerr := rgtH.Backend.GetResources(ctx, &rgtapibackend.GetResourcesInput{
					TagFilters: []rgtapibackend.TagFilter{{Key: "Team", Values: []string{"emu"}}},
				})
				require.NoError(t, gerr)

				for _, m := range out.ResourceTagMappingList {
					if m.ResourceARN == resourceARN {
						return true
					}
				}

				return false
			}

			assert.True(t, found(), "GetResources must list the tagged resource")

			untagOut, err := rgtH.Backend.UntagResources(ctx, &rgtapibackend.UntagResourcesInput{
				ResourceARNList: []string{resourceARN},
				TagKeys:         []string{"Team"},
			})
			require.NoError(t, err)
			require.Empty(t, untagOut.FailedResourcesMap)
			assert.False(t, found(), "UntagResources must remove the tag")
		})
	}
}
