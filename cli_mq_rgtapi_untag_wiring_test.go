package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	mqbackend "github.com/blackbirdworks/gopherstack/services/mq"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// TestInitializeServices_MQUntagWiring drives the actual composition root
// (initializeServices) and the real UntagResources call, so it is sensitive
// to wireTaggingMQ's untag closure discarding mqBk.DeleteTags's error
// instead of returning it. DeleteTags returns NotFoundException for an ARN
// that names no real broker/configuration (services/mq/tags.go); a swallowed
// error makes UntagResources report success for that same nonexistent ARN.
func TestInitializeServices_MQUntagWiring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, mqBk *mqbackend.InMemoryBackend) string
		name        string
		wantFailure bool
	}{
		{
			name: "nonexistent broker arn reports failure",
			setup: func(t *testing.T, _ *mqbackend.InMemoryBackend) string {
				t.Helper()

				return arn.Build("mq", "us-east-1", "000000000000", "broker:does-not-exist")
			},
			wantFailure: true,
		},
		{
			name: "existing broker succeeds",
			setup: func(t *testing.T, mqBk *mqbackend.InMemoryBackend) string {
				t.Helper()

				br, err := mqBk.CreateBroker(
					"wiring-broker", mqbackend.DeploymentModeSingleInstance,
					mqbackend.EngineTypeActiveMQ, "", "",
					false, false, nil, nil, nil,
					map[string]string{"stage": "prod"},
				)
				require.NoError(t, err)

				return br.BrokerArn
			},
			wantFailure: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cli := &CLI{AccountID: "000000000000", Region: "us-east-1"}
			appCtx := &service.AppContext{
				Logger:     slog.Default(),
				Config:     cli,
				JanitorCtx: t.Context(),
			}
			cli.faultStore = chaos.NewFaultStore()

			services, err := initializeServices(appCtx)
			require.NoError(t, err)

			byName := serviceByName(services)

			mqH, ok := byName["MQ"].(*mqbackend.Handler)
			require.True(t, ok, "MQ handler must be registered")

			mqBk, ok := mqH.Backend.(*mqbackend.InMemoryBackend)
			require.True(t, ok, "MQ backend must be an InMemoryBackend")

			rgtH, ok := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler)
			require.True(t, ok, "ResourceGroupsTaggingAPI handler must be registered")

			resourceARN := tt.setup(t, mqBk)

			ctx := t.Context()
			out, err := rgtH.Backend.UntagResources(ctx, &rgtapibackend.UntagResourcesInput{
				ResourceARNList: []string{resourceARN},
				TagKeys:         []string{"stage"},
			})
			require.NoError(t, err)
			require.NotNil(t, out)

			if tt.wantFailure {
				assert.Contains(t, out.FailedResourcesMap, resourceARN,
					"UntagResources must surface mqBk.DeleteTags's NotFoundException instead "+
						"of reporting success for an ARN with no backing broker/configuration")
			} else {
				assert.NotContains(t, out.FailedResourcesMap, resourceARN)
			}
		})
	}
}
