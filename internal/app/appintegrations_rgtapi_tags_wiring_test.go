package app

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	appintegrationsbackend "github.com/blackbirdworks/gopherstack/services/appintegrations"
)

func TestInitializeServices_AppIntegrationsTagsWiring(t *testing.T) {
	t.Parallel()

	byName, rgtH := newRGTWiringEnv(t)

	h, ok := byName["AppIntegrations"].(*appintegrationsbackend.Handler)
	require.True(t, ok)

	ctx := t.Context()
	tags := map[string]string{"Team": "emu"}

	ev, err := h.Backend.CreateEventIntegration(ctx, &appintegrationsbackend.CreateEventIntegrationInput{
		Name: "ev", EventBridgeBus: "default", Tags: tags,
		EventFilter: &appintegrationsbackend.EventFilter{Source: "aws.partner/x"},
	})
	require.NoError(t, err)

	di, err := h.Backend.CreateDataIntegration(ctx, &appintegrationsbackend.CreateDataIntegrationInput{
		Name: "di", KmsKey: "k", Tags: tags,
	})
	require.NoError(t, err)

	app, err := h.Backend.CreateApplication(ctx, &appintegrationsbackend.CreateApplicationInput{
		Name: "app", Namespace: "ns", Tags: tags,
		ApplicationSourceConfig: &appintegrationsbackend.ApplicationSourceConfig{
			ExternalURLConfig: &appintegrationsbackend.ExternalURLConfig{AccessURL: "https://example.com"},
		},
	})
	require.NoError(t, err)

	list := func(arn string) func(context.Context) (map[string]string, error) {
		return func(context.Context) (map[string]string, error) { return h.Backend.ListTagsForResource(arn) }
	}

	runRGTWiringCases(t, rgtH, []rgtWiringCase{
		{
			name: "event integration", arn: ev.EventIntegrationArn,
			resourceType: "app-integrations:event-integration",
			listTags:     list(ev.EventIntegrationArn),
		},
		{
			name: "data integration", arn: di.Arn,
			resourceType: "app-integrations:data-integration",
			listTags:     list(di.Arn),
		},
		{
			name: "application", arn: app.Arn,
			resourceType: "app-integrations:application",
			listTags:     list(app.Arn),
		},
	})
}
