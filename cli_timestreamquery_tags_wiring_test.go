package main

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	timestreamquerybackend "github.com/blackbirdworks/gopherstack/services/timestreamquery"
	timestreamwritebackend "github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

// TestInitializeServices_TimestreamQueryTagsWiring drives the actual
// composition root (initializeServices, the function cli.go's Run() calls)
// rather than invoking wireTimestreamQueryTags directly, so that deleting the
// wiring call from wireComputeAndObservabilityIntegrations -- not just
// breaking the helper function itself -- is what this test is sensitive to.
// This is the shape iot/appsync/sagemaker's undetected routing gaps had:
// correct code nothing reaches (gopherstack-2mwl).
func TestInitializeServices_TimestreamQueryTagsWiring(t *testing.T) {
	t.Parallel()

	cli := &CLI{}
	appCtx := &service.AppContext{
		Logger:     slog.Default(),
		Config:     cli,
		JanitorCtx: t.Context(),
	}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServices(appCtx)
	require.NoError(t, err)

	byName := serviceByName(services)

	tsqH, ok := byName["TimestreamQuery"].(*timestreamquerybackend.Handler)
	require.True(t, ok, "TimestreamQuery handler must be registered")

	tsqBk, ok := tsqH.Backend.(*timestreamquerybackend.InMemoryBackend)
	require.True(t, ok, "TimestreamQuery backend must be an InMemoryBackend")

	tswH, ok := byName["TimestreamWrite"].(*timestreamwritebackend.Handler)
	require.True(t, ok, "TimestreamWrite handler must be registered")

	sq, err := tsqBk.CreateScheduledQuery(
		t.Context(),
		"wiring-test-query", "SELECT 1", "rate(1 hour)",
		"arn:aws:iam::000000000000:role/tsq-role",
		"arn:aws:sns:us-east-1:000000000000:tsq-topic",
		"tsq-error-bucket", "", "", "",
		map[string]string{"stage": "prod"},
	)
	require.NoError(t, err)

	got := tswH.Backend.ListTagsForResource(sq.Arn)
	assert.Equal(t, map[string]string{"stage": "prod"}, got,
		"CreateScheduledQuery's Tags must be visible via TimestreamWrite's ListTagsForResource "+
			"through the actual cli.go composition root, not just the wiring helper called directly")
}
