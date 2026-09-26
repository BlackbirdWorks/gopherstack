package athena_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/athena"
)

// TestGetResourceDashboard_UnknownSession verifies an unknown ResourceARN
// returns ResourceNotFoundException instead of a fabricated dashboard URL.
func TestGetResourceDashboard_UnknownSession(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	h := athena.NewHandler(b)
	client := newTestAthenaClient(t, h)

	_, err := client.GetResourceDashboard(t.Context(), &athenasdk.GetResourceDashboardInput{
		ResourceARN: aws.String("does-not-exist"),
	})
	require.Error(t, err)

	var apiErr *types.ResourceNotFoundException
	require.ErrorAs(t, err, &apiErr, "expected ResourceNotFoundException, got %v", err)

	var genericErr *smithy.GenericAPIError
	assert.NotErrorAs(t, err, &genericErr, "must not fall back to an untyped error")
}

// TestGetResourceDashboard_ExistingSession verifies the happy path still
// returns a dashboard URL for a session that actually exists.
func TestGetResourceDashboard_ExistingSession(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	h := athena.NewHandler(b)
	client := newTestAthenaClient(t, h)

	require.NoError(t, b.CreateWorkGroup("wg", "", "ENABLED", athena.WorkGroupConfiguration{}, nil))
	sessionID, _, err := b.StartSession(
		"wg", "", "",
		athena.EngineConfiguration{},
		athena.SessionConfiguration{},
		athena.MonitoringConfiguration{},
		"",
	)
	require.NoError(t, err)

	out, err := client.GetResourceDashboard(t.Context(), &athenasdk.GetResourceDashboardInput{
		ResourceARN: aws.String(sessionID),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(out.Url))
}
