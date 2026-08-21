package efs_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	efssdk "github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/efs"
)

// newTestEFSSDKClient stands up the real aws-sdk-go-v2 efs client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newTestEFSSDKClient(t *testing.T, h *efs.Handler) *efssdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return efssdk.NewFromConfig(cfg, func(o *efssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateReplicationConfiguration_DestinationRegion proves the required
// Destination.Region member (efs@v1.44.4 types/types.go:116-119, "This
// member is required.") survives a real SDK client round trip for
// same-region replication, the case CreateReplicationConfigurationInput's
// own DestinationToCreate.Region documents as optional (no Region field is
// ever required on input -- a client omits it to replicate within the
// source's own region).
//
// Before the fix, ReplicationDestination.Region (models.go) was only ever
// set from the caller-supplied Destination and was never defaulted when
// omitted; tagged `omitempty`, the required wire key vanished entirely and
// a real client's typed Region field decoded to "" (indistinguishable from
// zero-value) -- gopherstack-r80d batch 17.
func TestCreateReplicationConfiguration_DestinationRegion(t *testing.T) {
	t.Parallel()

	backend := efs.NewInMemoryBackend("123456789012", "us-east-1")
	client := newTestEFSSDKClient(t, efs.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateFileSystem(ctx, &efssdk.CreateFileSystemInput{
		CreationToken: aws.String("rc-region-src"),
	})
	require.NoError(t, err)

	rc, err := client.CreateReplicationConfiguration(ctx, &efssdk.CreateReplicationConfigurationInput{
		SourceFileSystemId: created.FileSystemId,
		Destinations: []efstypes.DestinationToCreate{
			{}, // same-region replication: no Region supplied, matching real AWS's documented default
		},
	})
	require.NoError(t, err)
	require.Len(t, rc.Destinations, 1)
	require.NotNil(t, rc.Destinations[0].Region, "Destination.Region is required by the real SDK")
	require.Equal(t, "us-east-1", *rc.Destinations[0].Region)
}
