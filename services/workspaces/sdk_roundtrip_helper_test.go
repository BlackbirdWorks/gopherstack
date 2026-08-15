package workspaces_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	wssdk "github.com/aws/aws-sdk-go-v2/service/workspaces"
	"github.com/aws/aws-sdk-go-v2/service/workspaces/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/workspaces"
)

const (
	rtTestRegion    = "us-east-1"
	rtTestAccountID = "000000000000"
)

// newTestHandlerAndClient stands up a fresh in-memory workspaces backend and
// a real aws-sdk-go-v2 client against an httptest server running its
// Handler, wired through the same pkgs/service registry/router used in
// production. Round-tripping through the genuine SDK serializer/deserializer
// is what proves wire-compatibility that a direct handler call would miss.
func newTestHandlerAndClient(t *testing.T) *wssdk.Client {
	t.Helper()

	backend := workspaces.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
	h := workspaces.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return wssdk.NewFromConfig(cfg, func(o *wssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// createSDKWorkspace registers a directory and creates a real workspace
// through the given client, returning its WorkspaceId.
func createSDKWorkspace(t *testing.T, client *wssdk.Client) string {
	t.Helper()

	_, err := client.RegisterWorkspaceDirectory(t.Context(), &wssdk.RegisterWorkspaceDirectoryInput{
		DirectoryId:            aws.String("d-00000000"),
		WorkspaceDirectoryName: aws.String("dir"),
	})
	require.NoError(t, err)

	out, err := client.CreateWorkspaces(t.Context(), &wssdk.CreateWorkspacesInput{
		Workspaces: []types.WorkspaceRequest{
			{
				BundleId:    aws.String("wsb-00000000"),
				DirectoryId: aws.String("d-00000000"),
				UserName:    aws.String("alice"),
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.PendingRequests, 1)

	return aws.ToString(out.PendingRequests[0].WorkspaceId)
}
