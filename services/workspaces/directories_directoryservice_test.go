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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	directoryservicebackend "github.com/blackbirdworks/gopherstack/services/directoryservice"
	"github.com/blackbirdworks/gopherstack/services/workspaces"
)

// fakeWorkspacesSiblings structurally satisfies workspaces' unexported
// siblingServices interface (matched by SetAppConfig's type assertion),
// mirroring how the real *CLI wires GetDirectoryServiceHandler. See
// services/codedeploy/cross_service_test.go for the reference pattern.
type fakeWorkspacesSiblings struct {
	dsHandler service.Registerable
}

func (f *fakeWorkspacesSiblings) GetDirectoryServiceHandler() service.Registerable {
	return f.dsHandler
}

// newWorkspacesClientWithDirectoryService stands up a workspaces backend
// wired to dsBk via SetAppConfig, and a real aws-sdk-go-v2 workspaces client
// against an httptest server running its Handler -- proving
// RegisterWorkspaceDirectory's Directory Service lookup through the same
// wire path a real client uses.
func newWorkspacesClientWithDirectoryService(
	t *testing.T, dsBk directoryservicebackend.StorageBackend,
) *wssdk.Client {
	t.Helper()

	backend := workspaces.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
	backend.SetAppConfig(&fakeWorkspacesSiblings{dsHandler: directoryservicebackend.NewHandler(dsBk)})
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

// TestRegisterWorkspaceDirectory_PopulatesDirectoryInfo proves
// DirectoryName (and the other directory-derived fields this pass added)
// are populated on RegisterWorkspaceDirectory, instead of always being ""
// (gopherstack-bug: DirectoryName was never written anywhere).
func TestRegisterWorkspaceDirectory_PopulatesDirectoryInfo(t *testing.T) {
	t.Parallel()

	t.Run("simple ad directory resolved via directory service", func(t *testing.T) {
		t.Parallel()

		dsBk := directoryservicebackend.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
		dir, err := dsBk.CreateDirectory(
			t.Context(), "corp.example.com", "CORP", "", "Password123!",
			directoryservicebackend.DirectorySizeSmall, "", nil, nil,
		)
		require.NoError(t, err)

		client := newWorkspacesClientWithDirectoryService(t, dsBk)

		_, err = client.RegisterWorkspaceDirectory(t.Context(), &wssdk.RegisterWorkspaceDirectoryInput{
			DirectoryId: aws.String(dir.DirectoryID),
		})
		require.NoError(t, err)

		out, err := client.DescribeWorkspaceDirectories(t.Context(), &wssdk.DescribeWorkspaceDirectoriesInput{
			DirectoryIds: []string{dir.DirectoryID},
		})
		require.NoError(t, err)
		require.Len(t, out.Directories, 1)

		got := out.Directories[0]
		assert.Equal(t, "corp.example.com", aws.ToString(got.DirectoryName))
		assert.Equal(t, dir.Alias, aws.ToString(got.Alias))
		assert.Equal(t, types.WorkspaceDirectoryTypeSimpleAd, got.DirectoryType)
		assert.NotEmpty(t, got.DnsIpAddresses)
	})

	t.Run("ad connector directory carries customer user name", func(t *testing.T) {
		t.Parallel()

		dsBk := directoryservicebackend.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
		dir, err := dsBk.ConnectDirectory(
			t.Context(), "onprem.example.com", "ONPREM", "", "Password123!",
			directoryservicebackend.DirectorySizeSmall, "",
			directoryservicebackend.ConnectSettingsInput{
				CustomerUserName: "svc-workspaces",
				VpcID:            "vpc-1234",
				SubnetIDs:        []string{"subnet-1", "subnet-2"},
			},
			nil,
		)
		require.NoError(t, err)

		client := newWorkspacesClientWithDirectoryService(t, dsBk)

		_, err = client.RegisterWorkspaceDirectory(t.Context(), &wssdk.RegisterWorkspaceDirectoryInput{
			DirectoryId: aws.String(dir.DirectoryID),
		})
		require.NoError(t, err)

		out, err := client.DescribeWorkspaceDirectories(t.Context(), &wssdk.DescribeWorkspaceDirectoriesInput{
			DirectoryIds: []string{dir.DirectoryID},
		})
		require.NoError(t, err)
		require.Len(t, out.Directories, 1)

		got := out.Directories[0]
		assert.Equal(t, "onprem.example.com", aws.ToString(got.DirectoryName))
		assert.Equal(t, types.WorkspaceDirectoryTypeAdConnector, got.DirectoryType)
		assert.Equal(t, "svc-workspaces", aws.ToString(got.CustomerUserName))
	})

	t.Run("directory service not wired falls back to requested name", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		_, err := client.RegisterWorkspaceDirectory(t.Context(), &wssdk.RegisterWorkspaceDirectoryInput{
			DirectoryId:            aws.String("d-fallback"),
			WorkspaceDirectoryName: aws.String("fallback.example.com"),
		})
		require.NoError(t, err)

		out, err := client.DescribeWorkspaceDirectories(t.Context(), &wssdk.DescribeWorkspaceDirectoriesInput{
			DirectoryIds: []string{"d-fallback"},
		})
		require.NoError(t, err)
		require.Len(t, out.Directories, 1)
		assert.Equal(t, "fallback.example.com", aws.ToString(out.Directories[0].DirectoryName))
	})

	t.Run("directory service not wired and no name supplied stays empty", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		_, err := client.RegisterWorkspaceDirectory(t.Context(), &wssdk.RegisterWorkspaceDirectoryInput{
			DirectoryId: aws.String("d-noname"),
		})
		require.NoError(t, err)

		out, err := client.DescribeWorkspaceDirectories(t.Context(), &wssdk.DescribeWorkspaceDirectoriesInput{
			DirectoryIds: []string{"d-noname"},
		})
		require.NoError(t, err)
		require.Len(t, out.Directories, 1)
		assert.Empty(t, aws.ToString(out.Directories[0].DirectoryName), "must stay empty rather than fabricate a name")
	})
}

// TestDescribeWorkspaceDirectories_WorkspaceDirectoryNamesFilter proves the
// documented WorkspaceDirectoryNames request parameter is honoured, not
// silently ignored.
func TestDescribeWorkspaceDirectories_WorkspaceDirectoryNamesFilter(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	_, err := client.RegisterWorkspaceDirectory(t.Context(), &wssdk.RegisterWorkspaceDirectoryInput{
		DirectoryId:            aws.String("d-one"),
		WorkspaceDirectoryName: aws.String("corp-one.example.com"),
	})
	require.NoError(t, err)

	_, err = client.RegisterWorkspaceDirectory(t.Context(), &wssdk.RegisterWorkspaceDirectoryInput{
		DirectoryId:            aws.String("d-two"),
		WorkspaceDirectoryName: aws.String("corp-two.example.com"),
	})
	require.NoError(t, err)

	out, err := client.DescribeWorkspaceDirectories(t.Context(), &wssdk.DescribeWorkspaceDirectoriesInput{
		WorkspaceDirectoryNames: []string{"corp-two.example.com"},
	})
	require.NoError(t, err)
	require.Len(t, out.Directories, 1)
	assert.Equal(t, "d-two", aws.ToString(out.Directories[0].DirectoryId))
}

// TestDescribeWorkspaceDirectories_Limit proves the documented Limit
// request parameter caps the page size, matching the existing
// DirectoryIds/NextToken pagination behavior.
func TestDescribeWorkspaceDirectories_Limit(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	for _, id := range []string{"d-l1", "d-l2", "d-l3"} {
		_, err := client.RegisterWorkspaceDirectory(t.Context(), &wssdk.RegisterWorkspaceDirectoryInput{
			DirectoryId: aws.String(id),
		})
		require.NoError(t, err)
	}

	out, err := client.DescribeWorkspaceDirectories(t.Context(), &wssdk.DescribeWorkspaceDirectoriesInput{
		Limit: aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Len(t, out.Directories, 2)
	assert.NotNil(t, out.NextToken)
}
