package workspaces_test

import (
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	wssdk "github.com/aws/aws-sdk-go-v2/service/workspaces"
	"github.com/aws/aws-sdk-go-v2/service/workspaces/types"
	"github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/workspaces"
)

func clientForBackend(t *testing.T, backend *workspaces.InMemoryBackend) *wssdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(workspaces.NewHandler(backend)))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return wssdk.NewFromConfig(cfg, func(o *wssdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
}

type fakeClock struct {
	now time.Time
	mu  sync.Mutex
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.now = f.now.Add(d)
}

func TestWorkspaceLifecycle_PendingThenAvailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantState types.WorkspaceState
		wantDir   types.WorkspaceDirectoryState
		delay     time.Duration
		advance   time.Duration
	}{
		{
			name: "before deadline", delay: time.Minute, advance: 30 * time.Second,
			wantState: types.WorkspaceStatePending, wantDir: types.WorkspaceDirectoryStateRegistering,
		},
		{
			name: "after deadline", delay: time.Minute, advance: time.Minute,
			wantState: types.WorkspaceStateAvailable, wantDir: types.WorkspaceDirectoryStateRegistered,
		},
		{
			name:      "no delay",
			wantState: types.WorkspaceStateAvailable, wantDir: types.WorkspaceDirectoryStateRegistered,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
			backend := workspaces.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
			backend.SetClock(clk.Now)
			backend.SetLifecycleDelay(tt.delay)
			client := clientForBackend(t, backend)
			ctx := t.Context()

			reg, err := client.RegisterWorkspaceDirectory(ctx, &wssdk.RegisterWorkspaceDirectoryInput{
				DirectoryId: aws.String("d-00000000"), WorkspaceDirectoryName: aws.String("dir"),
			})
			require.NoError(t, err)
			assert.Equal(t, types.WorkspaceDirectoryStateRegistering, reg.State)

			created, err := client.CreateWorkspaces(ctx, &wssdk.CreateWorkspacesInput{
				Workspaces: []types.WorkspaceRequest{{
					BundleId:    aws.String("wsb-00000000"),
					DirectoryId: aws.String("d-00000000"),
					UserName:    aws.String("alice"),
				}},
			})
			require.NoError(t, err)
			require.Len(t, created.PendingRequests, 1)
			assert.Equal(t, types.WorkspaceStatePending, created.PendingRequests[0].State)

			clk.Advance(tt.advance)

			desc, err := client.DescribeWorkspaces(ctx, &wssdk.DescribeWorkspacesInput{})
			require.NoError(t, err)
			require.Len(t, desc.Workspaces, 1)
			assert.Equal(t, tt.wantState, desc.Workspaces[0].State)

			dirs, err := client.DescribeWorkspaceDirectories(ctx, &wssdk.DescribeWorkspaceDirectoriesInput{})
			require.NoError(t, err)
			require.Len(t, dirs.Directories, 1)
			assert.Equal(t, tt.wantDir, dirs.Directories[0].State)
		})
	}
}

func TestWorkspaces_RequestRealism(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(t *testing.T, c *wssdk.Client) error
		name     string
		wantCode string
	}{
		{
			name: "bad describe token",
			call: func(t *testing.T, c *wssdk.Client) error {
				t.Helper()

				_, err := c.DescribeWorkspaces(t.Context(), &wssdk.DescribeWorkspacesInput{NextToken: aws.String("!!")})

				return err
			},
			wantCode: "InvalidParameterValuesException",
		},
		{
			name: "tag unknown resource",
			call: func(t *testing.T, c *wssdk.Client) error {
				t.Helper()

				_, err := c.CreateTags(t.Context(), &wssdk.CreateTagsInput{
					ResourceId: aws.String("ws-deadbeef"),
					Tags:       []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
				})

				return err
			},
			wantCode: "ResourceNotFoundException",
		},
		{
			name: "deregister unknown directory",
			call: func(t *testing.T, c *wssdk.Client) error {
				t.Helper()

				_, err := c.DeregisterWorkspaceDirectory(t.Context(), &wssdk.DeregisterWorkspaceDirectoryInput{
					DirectoryId: aws.String("d-0000000000"),
				})

				return err
			},
			wantCode: "ResourceNotFoundException",
		},
		{
			name: "bad ip rule",
			call: func(t *testing.T, c *wssdk.Client) error {
				t.Helper()

				_, err := c.CreateIpGroup(t.Context(), &wssdk.CreateIpGroupInput{
					GroupName: aws.String("g"), UserRules: []types.IpRuleItem{{IpRule: aws.String("1.2.3.4/33")}},
				})

				return err
			},
			wantCode: "InvalidParameterValuesException",
		},
		{
			name: "good ip rule",
			call: func(t *testing.T, c *wssdk.Client) error {
				t.Helper()

				_, err := c.CreateIpGroup(t.Context(), &wssdk.CreateIpGroupInput{
					GroupName: aws.String("g"), UserRules: []types.IpRuleItem{{IpRule: aws.String("10.0.0.0/16")}},
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call(t, newTestHandlerAndClient(t))
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.NotEqual(t, tt.wantCode, apiErr.ErrorMessage(), "message must be descriptive, not the bare code")
		})
	}
}
