package gcpresourcemanager_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	serviceusage "cloud.google.com/go/serviceusage/apiv1"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/gcpresourcemanager"
)

const (
	orgName = "organizations/" + gcpresourcemanager.DefaultOrganizationID
	seedID  = gcpresourcemanager.DefaultSeedProjects
)

type fakeClock struct{ ns atomic.Int64 }

func newClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())

	return c
}

func (c *fakeClock) Now() time.Time { return time.Unix(0, c.ns.Load()).UTC() }

func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

// clients are the official Go clients of one transport.
type clients struct {
	projects *resourcemanager.ProjectsClient
	folders  *resourcemanager.FoldersClient
	orgs     *resourcemanager.OrganizationsClient
	tagKeys  *resourcemanager.TagKeysClient
	tagVals  *resourcemanager.TagValuesClient
	bindings *resourcemanager.TagBindingsClient
	usage    *serviceusage.Client
}

type env struct {
	backend *gcpresourcemanager.InMemoryBackend
	clock   *fakeClock
	grpc    *clients
	rest    *clients
	addr    string
}

type envOption func(*gcpresourcemanager.Settings)

func withRetention(d time.Duration) envOption {
	return func(s *gcpresourcemanager.Settings) { s.DeleteRetention = d }
}

func newEnv(t *testing.T, opts ...envOption) *env {
	t.Helper()

	settings := gcpresourcemanager.DefaultSettings()
	for _, o := range opts {
		o(&settings)
	}

	clk := newClock()
	b := gcpresourcemanager.NewBackend(settings)
	b.SetClock(clk.Now)

	h := gcpresourcemanager.NewHandler(b)
	h.Port = 0

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	grpcOpts := []option.ClientOption{option.WithGRPCConn(conn)}
	restOpts := []option.ClientOption{
		option.WithEndpoint(
			"http://" + h.Addr(),
		),
		option.WithoutAuthentication(),
		option.WithHTTPClient(testclient.New(t)),
	}

	return &env{
		backend: b, clock: clk, addr: h.Addr(),
		grpc: newClients(t, false, grpcOpts), rest: newClients(t, true, restOpts),
	}
}

func newClients(t *testing.T, rest bool, opts []option.ClientOption) *clients {
	t.Helper()

	ctx := t.Context()
	c := &clients{}

	var err error

	if rest {
		c.projects, err = resourcemanager.NewProjectsRESTClient(ctx, opts...)
		require.NoError(t, err)
		c.folders, err = resourcemanager.NewFoldersRESTClient(ctx, opts...)
		require.NoError(t, err)
		c.orgs, err = resourcemanager.NewOrganizationsRESTClient(ctx, opts...)
		require.NoError(t, err)
		c.tagKeys, err = resourcemanager.NewTagKeysRESTClient(ctx, opts...)
		require.NoError(t, err)
		c.tagVals, err = resourcemanager.NewTagValuesRESTClient(ctx, opts...)
		require.NoError(t, err)
		c.bindings, err = resourcemanager.NewTagBindingsRESTClient(ctx, opts...)
		require.NoError(t, err)
		c.usage, err = serviceusage.NewRESTClient(ctx, opts...)
		require.NoError(t, err)
	} else {
		c.projects, err = resourcemanager.NewProjectsClient(ctx, opts...)
		require.NoError(t, err)
		c.folders, err = resourcemanager.NewFoldersClient(ctx, opts...)
		require.NoError(t, err)
		c.orgs, err = resourcemanager.NewOrganizationsClient(ctx, opts...)
		require.NoError(t, err)
		c.tagKeys, err = resourcemanager.NewTagKeysClient(ctx, opts...)
		require.NoError(t, err)
		c.tagVals, err = resourcemanager.NewTagValuesClient(ctx, opts...)
		require.NoError(t, err)
		c.bindings, err = resourcemanager.NewTagBindingsClient(ctx, opts...)
		require.NoError(t, err)
		c.usage, err = serviceusage.NewClient(ctx, opts...)
		require.NoError(t, err)
	}

	t.Cleanup(func() {
		_ = c.projects.Close()
		_ = c.folders.Close()
		_ = c.orgs.Close()
		_ = c.tagKeys.Close()
		_ = c.tagVals.Close()
		_ = c.bindings.Close()
		_ = c.usage.Close()
	})

	return c
}

func (c *clients) createProject(t *testing.T, id, parent string) *resourcemanagerpb.Project {
	t.Helper()

	op, err := c.projects.CreateProject(t.Context(), &resourcemanagerpb.CreateProjectRequest{
		Project: &resourcemanagerpb.Project{ProjectId: id, Parent: parent, DisplayName: "Project " + id},
	})
	require.NoError(t, err)

	p, err := op.Wait(t.Context())
	require.NoError(t, err)

	return p
}

func (c *clients) createFolder(t *testing.T, parent, name string) *resourcemanagerpb.Folder {
	t.Helper()

	op, err := c.folders.CreateFolder(t.Context(), &resourcemanagerpb.CreateFolderRequest{
		Folder: &resourcemanagerpb.Folder{Parent: parent, DisplayName: name},
	})
	require.NoError(t, err)

	f, err := op.Wait(t.Context())
	require.NoError(t, err)

	return f
}

// codeOf reads the canonical code of a gRPC or REST error; the REST client folds HTTP 400 and 409 into
// other codes, so the JSON status string is parsed instead.
func codeOf(err error) codes.Code {
	if err == nil {
		return codes.OK
	}

	if ge, ok := errors.AsType[*googleapi.Error](err); ok {
		var body struct {
			Error struct {
				Status string `json:"status"`
			} `json:"error"`
		}

		var c codes.Code

		if json.Unmarshal([]byte(ge.Body), &body) == nil && c.UnmarshalJSON([]byte(`"`+body.Error.Status+`"`)) == nil {
			return c
		}
	}

	return status.Code(err)
}

// eachTransport runs fn once per transport, each against its own backend.
func eachTransport(t *testing.T, fn func(t *testing.T, e *env, c *clients)) {
	t.Helper()

	for _, tp := range []string{"grpc", "rest"} {
		t.Run(tp, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			c := e.grpc

			if tp == "rest" {
				c = e.rest
			}

			fn(t, e, c)
		})
	}
}
