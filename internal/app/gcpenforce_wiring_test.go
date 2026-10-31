package app

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpenforce"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	gcpcomputebackend "github.com/blackbirdworks/gopherstack/services/gcpcompute"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcprmbackend "github.com/blackbirdworks/gopherstack/services/gcpresourcemanager"
	gcpsecretmanagerbackend "github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
)

const (
	enfProject   = "enforce-proj"
	enfPubSub    = "pubsub.googleapis.com"
	enfSecrets   = "secretmanager.googleapis.com"
	enfCompute   = "compute.googleapis.com"
	enfUnknownID = "no-such-project"
)

type enforceEnv struct {
	rm      *gcprmbackend.Handler
	pubsub  *gcppubsubbackend.Handler
	secrets *gcpsecretmanagerbackend.Handler
	compute *gcpcomputebackend.Handler
}

func startEnforceEnv(t *testing.T, enf gcpenforce.Settings) *enforceEnv {
	t.Helper()

	cli := &CLI{
		AccountID:  "000000000000",
		Region:     "us-east-1",
		GCPEnforce: enf,
		GCPResourceManager: gcprmbackend.Settings{
			Domain: "x.test", DeleteRetention: gcprmbackend.DefaultDeleteRetention,
		},
	}
	portAlloc, err := portalloc.New(19700, 19800)
	require.NoError(t, err)

	services, err := initializeServicesT(t, &service.AppContext{
		Logger: slog.Default(), Config: cli, JanitorCtx: t.Context(), PortAlloc: portAlloc,
	})
	require.NoError(t, err)

	byName := serviceByName(services)
	ctx := gcpenforce.WithGateFrom(t.Context(), services)

	env := &enforceEnv{}
	env.rm, _ = byName["GCPResourceManager"].(*gcprmbackend.Handler)
	env.pubsub, _ = byName["GCPPubSub"].(*gcppubsubbackend.Handler)
	env.secrets, _ = byName["GCPSecretManager"].(*gcpsecretmanagerbackend.Handler)
	env.compute, _ = byName["GCPCompute"].(*gcpcomputebackend.Handler)

	for _, h := range []interface {
		StartWorker(context.Context) error
		Shutdown(context.Context)
	}{env.pubsub, env.secrets, env.compute} {
		require.NoError(t, h.StartWorker(ctx))
		t.Cleanup(func() { h.Shutdown(context.Background()) })
	}

	return env
}

func (e *enforceEnv) createProject(t *testing.T, id string) {
	t.Helper()

	_, err := e.rm.Backend.CreateProject(&resourcemanagerpb.CreateProjectRequest{
		Project: &resourcemanagerpb.Project{ProjectId: id, DisplayName: id + " name"},
	})
	require.NoError(t, err)
}

func (e *enforceEnv) toggle(t *testing.T, project, svc string, enable bool) {
	t.Helper()

	name := "projects/" + project + "/services/" + svc

	var err error
	if enable {
		_, err = e.rm.Backend.EnableService(&serviceusagepb.EnableServiceRequest{Name: name})
	} else {
		_, err = e.rm.Backend.DisableService(&serviceusagepb.DisableServiceRequest{Name: name})
	}

	require.NoError(t, err)
}

func (e *enforceEnv) publisher(t *testing.T) pubsubpb.PublisherClient {
	t.Helper()

	conn, err := grpc.NewClient(e.pubsub.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return pubsubpb.NewPublisherClient(conn)
}

func errorInfoOf(t *testing.T, err error) *errdetails.ErrorInfo {
	t.Helper()

	for _, d := range status.Convert(err).Details() {
		if ei, ok := d.(*errdetails.ErrorInfo); ok {
			return ei
		}
	}

	require.Fail(t, "no ErrorInfo in status details", "%v", err)

	return nil
}

func TestGCPEnforcementGRPC(t *testing.T) {
	t.Parallel()

	env := startEnforceEnv(t, gcpenforce.Settings{Projects: true, ServiceEnablement: true})
	pub := env.publisher(t)
	topic := &pubsubpb.Topic{Name: "projects/" + enfProject + "/topics/topic-1"}

	_, err := pub.CreateTopic(t.Context(), &pubsubpb.Topic{Name: "projects/" + enfUnknownID + "/topics/topic-1"})
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
	ei := errorInfoOf(t, err)
	assert.Equal(t, "CONSUMER_INVALID", ei.GetReason())
	assert.Equal(t, "googleapis.com", ei.GetDomain())
	assert.Equal(t, enfPubSub, ei.GetMetadata()["service"])
	assert.Equal(t, "projects/"+enfUnknownID, ei.GetMetadata()["consumer"])

	env.createProject(t, enfProject)

	_, err = pub.CreateTopic(t.Context(), topic)
	require.Error(t, err, "project exists but pubsub is not enabled")
	assert.Equal(t, "SERVICE_DISABLED", errorInfoOf(t, err).GetReason())

	env.toggle(t, enfProject, enfPubSub, true)

	_, err = pub.CreateTopic(t.Context(), topic)
	require.NoError(t, err)

	env.toggle(t, enfProject, enfPubSub, false)

	_, err = pub.GetTopic(t.Context(), &pubsubpb.GetTopicRequest{Topic: topic.GetName()})
	require.Error(t, err)

	ei = errorInfoOf(t, err)
	assert.Equal(t, "SERVICE_DISABLED", ei.GetReason())
	assert.Equal(t, enfPubSub, ei.GetMetadata()["service"])
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

func TestGCPEnforcementProjectsOnlyIgnoresEnablement(t *testing.T) {
	t.Parallel()

	env := startEnforceEnv(t, gcpenforce.Settings{Projects: true})
	pub := env.publisher(t)

	_, err := pub.CreateTopic(t.Context(), &pubsubpb.Topic{Name: "projects/" + enfUnknownID + "/topics/topic-1"})
	require.Error(t, err)

	env.createProject(t, enfProject)

	_, err = pub.CreateTopic(t.Context(), &pubsubpb.Topic{Name: "projects/" + enfProject + "/topics/topic-1"})
	require.NoError(t, err, "pubsub is not enabled but only the project check is on")

	_, err = env.rm.Backend.DeleteProject(&resourcemanagerpb.DeleteProjectRequest{Name: "projects/" + enfProject})
	require.NoError(t, err)

	_, err = pub.GetTopic(t.Context(), &pubsubpb.GetTopicRequest{Topic: "projects/" + enfProject + "/topics/topic-1"})
	require.Error(t, err, "DELETE_REQUESTED projects are rejected")
	assert.Equal(t, "CONSUMER_INVALID", errorInfoOf(t, err).GetReason())
}

func TestGCPEnforcementOffKeepsAcceptingAnyProject(t *testing.T) {
	t.Parallel()

	env := startEnforceEnv(t, gcpenforce.Settings{})
	pub := env.publisher(t)

	_, err := pub.CreateTopic(t.Context(), &pubsubpb.Topic{Name: "projects/" + enfUnknownID + "/topics/topic-1"})
	require.NoError(t, err)
	assert.Nil(t, env.rm.EnforcementGate())
}

type restEnvelope struct {
	Error struct {
		Status  string `json:"status"`
		Message string `json:"message"`
		Details []struct {
			Metadata map[string]string `json:"metadata"`
			Type     string            `json:"@type"`
			Reason   string            `json:"reason"`
			Domain   string            `json:"domain"`
		} `json:"details"`
		Code int `json:"code"`
	} `json:"error"`
}

func getREST(t *testing.T, url string) (int, restEnvelope) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var env restEnvelope

	_ = json.Unmarshal(body, &env)

	return resp.StatusCode, env
}

func TestGCPEnforcementREST(t *testing.T) {
	t.Parallel()

	env := startEnforceEnv(t, gcpenforce.Settings{ServiceEnablement: true})
	env.createProject(t, enfProject)

	tests := []struct {
		name    string
		base    string
		path    string
		service string
	}{
		{
			"gcprest transcoded secretmanager",
			"http://" + env.secrets.Addr(), "/v1/projects/" + enfProject + "/secrets", enfSecrets,
		},
		{
			"http handler compute",
			"http://" + env.compute.Addr(), "/compute/v1/projects/" + enfProject + "/zones", enfCompute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, body := getREST(t, tt.base+tt.path)
			assert.Equal(t, http.StatusForbidden, code)
			assert.Equal(t, "PERMISSION_DENIED", body.Error.Status)
			assert.Equal(t, http.StatusForbidden, body.Error.Code)
			require.NotEmpty(t, body.Error.Details)

			ei := body.Error.Details[0]
			assert.Equal(t, "type.googleapis.com/google.rpc.ErrorInfo", ei.Type)
			assert.Equal(t, "SERVICE_DISABLED", ei.Reason)
			assert.Equal(t, "googleapis.com", ei.Domain)
			assert.Equal(t, tt.service, ei.Metadata["service"])

			unknown, ub := getREST(t, tt.base+replaceProject(tt.path, enfUnknownID))
			assert.Equal(t, http.StatusForbidden, unknown)
			require.NotEmpty(t, ub.Error.Details)
			assert.Equal(t, "CONSUMER_INVALID", ub.Error.Details[0].Reason)

			env.toggle(t, enfProject, tt.service, true)

			code, _ = getREST(t, tt.base+tt.path)
			assert.NotEqual(t, http.StatusForbidden, code)
		})
	}
}

func replaceProject(path, project string) string {
	return strings.Replace(path, "/projects/"+enfProject+"/", "/projects/"+project+"/", 1)
}

func TestCLIValidateRejectsEnforcementWithoutResourceManager(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cli     CLI
		wantErr bool
	}{
		{"off and filtered", CLI{
			ServiceFilter: ServiceFilterSettings{DisableServices: []string{"GCPResourceManager"}},
		}, false},
		{"projects on, rm filtered out", CLI{
			GCPEnforce:    gcpenforce.Settings{Projects: true},
			ServiceFilter: ServiceFilterSettings{DisableServices: []string{"GCPResourceManager"}},
		}, true},
		{"enablement on, rm not in allow list", CLI{
			GCPEnforce:    gcpenforce.Settings{ServiceEnablement: true},
			ServiceFilter: ServiceFilterSettings{Services: []string{"S3"}},
		}, true},
		{"enablement on, rm allowed", CLI{
			GCPEnforce:    gcpenforce.Settings{ServiceEnablement: true},
			ServiceFilter: ServiceFilterSettings{Services: []string{"S3", "GCPResourceManager"}},
		}, false},
		{"on and no filter", CLI{GCPEnforce: gcpenforce.Settings{Projects: true}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cli.Validate()
			if tt.wantErr {
				require.ErrorIs(t, err, errEnforceNeedsResourceManager)

				return
			}

			require.NoError(t, err)
		})
	}
}
