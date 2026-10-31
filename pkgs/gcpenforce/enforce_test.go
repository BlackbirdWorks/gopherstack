package gcpenforce_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpenforce"
)

type fakeEnforcer struct {
	projectErr, serviceErr error
	calls                  []string
}

func (f *fakeEnforcer) CheckProject(project, service string) error {
	f.calls = append(f.calls, "project:"+project+":"+service)

	return f.projectErr
}

func (f *fakeEnforcer) CheckServiceEnabled(project, service string) error {
	f.calls = append(f.calls, "service:"+project+":"+service)

	return f.serviceErr
}

func TestGateCheck(t *testing.T) {
	t.Parallel()

	errDeny := status.Error(codes.PermissionDenied, "no")

	tests := []struct {
		name    string
		gate    func(f *fakeEnforcer) *gcpenforce.Gate
		project string
		calls   []string
		wantErr bool
	}{
		{"nil gate", func(*fakeEnforcer) *gcpenforce.Gate { return nil }, "p", nil, false},
		{"all off", func(f *fakeEnforcer) *gcpenforce.Gate { return &gcpenforce.Gate{Enforcer: f} }, "p", nil, false},
		{"no project named", func(f *fakeEnforcer) *gcpenforce.Gate {
			return &gcpenforce.Gate{Enforcer: f, Settings: gcpenforce.Settings{Projects: true}}
		}, "", nil, false},
		{"projects only", func(f *fakeEnforcer) *gcpenforce.Gate {
			f.projectErr = errDeny

			return &gcpenforce.Gate{Enforcer: f, Settings: gcpenforce.Settings{Projects: true}}
		}, "p", []string{"project:p:s.googleapis.com"}, true},
		{"service enablement implies project check", func(f *fakeEnforcer) *gcpenforce.Gate {
			f.serviceErr = errDeny

			return &gcpenforce.Gate{Enforcer: f, Settings: gcpenforce.Settings{Projects: true, ServiceEnablement: true}}
		}, "p", []string{"service:p:s.googleapis.com"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := &fakeEnforcer{}
			err := tt.gate(f).Check(tt.project, "s.googleapis.com")

			assert.Equal(t, tt.wantErr, err != nil)
			assert.Equal(t, tt.calls, f.calls)
		})
	}
}

func TestNoGateInContextAddsNothing(t *testing.T) {
	t.Parallel()

	assert.Nil(t, gcpenforce.ServerOptions(t.Context(), gcpenforce.Target{Host: "x"}))
	assert.Nil(t, gcpenforce.GateFrom(t.Context()))
	assert.Equal(t, t.Context(), gcpenforce.WithGate(t.Context(), nil))

	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	assert.NotNil(t, gcpenforce.Middleware(t.Context(), gcpenforce.Target{Host: "x"}, next))
}

type publisher struct {
	pubsubpb.UnimplementedPublisherServer
}

func (publisher) CreateTopic(_ context.Context, in *pubsubpb.Topic) (*pubsubpb.Topic, error) {
	return in, nil
}

func (publisher) DeleteTopic(context.Context, *pubsubpb.DeleteTopicRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func TestInterceptorAnswersWithTheEnforcerError(t *testing.T) {
	t.Parallel()

	errDenied := status.Error(codes.PermissionDenied, "denied")
	f := &fakeEnforcer{serviceErr: errDenied}
	ctx := gcpenforce.WithGate(t.Context(),
		&gcpenforce.Gate{Enforcer: f, Settings: gcpenforce.Settings{ServiceEnablement: true}})

	srv := gcpenforce.NewServer(ctx, "pubsub.googleapis.com")
	pubsubpb.RegisterPublisherServer(srv, publisher{})

	var lc net.ListenConfig

	l, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = srv.Serve(l) }()

	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	pub := pubsubpb.NewPublisherClient(conn)

	_, err = pub.CreateTopic(t.Context(), &pubsubpb.Topic{Name: "projects/p/topics/t"})
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
	assert.Equal(t, []string{"service:p:pubsub.googleapis.com"}, f.calls)

	f.serviceErr = nil

	_, err = pub.CreateTopic(t.Context(), &pubsubpb.Topic{Name: "projects/p/topics/t"})
	require.NoError(t, err)
}

func TestMiddlewareWritesJSONEnvelope(t *testing.T) {
	t.Parallel()

	f := &fakeEnforcer{serviceErr: status.Error(codes.PermissionDenied, "disabled")}
	ctx := gcpenforce.WithGate(t.Context(),
		&gcpenforce.Gate{Enforcer: f, Settings: gcpenforce.Settings{ServiceEnablement: true}})

	reached := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true

		w.WriteHeader(http.StatusOK)
	})
	h := gcpenforce.Middleware(ctx, gcpenforce.Target{Host: "compute.googleapis.com"}, next)

	tests := []struct {
		name      string
		path      string
		wantCode  int
		wantReach bool
	}{
		{"blocked", "/compute/v1/projects/p/zones/z/instances", http.StatusForbidden, false},
		{"operations skipped", "/compute/v1/projects/p/global/operations/op1", http.StatusOK, true},
		{"no project", "/compute/v1/other", http.StatusOK, true},
	}

	for _, tt := range tests {
		reached = false
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, tt.path, nil))

		assert.Equal(t, tt.wantCode, rec.Code, tt.name)
		assert.Equal(t, tt.wantReach, reached, tt.name)
	}

	assert.Contains(t, func() string {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/compute/v1/projects/p/x", nil))

		return rec.Body.String()
	}(), `"status":"PERMISSION_DENIED"`)

	e := echo.New()
	e.Use(gcpenforce.EchoMiddleware(ctx, gcpenforce.Target{Host: "compute.googleapis.com"}))
	e.Any("/*", func(c *echo.Context) error { return c.NoContent(http.StatusNoContent) })

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/compute/v1/projects/p/x", nil))
	assert.Equal(t, http.StatusForbidden, rec.Code)

	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/compute/v1/none", nil))
	assert.Equal(t, http.StatusNoContent, rec.Code)
}
