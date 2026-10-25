package gcpeventarc_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	eventarc "cloud.google.com/go/eventarc/apiv1"
	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/storage"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/services/gcpeventarc"
	"github.com/blackbirdworks/gopherstack/services/gcppubsub"
	"github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

const (
	project  = "test-project"
	location = "us-central1"
	parent   = "projects/" + project + "/locations/" + location
	waitFor  = 30 * time.Second
	tick     = 20 * time.Millisecond
)

type env struct {
	handler  *gcpeventarc.Handler
	conn     *grpc.ClientConn
	client   *eventarc.Client
	pubsub   *gcppubsub.Handler
	psClient *pubsub.Client
	gcs      *storage.Client
	gcsBack  *gcpstorage.InMemoryBackend
	dest     *destination
}

// request is one delivery received by a destination.
type request struct {
	header http.Header
	path   string
	body   []byte
}

type destination struct {
	srv      *httptest.Server
	got      chan request
	mu       sync.Mutex
	failures int
	calls    int
}

func newDestination(t *testing.T) *destination {
	t.Helper()

	d := &destination{got: make(chan request, 64)}
	d.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		d.mu.Lock()
		d.calls++
		fail := d.failures > 0
		if fail {
			d.failures--
		}
		d.mu.Unlock()

		if fail {
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		d.got <- request{header: r.Header.Clone(), path: r.URL.Path, body: body}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(d.srv.Close)

	return d
}

func (d *destination) failNext(n int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.failures = n
}

func (d *destination) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.calls
}

func (d *destination) next(t *testing.T) request {
	t.Helper()

	select {
	case r := <-d.got:
		return r
	case <-time.After(waitFor):
		t.Fatal("no delivery arrived")

		return request{}
	}
}

func (d *destination) none(t *testing.T) {
	t.Helper()

	select {
	case r := <-d.got:
		t.Fatalf("unexpected delivery to %s", r.path)
	default:
	}
}

// fakeTargets serves Cloud Run service URLs from a map keyed by project/region/service.
type fakeTargets map[string]string

func (f fakeTargets) ServiceURL(project, region, service string) (string, error) {
	if u, ok := f[project+"/"+region+"/"+service]; ok {
		return u, nil
	}

	return "", status.Error(codes.NotFound, "service not found")
}

func newEnv(t *testing.T) *env {
	t.Helper()

	ps := gcppubsub.NewHandler(gcppubsub.NewInMemoryBackend())
	ps.Port = 0
	require.NoError(t, ps.StartWorker(t.Context()))
	t.Cleanup(func() { ps.Shutdown(context.Background()) })

	psClient, err := pubsub.NewClient(t.Context(), project,
		option.WithEndpoint(ps.Addr()), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	require.NoError(t, err)
	t.Cleanup(func() { _ = psClient.Close() })

	gcsBack := gcpstorage.NewInMemoryBackend()
	echoSrv := echo.New()
	echoSrv.Any("/*", gcpstorage.NewHandler(gcsBack).Handler())

	gcsSrv := httptest.NewServer(echoSrv)
	t.Cleanup(gcsSrv.Close)

	gcs, err := storage.NewClient(t.Context(),
		option.WithEndpoint(gcsSrv.URL+"/storage/v1/"), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { _ = gcs.Close() })

	h := gcpeventarc.NewHandler(gcpeventarc.NewInMemoryBackend())
	h.Port = 0
	h.Backend.SetRetryBackoff(time.Second, time.Second)
	dest := newDestination(t)
	h.Backend.SetServices(gcpeventarc.Services{
		PubSub:  ps.Backend,
		Targets: fakeTargets{project + "/" + location + "/receiver": dest.srv.URL},
	})
	gcsBack.ObserveObjects(func(ev gcpstorage.ObjectEvent) {
		h.Backend.HandleStorageEvent(gcpeventarc.StorageEvent{
			Type: string(ev.Type), Bucket: ev.Object.Bucket, Name: ev.Object.Name, Data: ev.Data(), Time: ev.Time,
		})
	})

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client, err := eventarc.NewClient(t.Context(), option.WithGRPCConn(conn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return &env{
		handler: h, conn: conn, client: client, pubsub: ps, psClient: psClient,
		gcs: gcs, gcsBack: gcsBack, dest: dest,
	}
}

func runDestination(service, path string) *eventarcpb.Destination {
	return &eventarcpb.Destination{Descriptor_: &eventarcpb.Destination_CloudRun{
		CloudRun: &eventarcpb.CloudRun{Service: service, Region: location, Path: path},
	}}
}

func httpDestination(uri string) *eventarcpb.Destination {
	return &eventarcpb.Destination{Descriptor_: &eventarcpb.Destination_HttpEndpoint{
		HttpEndpoint: &eventarcpb.HttpEndpoint{Uri: uri},
	}}
}

func typeFilter(t string) *eventarcpb.EventFilter {
	return &eventarcpb.EventFilter{Attribute: "type", Value: t}
}

func pubsubTrigger(topic string, dest *eventarcpb.Destination) *eventarcpb.Trigger {
	t := &eventarcpb.Trigger{
		EventFilters: []*eventarcpb.EventFilter{typeFilter("google.cloud.pubsub.topic.v1.messagePublished")},
		Destination:  dest,
	}
	if topic != "" {
		t.Transport = &eventarcpb.Transport{Intermediary: &eventarcpb.Transport_Pubsub{
			Pubsub: &eventarcpb.Pubsub{Topic: topic},
		}}
	}

	return t
}

func storageTrigger(
	typ, bucket string,
	dest *eventarcpb.Destination,
	extra ...*eventarcpb.EventFilter,
) *eventarcpb.Trigger {
	filters := make([]*eventarcpb.EventFilter, 0, 2+len(extra))
	filters = append(filters,
		typeFilter("google.cloud.storage.object.v1."+typ),
		&eventarcpb.EventFilter{Attribute: "bucket", Value: bucket},
	)

	return &eventarcpb.Trigger{EventFilters: append(filters, extra...), Destination: dest}
}

func (e *env) createTrigger(t *testing.T, id string, trig *eventarcpb.Trigger) *eventarcpb.Trigger {
	t.Helper()

	op, err := e.client.CreateTrigger(t.Context(), &eventarcpb.CreateTriggerRequest{
		Parent: parent, TriggerId: id, Trigger: trig,
	})
	require.NoError(t, err)

	got, err := op.Wait(t.Context())
	require.NoError(t, err)

	return got
}

func (e *env) publish(t *testing.T, topic string, msg *pubsub.Message) {
	t.Helper()

	pub := e.psClient.Publisher(topic)
	defer pub.Stop()

	_, err := pub.Publish(t.Context(), msg).Get(t.Context())
	require.NoError(t, err)
}

func code(err error) codes.Code { return status.Code(err) }

func topicPB(name string) *pubsubpb.Topic { return &pubsubpb.Topic{Name: name} }

func opRequest(name string) *longrunningpb.GetOperationRequest {
	return &longrunningpb.GetOperationRequest{Name: name}
}

func opListRequest(name string) *longrunningpb.ListOperationsRequest {
	return &longrunningpb.ListOperationsRequest{Name: name}
}
