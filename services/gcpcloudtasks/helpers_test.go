package gcpcloudtasks_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/services/gcpcloudtasks"
)

const location = "projects/test-project/locations/us-central1"

type clock struct{ ns atomic.Int64 }

func (c *clock) now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *clock) advance(d time.Duration) { c.ns.Add(int64(d)) }

type env struct {
	client  *cloudtasks.Client
	handler *gcpcloudtasks.Handler
	clk     *clock
}

func newEnv(t *testing.T) *env {
	t.Helper()

	h := gcpcloudtasks.NewHandler(gcpcloudtasks.NewInMemoryBackend())
	h.Port = 0
	h.DispatchInterval = 0

	clk := &clock{}
	clk.ns.Store(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	h.Backend.SetClock(clk.now)

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	client, err := cloudtasks.NewClient(t.Context(),
		option.WithEndpoint(h.Addr()),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return &env{client: client, handler: h, clk: clk}
}

// dispatch runs one dispatcher pass and waits for its attempts to finish.
func (e *env) dispatch() {
	e.handler.Backend.DispatchDue()
	e.handler.Backend.Drain()
}

func (e *env) queue(t *testing.T, id string, mut ...func(*cloudtaskspb.Queue)) *cloudtaskspb.Queue {
	t.Helper()

	q := &cloudtaskspb.Queue{Name: location + "/queues/" + id}
	for _, m := range mut {
		m(q)
	}

	out, err := e.client.CreateQueue(t.Context(), &cloudtaskspb.CreateQueueRequest{Parent: location, Queue: q})
	require.NoError(t, err)

	return out
}

func (e *env) task(t *testing.T, queue string, mut ...func(*cloudtaskspb.Task)) *cloudtaskspb.Task {
	t.Helper()

	task := &cloudtaskspb.Task{}
	for _, m := range mut {
		m(task)
	}

	out, err := e.client.CreateTask(t.Context(), &cloudtaskspb.CreateTaskRequest{Parent: queue, Task: task})
	require.NoError(t, err)

	return out
}

func (e *env) getTask(t *testing.T, name string) (*cloudtaskspb.Task, error) {
	t.Helper()

	return e.client.GetTask(t.Context(), &cloudtaskspb.GetTaskRequest{Name: name})
}

func httpTask(url string) func(*cloudtaskspb.Task) {
	return func(task *cloudtaskspb.Task) {
		task.MessageType = &cloudtaskspb.Task_HttpRequest{HttpRequest: &cloudtaskspb.HttpRequest{Url: url}}
	}
}

func fastRetry(maxAttempts int32) func(*cloudtaskspb.Queue) {
	return func(q *cloudtaskspb.Queue) {
		q.RetryConfig = &cloudtaskspb.RetryConfig{
			MaxAttempts:  maxAttempts,
			MinBackoff:   durationpb.New(time.Second),
			MaxBackoff:   durationpb.New(10 * time.Second),
			MaxDoublings: 2,
		}
	}
}

type hit struct {
	header http.Header
	method string
	path   string
	body   []byte
}

// target is an httptest server that records requests and answers with scripted statuses.
type target struct {
	srv      *httptest.Server
	statuses func(n int) int
	hits     []hit
	mu       sync.Mutex
}

func newTarget(t *testing.T, statuses func(n int) int) *target {
	t.Helper()

	tg := &target{statuses: statuses}
	tg.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf, _ := io.ReadAll(r.Body)

		tg.mu.Lock()
		tg.hits = append(tg.hits, hit{method: r.Method, path: r.URL.RequestURI(), header: r.Header.Clone(), body: buf})
		n := len(tg.hits)
		tg.mu.Unlock()

		w.WriteHeader(tg.statuses(n))
	}))
	t.Cleanup(tg.srv.Close)

	return tg
}

func (tg *target) count() int {
	tg.mu.Lock()
	defer tg.mu.Unlock()

	return len(tg.hits)
}

func (tg *target) hit(i int) hit {
	tg.mu.Lock()
	defer tg.mu.Unlock()

	return tg.hits[i]
}

func always(code int) func(int) int { return func(int) int { return code } }

func tsAfter(e *env, d time.Duration) *timestamppb.Timestamp {
	return timestamppb.New(e.clk.now().Add(d))
}

// newBlockingTarget signals started on each request and holds it until release closes.
func newBlockingTarget(t *testing.T, started chan<- struct{}, release <-chan struct{}) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}

		<-release
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	return srv
}

func decodeJWT(t *testing.T, token string) map[string]any {
	t.Helper()

	parts := strings.Split(token, ".")
	require.Len(t, parts, 3)
	assert.Empty(t, parts[2], "token is unsigned")

	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)

	var claims map[string]any
	require.NoError(t, json.Unmarshal(raw, &claims))

	return claims
}
