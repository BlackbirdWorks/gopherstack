package gcpscheduler_test

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

	scheduler "cloud.google.com/go/scheduler/apiv1"
	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/blackbirdworks/gopherstack/services/gcpscheduler"
)

const location = "projects/test-project/locations/us-central1"

type clock struct{ ns atomic.Int64 }

func (c *clock) now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *clock) advance(d time.Duration) { c.ns.Add(int64(d)) }

type env struct {
	client  *scheduler.CloudSchedulerClient
	handler *gcpscheduler.Handler
	clk     *clock
}

// epoch is a Tuesday, 10:07:30 UTC.
func epoch() time.Time { return time.Date(2030, 1, 15, 10, 7, 30, 0, time.UTC) }

func newEnv(t *testing.T) *env {
	t.Helper()

	h := gcpscheduler.NewHandler(gcpscheduler.NewInMemoryBackend())
	h.Port = 0
	h.DispatchInterval = 0

	clk := &clock{}
	clk.ns.Store(epoch().UnixNano())
	h.Backend.SetClock(clk.now)

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	client, err := scheduler.NewCloudSchedulerClient(t.Context(),
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

func (e *env) job(t *testing.T, id string, mut ...func(*schedulerpb.Job)) *schedulerpb.Job {
	t.Helper()

	j := &schedulerpb.Job{Name: location + "/jobs/" + id, Schedule: "*/5 * * * *"}
	for _, m := range mut {
		m(j)
	}

	out, err := e.client.CreateJob(t.Context(), &schedulerpb.CreateJobRequest{Parent: location, Job: j})
	require.NoError(t, err)

	return out
}

func (e *env) get(t *testing.T, name string) *schedulerpb.Job {
	t.Helper()

	j, err := e.client.GetJob(t.Context(), &schedulerpb.GetJobRequest{Name: name})
	require.NoError(t, err)

	return j
}

func httpJob(url string) func(*schedulerpb.Job) {
	return func(j *schedulerpb.Job) {
		j.Target = &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{Uri: url}}
	}
}

func withSchedule(s string) func(*schedulerpb.Job) {
	return func(j *schedulerpb.Job) { j.Schedule = s }
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

// newBlockingServer signals started on each request and holds it until release closes.
func newBlockingServer(t *testing.T, started chan<- struct{}, release <-chan struct{}) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		started <- struct{}{}

		<-release
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	return srv
}
