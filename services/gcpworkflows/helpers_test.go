package gcpworkflows_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	workflows "cloud.google.com/go/workflows/apiv1"
	"cloud.google.com/go/workflows/apiv1/workflowspb"
	executions "cloud.google.com/go/workflows/executions/apiv1"
	"cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/services/gcpworkflows"
)

const (
	project  = "test-project"
	location = "us-central1"
	parent   = "projects/" + project + "/locations/" + location
	waitFor  = 30 * time.Second
	tick     = 5 * time.Millisecond
)

// testClock makes sleeps instant, records them, and can hold a sleeper until the test releases it.
type testClock struct {
	gate     chan struct{}
	sleeping chan struct{}
	now      time.Time
	sleeps   []time.Duration
	mu       sync.Mutex
	hold     bool
}

func newTestClock() *testClock {
	return &testClock{
		now:      time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC),
		gate:     make(chan struct{}),
		sleeping: make(chan struct{}, 16),
	}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *testClock) holdSleeps() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.hold = true
}

func (c *testClock) release() { close(c.gate) }

func (c *testClock) Sleep(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	c.sleeps = append(c.sleeps, d)
	hold := c.hold
	c.now = c.now.Add(d)
	c.mu.Unlock()

	if !hold {
		return ctx.Err()
	}

	c.sleeping <- struct{}{}

	select {
	case <-c.gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *testClock) slept() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()

	return append([]time.Duration(nil), c.sleeps...)
}

type env struct {
	handler *gcpworkflows.Handler
	conn    *grpc.ClientConn
	wf      *workflows.Client
	ex      *executions.Client
	clock   *testClock
}

func newEnv(t *testing.T) *env {
	t.Helper()

	h := gcpworkflows.NewHandler(gcpworkflows.NewInMemoryBackend())
	h.Port = 0

	clock := newTestClock()
	h.Backend.SetClock(clock)

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	wf, err := workflows.NewClient(t.Context(), option.WithGRPCConn(conn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = wf.Close() })

	ex, err := executions.NewClient(t.Context(), option.WithGRPCConn(conn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ex.Close() })

	return &env{handler: h, conn: conn, wf: wf, ex: ex, clock: clock}
}

func code(err error) codes.Code { return status.Code(err) }

func (e *env) createWorkflow(
	t *testing.T,
	id, source string,
	mutate ...func(*workflowspb.Workflow),
) *workflowspb.Workflow {
	t.Helper()

	w := &workflowspb.Workflow{SourceCode: &workflowspb.Workflow_SourceContents{SourceContents: source}}
	for _, m := range mutate {
		m(w)
	}

	op, err := e.wf.CreateWorkflow(t.Context(), &workflowspb.CreateWorkflowRequest{
		Parent: parent, WorkflowId: id, Workflow: w,
	})
	require.NoError(t, err)

	got, err := op.Wait(t.Context())
	require.NoError(t, err)

	return got
}

func (e *env) execute(t *testing.T, wfName, arg string) *executionspb.Execution {
	t.Helper()

	ex, err := e.ex.CreateExecution(t.Context(), &executionspb.CreateExecutionRequest{
		Parent: wfName, Execution: &executionspb.Execution{Argument: arg},
	})
	require.NoError(t, err)

	return ex
}

// await polls until the execution leaves the ACTIVE state and returns its full view.
func (e *env) await(t *testing.T, name string) *executionspb.Execution {
	t.Helper()

	var got *executionspb.Execution

	require.Eventually(t, func() bool {
		var err error

		got, err = e.ex.GetExecution(t.Context(), &executionspb.GetExecutionRequest{Name: name})

		return err == nil && got.GetState() != executionspb.Execution_ACTIVE
	}, waitFor, tick)

	return got
}

// run creates a workflow, executes it with arg and waits for the result.
func (e *env) run(t *testing.T, id, source, arg string) *executionspb.Execution {
	t.Helper()

	w := e.createWorkflow(t, id, source)

	return e.await(t, e.execute(t, w.GetName(), arg).GetName())
}

func opRequest(name string) *longrunningpb.GetOperationRequest {
	return &longrunningpb.GetOperationRequest{Name: name}
}

func sameProto(t *testing.T, want, got proto.Message) {
	t.Helper()
	require.True(t, proto.Equal(want, got), "want %v got %v", want, got)
}

// recorder is an httptest server that remembers its requests.
type recorder struct {
	srv  *httptest.Server
	got  chan []byte
	hits []string
	mu   sync.Mutex
}

func newRecorder(t *testing.T, reply func(w http.ResponseWriter, r *http.Request)) *recorder {
	t.Helper()

	rec := &recorder{got: make(chan []byte, 16)}
	rec.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, 1<<16)
		n, _ := r.Body.Read(body)

		rec.mu.Lock()
		rec.hits = append(rec.hits, r.Method+" "+r.URL.RequestURI()+" auth="+r.Header.Get("Authorization"))
		rec.mu.Unlock()

		rec.got <- body[:n]

		if reply != nil {
			reply(w, r)
		}
	}))
	t.Cleanup(rec.srv.Close)

	return rec
}

func (r *recorder) requests() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.hits...)
}

// projectNumberOf reads the project number a run saw, so tests do not repeat the derivation.
func projectNumberOf(ex *executionspb.Execution) string {
	const key = `"number":"`

	r := ex.GetResult()
	i := strings.Index(r, key) + len(key)

	return r[i : i+strings.Index(r[i:], `"`)]
}
