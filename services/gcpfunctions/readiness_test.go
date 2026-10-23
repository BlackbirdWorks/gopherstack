package gcpfunctions_test

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const fnName = parent + "/functions/slow"

func (e *env) stateOf(t *testing.T) *functionspb.Function {
	t.Helper()

	got, err := e.client.GetFunction(t.Context(), &functionspb.GetFunctionRequest{Name: fnName})
	require.NoError(t, err)

	return got
}

func (e *env) startGated(t *testing.T, fn *functionspb.Function) func() (*functionspb.Function, error) {
	t.Helper()

	e.rt.gate = make(chan struct{})

	op, err := e.client.CreateFunction(t.Context(), &functionspb.CreateFunctionRequest{
		Parent: parent, FunctionId: "slow", Function: fn,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return len(e.rt.specs()) == 1 }, 5*time.Second, time.Millisecond,
		"container starts during deploy")

	return func() (*functionspb.Function, error) { return op.Wait(t.Context()) }
}

func TestDeploy_PendingUntilReady(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	wait := e.startGated(t, fnSpec(e.upload(t, zipOf(t, nodeFilesFn()))))

	got := e.stateOf(t)
	assert.Equal(t, functionspb.Function_DEPLOYING, got.GetState())

	invoked := make(chan getResult, 1)

	go func() { invoked <- e.rawGet(hostOf(got)) }()

	select {
	case <-invoked:
		t.Fatal("invoke returned before the container was ready")
	default:
	}

	close(e.rt.gate)

	fn, err := wait()
	require.NoError(t, err)
	assert.Equal(t, functionspb.Function_ACTIVE, fn.GetState())

	res := <-invoked
	require.NoError(t, res.err)
	assert.Equal(t, http.StatusOK, res.code)
	assert.Equal(t, "/", decode(t, res.body).Path)
}

func TestDeploy_PushBeforeReadyIsRetryable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		retry functionspb.EventTrigger_RetryPolicy
	}{
		{"no retry policy", functionspb.EventTrigger_RETRY_POLICY_DO_NOT_RETRY},
		{"retry policy", functionspb.EventTrigger_RETRY_POLICY_RETRY},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			wait := e.startGated(t, eventSpec(e.upload(t, zipOf(t, nodeFilesFn())), tt.retry))

			code, _ := e.call(t, http.MethodPost, "localhost", "/_gcf/events/"+fnName, envelope)
			assert.Equal(t, http.StatusServiceUnavailable, code)

			close(e.rt.gate)

			_, err := wait()
			require.NoError(t, err)

			code, _ = e.call(t, http.MethodPost, "localhost", "/_gcf/events/"+fnName, envelope)
			assert.Equal(t, http.StatusOK, code)
		})
	}
}

func TestDeploy_TimeoutFails(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	e.handler.Backend.SetStartupTimeout(200 * time.Millisecond)
	wait := e.startGated(t, fnSpec(e.upload(t, zipOf(t, nodeFilesFn()))))

	_, err := wait()
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "did not become ready within")

	got := e.stateOf(t)
	assert.Equal(t, functionspb.Function_FAILED, got.GetState())
	require.NotEmpty(t, got.GetStateMessages())
	assert.Contains(t, got.GetStateMessages()[0].GetMessage(), "did not become ready")

	close(e.rt.gate)
}

type getResult struct {
	err  error
	body string
	code int
}

func (e *env) rawGet(host string) getResult {
	url := "http://" + e.handler.IngressAddr() + "/"

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return getResult{err: err}
	}

	req.Host = host

	resp, err := e.http.Do(req)
	if err != nil {
		return getResult{err: err}
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)

	return getResult{code: resp.StatusCode, body: string(b), err: err}
}
