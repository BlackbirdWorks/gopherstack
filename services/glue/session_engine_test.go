package glue_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

var errFakeStart = errors.New("fake start failure")

// fakeEngine is an in-memory REPL: "x = v" sets a variable, "x" reads it,
// "boom" fails, "block" waits for an interrupt.
type fakeEngine struct {
	startErr error
	procs    []*fakeProc
	mu       sync.Mutex
}

type fakeProc struct {
	vars        map[string]string
	interrupt   chan struct{}
	executed    []string
	mu          sync.Mutex
	count       int32
	stopped     atomic.Bool
	interrupted atomic.Bool
}

func (e *fakeEngine) StartSession(_ context.Context, _ glue.SessionSpec) (glue.SessionProcess, error) {
	if e.startErr != nil {
		return nil, e.startErr
	}

	p := &fakeProc{vars: map[string]string{}, interrupt: make(chan struct{}, 1)}

	e.mu.Lock()
	e.procs = append(e.procs, p)
	e.mu.Unlock()

	return p, nil
}

func (e *fakeEngine) proc() *fakeProc {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.procs) == 0 {
		return nil
	}

	return e.procs[0]
}

func (p *fakeProc) Run(ctx context.Context, code string) (glue.StatementResult, error) {
	p.mu.Lock()
	p.executed = append(p.executed, code)
	p.count++
	count := p.count
	p.mu.Unlock()

	switch {
	case code == "boom":
		return glue.StatementResult{
			Failed: true, ErrorName: "ValueError", ErrorValue: "bad", Traceback: []string{"tb"}, ExecutionCount: count,
		}, nil
	case code == "block":
		select {
		case <-p.interrupt:
			return glue.StatementResult{Failed: true, ErrorName: "KeyboardInterrupt", ExecutionCount: count}, nil
		case <-ctx.Done():
			return glue.StatementResult{}, ctx.Err()
		}
	case strings.Contains(code, "="):
		k, v, _ := strings.Cut(code, "=")
		p.mu.Lock()
		p.vars[strings.TrimSpace(k)] = strings.TrimSpace(v)
		p.mu.Unlock()

		return glue.StatementResult{ExecutionCount: count}, nil
	default:
		p.mu.Lock()
		v := p.vars[code]
		p.mu.Unlock()

		return glue.StatementResult{Text: v, ExecutionCount: count}, nil
	}
}

func (p *fakeProc) Interrupt(context.Context) error {
	p.interrupted.Store(true)
	p.interrupt <- struct{}{}

	return nil
}

func (p *fakeProc) Stop(context.Context) error {
	p.stopped.Store(true)

	return nil
}

func (p *fakeProc) codes() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]string(nil), p.executed...)
}

type fakeClock struct{ now atomic.Int64 }

func (c *fakeClock) Now() time.Time { return time.Unix(0, c.now.Load()) }

func (c *fakeClock) Advance(d time.Duration) { c.now.Add(int64(d)) }

func newEngineBackend(t *testing.T, eng glue.SessionEngine) (*glue.InMemoryBackend, *fakeClock) {
	t.Helper()

	b := glue.NewInMemoryBackend(testAccountID, testRegion)
	clk := &fakeClock{}
	clk.now.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	b.SetClock(clk.Now)
	b.SetSessionEngine(eng)
	t.Cleanup(b.StopReconciler)

	return b, clk
}

func createReady(t *testing.T, b *glue.InMemoryBackend, id string, opts glue.Session) {
	t.Helper()

	_, err := b.CreateSession(id, "arn:aws:iam::123456789012:role/r", glue.SessionCommand{Name: "glueetl"}, opts)
	require.NoError(t, err)
	waitSessionStatus(t, b, id, "READY")
}

func waitSessionStatus(t *testing.T, b *glue.InMemoryBackend, id, want string) {
	t.Helper()

	require.Eventually(t, func() bool {
		s, err := b.GetSession(id)
		require.NoError(t, err)

		return s.Status == want
	}, 5*time.Second, time.Millisecond, "session %s never reached %s", id, want)
}

func waitStatement(t *testing.T, b *glue.InMemoryBackend, sid string, id int32, want string) *glue.Statement {
	t.Helper()

	var got *glue.Statement

	require.Eventually(t, func() bool {
		st, err := b.GetStatement(sid, id)
		require.NoError(t, err)
		got = st

		return st.State == want
	}, 5*time.Second, time.Millisecond, "statement %d never reached %s", id, want)

	return got
}

func outputOf(t *testing.T, st *glue.Statement) *glue.StatementOutput {
	t.Helper()

	out, ok := st.Output.(*glue.StatementOutput)
	require.True(t, ok, "output type %T", st.Output)

	return out
}

func TestSessionEngine_Statements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		codes     []string
		wantState []string
		wantText  []string
	}{
		{
			name:      "variables persist",
			codes:     []string{"x = 42", "x"},
			wantState: []string{"AVAILABLE", "AVAILABLE"},
			wantText:  []string{"", "42"},
		},
		{
			name:      "error then continue",
			codes:     []string{"a = 1", "boom", "a"},
			wantState: []string{"AVAILABLE", "ERROR", "AVAILABLE"},
			wantText:  []string{"", "", "1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			eng := &fakeEngine{}
			b, _ := newEngineBackend(t, eng)
			createReady(t, b, "s", glue.Session{})

			for i, code := range tt.codes {
				st, err := b.RunStatement("s", code)
				require.NoError(t, err)

				got := waitStatement(t, b, "s", st.Id, tt.wantState[i])
				out := outputOf(t, got)
				assert.Equal(t, tt.wantState[i], out.Status)
				assert.Equal(t, int32(i+1), out.ExecutionCount)
				assert.InDelta(t, 1.0, got.Progress, 0)
				assert.NotZero(t, got.StartedOn)
				assert.NotZero(t, got.CompletedOn)

				if tt.wantState[i] == "AVAILABLE" {
					require.NotNil(t, out.Data)
					assert.Equal(t, tt.wantText[i], out.Data.TextPlain)
				} else {
					assert.Equal(t, "ValueError", out.ErrorName)
					assert.Equal(t, "bad", out.ErrorValue)
					assert.Equal(t, []string{"tb"}, out.Traceback)
				}
			}
		})
	}
}

func TestSessionEngine_OrderingAndState(t *testing.T) {
	t.Parallel()

	eng := &fakeEngine{}
	b, _ := newEngineBackend(t, eng)
	createReady(t, b, "s", glue.Session{})

	want := []string{"a = 1", "b = 2", "a", "b", "c = 3"}
	var last int32

	for _, c := range want {
		st, err := b.RunStatement("s", c)
		require.NoError(t, err)
		last = st.Id
	}

	waitStatement(t, b, "s", last, "AVAILABLE")
	assert.Equal(t, want, eng.proc().codes())
}

func TestSessionEngine_Cancel(t *testing.T) {
	t.Parallel()

	eng := &fakeEngine{}
	b, _ := newEngineBackend(t, eng)
	createReady(t, b, "s", glue.Session{})

	blocked, err := b.RunStatement("s", "block")
	require.NoError(t, err)
	queued, err := b.RunStatement("s", "x = 1")
	require.NoError(t, err)

	waitStatement(t, b, "s", blocked.Id, "RUNNING")
	require.NoError(t, b.CancelStatement("s", queued.Id))
	waitStatement(t, b, "s", queued.Id, "CANCELLED")

	require.NoError(t, b.CancelStatement("s", blocked.Id))
	waitStatement(t, b, "s", blocked.Id, "CANCELLED")
	assert.True(t, eng.proc().interrupted.Load())

	after, err := b.RunStatement("s", "y = 1")
	require.NoError(t, err)
	waitStatement(t, b, "s", after.Id, "AVAILABLE")
	assert.Equal(t, []string{"block", "y = 1"}, eng.proc().codes())
}

func TestSessionEngine_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act  func(t *testing.T, b *glue.InMemoryBackend, clk *fakeClock)
		name string
		want string
		opts glue.Session
	}{
		{
			name: "idle timeout",
			opts: glue.Session{IdleTimeout: 5},
			act: func(_ *testing.T, b *glue.InMemoryBackend, clk *fakeClock) {
				clk.Advance(6 * time.Minute)
				glue.AdvanceStatesForTest(b, clk.Now())
			},
			want: "TIMEOUT",
		},
		{
			name: "total timeout despite activity",
			opts: glue.Session{Timeout: 10, IdleTimeout: 5},
			act: func(t *testing.T, b *glue.InMemoryBackend, clk *fakeClock) {
				t.Helper()
				clk.Advance(4 * time.Minute)
				_, err := b.RunStatement("s", "a = 1")
				require.NoError(t, err)
				clk.Advance(7 * time.Minute)
				glue.AdvanceStatesForTest(b, clk.Now())
			},
			want: "TIMEOUT",
		},
		{
			name: "stop",
			act: func(t *testing.T, b *glue.InMemoryBackend, _ *fakeClock) {
				t.Helper()
				require.NoError(t, b.StopSession("s"))
			},
			want: "STOPPED",
		},
		{
			name: "not timed out before idle limit",
			opts: glue.Session{IdleTimeout: 5},
			act: func(_ *testing.T, b *glue.InMemoryBackend, clk *fakeClock) {
				clk.Advance(4 * time.Minute)
				glue.AdvanceStatesForTest(b, clk.Now())
			},
			want: "READY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			eng := &fakeEngine{}
			b, clk := newEngineBackend(t, eng)
			createReady(t, b, "s", tt.opts)
			tt.act(t, b, clk)
			waitSessionStatus(t, b, "s", tt.want)

			p := eng.proc()
			if tt.want == "READY" {
				assert.False(t, p.stopped.Load())

				return
			}

			require.Eventually(t, p.stopped.Load, 5*time.Second, time.Millisecond)

			_, err := b.RunStatement("s", "x")
			require.ErrorIs(t, err, glue.ErrIllegalSessionState)
		})
	}
}

func TestSessionEngine_BusySessionDoesNotIdleOut(t *testing.T) {
	t.Parallel()

	eng := &fakeEngine{}
	b, clk := newEngineBackend(t, eng)
	createReady(t, b, "s", glue.Session{IdleTimeout: 5})

	st, err := b.RunStatement("s", "block")
	require.NoError(t, err)
	waitStatement(t, b, "s", st.Id, "RUNNING")

	clk.Advance(30 * time.Minute)
	glue.AdvanceStatesForTest(b, clk.Now())

	s, err := b.GetSession("s")
	require.NoError(t, err)
	assert.Equal(t, "READY", s.Status)
}

func TestSessionEngine_TeardownStopsContainers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act  func(b *glue.InMemoryBackend)
		name string
	}{
		{name: "delete", act: func(b *glue.InMemoryBackend) { require.NoError(t, b.DeleteSession("s0")) }},
		{name: "reset", act: func(b *glue.InMemoryBackend) { b.Reset() }},
		{name: "close", act: func(b *glue.InMemoryBackend) { b.Close() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			eng := &fakeEngine{}
			b, _ := newEngineBackend(t, eng)
			createReady(t, b, "s0", glue.Session{})

			st, err := b.RunStatement("s0", "block")
			require.NoError(t, err)
			waitStatement(t, b, "s0", st.Id, "RUNNING")

			tt.act(b)
			require.Eventually(t, eng.proc().stopped.Load, 5*time.Second, time.Millisecond)
		})
	}
}

func TestSessionEngine_StartFailure(t *testing.T) {
	t.Parallel()

	b, _ := newEngineBackend(t, &fakeEngine{startErr: errFakeStart})
	_, err := b.CreateSession("s", "r", glue.SessionCommand{Name: "glueetl"}, glue.Session{})
	require.NoError(t, err)
	waitSessionStatus(t, b, "s", "FAILED")

	s, err := b.GetSession("s")
	require.NoError(t, err)
	assert.Contains(t, s.ErrorMessage, "fake start failure")

	_, err = b.RunStatement("s", "x")
	require.ErrorIs(t, err, glue.ErrIllegalSessionState)
}

func TestSessionEngine_RunBeforeReady(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateSession("s", "r", glue.SessionCommand{Name: "glueetl"}, glue.Session{})
	require.NoError(t, err)

	_, err = b.RunStatement("s", "x")
	require.ErrorIs(t, err, glue.ErrIllegalSessionState)
}

func TestSessionEngineDisabled_StatementsError(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateSession(ctx, &gluesdk.CreateSessionInput{
		Id: aws.String("s"), Role: aws.String("arn:aws:iam::123456789012:role/r"),
		Command: &types.SessionCommand{Name: aws.String("glueetl")},
	})
	require.NoError(t, err)

	_, err = client.RunStatement(ctx, &gluesdk.RunStatementInput{SessionId: aws.String("s"), Code: aws.String("1")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "IllegalSessionStateException")

	require.Eventually(t, func() bool {
		g, gErr := client.GetSession(ctx, &gluesdk.GetSessionInput{Id: aws.String("s")})
		require.NoError(t, gErr)

		return g.Session.Status == types.SessionStatusReady
	}, 2*time.Second, 10*time.Millisecond)

	run, err := client.RunStatement(ctx, &gluesdk.RunStatementInput{SessionId: aws.String("s"), Code: aws.String("1")})
	require.NoError(t, err)

	got, err := client.GetStatement(ctx, &gluesdk.GetStatementInput{SessionId: aws.String("s"), Id: run.Id})
	require.NoError(t, err)
	assert.Equal(t, types.StatementStateError, got.Statement.State)
	require.NotNil(t, got.Statement.Output)
	assert.Equal(t, types.StatementStateError, got.Statement.Output.Status)
	assert.Contains(t, aws.ToString(got.Statement.Output.ErrorValue), "--glue-engine=docker")
}

func TestSessionEngine_SDK(t *testing.T) {
	t.Parallel()

	eng := &fakeEngine{}
	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	backend.SetSessionEngine(eng)
	t.Cleanup(backend.StopReconciler)

	client := newTestGlueClient(t, glue.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateSession(ctx, &gluesdk.CreateSessionInput{
		Id: aws.String("s"), Role: aws.String("arn:aws:iam::123456789012:role/r"),
		Command: &types.SessionCommand{Name: aws.String("glueetl")},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		g, gErr := client.GetSession(ctx, &gluesdk.GetSessionInput{Id: aws.String("s")})
		require.NoError(t, gErr)

		return g.Session.Status == types.SessionStatusReady
	}, 5*time.Second, time.Millisecond)

	var last int32

	for _, code := range []string{"x = 7", "x"} {
		run, rErr := client.RunStatement(ctx, &gluesdk.RunStatementInput{
			SessionId: aws.String("s"), Code: aws.String(code),
		})
		require.NoError(t, rErr)
		last = run.Id
	}

	require.Eventually(t, func() bool {
		g, gErr := client.GetStatement(ctx, &gluesdk.GetStatementInput{SessionId: aws.String("s"), Id: last})
		require.NoError(t, gErr)

		return g.Statement.State == types.StatementStateAvailable
	}, 5*time.Second, time.Millisecond)

	got, err := client.GetStatement(ctx, &gluesdk.GetStatementInput{SessionId: aws.String("s"), Id: last})
	require.NoError(t, err)
	require.NotNil(t, got.Statement.Output.Data)
	assert.Equal(t, "7", aws.ToString(got.Statement.Output.Data.TextPlain))
	assert.Equal(t, types.StatementStateAvailable, got.Statement.Output.Status)
	assert.Equal(t, int32(2), got.Statement.Output.ExecutionCount)
	assert.InDelta(t, 1.0, got.Statement.Progress, 0)
}

func waitHandlerSessionReady(t *testing.T, h *glue.Handler, id string) {
	t.Helper()

	require.Eventually(t, func() bool {
		out := dispatchNewOp(t, h, "GetSession", map[string]any{"Id": id})
		sess, ok := out["Session"].(map[string]any)

		return ok && sess["Status"] == "READY"
	}, 5*time.Second, 10*time.Millisecond)
}

func waitClientSessionReady(t *testing.T, client *gluesdk.Client, id string) {
	t.Helper()

	require.Eventually(t, func() bool {
		g, err := client.GetSession(t.Context(), &gluesdk.GetSessionInput{Id: aws.String(id)})
		require.NoError(t, err)

		return g.Session.Status == types.SessionStatusReady
	}, 5*time.Second, 10*time.Millisecond)
}
