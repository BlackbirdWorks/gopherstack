package wfinterp_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

type stepSink struct {
	events []wfinterp.StepEvent
	mu     sync.Mutex
}

func (s *stepSink) add(e wfinterp.StepEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.events = append(s.events, e)
}

func (s *stepSink) finished() map[string]wfinterp.StepEvent {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := map[string]wfinterp.StepEvent{}

	for _, e := range s.events {
		if e.State != wfinterp.StepInProgress {
			out[e.Routine+"."+e.Step] = e
		}
	}

	return out
}

func TestStepLog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check   func(t *testing.T, ev map[string]wfinterp.StepEvent)
		name    string
		src     string
		wantErr bool
	}{
		{
			name: "sequence links previous and types",
			src: `
main:
  steps:
    - a:
        assign: [{x: 1}]
    - b:
        call: sys.now
        result: n
    - c:
        return: ${x}`,
			check: func(t *testing.T, ev map[string]wfinterp.StepEvent) {
				t.Helper()
				assert.Equal(t, wfinterp.StepTypeAssign, ev["main.a"].Type)
				assert.Equal(t, wfinterp.StepTypeStdLibCall, ev["main.b"].Type)
				assert.Equal(t, wfinterp.StepTypeReturn, ev["main.c"].Type)
				assert.Equal(t, ev["main.a"].ID, ev["main.b"].Prev)
				assert.Equal(t, ev["main.b"].ID, ev["main.c"].Prev)
				assert.Zero(t, ev["main.a"].Prev)
				assert.Equal(t, wfinterp.StepSucceeded, ev["main.c"].State)
				assert.EqualValues(t, 1, ev["main.c"].Vars["x"])
			},
		},
		{
			name: "subworkflow call is the parent",
			src: `
main:
  steps:
    - go:
        call: helper
        result: out
    - r:
        return: ${out}
helper:
  steps:
    - inner:
        return: 1`,
			check: func(t *testing.T, ev map[string]wfinterp.StepEvent) {
				t.Helper()
				assert.Equal(t, wfinterp.StepTypeSubworkflow, ev["main.go"].Type)
				assert.Equal(t, ev["main.go"].ID, ev["helper.inner"].Parent)
				assert.Zero(t, ev["main.go"].Parent)
			},
		},
		{
			name: "failure is recorded and bubbles",
			src: `
main:
  steps:
    - boom:
        raise: "bad"`,
			wantErr: true,
			check: func(t *testing.T, ev map[string]wfinterp.StepEvent) {
				t.Helper()
				assert.Equal(t, wfinterp.StepFailed, ev["main.boom"].State)
				assert.JSONEq(t, `"bad"`, ev["main.boom"].Exception)
			},
		},
		{
			name: "handled error leaves the try succeeded",
			src: `
main:
  steps:
    - guarded:
        try:
          steps:
            - boom:
                raise: "bad"
        except:
          as: e
          steps:
            - ok:
                return: 1`,
			check: func(t *testing.T, ev map[string]wfinterp.StepEvent) {
				t.Helper()
				assert.Equal(t, wfinterp.StepTypeTry, ev["main.guarded"].Type)
				assert.Equal(t, wfinterp.StepFailed, ev["main.boom"].State)
				assert.NotEqual(t, ev["main.guarded"].ID, ev["main.boom"].Parent)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sink := &stepSink{}
			r := run(t, tt.src, nil, func(h *wfinterp.Host) {
				h.StepLog = sink.add
				h.StepVariables = true
			})

			if tt.wantErr {
				require.Error(t, r.err)
			} else {
				require.NoError(t, r.err)
			}

			tt.check(t, sink.finished())
		})
	}
}

func TestStepLogOffByDefault(t *testing.T) {
	t.Parallel()

	sink := &stepSink{}
	r := run(t, "- r:\n    return: 1", nil, nil)
	require.NoError(t, r.err)
	assert.Empty(t, sink.events)
}

func TestStepBudgetStopsEventsAndSnapshots(t *testing.T) {
	t.Parallel()

	const loop = `
- l:
    for:
      value: i
      range: [1, 200]
      steps:
        - a:
            assign: [{x: "${i}"}]
- r:
    return: 1`

	tests := []struct {
		budget    func(begun *int) (bool, bool)
		name      string
		wantBegun int
		wantVars  int
	}{
		{func(*int) (bool, bool) { return true, true }, "unlimited", 402, 402},
		{func(b *int) (bool, bool) { return *b < 10, true }, "log_cap", 10, 10},
		{func(*int) (bool, bool) { return true, false }, "vars_cap", 402, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var begun, withVars int

			r := run(t, loop, nil, func(h *wfinterp.Host) {
				h.StepVariables = true
				h.StepBudget = func() (bool, bool) { return tt.budget(&begun) }
				h.StepLog = func(e wfinterp.StepEvent) {
					if e.State == wfinterp.StepInProgress {
						begun++
					} else if e.Vars != nil {
						withVars++
					}
				}
			})
			require.NoError(t, r.err)
			assert.Equal(t, tt.wantBegun, begun)
			assert.Equal(t, tt.wantVars, withVars)
		})
	}
}

func BenchmarkStepLogLoop(b *testing.B) {
	const src = `
- init:
    assign: [{big: {a: 1, b: [1, 2, 3, 4, 5, 6, 7, 8], c: {d: "xxxxxxxx"}}}]
- l:
    for:
      value: i
      range: [1, 40000]
      steps:
        - a:
            assign: [{x: "${i}"}]
- r:
    return: 1`

	for _, mode := range []string{"off", "log", "log_vars", "log_vars_capped"} {
		b.Run(mode, func(b *testing.B) {
			eng := wfinterp.NewEngine()
			prog, err := eng.Compile(src)
			require.NoError(b, err)

			for b.Loop() {
				n := 0
				host := &wfinterp.Host{Clock: newClock()}

				if mode != "off" {
					host.StepLog = func(wfinterp.StepEvent) { n++ }
					host.StepVariables = mode != "log"
				}

				if mode == "log_vars_capped" {
					host.StepBudget = func() (bool, bool) { return n < 20000, n < 20000 }
				}

				_, err = eng.Run(b.Context(), prog, nil, host)
				require.NoError(b, err)
			}
		})
	}
}

func TestStepLogSubEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		src       string
		want      []string
		unordered bool
	}{
		{
			name: "switch conditions",
			src: `
main:
  steps:
    - pick:
        switch:
          - condition: ${false}
            next: end
          - condition: ${true}
            steps:
              - hit:
                  assign: [{x: 1}]
    - r:
        return: 1`,
			want: []string{
				wfinterp.StepTypeSwitch, wfinterp.StepTypeCondition, wfinterp.StepTypeCondition, "",
				wfinterp.StepTypeAssign, wfinterp.StepTypeReturn,
			},
		},
		{
			name: "for iterations",
			src: `
main:
  steps:
    - l:
        for:
          value: i
          range: [1, 3]
          steps:
            - a:
                assign: [{x: "${i}"}]`,
			want: []string{
				wfinterp.StepTypeFor,
				wfinterp.StepTypeIteration, wfinterp.StepTypeAssign,
				wfinterp.StepTypeIteration, wfinterp.StepTypeAssign,
				wfinterp.StepTypeIteration, wfinterp.StepTypeAssign,
			},
		},
		{
			name:      "parallel branches",
			unordered: true,
			src: `
main:
  steps:
    - p:
        parallel:
          branches:
            - one:
                steps:
                  - a:
                      assign: [{x: 1}]
            - two:
                steps:
                  - b:
                      assign: [{y: 1}]`,
			want: []string{
				wfinterp.StepTypeParallel, wfinterp.StepTypeBranchEntry, wfinterp.StepTypeAssign,
				wfinterp.StepTypeBranchEntry, wfinterp.StepTypeAssign,
			},
		},
		{
			name: "try retry except",
			src: `
main:
  steps:
    - g:
        try:
          steps:
            - boom:
                raise: "bad"
        retry:
          predicate: ${always}
          max_retries: 1
          backoff: {initial_delay: 1, max_delay: 1, multiplier: 1}
        except:
          steps:
            - ok:
                return: 1
always:
  params: [e]
  steps:
    - t:
        return: true`,
			want: []string{
				wfinterp.StepTypeTry, wfinterp.StepTypeTryPart, wfinterp.StepTypeRaise, wfinterp.StepTypeRetryPart,
				wfinterp.StepTypeTryPart, wfinterp.StepTypeRaise, wfinterp.StepTypeExceptPart, wfinterp.StepTypeReturn,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sink := &stepSink{}
			r := run(t, tt.src, nil, func(h *wfinterp.Host) { h.StepLog = sink.add })
			require.NoError(t, r.err)

			var got []string

			for _, e := range sink.events {
				if e.State == wfinterp.StepInProgress && e.Routine == "main" {
					got = append(got, e.Type)
				}
			}

			if tt.unordered {
				assert.ElementsMatch(t, tt.want, got)

				return
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestStepLogProgress(t *testing.T) {
	t.Parallel()

	sink := &stepSink{}
	r := run(t, `
main:
  steps:
    - l:
        for:
          value: i
          range: [1, 3]
          steps:
            - a:
                assign: [{x: "${i}"}]`, nil, func(h *wfinterp.Host) { h.StepLog = sink.add })
	require.NoError(t, r.err)

	var iters []wfinterp.StepEvent

	for _, e := range sink.events {
		if e.State == wfinterp.StepInProgress && e.Type == wfinterp.StepTypeIteration {
			iters = append(iters, e)
		}
	}

	require.Len(t, iters, 3)

	for i, e := range iters {
		assert.Equal(t, wfinterp.ProgressFor, e.ProgressType)
		assert.EqualValues(t, i+1, e.Progress)
		assert.EqualValues(t, 3, e.Expected)
	}

	assert.Equal(t, iters[0].ID, iters[1].Prev)
}
