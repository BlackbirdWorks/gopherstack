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
				assert.Equal(t, ev["main.guarded"].ID, ev["main.boom"].Parent)
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
