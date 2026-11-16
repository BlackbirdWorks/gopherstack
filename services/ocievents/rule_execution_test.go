package ocievents_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ocievents"
)

func TestRuleExecutionSink(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		condition string
		want      int
	}{
		{"matching rule", matchAll, 1},
		{"other event type", `{"eventType":"never.matches"}`, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.backend.SetFunctionInvoker(&fakeFunctions{})
			rule := e.createRule(t, "r", tt.condition, faasAction())

			var (
				mu  sync.Mutex
				got []ocievents.RuleExecution
			)

			e.backend.SetRuleExecutionSink(func(x ocievents.RuleExecution) {
				mu.Lock()
				got = append(got, x)
				mu.Unlock()
			})

			e.backend.Emit(eventAt(testCompartment, "a.b", "x"))

			mu.Lock()
			defer mu.Unlock()

			if !assert.Len(t, got, tt.want) || tt.want == 0 {
				return
			}

			assert.Equal(t, *rule.Id, got[0].RuleID)
			assert.Equal(t, testCompartment, got[0].CompartmentID)
			assert.NotEmpty(t, got[0].EventID)
			assert.Equal(t, testFunction, got[0].Target)
		})
	}
}

func TestRuleExecutionSinkIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sink func(b *ocievents.InMemoryBackend) func(ocievents.RuleExecution)
		name string
	}{
		{name: "panics", sink: func(*ocievents.InMemoryBackend) func(ocievents.RuleExecution) {
			return func(ocievents.RuleExecution) { panic("sink failure") }
		}},
		{name: "reenters the backend", sink: func(b *ocievents.InMemoryBackend) func(ocievents.RuleExecution) {
			return func(ocievents.RuleExecution) { b.SetNotifier(nil); _ = b.Deliveries() }
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			fn := &fakeFunctions{}
			e.backend.SetFunctionInvoker(fn)
			e.createRule(t, "r", matchAll, faasAction())
			e.backend.SetRuleExecutionSink(tt.sink(e.backend))

			e.backend.Emit(eventAt(testCompartment, "a.b", "x"))

			assert.Len(t, fn.invoked(), 1, "the event is still delivered")
			require.Len(t, e.backend.Deliveries(), 1)
			assert.Equal(t, "DELIVERED", e.backend.Deliveries()[0].Status)
		})
	}
}
