package ocievents_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

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
