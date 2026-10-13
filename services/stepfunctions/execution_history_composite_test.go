package stepfunctions_test

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

func TestExecutionHistory_ParallelAndMapLifecycleEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		definition string
		input      string
		wantTypes  []string
		wantAbsent []string
	}{
		{
			name: "parallel",
			definition: `{"StartAt":"P","States":{"P":{"Type":"Parallel","End":true,"Branches":[` +
				`{"StartAt":"a","States":{"a":{"Type":"Pass","End":true}}}]}}}`,
			input: `{}`,
			wantTypes: []string{
				"ParallelStateEntered", "ParallelStateStarted", "ParallelStateSucceeded", "ParallelStateExited",
			},
			wantAbsent: []string{"MapStateStarted"},
		},
		{
			name: "map",
			definition: `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","End":true,` +
				`"Iterator":{"StartAt":"a","States":{"a":{"Type":"Pass","End":true}}}}}}`,
			input: `{"items":[1,2]}`,
			wantTypes: []string{
				"MapStateEntered", "MapStateStarted", "MapIterationStarted", "MapIterationSucceeded",
				"MapStateSucceeded", "MapStateExited",
			},
		},
		{
			name: "map failure",
			definition: `{"StartAt":"M","States":{"M":{"Type":"Map","ItemsPath":"$.items","Next":"E",` +
				`"Catch":[{"ErrorEquals":["States.ALL"],"Next":"E"}],` +
				`"Iterator":{"StartAt":"f","States":{"f":{"Type":"Fail","Error":"x"}}}},"E":{"Type":"Succeed"}}}`,
			input: `{"items":[1]}`,
			wantTypes: []string{
				"MapStateStarted",
				"MapIterationStarted",
				"MapIterationFailed",
				"MapStateFailed",
				"MapStateExited",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
				sm, err := b.CreateStateMachine(context.Background(), "sm", tt.definition, "arn:role", "STANDARD")
				require.NoError(t, err)

				exec, err := b.StartExecution(sm.StateMachineArn, "e1", tt.input)
				require.NoError(t, err)
				synctest.Wait()

				events, _, err := b.GetExecutionHistory(exec.ExecutionArn, "", 0, false)
				require.NoError(t, err)

				var got []string

				for _, ev := range events {
					if strings.HasPrefix(ev.Type, "Map") || strings.HasPrefix(ev.Type, "Parallel") {
						got = append(got, ev.Type)
					}

					switch ev.Type {
					case "MapStateStarted":
						require.NotNil(t, ev.MapStateStartedEventDetails)
						assert.Positive(t, ev.MapStateStartedEventDetails.Length)
					case "MapIterationStarted":
						require.NotNil(t, ev.MapIterationStartedEventDetails)
						assert.Equal(t, "M", ev.MapIterationStartedEventDetails.Name)
					}
				}

				for _, want := range tt.wantTypes {
					assert.Contains(t, got, want)
				}

				for _, absent := range tt.wantAbsent {
					assert.NotContains(t, got, absent)
				}
			})
		})
	}
}
