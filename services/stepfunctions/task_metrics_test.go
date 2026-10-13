package stepfunctions_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

type metricPoints struct {
	points []cwmetric.Point
	mu     sync.Mutex
}

func (m *metricPoints) EmitMetric(p cwmetric.Point) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.points = append(m.points, p)

	return nil
}

func (m *metricPoints) find(name string) []cwmetric.Point {
	m.mu.Lock()
	defer m.mu.Unlock()

	var out []cwmetric.Point

	for _, p := range m.points {
		if p.Name == name {
			out = append(out, p)
		}
	}

	return out
}

var errBoom = errors.New("boom")

type failingLambda struct{}

func (failingLambda) InvokeFunction(context.Context, string, string, []byte) ([]byte, int, error) {
	return nil, 0, errBoom
}

func TestTaskMetrics_ByResourceFamily(t *testing.T) {
	t.Parallel()

	const (
		lambdaARN = "arn:aws:lambda:us-east-1:000000000000:function:fn"
		invokeARN = "arn:aws:states:::lambda:invoke"
	)

	tests := []struct {
		name       string
		definition string
		dimName    string
		dimValue   string
		counts     []string
		notCounts  []string
		times      []string
		failing    bool
	}{
		{
			name:       "direct_lambda_success",
			definition: taskLambdaDefinition,
			dimName:    "LambdaFunctionArn", dimValue: lambdaARN,
			counts:    []string{"LambdaFunctionsScheduled", "LambdaFunctionsStarted", "LambdaFunctionsSucceeded"},
			notCounts: []string{"LambdaFunctionsFailed", "ServiceIntegrationsSucceeded"},
			times:     []string{"LambdaFunctionRunTime", "LambdaFunctionScheduleTime", "LambdaFunctionTime"},
		},
		{
			name: "service_integration_success",
			definition: `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"` + invokeARN + `",` +
				`"Parameters":{"FunctionName":"fn","Payload":{}},"End":true}}}`,
			dimName: "ServiceIntegrationResourceArn", dimValue: invokeARN,
			counts: []string{
				"ServiceIntegrationsScheduled",
				"ServiceIntegrationsStarted",
				"ServiceIntegrationsSucceeded",
			},
			notCounts: []string{"ServiceIntegrationsFailed", "LambdaFunctionsSucceeded"},
			times: []string{
				"ServiceIntegrationRunTime",
				"ServiceIntegrationScheduleTime",
				"ServiceIntegrationTime",
			},
		},
		{
			name:       "direct_lambda_failure",
			definition: taskLambdaDefinition,
			failing:    true,
			dimName:    "LambdaFunctionArn", dimValue: lambdaARN,
			counts:    []string{"LambdaFunctionsFailed"},
			notCounts: []string{"LambdaFunctionsSucceeded", "LambdaFunctionsTimedOut"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
				rec := &metricPoints{}
				b.SetMetricEmitter(rec)

				if tt.failing {
					b.SetLambdaInvoker(failingLambda{})
				} else {
					b.SetLambdaInvoker(&mockLambdaForBackend{})
				}

				sm, err := b.CreateStateMachine(context.Background(), "sm", tt.definition, "arn:role", "STANDARD")
				require.NoError(t, err)
				_, err = b.StartExecution(sm.StateMachineArn, "e1", "{}")
				require.NoError(t, err)
				synctest.Wait()

				for _, name := range tt.counts {
					pts := rec.find(name)
					require.Len(t, pts, 1, name)
					assert.InDelta(t, 1, pts[0].Value, 0)
					assert.Equal(t, "Count", pts[0].Unit)
					assert.Equal(t, []cwmetric.Dimension{{Name: tt.dimName, Value: tt.dimValue}}, pts[0].Dimensions)
				}

				for _, name := range tt.notCounts {
					assert.Empty(t, rec.find(name), name)
				}

				for _, name := range tt.times {
					pts := rec.find(name)
					require.Len(t, pts, 1, name)
					assert.Equal(t, "Milliseconds", pts[0].Unit)
				}
			})
		})
	}
}
