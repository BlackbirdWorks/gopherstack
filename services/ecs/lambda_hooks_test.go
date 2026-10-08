package ecs_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

var errHookFunction = errors.New("Unhandled")

type scriptedInvoker struct {
	payloads  [][]byte
	arns      []string
	responses []string
	errs      []error
	mu        sync.Mutex
}

func (s *scriptedInvoker) Invoke(_ context.Context, arn string, payload []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	i := len(s.payloads)
	s.payloads = append(s.payloads, payload)
	s.arns = append(s.arns, arn)

	var err error
	if i < len(s.errs) {
		err = s.errs[i]
	}

	resp := s.responses[len(s.responses)-1]
	if i < len(s.responses) {
		resp = s.responses[i]
	}

	return []byte(resp), err
}

func (s *scriptedInvoker) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.payloads)
}

func TestBlueGreen_LambdaHooks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		invoker    *scriptedInvoker
		name       string
		wantStatus string
		wantHook   string
		wantCalls  int
	}{
		{
			name: "succeeded", invoker: &scriptedInvoker{responses: []string{`{"hookStatus":"SUCCEEDED"}`}},
			wantStatus: "SUCCESSFUL", wantHook: "SUCCEEDED", wantCalls: 1,
		},
		{
			name: "no_hook_status_is_success", invoker: &scriptedInvoker{responses: []string{`{}`}},
			wantStatus: "SUCCESSFUL", wantHook: "SUCCEEDED", wantCalls: 1,
		},
		{
			name: "hook_status_failed", invoker: &scriptedInvoker{responses: []string{`{"hookStatus":"FAILED"}`}},
			wantStatus: "ROLLBACK_FAILED", wantHook: "FAILED", wantCalls: 1,
		},
		{
			name:       "invoke_error",
			invoker:    &scriptedInvoker{responses: []string{``}, errs: []error{errHookFunction}},
			wantStatus: "ROLLBACK_FAILED", wantHook: "FAILED", wantCalls: 1,
		},
		{
			name: "in_progress_then_succeeded",
			invoker: &scriptedInvoker{responses: []string{
				`{"hookStatus":"IN_PROGRESS","callBackDelay":0}`, `{"hookStatus":"SUCCEEDED"}`,
			}},
			wantStatus: "SUCCESSFUL", wantHook: "SUCCEEDED", wantCalls: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
			backend.SetLambdaInvoker(tt.invoker)

			_, err := backend.CreateCluster(ecs.CreateClusterInput{ClusterName: "c"})
			require.NoError(t, err)

			td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
				Family:               "td",
				ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
			})
			require.NoError(t, err)

			zero := 0
			fnArn := "arn:aws:lambda:us-east-1:000000000000:function:gate"
			_, err = backend.CreateService(ecs.CreateServiceInput{
				Cluster: "c", ServiceName: "s", TaskDefinition: td.TaskDefinitionArn,
				DeploymentConfiguration: &ecs.DeploymentConfiguration{
					Strategy: "BLUE_GREEN", BakeTimeInMinutes: &zero,
					LifecycleHooks: []ecs.DeploymentLifecycleHook{{
						TargetType: "AWS_LAMBDA", HookTargetArn: fnArn, RoleArn: "arn:aws:iam::000000000000:role/hook",
						LifecycleStages: []string{"PRE_SCALE_UP"}, HookDetails: map[string]any{"gate": "x"},
					}},
				},
			})
			require.NoError(t, err)

			list, err := backend.ListServiceDeployments("c", "s")
			require.NoError(t, err)
			require.Len(t, list, 1)

			require.Eventually(t, func() bool {
				got, _, descErr := backend.DescribeServiceDeployments([]string{list[0].ServiceDeploymentArn})

				return descErr == nil && got[0].Status == tt.wantStatus
			}, 5*time.Second, 10*time.Millisecond)

			got, _, err := backend.DescribeServiceDeployments([]string{list[0].ServiceDeploymentArn})
			require.NoError(t, err)
			require.Len(t, got[0].LifecycleHookDetails, 1)
			assert.Equal(t, tt.wantHook, got[0].LifecycleHookDetails[0].Status)
			assert.Equal(t, fnArn, got[0].LifecycleHookDetails[0].TargetArn)
			assert.Equal(t, tt.wantCalls, tt.invoker.calls())
			assert.Equal(t, fnArn, tt.invoker.arns[0])
			assert.Contains(t, string(tt.invoker.payloads[0]), `"hookDetails":{"gate":"x"}`)
			assert.Contains(t, string(tt.invoker.payloads[0]), `"lifecycleStage":"PRE_SCALE_UP"`)
		})
	}
}
