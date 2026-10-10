package ecs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type healthReadingRegistrar struct {
	state string
	fakeELBv2Registrar
}

func (h *healthReadingRegistrar) TargetHealthState(_ context.Context, _ string, _ ELBTarget) (string, bool) {
	return h.state, true
}

func TestStopELBUnhealthyTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		state       string
		wantStopped bool
	}{
		{name: "unhealthy", state: "unhealthy", wantStopped: true},
		{name: "healthy", state: "healthy"},
		{name: "initial", state: "initial"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
			b.SetELBv2Registrar(&healthReadingRegistrar{state: tt.state})
			s := newFargateServiceWithLB(t, b, "svc-health")
			require.NoError(t, b.StartTaskForService(s.cluster, s.service, s.tdArn))

			b.stopELBUnhealthyTasks(t.Context())

			tasks, _, err := b.DescribeTasks(s.cluster, nil)
			require.NoError(t, err)

			stopped := 0

			for _, task := range tasks {
				if task.DesiredStatus == statusStopped {
					stopped++

					assert.Contains(t, task.StoppedReason, "Task failed ELB health checks in (target-group "+testTGArn)
				}
			}

			if tt.wantStopped {
				assert.Equal(t, 1, stopped)
			} else {
				assert.Zero(t, stopped)
			}
		})
	}
}
