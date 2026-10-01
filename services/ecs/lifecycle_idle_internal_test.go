package ecs

import (
	"testing"
	"testing/synctest"
	"time"
)

func TestStepTaskLifecycle_TransitionsOnSchedule(t *testing.T) {
	t.Parallel()

	const delay = 3 * time.Second

	tests := []struct {
		name  string
		want  []string
		start bool
	}{
		{
			name: "stop",
			want: []string{
				statusDeactivating, statusDeactivating, statusStopping, statusStopping, statusStopping,
				statusDeprovisioning, statusDeprovisioning, statusDeprovisioning, statusStopped,
			},
		},
		{
			name:  "start",
			start: true,
			want: []string{
				statusProvisioning, statusProvisioning, statusPending, statusPending, statusPending,
				statusRunning, statusRunning, statusRunning, statusRunning,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newTestBackend()
				if tc.start {
					b = NewInMemoryBackend("123456789012", "us-east-1", nil)
				}

				for range 3 {
					time.Sleep(time.Second)
					b.stepTaskLifecycle(time.Now())
				}

				var arn string

				if tc.start {
					b.SetStartDelay(delay)
					arn = runOneTask(t, b)
				} else {
					arn = runOneTask(t, b)
					b.SetStopDelay(delay)

					if _, err := b.StopTask(lcCluster, arn, "r"); err != nil {
						t.Fatalf("StopTask: %v", err)
					}
				}

				for i, want := range tc.want {
					time.Sleep(time.Second)
					b.stepTaskLifecycle(time.Now())

					if got := taskStatus(t, b, arn); got != want {
						t.Fatalf("tick %d: status = %q, want %q", i+1, got, want)
					}
				}

				b.mu.RLock("check")
				tracked := len(b.lifecycle)
				b.mu.RUnlock()

				if tracked != 0 {
					t.Fatalf("tracked = %d after terminal state, want 0", tracked)
				}
			})
		})
	}
}

func BenchmarkStepTaskLifecycleIdle(b *testing.B) {
	be := newTestBackend()
	now := time.Now()

	b.ReportAllocs()

	for b.Loop() {
		be.stepTaskLifecycle(now)
	}
}
