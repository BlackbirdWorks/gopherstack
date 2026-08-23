package ecs

import (
	"fmt"
	"sync"
	"testing"
)

// TestReconciler_ConcurrentFailureAttribution_NoDataRace reproduces the race
// class from gopherstack-urw6: getServicesForReconciler's `service: *svc` copy
// is shallow -- its Deployments slice shares the live stored Service's backing
// array. The reconciler reads Deployments[idx].RolloutState from that snapshot
// with no lock held (via reconcileService -> primaryDeploymentFailed), while a
// concurrent failed task launch mutates the SAME backing-array element in
// place under the write lock (recordServiceTaskFailureLocked ->
// evaluateCircuitBreakerLocked). The mutex is irrelevant once the slice
// escapes: the reader never takes it. Run with `go test -race` to verify.
//
// Each of N independent services gives one shot at the race window (the
// circuit breaker trips -- and so writes RolloutState -- exactly once per
// service), so N runs concurrently to make a single -race run reliable
// instead of depending on a lucky interleaving.
func TestReconciler_ConcurrentFailureAttribution_NoDataRace(t *testing.T) {
	t.Parallel()

	const services = 20

	b := NewInMemoryBackend("123456789012", "us-east-1", failingRunner{})
	tdArn := registerSimpleTaskDef(t, b, "race-app", "nginx:race")

	if _, err := b.CreateCluster(CreateClusterInput{ClusterName: "race"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	var wg sync.WaitGroup

	for i := range services {
		name := fmt.Sprintf("svc-%d", i)

		if _, err := b.CreateService(CreateServiceInput{
			ServiceName:    name,
			Cluster:        "race",
			TaskDefinition: tdArn,
			DesiredCount:   5,
			DeploymentConfiguration: &DeploymentConfiguration{
				DeploymentCircuitBreaker: &DeploymentCircuitBreaker{Enable: true, Rollback: false},
			},
		}); err != nil {
			t.Fatalf("CreateService(%s): %v", name, err)
		}

		// Capture a single reconciler snapshot per service -- the shallow copy
		// under test. The reader goroutine below never touches b.mu again after
		// this point, so no further synchronization on that mutex can
		// accidentally order its unlocked reads relative to the writer's locked
		// writes -- this isolates the actual unprotected access instead of
		// letting unrelated Lock/RLock round trips on the same mutex launder it
		// away.
		snapshots := b.getServicesForReconciler()

		var snap serviceSnapshot

		found := false

		for _, s := range snapshots {
			if s.service.ServiceName == name {
				snap = s
				found = true

				break
			}
		}

		if !found {
			t.Fatalf("no snapshot for service %s", name)
		}

		done := make(chan struct{})

		wg.Add(2)

		go func() {
			defer wg.Done()

			for {
				select {
				case <-done:
					return
				default:
					_ = primaryDeploymentFailed(&snap.service)
				}
			}
		}()

		go func(name string) {
			defer wg.Done()
			defer close(done)

			// Three failed launches trip the breaker for desiredCount=5
			// (threshold=3), writing Deployments[idx].RolloutState under the
			// write lock -- the same backing array snap.service.Deployments
			// aliases.
			if _, err := b.RunTask(RunTaskInput{
				Cluster:        "race",
				TaskDefinition: tdArn,
				Group:          "service:" + name,
				Count:          3,
			}); err != nil {
				t.Errorf("RunTask(%s): %v", name, err)
			}
		}(name)
	}

	wg.Wait()
}
