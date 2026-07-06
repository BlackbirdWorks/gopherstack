package ecs

import (
	"testing"
	"time"
)

func TestReconcilerSemEviction_OnClusterDelete(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	r := NewReconciler(b)
	b.RegisterClusterDeleteHook(r.EvictCluster)

	tdArn := registerSimpleTaskDef(t, b, "sem-app", "nginx")

	makeService := func(cluster string) {
		if _, err := b.CreateCluster(CreateClusterInput{ClusterName: cluster}); err != nil {
			t.Fatalf("CreateCluster(%s): %v", cluster, err)
		}
		if _, err := b.CreateService(CreateServiceInput{
			ServiceName:    "svc",
			Cluster:        cluster,
			TaskDefinition: tdArn,
			DesiredCount:   1,
		}); err != nil {
			t.Fatalf("CreateService(%s): %v", cluster, err)
		}
	}

	makeService("c1")
	makeService("c2")

	// Reconciling scales up each service, which creates a per-cluster semaphore.
	r.RunOnce(t.Context())

	if got := r.SemCount(); got != 2 {
		t.Fatalf("SemCount after reconcile = %d, want 2", got)
	}

	if _, err := b.DeleteCluster("c1"); err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if got := r.SemCount(); got != 1 {
		t.Errorf("SemCount after DeleteCluster = %d, want 1 (sem evicted)", got)
	}

	// Purge removes the remaining cluster and must also fire the eviction hook.
	b.Purge(t.Context(), time.Now().Add(time.Hour))

	if got := r.SemCount(); got != 0 {
		t.Errorf("SemCount after Purge = %d, want 0", got)
	}
}

func TestPurge_CleansServiceAndDaemonState(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	tdArn := registerSimpleTaskDef(t, b, "purge-app", "nginx")

	if _, err := b.CreateCluster(CreateClusterInput{ClusterName: "pc"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	svc, err := b.CreateService(CreateServiceInput{
		ServiceName:    "svc",
		Cluster:        "pc",
		TaskDefinition: tdArn,
		DesiredCount:   1,
	})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	// Seed the auxiliary maps that older Purge code paths leaked.
	daemonArn := "arn:aws:ecs:us-east-1:123456789012:daemon/pc/d1"

	b.mu.Lock("seed")
	b.serviceDeployments.Put(&ServiceDeployment{
		ServiceDeploymentArn: "sd-1",
		ServiceArn:           svc.ServiceArn,
	})
	b.daemons.Put(&Daemon{DaemonArn: daemonArn, DaemonName: "d1", ClusterArn: svc.ClusterArn})
	b.daemonDeployments.Put(&DaemonDeployment{DaemonDeploymentArn: "dd-1", DaemonArn: daemonArn})
	b.daemonTaskDefs[daemonArn] = []*DaemonTaskDefinition{{DaemonTaskDefinitionArn: daemonArn}}
	b.mu.Unlock()

	b.Purge(t.Context(), time.Now().Add(time.Hour))

	if got := b.ServiceDeploymentCount(); got != 0 {
		t.Errorf("serviceDeployments after purge = %d, want 0", got)
	}

	b.mu.RLock("verify")
	defer b.mu.RUnlock()

	if got := b.daemons.Len(); got != 0 {
		t.Errorf("daemons after purge = %d, want 0", got)
	}
	if got := b.daemonDeployments.Len(); got != 0 {
		t.Errorf("daemonDeployments after purge = %d, want 0", got)
	}
	if len(b.daemonTaskDefs) != 0 {
		t.Errorf("daemonTaskDefs after purge = %d, want 0", len(b.daemonTaskDefs))
	}
	if got := b.clusters.Len(); got != 0 {
		t.Errorf("clusters after purge = %d, want 0", got)
	}
	if len(b.serviceIndex) != 0 {
		t.Errorf("serviceIndex after purge = %d, want 0", len(b.serviceIndex))
	}
}

func TestPurge_RevisionCutoff(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	// Register two families with two revisions each.
	for _, fam := range []string{"famA", "famB"} {
		registerSimpleTaskDef(t, b, fam, "img:1")
		registerSimpleTaskDef(t, b, fam, "img:2")
	}

	cutoff := time.Now()

	// Age the first revision of each family to before the cutoff; leave the
	// second revision newer than the cutoff.
	b.mu.Lock("age")
	for _, fam := range []string{"famA", "famB"} {
		b.taskDefinitions[fam][0].RegisteredAt = cutoff.Add(-time.Hour)
		b.taskDefinitions[fam][1].RegisteredAt = cutoff.Add(time.Hour)
	}
	b.mu.Unlock()

	b.Purge(t.Context(), cutoff)

	b.mu.RLock("verify")
	defer b.mu.RUnlock()

	// Each family should retain only its single newer-than-cutoff revision.
	for _, fam := range []string{"famA", "famB"} {
		if got := len(b.taskDefinitions[fam]); got != 1 {
			t.Errorf("family %s revisions after purge = %d, want 1", fam, got)
		}
	}

	// The aged revisions must also be gone from the ARN index.
	if got := b.taskDefByArn.Len(); got != 2 {
		t.Errorf("taskDefByArn size after purge = %d, want 2", got)
	}
}
