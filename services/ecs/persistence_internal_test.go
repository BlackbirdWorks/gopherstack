package ecs

import (
	"testing"
)

// Test_Restore_RebuildsServiceIndex proves that after a Snapshot/Restore
// round-trip, the deployment reconciler still sees pre-existing services.
//
// getServicesForReconciler (backend.go) reads only the flat serviceIndex map
// with no linear-scan fallback (unlike tasksByInstance, which
// enrichContainerInstance falls back to scanning for). Restore previously
// loaded b.services from the snapshot but never rebuilt b.serviceIndex, so a
// restored service was permanently invisible to the reconciler: its
// DesiredCount would never be reconciled again after a restart, silently
// freezing scaling and deployments for every service that existed at
// snapshot time.
func Test_Restore_RebuildsServiceIndex(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		desiredCount int
		wantTasks    int
	}{
		{name: "desired count 1 reconciles after restore", desiredCount: 1, wantTasks: 1},
		{name: "desired count 3 reconciles after restore", desiredCount: 3, wantTasks: 3},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())

			tdArn := registerSimpleTaskDef(t, src, "restore-app-"+tt.name, "nginx:good")

			if _, err := src.CreateCluster(CreateClusterInput{ClusterName: "restore-cluster"}); err != nil {
				t.Fatalf("CreateCluster: %v", err)
			}

			if _, err := src.CreateService(CreateServiceInput{
				ServiceName:    "restore-svc",
				Cluster:        "restore-cluster",
				TaskDefinition: tdArn,
				DesiredCount:   tt.desiredCount,
			}); err != nil {
				t.Fatalf("CreateService: %v", err)
			}

			snap := src.Snapshot(t.Context())
			if len(snap) == 0 {
				t.Fatalf("Snapshot returned empty data")
			}

			dst := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
			if err := dst.Restore(t.Context(), snap); err != nil {
				t.Fatalf("Restore: %v", err)
			}

			// No tasks exist yet on the restored backend: only the persisted
			// Service (with its DesiredCount) survives the round-trip.
			before, err := dst.ListTasksFiltered(ListTasksInput{
				Cluster: "restore-cluster", ServiceName: "restore-svc",
			})
			if err != nil {
				t.Fatalf("ListTasksFiltered (before): %v", err)
			}
			if len(before) != 0 {
				t.Fatalf("task count before reconcile = %d, want 0", len(before))
			}

			r := NewReconciler(dst)
			r.RunOnce(t.Context())

			after, err := dst.ListTasksFiltered(ListTasksInput{
				Cluster: "restore-cluster", ServiceName: "restore-svc",
			})
			if err != nil {
				t.Fatalf("ListTasksFiltered (after): %v", err)
			}
			if len(after) != tt.wantTasks {
				t.Errorf(
					"task count after RunOnce = %d, want %d (reconciler did not see restored service)",
					len(after), tt.wantTasks,
				)
			}
		})
	}
}

// Test_Snapshot_Restore_PreservesResourceTags proves that tags applied via
// TagResource on resources tracked only in the resourceTags side map (task
// definitions and daemon task definitions — clusters/services carry Tags
// inline on their own struct) survive a Snapshot/Restore round-trip.
// resourceTags was previously absent from backendSnapshot entirely, so every
// such tag was silently dropped on restore.
func Test_Snapshot_Restore_PreservesResourceTags(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		tags []Tag
	}{
		{
			name: "single tag",
			tags: []Tag{{Key: "env", Value: "prod"}},
		},
		{
			name: "multiple tags",
			tags: []Tag{{Key: "env", Value: "staging"}, {Key: "team", Value: "platform"}},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())

			td, err := src.RegisterTaskDefinition(RegisterTaskDefinitionInput{
				Family:               "tag-app-" + tt.name,
				ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx:good"}},
			})
			if err != nil {
				t.Fatalf("RegisterTaskDefinition: %v", err)
			}

			if tagErr := src.TagResource(td.TaskDefinitionArn, tt.tags); tagErr != nil {
				t.Fatalf("TagResource: %v", tagErr)
			}

			snap := src.Snapshot(t.Context())

			dst := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())
			if restoreErr := dst.Restore(t.Context(), snap); restoreErr != nil {
				t.Fatalf("Restore: %v", restoreErr)
			}

			got, err := dst.ListTagsForResource(td.TaskDefinitionArn)
			if err != nil {
				t.Fatalf("ListTagsForResource: %v", err)
			}

			if len(got) != len(tt.tags) {
				t.Fatalf("tag count after restore = %d, want %d (tags lost across restore)",
					len(got), len(tt.tags))
			}

			gotSet := make(map[string]string, len(got))
			for _, tag := range got {
				gotSet[tag.Key] = tag.Value
			}

			for _, want := range tt.tags {
				if gotSet[want.Key] != want.Value {
					t.Errorf("tag %q = %q, want %q", want.Key, gotSet[want.Key], want.Value)
				}
			}
		})
	}
}
