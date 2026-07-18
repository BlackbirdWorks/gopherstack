package ecs

import (
	"testing"
)

// Test_Snapshot_Restore_FullState is the Phase 3.3 (pkgs/store conversion)
// round-trip test: it seeds one instance of every resource kind the backend
// tracks -- both the store.Table-backed resources (clusters, services, tasks,
// container instances, task sets, capacity providers, account settings, task
// protections, service deployments, express gateway services, daemons, daemon
// revisions, daemon deployments) and the resources deliberately left as raw
// maps by the conversion (attributes, resourceTags, daemonTaskDefinitions) --
// snapshots the backend, restores into a fresh one, and asserts every
// resource survived the round-trip with its identifying fields intact.
func Test_Snapshot_Restore_FullState(t *testing.T) {
	t.Parallel()

	src := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())

	tdArn := registerSimpleTaskDef(t, src, "full-state-app", "nginx:good")

	_, err := src.CreateCluster(CreateClusterInput{ClusterName: "full-state-cluster"})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	svc, err := src.CreateService(CreateServiceInput{
		ServiceName:    "full-state-svc",
		Cluster:        "full-state-cluster",
		TaskDefinition: tdArn,
		DesiredCount:   1,
	})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}

	tasks, err := src.RunTask(RunTaskInput{
		Cluster: "full-state-cluster", TaskDefinition: tdArn, Count: 1,
	})
	if err != nil {
		t.Fatalf("RunTask: %v", err)
	}

	taskArn := tasks[0].TaskArn

	_, _, err = src.UpdateTaskProtection("full-state-cluster", []string{taskArn}, true, nil)
	if err != nil {
		t.Fatalf("UpdateTaskProtection: %v", err)
	}

	ci, err := src.RegisterContainerInstance("full-state-cluster", "i-0123456789")
	if err != nil {
		t.Fatalf("RegisterContainerInstance: %v", err)
	}

	ts, err := src.CreateTaskSet(CreateTaskSetInput{
		Cluster:        "full-state-cluster",
		Service:        "full-state-svc",
		TaskDefinition: tdArn,
	})
	if err != nil {
		t.Fatalf("CreateTaskSet: %v", err)
	}

	_, err = src.CreateCapacityProvider(CreateCapacityProviderInput{Name: "full-state-cp"})
	if err != nil {
		t.Fatalf("CreateCapacityProvider: %v", err)
	}

	_, err = src.PutAccountSetting("containerInsights", "enabled", "")
	if err != nil {
		t.Fatalf("PutAccountSetting: %v", err)
	}

	egs, err := src.CreateExpressGatewayService(CreateExpressGatewayServiceInput{
		ServiceName:           "full-state-egs",
		Cluster:               "full-state-cluster",
		ExecutionRoleArn:      "arn:aws:iam::123456789012:role/exec",
		InfrastructureRoleArn: "arn:aws:iam::123456789012:role/infra",
	})
	if err != nil {
		t.Fatalf("CreateExpressGatewayService: %v", err)
	}

	daemonTD, err := src.RegisterDaemonTaskDefinition(RegisterDaemonTaskDefinitionInput{
		Family:               "full-state-daemon-td",
		ContainerDefinitions: []DaemonContainerDefinition{{Name: "agent", Image: "agent:good"}},
	})
	if err != nil {
		t.Fatalf("RegisterDaemonTaskDefinition: %v", err)
	}

	daemon, err := src.CreateDaemon(CreateDaemonInput{
		DaemonName:              "full-state-daemon",
		ClusterArn:              "full-state-cluster",
		DaemonTaskDefinitionArn: daemonTD.DaemonTaskDefinitionArn,
		CapacityProviderArns:    []string{"FARGATE"},
	})
	if err != nil {
		t.Fatalf("CreateDaemon: %v", err)
	}

	_, err = src.PutAttributes("full-state-cluster", []Attribute{
		{Name: "custom", Value: "value", TargetID: ci.ContainerInstanceArn},
	})
	if err != nil {
		t.Fatalf("PutAttributes: %v", err)
	}

	err = src.TagResource(tdArn, []Tag{{Key: "env", Value: "prod"}})
	if err != nil {
		t.Fatalf("TagResource: %v", err)
	}

	snap := src.Snapshot(t.Context())
	if len(snap) == 0 {
		t.Fatalf("Snapshot returned empty data")
	}

	dst := NewInMemoryBackend("123456789012", "us-east-1", NewNoopRunner())

	err = dst.Restore(t.Context(), snap)
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}

	assertFullStateRestored(t, dst, fullStateFixture{
		taskArn:      taskArn,
		containerArn: ci.ContainerInstanceArn,
		serviceArn:   svc.ServiceArn,
		taskSetArn:   ts.TaskSetArn,
		egsArn:       egs.ServiceArn,
		daemonArn:    daemon.DaemonArn,
		tdArn:        tdArn,
	})
}

// fullStateFixture carries the identifiers Test_Snapshot_Restore_FullState
// seeded on the source backend, so assertFullStateRestored can look each one
// up again on the restored backend.
type fullStateFixture struct {
	taskArn      string
	containerArn string
	serviceArn   string
	taskSetArn   string
	egsArn       string
	daemonArn    string
	tdArn        string
}

// assertFullStateRestored verifies every resource kind seeded by
// Test_Snapshot_Restore_FullState is present on the restored backend b. Split
// across a handful of helpers (rather than one large function) to keep each
// one's cyclomatic complexity down.
func assertFullStateRestored(t *testing.T, b *InMemoryBackend, f fullStateFixture) {
	t.Helper()

	assertCoreResourcesRestored(t, b, f)
	assertDaemonResourcesRestored(t, b, f)
	assertRawMapResourcesRestored(t, b, f)
}

// assertCoreResourcesRestored checks the store.Table-backed cluster/service/
// task/container-instance/task-set/capacity-provider/account-setting/
// express-gateway-service resources.
func assertCoreResourcesRestored(t *testing.T, b *InMemoryBackend, f fullStateFixture) {
	t.Helper()

	clusters, _, err := b.DescribeClusters([]string{"full-state-cluster"})
	if err != nil || len(clusters) != 1 {
		t.Fatalf("DescribeClusters: got %d clusters, err=%v, want 1 cluster", len(clusters), err)
	}

	services, _, err := b.DescribeServices("full-state-cluster", []string{"full-state-svc"})
	if err != nil || len(services) != 1 {
		t.Fatalf("DescribeServices: got %d services, err=%v, want 1 service", len(services), err)
	}
	if services[0].ServiceArn != f.serviceArn {
		t.Errorf("restored service ARN = %q, want %q", services[0].ServiceArn, f.serviceArn)
	}

	tasksOut, _, err := b.DescribeTasks("full-state-cluster", []string{f.taskArn})
	if err != nil || len(tasksOut) != 1 {
		t.Fatalf("DescribeTasks: got %d tasks, err=%v, want 1 task", len(tasksOut), err)
	}

	protections, _, err := b.GetTaskProtection("full-state-cluster", []string{f.taskArn})
	if err != nil || len(protections) != 1 || !protections[0].ProtectionEnabled {
		t.Fatalf("GetTaskProtection: got %+v, err=%v, want one enabled protection", protections, err)
	}

	instances, _, err := b.DescribeContainerInstances("full-state-cluster", []string{f.containerArn})
	if err != nil || len(instances) != 1 {
		t.Fatalf("DescribeContainerInstances: got %d instances, err=%v, want 1 instance", len(instances), err)
	}

	taskSets, err := b.DescribeTaskSets("full-state-cluster", "full-state-svc", []string{f.taskSetArn})
	if err != nil || len(taskSets) != 1 {
		t.Fatalf("DescribeTaskSets: got %d task sets, err=%v, want 1 task set", len(taskSets), err)
	}

	caps, _, err := b.DescribeCapacityProviders([]string{"full-state-cp"})
	if err != nil || len(caps) != 1 {
		t.Fatalf("DescribeCapacityProviders: got %d providers, err=%v, want 1 provider", len(caps), err)
	}

	settings, err := b.ListAccountSettings("containerInsights", "")
	if err != nil || len(settings) != 1 {
		t.Fatalf("ListAccountSettings: got %d settings, err=%v, want 1 setting", len(settings), err)
	}

	egs, err := b.DescribeExpressGatewayService(f.egsArn)
	if err != nil || egs == nil {
		t.Fatalf("DescribeExpressGatewayService: %v, err=%v", egs, err)
	}
}

// assertDaemonResourcesRestored checks the daemon, daemon-revision, and
// daemon-deployment store.Table resources plus the raw daemonTaskDefinitions map.
func assertDaemonResourcesRestored(t *testing.T, b *InMemoryBackend, f fullStateFixture) {
	t.Helper()

	daemon, err := b.DescribeDaemon(f.daemonArn)
	if err != nil || daemon == nil {
		t.Fatalf("DescribeDaemon: %v, err=%v", daemon, err)
	}

	if daemon.CurrentDaemonRevisionArn == "" {
		t.Error("restored daemon has no CurrentDaemonRevisionArn")
	} else {
		revs, failures, revErr := b.DescribeDaemonRevisions([]string{daemon.CurrentDaemonRevisionArn})
		if revErr != nil || len(revs) != 1 || len(failures) != 0 {
			t.Errorf("DescribeDaemonRevisions: got %d revs, %d failures, err=%v, want 1 rev, 0 failures",
				len(revs), len(failures), revErr)
		}
	}

	if daemon.DeploymentArn == "" {
		t.Error("restored daemon has no DeploymentArn")
	} else {
		assertDaemonDeploymentRestored(t, b, daemon.DeploymentArn)
	}

	daemonTDs, err := b.ListDaemonTaskDefinitions(ListDaemonTaskDefinitionsInput{Family: "full-state-daemon-td"})
	if err != nil || len(daemonTDs) != 1 {
		t.Fatalf("ListDaemonTaskDefinitions: got %d, err=%v, want 1", len(daemonTDs), err)
	}
}

// assertDaemonDeploymentRestored checks a single daemon deployment ARN.
func assertDaemonDeploymentRestored(t *testing.T, b *InMemoryBackend, deploymentArn string) {
	t.Helper()

	deps, failures, err := b.DescribeDaemonDeployments([]string{deploymentArn})
	if err != nil || len(deps) != 1 || len(failures) != 0 {
		t.Errorf("DescribeDaemonDeployments: got %d deployments, %d failures, err=%v, want 1, 0",
			len(deps), len(failures), err)
	}
}

// assertRawMapResourcesRestored checks the resources deliberately left as raw
// maps by the Phase 3.3 conversion: attributes and resourceTags.
func assertRawMapResourcesRestored(t *testing.T, b *InMemoryBackend, f fullStateFixture) {
	t.Helper()

	attrs, err := b.ListAttributes("full-state-cluster", "", "custom", "")
	if err != nil || len(attrs) != 1 {
		t.Fatalf("ListAttributes: got %d attributes, err=%v, want 1 attribute", len(attrs), err)
	}

	tags, err := b.ListTagsForResource(f.tdArn)
	if err != nil || len(tags) != 1 || tags[0].Key != "env" || tags[0].Value != "prod" {
		t.Fatalf("ListTagsForResource: got %+v, err=%v, want one env=prod tag", tags, err)
	}
}

// Test_Restore_RebuildsServiceIndex proves that after a Snapshot/Restore
// round-trip, the deployment reconciler still sees pre-existing services.
//
// getServicesForReconciler (store.go) reads only the flat serviceIndex map
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
