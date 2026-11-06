package ecs

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	eventuallyTimeout = 10 * time.Second
	eventuallyTick    = time.Millisecond
	k8sTestNS         = "ecs-test"
	k8sTestTaskArn    = "arn:aws:ecs:us-east-1:000000000000:task/c/0123456789abcdef0123456789abcdef"
	k8sTestPod        = "ecs-0123456789abcdef0123456789abcdef"
)

type k8sHarness struct {
	cs     *fake.Clientset
	rt     *container.KubernetesRuntime
	runner *k8sRunner
	ev     *k8sRecorder
}

type k8sExit struct {
	name string
	code int
}

type k8sRecorder struct {
	exits   []k8sExit
	health  []string
	reasons []string
	mu      sync.Mutex
}

func (r *k8sRecorder) exit(_, name string, code int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.exits = append(r.exits, k8sExit{name: name, code: code})
}

func (r *k8sRecorder) healthChange(_, name, status string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.health = append(r.health, name+"="+status)
}

func (r *k8sRecorder) reason(_, name, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.reasons = append(r.reasons, name+": "+reason)
}

func (r *k8sRecorder) exitsSeen() []k8sExit {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]k8sExit(nil), r.exits...)
}

func (r *k8sRecorder) healthSeen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.health...)
}

func (r *k8sRecorder) reasonsSeen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.reasons...)
}

// podRunning marks every container of a new Pod running and gives the Pod an IP.
func podRunning(p *corev1.Pod) {
	p.Status.Phase = corev1.PodRunning
	p.Status.PodIP = "10.42.0.7"
	p.Status.ContainerStatuses = nil

	for _, c := range p.Spec.Containers {
		p.Status.ContainerStatuses = append(p.Status.ContainerStatuses, corev1.ContainerStatus{
			Name: c.Name, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		})
	}
}

func newK8sHarness(t *testing.T, onAdded func(*corev1.Pod)) *k8sHarness {
	t.Helper()

	cs := fake.NewClientset()

	if onAdded != nil {
		ctx, cancel := context.WithCancel(t.Context())
		w, err := cs.CoreV1().Pods(k8sTestNS).Watch(ctx, metav1.ListOptions{})
		require.NoError(t, err)

		var wg sync.WaitGroup

		wg.Go(func() {
			for ev := range w.ResultChan() {
				pod, ok := ev.Object.(*corev1.Pod)
				if !ok || ev.Type != watch.Added {
					continue
				}

				upd := pod.DeepCopy()
				onAdded(upd)

				_, _ = cs.CoreV1().Pods(k8sTestNS).UpdateStatus(ctx, upd, metav1.UpdateOptions{})
			}
		})

		t.Cleanup(func() {
			cancel()
			w.Stop()
			wg.Wait()
		})
	}

	rt, err := container.NewRuntime(container.Config{
		Runtime:    container.RuntimeKubernetes,
		Kubernetes: &container.KubernetesConfig{Client: cs, Namespace: k8sTestNS},
	})
	require.NoError(t, err)

	k, ok := rt.(*container.KubernetesRuntime)
	require.True(t, ok)

	t.Cleanup(func() { _ = rt.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	runner := newK8sRunner(ctx, k)
	rec := &k8sRecorder{}
	runner.SetTaskCompletionHandler(rec.exit)
	runner.SetContainerHealthHandler(rec.healthChange)
	runner.SetContainerReasonHandler(rec.reason)

	return &k8sHarness{cs: cs, rt: k, runner: runner, ev: rec}
}

func (e *k8sHarness) pods(t *testing.T) []corev1.Pod {
	t.Helper()

	list, err := e.cs.CoreV1().Pods(k8sTestNS).List(t.Context(), metav1.ListOptions{})
	require.NoError(t, err)

	return list.Items
}

func (e *k8sHarness) updatePod(t *testing.T, mutate func(*corev1.Pod)) {
	t.Helper()

	pod, err := e.cs.CoreV1().Pods(k8sTestNS).Get(t.Context(), k8sTestPod, metav1.GetOptions{})
	require.NoError(t, err)

	mutate(pod)

	_, err = e.cs.CoreV1().Pods(k8sTestNS).UpdateStatus(t.Context(), pod, metav1.UpdateOptions{})
	require.NoError(t, err)
}

func newTestTask() *Task {
	return &Task{TaskArn: k8sTestTaskArn, ClusterArn: "arn:aws:ecs:us-east-1:0:cluster/c"}
}

func twoContainerTD() *TaskDefinition {
	return &TaskDefinition{ContainerDefinitions: []ContainerDefinition{
		{Name: "web", Image: "nginx", Essential: true},
		{Name: "side", Image: "busybox"},
	}}
}

func terminate(name string, code int32, reason string) func(*corev1.Pod) {
	return func(p *corev1.Pod) {
		for i := range p.Status.ContainerStatuses {
			if p.Status.ContainerStatuses[i].Name == name {
				p.Status.ContainerStatuses[i].State = corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: code, Reason: reason},
				}
			}
		}
	}
}

func TestK8sRunner_RunAndStop(t *testing.T) {
	t.Parallel()

	env := newK8sHarness(t, podRunning)
	task := newTestTask()

	require.NoError(t, env.runner.RunTask(task, twoContainerTD()))

	pods := env.pods(t)
	require.Len(t, pods, 1)
	assert.Equal(t, k8sTestPod, pods[0].Name)
	assert.Equal(t, task.TaskArn, pods[0].Annotations["gopherstack.resource"][:len(task.TaskArn)])
	require.Len(t, pods[0].Spec.Containers, 2, "one Pod per task, every container in it")

	assert.Equal(t, []string{k8sTestPod + "/web", k8sTestPod + "/side"}, env.runner.ContainerIDs(task.TaskArn))
	assert.Equal(t, "10.42.0.7", env.runner.TaskAddress(task.TaskArn))

	env.updatePod(t, terminate("side", 0, "Completed"))
	require.Eventually(t, func() bool { return len(env.ev.exitsSeen()) == 1 }, eventuallyTimeout, eventuallyTick)

	env.updatePod(t, terminate("web", 7, "Error"))
	require.Eventually(t, func() bool { return len(env.ev.exitsSeen()) == 2 }, eventuallyTimeout, eventuallyTick)

	assert.Equal(t, []k8sExit{{"side", 0}, {"web", 7}}, env.ev.exitsSeen())

	require.NoError(t, env.runner.StopTask(task))
	assert.Empty(t, env.pods(t))
	assert.Empty(t, env.runner.ContainerIDs(task.TaskArn))
	require.NoError(t, env.runner.StopTask(task), "stopping twice is fine")
}

func TestK8sRunner_Secrets(t *testing.T) {
	t.Parallel()

	env := newK8sHarness(t, func(p *corev1.Pod) {
		podRunning(p)
		p.UID = "uid-9"
	})
	env.runner.SetSecretResolver(func(_ context.Context, valueFrom string) (string, error) {
		return "resolved:" + valueFrom, nil
	})

	td := &TaskDefinition{ContainerDefinitions: []ContainerDefinition{
		{
			Name:        "web",
			Image:       "nginx",
			Essential:   true,
			Environment: []KeyValuePair{{Name: "PLAIN", Value: "yes"}},
			Secrets: []SecretReference{
				{Name: "DB_PASSWORD", ValueFrom: "arn:aws:secretsmanager:us-east-1:0:secret:db"},
			},
		},
	}}

	task := newTestTask()
	require.NoError(t, env.runner.RunTask(task, td))

	sec, err := env.cs.CoreV1().Secrets(k8sTestNS).Get(t.Context(), k8sTestPod, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "resolved:arn:aws:secretsmanager:us-east-1:0:secret:db", string(sec.Data["web.DB_PASSWORD"]))
	require.Len(t, sec.OwnerReferences, 1, "the Pod owns the Secret so it is collected with it")
	assert.Equal(t, k8sTestPod, sec.OwnerReferences[0].Name)

	envs := env.pods(t)[0].Spec.Containers[0].Env
	byName := map[string]corev1.EnvVar{}

	for _, e := range envs {
		byName[e.Name] = e
	}

	assert.Equal(t, "yes", byName["PLAIN"].Value)
	assert.Empty(t, byName["DB_PASSWORD"].Value, "secret value is not in the Pod spec")
	assert.Equal(t, "web.DB_PASSWORD", byName["DB_PASSWORD"].ValueFrom.SecretKeyRef.Key)

	require.NoError(t, env.runner.StopTask(task))

	_, err = env.cs.CoreV1().Secrets(k8sTestNS).Get(t.Context(), k8sTestPod, metav1.GetOptions{})
	assert.True(t, apierrors.IsNotFound(err), "stopping the task removes its Secret")
}

func TestK8sRunner_LaunchFailures(t *testing.T) {
	t.Parallel()

	pullFail := func(p *corev1.Pod) {
		p.Status.Phase = corev1.PodPending
		p.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name: "web", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason: "ImagePullBackOff", Message: "pull access denied",
			}},
		}}
	}
	initFail := func(p *corev1.Pod) {
		p.Status.Phase = corev1.PodPending
		p.Status.InitContainerStatuses = []corev1.ContainerStatus{
			{
				Name: "migrate",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: 4, Reason: "Error"},
				},
			},
		}
	}

	tests := []struct {
		onAdded    func(*corev1.Pod)
		td         *TaskDefinition
		name       string
		wantReason string
		wantPods   int
	}{
		{
			name: "image pull", onAdded: pullFail, td: twoContainerTD(),
			wantReason: "CannotPullContainerError: ",
		},
		{
			name: "init container fails", onAdded: initFail,
			td: &TaskDefinition{ContainerDefinitions: []ContainerDefinition{
				{
					Name:      "web",
					Image:     "nginx",
					Essential: true,
					DependsOn: []ContainerDependency{{ContainerName: "migrate", Condition: dependsSuccess}},
				},
				{Name: "migrate", Image: "m"},
			}},
			wantReason: "Task failed to start: ",
		},
		{
			name: "unsupported spec is refused before any Pod exists", onAdded: podRunning,
			td: &TaskDefinition{
				Volumes:              []Volume{{Name: "h", Host: &HostVolumeProperties{SourcePath: "/var/lib"}}},
				ContainerDefinitions: []ContainerDefinition{{Name: "web", Image: "nginx", Essential: true}},
			},
			wantReason: "Task failed to start: ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newK8sHarness(t, tt.onAdded)

			err := env.runner.RunTask(newTestTask(), tt.td)
			require.Error(t, err)

			le, ok := err.(*taskLaunchError) //nolint:errorlint // asserting the concrete type
			require.True(t, ok, "%T", err)
			assert.Contains(t, le.Reason, tt.wantReason)
			assert.Empty(t, env.pods(t), "a failed launch leaves nothing behind")
			assert.Empty(t, env.runner.ContainerIDs(k8sTestTaskArn))
		})
	}
}

func TestK8sRunner_UnsupportedSpecIsErrSpecUnsupported(t *testing.T) {
	t.Parallel()

	env := newK8sHarness(t, podRunning)
	td := &TaskDefinition{
		ContainerDefinitions: []ContainerDefinition{{Name: "a", Image: "i", Essential: true, Privileged: true}},
	}

	err := env.runner.RunTask(newTestTask(), td)
	require.ErrorIs(t, err, container.ErrSpecUnsupported)
}

func TestK8sRunner_PodDeletedExternally(t *testing.T) {
	t.Parallel()

	env := newK8sHarness(t, podRunning)
	require.NoError(t, env.runner.RunTask(newTestTask(), twoContainerTD()))

	require.NoError(t, env.cs.CoreV1().Pods(k8sTestNS).Delete(t.Context(), k8sTestPod, metav1.DeleteOptions{}))

	require.Eventually(t, func() bool { return len(env.ev.exitsSeen()) == 2 }, eventuallyTimeout, eventuallyTick)

	for _, x := range env.ev.exitsSeen() {
		assert.Equal(t, 137, x.code, x.name)
	}

	require.NoError(t, env.runner.StopTask(newTestTask()))
}

func TestK8sRunner_OOMReason(t *testing.T) {
	t.Parallel()

	env := newK8sHarness(t, podRunning)
	require.NoError(t, env.runner.RunTask(newTestTask(), twoContainerTD()))

	env.updatePod(t, terminate("web", 137, "OOMKilled"))
	require.Eventually(t, func() bool { return len(env.ev.exitsSeen()) == 1 }, eventuallyTimeout, eventuallyTick)

	assert.Equal(t, []string{"web: " + oomReason}, env.ev.reasonsSeen())
	require.NoError(t, env.runner.StopTask(newTestTask()))
}

func TestK8sRunner_Health(t *testing.T) {
	t.Parallel()

	env := newK8sHarness(t, podRunning)
	td := &TaskDefinition{ContainerDefinitions: []ContainerDefinition{
		{Name: "web", Image: "nginx", Essential: true, HealthCheck: &HealthCheck{Command: []string{"CMD", "true"}}},
		{Name: "plain", Image: "busybox"},
	}}

	require.NoError(t, env.runner.RunTask(newTestTask(), td))

	ready := func(v bool) func(*corev1.Pod) {
		return func(p *corev1.Pod) {
			for i := range p.Status.ContainerStatuses {
				p.Status.ContainerStatuses[i].Ready = v
			}
		}
	}

	require.Eventually(t, func() bool { return len(env.ev.healthSeen()) == 1 }, eventuallyTimeout, eventuallyTick)

	env.updatePod(t, ready(true))
	require.Eventually(t, func() bool { return len(env.ev.healthSeen()) == 2 }, eventuallyTimeout, eventuallyTick)

	env.updatePod(t, ready(false))
	require.Eventually(t, func() bool { return len(env.ev.healthSeen()) == 3 }, eventuallyTimeout, eventuallyTick)

	assert.Equal(t, []string{"web=UNKNOWN", "web=HEALTHY", "web=UNHEALTHY"}, env.ev.healthSeen(),
		"only containers with a health check report, and UNHEALTHY needs a prior HEALTHY")
	require.NoError(t, env.runner.StopTask(newTestTask()))
}

func TestK8sRunner_Logs(t *testing.T) {
	t.Parallel()

	env := newK8sHarness(t, podRunning)
	cw := &mockECSCWLogsBackend{}
	env.runner.SetCWLogsBackend(cw)

	td := &TaskDefinition{ContainerDefinitions: []ContainerDefinition{
		{
			Name: "web", Image: "nginx", Essential: true,
			LogConfiguration: &LogConfiguration{LogDriver: logDriverAwslogs, Options: map[string]string{
				optAwslogsGroup: "/ecs/app", optAwslogsStreamPrefix: "web",
			}},
		},
		{Name: "quiet", Image: "busybox"},
	}}

	task := newTestTask()
	require.NoError(t, env.runner.RunTask(task, td))

	require.Eventually(t, func() bool { return len(cw.putCalls()) == 1 }, eventuallyTimeout, eventuallyTick)

	put := cw.putCalls()[0]
	assert.Equal(t, "/ecs/app", put.group)
	assert.Equal(t, "web/web/"+taskIDFromARN(task.TaskArn), put.stream)
	assert.Equal(t, []string{"fake logs"}, put.messages)
	assert.Equal(t, []ensureCall{{group: "/ecs/app", stream: "web/web/" + taskIDFromARN(task.TaskArn)}}, cw.calls())

	require.NoError(t, env.runner.StopTask(task))
}

func TestK8sRunner_StopDuringLaunch(t *testing.T) {
	t.Parallel()

	env := newK8sHarness(t, func(p *corev1.Pod) { p.Status.Phase = corev1.PodPending })
	task := newTestTask()

	errc := make(chan error, 1)

	go func() { errc <- env.runner.RunTask(task, twoContainerTD()) }()

	require.Eventually(t, func() bool { return len(env.pods(t)) == 1 }, eventuallyTimeout, eventuallyTick)

	require.NoError(t, env.runner.StopTask(task))
	require.ErrorIs(t, <-errc, errTaskStoppedDuringLaunch)
	require.Eventually(t, func() bool { return len(env.pods(t)) == 0 }, eventuallyTimeout, eventuallyTick)
}

func TestNewTaskRunner_KubernetesSelection(t *testing.T) {
	t.Setenv("GOPHERSTACK_ECS_RUNTIME", "docker")
	t.Setenv("CONTAINER_RUNTIME", "kubernetes")
	t.Setenv("CONTAINER_K8S_KUBECONFIG", "/nonexistent/kubeconfig")

	_, err := newTaskRunner(t.Context())
	require.ErrorIs(t, err, container.ErrUnavailable, "kubernetes is chosen over the docker daemon")
}

func TestK8sRunner_BackendLifecycle(t *testing.T) {
	t.Parallel()

	h := newK8sHarness(t, podRunning)
	b := NewInMemoryBackend("123456789012", "us-east-1", h.runner)
	b.wireRunnerHooks(h.runner)

	reg := &fakeELBv2Registrar{}
	b.SetELBv2Registrar(reg)

	s := newFargateServiceWithLB(t, b, "svc-k8s")
	require.NoError(t, b.StartTaskForService(s.cluster, s.service, s.tdArn))
	b.awaitLaunches()

	arns, err := b.ListTasks(s.cluster)
	require.NoError(t, err)
	require.Len(t, arns, 1)

	got, _, err := b.DescribeTasks(s.cluster, arns)
	require.NoError(t, err)
	require.Equal(t, statusRunning, got[0].LastStatus)

	ip, ok := privateIPFromAttachments(got[0].Attachments)
	require.True(t, ok)
	assert.Equal(t, "10.42.0.7", ip, "the task's address is its Pod IP")
	assert.Contains(t, got[0].Containers[0].RuntimeID, "/app", "runtime id is pod/container")

	require.Equal(t, 1, reg.registeredCount())
	assert.Equal(t, []ELBTarget{{ID: "10.42.0.7", Port: 8080}}, reg.registered[0].targets)

	pods := h.pods(t)
	require.Len(t, pods, 1)

	pod := pods[0].DeepCopy()
	terminate("app", 3, "Error")(pod)
	_, err = h.cs.CoreV1().Pods(k8sTestNS).UpdateStatus(t.Context(), pod, metav1.UpdateOptions{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		cur, _, descErr := b.DescribeTasks(s.cluster, arns)

		return descErr == nil && cur[0].LastStatus == statusStopped
	}, eventuallyTimeout, eventuallyTick)

	cur, _, err := b.DescribeTasks(s.cluster, arns)
	require.NoError(t, err)
	assert.Equal(t, stopCodeEssentialContainerExited, cur[0].StopCode)
	require.NotNil(t, cur[0].Containers[0].ExitCode)
	assert.Equal(t, 3, *cur[0].Containers[0].ExitCode)

	require.Eventually(t, func() bool { return len(h.pods(t)) == 0 }, eventuallyTimeout, eventuallyTick)
	assert.Equal(t, 1, reg.deregisteredCount())
}
