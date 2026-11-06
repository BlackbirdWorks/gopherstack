package container_test

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

func twoContainerSpec() corev1.PodSpec {
	return corev1.PodSpec{
		Containers: []corev1.Container{
			{Name: "web", Image: "nginx"},
			{Name: "side", Image: "busybox"},
		},
	}
}

func waiting(name, reason string) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:  name,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason, Message: "boom"}},
	}
}

func terminated(name string, code int32) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:  name,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code, Reason: "Error"}},
	}
}

func TestKubernetesRuntime_LaunchPod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate     func(*corev1.Pod)
		secret     map[string][]byte
		name       string
		wantReason string
		wantErr    bool
		wantSecret bool
	}{
		{
			name: "running with secret",
			mutate: func(p *corev1.Pod) {
				p.UID = types.UID("uid-1")
				p.Status.Phase = corev1.PodRunning
				p.Status.PodIP = "10.1.2.3"
			},
			secret:     map[string][]byte{"web.TOKEN": []byte("s3cret")},
			wantSecret: true,
		},
		{
			name: "main container already failed is a launch",
			mutate: func(p *corev1.Pod) {
				p.Status.Phase = corev1.PodFailed
				p.Status.ContainerStatuses = []corev1.ContainerStatus{terminated("web", 3)}
			},
		},
		{
			name: "image pull failure removes pod and secret",
			mutate: func(p *corev1.Pod) {
				p.Status.Phase = corev1.PodPending
				p.Status.ContainerStatuses = []corev1.ContainerStatus{waiting("web", "ImagePullBackOff")}
			},
			secret:     map[string][]byte{"k": []byte("v")},
			wantErr:    true,
			wantReason: "ImagePullBackOff",
		},
		{
			name: "init container failure",
			mutate: func(p *corev1.Pod) {
				p.Status.Phase = corev1.PodPending
				p.Status.InitContainerStatuses = []corev1.ContainerStatus{terminated("setup", 2)}
			},
			wantErr:    true,
			wantReason: "Error",
		},
		{
			name:       "evicted",
			mutate:     func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed; p.Status.Reason = "Evicted" },
			wantErr:    true,
			wantReason: "Evicted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt, cs := newK8s(t, tt.mutate, container.KubernetesConfig{})

			pod, err := rt.LaunchPod(t.Context(), container.PodLaunch{
				Name: "ecs-abc", Spec: twoContainerSpec(), Secret: tt.secret,
				Labels: map[string]string{"gopherstack.ecs.task": "arn:aws:ecs:us-east-1:1:task/c/abc"},
			})

			pods, listErr := cs.CoreV1().Pods(testNS).List(t.Context(), metav1.ListOptions{})
			require.NoError(t, listErr)

			sec, secErr := cs.CoreV1().Secrets(testNS).Get(t.Context(), "ecs-abc", metav1.GetOptions{})

			if tt.wantErr {
				require.ErrorIs(t, err, container.ErrPodFailed)

				pf, ok := errors.AsType[*container.PodFailedError](err)
				require.True(t, ok)
				assert.Equal(t, tt.wantReason, pf.Reason)
				assert.Empty(t, pods.Items, "failed pod must be removed")
				assert.True(t, apierrors.IsNotFound(secErr), "secret must be removed with the pod")

				return
			}

			require.NoError(t, err)
			require.Len(t, pods.Items, 1)
			assert.Equal(t, "ecs-abc", pod.Name)
			assert.Equal(t, corev1.RestartPolicyNever, pods.Items[0].Spec.RestartPolicy)
			assert.False(t, *pods.Items[0].Spec.AutomountServiceAccountToken)
			assert.Equal(t, "true", pods.Items[0].Labels[container.LabelManaged])
			assert.Equal(t, "arn:aws:ecs:us-east-1:1:task/c/abc", pods.Items[0].Annotations["gopherstack.ecs.task"])

			if !tt.wantSecret {
				assert.True(t, apierrors.IsNotFound(secErr))

				return
			}

			require.NoError(t, secErr)
			assert.Equal(t, tt.secret, sec.Data)
			require.Len(t, sec.OwnerReferences, 1)
			assert.Equal(t, "Pod", sec.OwnerReferences[0].Kind)
			assert.Equal(t, "ecs-abc", sec.OwnerReferences[0].Name)
		})
	}
}

func TestKubernetesRuntime_DeletePod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		withSecret bool
	}{
		{name: "pod and secret", withSecret: true},
		{name: "pod only"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			secret := map[string][]byte{"k": []byte("v")}
			rt, cs := newK8s(t, running, container.KubernetesConfig{})

			_, err := rt.LaunchPod(
				t.Context(),
				container.PodLaunch{Name: "ecs-del", Spec: twoContainerSpec(), Secret: secret},
			)
			require.NoError(t, err)

			require.NoError(t, rt.DeletePod(t.Context(), "ecs-del", tt.withSecret))
			require.NoError(t, rt.DeletePod(t.Context(), "ecs-del", tt.withSecret), "deleting twice is fine")

			_, getErr := cs.CoreV1().Pods(testNS).Get(t.Context(), "ecs-del", metav1.GetOptions{})
			assert.True(t, apierrors.IsNotFound(getErr))

			_, secErr := cs.CoreV1().Secrets(testNS).Get(t.Context(), "ecs-del", metav1.GetOptions{})
			assert.Equal(t, tt.withSecret, apierrors.IsNotFound(secErr))
		})
	}
}

type podEvent struct {
	pod  corev1.Pod
	gone bool
}

func TestKubernetesRuntime_FollowPod(t *testing.T) {
	t.Parallel()

	rt, cs := newK8s(t, running, container.KubernetesConfig{})

	_, err := rt.LaunchPod(t.Context(), container.PodLaunch{Name: "ecs-follow", Spec: twoContainerSpec()})
	require.NoError(t, err)

	var (
		mu     sync.Mutex
		events []podEvent
	)

	seen := make(chan struct{}, 8)
	done := make(chan error, 1)

	go func() {
		done <- rt.FollowPod(t.Context(), "ecs-follow", func(p *corev1.Pod, gone bool) bool {
			mu.Lock()
			events = append(events, podEvent{pod: *p, gone: gone})
			mu.Unlock()

			seen <- struct{}{}

			return gone
		})
	}()

	<-seen

	cur, err := cs.CoreV1().Pods(testNS).Get(t.Context(), "ecs-follow", metav1.GetOptions{})
	require.NoError(t, err)

	cur.Status.ContainerStatuses = []corev1.ContainerStatus{terminated("side", 0)}
	_, err = cs.CoreV1().Pods(testNS).UpdateStatus(t.Context(), cur, metav1.UpdateOptions{})
	require.NoError(t, err)

	<-seen

	require.NoError(t, cs.CoreV1().Pods(testNS).Delete(t.Context(), "ecs-follow", metav1.DeleteOptions{}))
	require.NoError(t, <-done)

	mu.Lock()
	defer mu.Unlock()

	require.GreaterOrEqual(t, len(events), 3)
	assert.False(t, events[0].gone)
	assert.Empty(t, events[0].pod.Status.ContainerStatuses)
	assert.True(t, events[len(events)-1].gone)

	exited := false

	for _, ev := range events {
		for _, cs := range ev.pod.Status.ContainerStatuses {
			exited = exited || (cs.Name == "side" && cs.State.Terminated != nil)
		}
	}

	assert.True(t, exited, "the update must be delivered")
}

func TestKubernetesRuntime_FollowPodMissing(t *testing.T) {
	t.Parallel()

	rt, _ := newK8s(t, nil, container.KubernetesConfig{})

	var gone bool

	require.NoError(t, rt.FollowPod(t.Context(), "nope", func(_ *corev1.Pod, g bool) bool {
		gone = g

		return true
	}))
	assert.True(t, gone)
}

func TestKubernetesRuntime_StreamPodLogs(t *testing.T) {
	t.Parallel()

	rt, _ := newK8s(t, running, container.KubernetesConfig{})

	_, err := rt.LaunchPod(t.Context(), container.PodLaunch{Name: "ecs-logs", Spec: twoContainerSpec()})
	require.NoError(t, err)

	var sb stringsBuilder

	require.NoError(t, rt.StreamPodLogs(t.Context(), "ecs-logs", "web", &sb))
	assert.Equal(t, "fake logs", sb.String())
}

type stringsBuilder struct{ b []byte }

func (s *stringsBuilder) Write(p []byte) (int, error) {
	s.b = append(s.b, p...)

	return len(p), nil
}
func (s *stringsBuilder) String() string { return string(s.b) }

type fakeExecutor struct {
	got chan container.ExecRequest
}

func (f *fakeExecutor) StreamExec(ctx context.Context, req container.ExecRequest) error {
	f.got <- req

	buf := make([]byte, 64)

	n, err := req.Stdin.Read(buf)
	if err != nil {
		return nil //nolint:nilerr // stdin closed ends the session
	}

	_, _ = req.Stdout.Write(append([]byte("echo:"), buf[:n]...))

	size := <-req.Resize
	_, _ = req.Stdout.Write([]byte{byte(size.Cols), byte(size.Rows)})

	<-ctx.Done()

	return nil
}

func TestKubernetesRuntime_ExecInteractive(t *testing.T) {
	t.Parallel()

	ex := &fakeExecutor{got: make(chan container.ExecRequest, 1)}
	rt, _ := newK8s(t, nil, container.KubernetesConfig{Executor: ex})

	sh, err := rt.ExecInteractive(t.Context(), "ecs-x", "web", []string{"sh"}, 80, 24)
	require.NoError(t, err)

	req := <-ex.got
	assert.Equal(t, "ecs-x", req.Pod)
	assert.Equal(t, "web", req.Container)
	assert.Equal(t, []string{"sh"}, req.Command)

	_, err = sh.Write([]byte("hi"))
	require.NoError(t, err)

	buf := make([]byte, 7)
	_, err = io.ReadFull(sh, buf)
	require.NoError(t, err)
	assert.Equal(t, "echo:hi", string(buf))

	size := make([]byte, 2)
	_, err = io.ReadFull(sh, size)
	require.NoError(t, err)
	assert.Equal(t, []byte{80, 24}, size)

	require.NoError(t, sh.Close())
	require.NoError(t, sh.Close(), "close is idempotent")
}

func TestKubernetesRuntime_ExecWithoutCluster(t *testing.T) {
	t.Parallel()

	rt, _ := newK8s(t, nil, container.KubernetesConfig{})

	_, err := rt.ExecInteractive(t.Context(), "p", "c", []string{"sh"}, 1, 1)
	require.ErrorIs(t, err, container.ErrExecNoConfig)
}

func TestSplitContainerRef(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ref, pod, name string
		ok             bool
	}{
		{ref: "ecs-a/web", pod: "ecs-a", name: "web", ok: true},
		{ref: "web"},
		{ref: "/web"},
		{ref: "pod/"},
	}

	for _, tt := range tests {
		t.Run(tt.ref, func(t *testing.T) {
			t.Parallel()

			pod, name, ok := container.SplitContainerRef(tt.ref)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.pod, pod)
			assert.Equal(t, tt.name, name)
		})
	}
}
