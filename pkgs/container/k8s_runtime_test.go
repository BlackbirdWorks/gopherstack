package container_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

const testNS = "gs-test"

var errList = errors.New("forbidden")

// podDriver reacts to every pod added to cs by passing it through onAdded and storing the result.
type podDriver struct {
	cs      *fake.Clientset
	onAdded func(*corev1.Pod)
}

func (d *podDriver) run(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	w, err := d.cs.CoreV1().Pods(testNS).Watch(ctx, metav1.ListOptions{})
	require.NoError(t, err)

	var wg sync.WaitGroup

	wg.Go(func() {
		for ev := range w.ResultChan() {
			pod, ok := ev.Object.(*corev1.Pod)
			if !ok || ev.Type != watch.Added {
				continue
			}

			upd := pod.DeepCopy()
			d.onAdded(upd)

			_, _ = d.cs.CoreV1().Pods(testNS).UpdateStatus(ctx, upd, metav1.UpdateOptions{})
		}
	})

	t.Cleanup(func() {
		cancel()
		w.Stop()
		wg.Wait()
	})
}

func running(p *corev1.Pod) { p.Status.Phase = corev1.PodRunning }

func newK8s(
	t *testing.T,
	onAdded func(*corev1.Pod),
	kc container.KubernetesConfig,
) (*container.KubernetesRuntime, *fake.Clientset) {
	t.Helper()

	cs := fake.NewClientset()
	if onAdded != nil {
		(&podDriver{cs: cs, onAdded: onAdded}).run(t)
	}

	kc.Client = cs
	kc.Namespace = testNS

	rt, err := container.NewRuntime(container.Config{Runtime: container.RuntimeKubernetes, Kubernetes: &kc})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Close() })

	k, ok := rt.(*container.KubernetesRuntime)
	require.True(t, ok)

	return k, cs
}

func onlyPod(t *testing.T, cs *fake.Clientset) corev1.Pod {
	t.Helper()

	list, err := cs.CoreV1().Pods(testNS).List(t.Context(), metav1.ListOptions{})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	return list.Items[0]
}

func TestKubernetesRuntime_PodSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(*testing.T, corev1.Pod)
		name  string
		spec  container.Spec
	}{
		{
			name: "env cmd entrypoint resources",
			spec: container.Spec{
				Image: "img:1", Name: "fn", Env: []string{"A=1", "B=x=y"}, Cmd: []string{"app.handler"},
				Entrypoint: []string{"/bin/run"}, MemoryBytes: 256 << 20, NanoCPUs: 500_000_000, WorkingDir: "/w",
			},
			check: func(t *testing.T, p corev1.Pod) {
				t.Helper()
				c := p.Spec.Containers[0]
				assert.Equal(t, "img:1", c.Image)
				assert.Equal(t, []string{"/bin/run"}, c.Command)
				assert.Equal(t, []string{"app.handler"}, c.Args)
				assert.Equal(t, "/w", c.WorkingDir)
				assert.Equal(t, []corev1.EnvVar{{Name: "A", Value: "1"}, {Name: "B", Value: "x=y"}}, c.Env)
				assert.Equal(t, int64(256<<20), c.Resources.Limits.Memory().Value())
				assert.Equal(t, int64(256<<20), c.Resources.Requests.Memory().Value())
				assert.Equal(t, int64(500), c.Resources.Limits.Cpu().MilliValue())
				assert.Equal(t, corev1.RestartPolicyNever, p.Spec.RestartPolicy)
				assert.False(t, *p.Spec.AutomountServiceAccountToken)
				assert.False(t, *c.SecurityContext.AllowPrivilegeEscalation)
				assert.Empty(t, p.Spec.InitContainers)
			},
		},
		{
			name: "runtime class user dns hosts tmpfs",
			spec: container.Spec{
				Image: "img", Name: "fn", OCIRuntime: "gvisor", User: "1000:1000", DNS: []string{"10.0.0.2"},
				ExtraHosts: []string{"host.test:10.1.1.1"}, Tmpfs: []string{"/scratch"},
			},
			check: func(t *testing.T, p corev1.Pod) {
				t.Helper()
				assert.Equal(t, "gvisor", *p.Spec.RuntimeClassName)
				assert.Equal(t, int64(1000), *p.Spec.Containers[0].SecurityContext.RunAsUser)
				assert.Equal(t, []string{"10.0.0.2"}, p.Spec.DNSConfig.Nameservers)
				assert.Equal(t, "10.1.1.1", p.Spec.HostAliases[0].IP)
				require.Len(t, p.Spec.Volumes, 1)
				assert.Equal(t, corev1.StorageMediumMemory, p.Spec.Volumes[0].EmptyDir.Medium)
				assert.Equal(t, "/scratch", p.Spec.Containers[0].VolumeMounts[0].MountPath)
			},
		},
		{
			name: "labels",
			spec: container.Spec{Image: "img", Name: "fn", Labels: map[string]string{
				container.LabelResource: "my resource", container.LabelPID: "12:34",
			}},
			check: func(t *testing.T, p corev1.Pod) {
				t.Helper()
				assert.Equal(t, "true", p.Labels[container.LabelManaged])
				assert.Equal(t, "12:34", p.Annotations[container.LabelPID])
				assert.Equal(t, "my resource", p.Annotations[container.LabelResource])

				for k, v := range p.Labels {
					assert.Empty(t, validation.IsQualifiedName(k), k)
					assert.Empty(t, validation.IsValidLabelValue(v), v)
				}

				assert.NotContains(t, p.Labels, container.LabelPID)
			},
		},
		{
			name: "name sanitised",
			spec: container.Spec{Image: "img", Name: "gopherstack-lambda-My_Func-ab12cd34"},
			check: func(t *testing.T, p corev1.Pod) {
				t.Helper()
				assert.Empty(t, validation.IsDNS1123Label(p.Name), p.Name)
				assert.True(t, strings.HasPrefix(p.Name, "gopherstack-lambda-my-func-ab12cd34-"))
			},
		},
		{
			name: "long name",
			spec: container.Spec{Image: "img", Name: strings.Repeat("a", 100)},
			check: func(t *testing.T, p corev1.Pod) {
				t.Helper()
				assert.Empty(t, validation.IsDNS1123Label(p.Name), p.Name)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt, cs := newK8s(t, running, container.KubernetesConfig{})

			id, err := rt.CreateAndStart(t.Context(), tt.spec)
			require.NoError(t, err)

			pod := onlyPod(t, cs)
			assert.Equal(t, pod.Name, id)
			tt.check(t, pod)
		})
	}
}

func TestKubernetesRuntime_UnsupportedSpec(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec container.Spec
	}{
		{"mounts", container.Spec{Image: "i", Mounts: []string{"/a:/b"}}},
		{"ports", container.Spec{Image: "i", Ports: []string{"80:80"}}},
		{"privileged", container.Spec{Image: "i", Privileged: true}},
		{"network", container.Spec{Image: "i", Network: "n"}},
		{"group add", container.Spec{Image: "i", GroupAdd: []string{"g"}}},
		{"named user", container.Spec{Image: "i", User: "nobody"}},
		{"bad extra host", container.Spec{Image: "i", ExtraHosts: []string{"nocolon"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt, cs := newK8s(t, running, container.KubernetesConfig{})

			_, err := rt.CreateAndStart(t.Context(), tt.spec)
			require.ErrorIs(t, err, container.ErrSpecUnsupported)

			list, listErr := cs.CoreV1().Pods(testNS).List(t.Context(), metav1.ListOptions{})
			require.NoError(t, listErr)
			assert.Empty(t, list.Items)
		})
	}
}

func TestKubernetesRuntime_StartFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*corev1.Pod)
		name   string
		want   string
	}{
		{
			name: "image pull backoff",
			mutate: func(p *corev1.Pod) {
				p.Status.Phase = corev1.PodPending
				p.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						Name: "main",
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"},
						},
					},
				}
			},
			want: "ImagePullBackOff",
		},
		{
			name: "init container fails",
			mutate: func(p *corev1.Pod) {
				p.Status.Phase = corev1.PodPending
				p.Status.InitContainerStatuses = []corev1.ContainerStatus{
					{
						Name: "fetch",
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{ExitCode: 8, Reason: "Error"},
						},
					},
				}
			},
			want: "exited 8",
		},
		{
			name:   "pod failed",
			mutate: func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed; p.Status.Reason = "Evicted" },
			want:   "Evicted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt, cs := newK8s(t, tt.mutate, container.KubernetesConfig{})

			_, err := rt.CreateAndStart(t.Context(), container.Spec{Image: "i", Name: "fn"})
			require.ErrorIs(t, err, container.ErrPodFailed)
			assert.Contains(t, err.Error(), tt.want)

			list, listErr := cs.CoreV1().Pods(testNS).List(t.Context(), metav1.ListOptions{})
			require.NoError(t, listErr)
			assert.Empty(t, list.Items, "failed pod must be removed")
		})
	}
}

func TestKubernetesRuntime_CreateErrors(t *testing.T) {
	t.Parallel()

	rt, cs := newK8s(t, nil, container.KubernetesConfig{})
	cs.PrependReactor(
		"create",
		"pods",
		func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, errList },
	)

	_, err := rt.CreateAndStart(t.Context(), container.Spec{Image: "i", Name: "fn"})
	require.ErrorIs(t, err, errList)
}

func TestKubernetesRuntime_ContextCancelRemovesPod(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	rt, cs := newK8s(t, func(*corev1.Pod) { cancel() }, container.KubernetesConfig{})

	_, err := rt.CreateAndStart(ctx, container.Spec{Image: "i", Name: "fn"})
	require.ErrorIs(t, err, container.ErrPodFailed)

	list, listErr := cs.CoreV1().Pods(testNS).List(t.Context(), metav1.ListOptions{})
	require.NoError(t, listErr)
	assert.Empty(t, list.Items)
}

func TestKubernetesRuntime_StopAndRemove(t *testing.T) {
	t.Parallel()

	rt, cs := newK8s(t, running, container.KubernetesConfig{})

	id, err := rt.CreateAndStart(t.Context(), container.Spec{Image: "i", Name: "fn"})
	require.NoError(t, err)

	require.NoError(t, rt.StopAndRemove(t.Context(), id))

	list, listErr := cs.CoreV1().Pods(testNS).List(t.Context(), metav1.ListOptions{})
	require.NoError(t, listErr)
	assert.Empty(t, list.Items)

	require.NoError(t, rt.StopAndRemove(t.Context(), id), "removing a missing pod is not an error")
}

func TestKubernetesRuntime_Unsupported(t *testing.T) {
	t.Parallel()

	rt, _ := newK8s(t, nil, container.KubernetesConfig{})

	require.ErrorIs(t, rt.StopContainer(t.Context(), "x"), container.ErrK8sUnsupported)
	require.ErrorIs(t, rt.StartContainer(t.Context(), "x"), container.ErrK8sUnsupported)
	require.ErrorIs(t, rt.ReleaseContainer("x"), container.ErrK8sUnsupported)

	_, err := rt.Exec(t.Context(), "x", []string{"ls"})
	require.ErrorIs(t, err, container.ErrExecUnsupported)

	_, err = rt.AcquireWarm(t.Context(), container.Spec{})
	require.ErrorIs(t, err, container.ErrK8sUnsupported)

	assert.True(t, rt.DeliversArchives())
}

func TestKubernetesRuntime_Ping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err  error
		name string
	}{
		{name: "ok"},
		{name: "forbidden", err: errList},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt, cs := newK8s(t, nil, container.KubernetesConfig{})
			if tt.err != nil {
				cs.PrependReactor(
					"list",
					"pods",
					func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, tt.err },
				)
			}

			err := rt.Ping(t.Context())
			if tt.err == nil {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, container.ErrUnavailable)
			require.ErrorIs(t, err, tt.err)
		})
	}
}

func TestKubernetesRuntime_Logs(t *testing.T) {
	t.Parallel()

	rt, _ := newK8s(t, running, container.KubernetesConfig{})

	id, err := rt.CreateAndStart(t.Context(), container.Spec{Image: "i", Name: "fn"})
	require.NoError(t, err)

	var out bytes.Buffer

	require.NoError(t, rt.ReadLogs(t.Context(), id, "10", false, &out, io.Discard))
	assert.Equal(t, "fake logs", out.String())

	out.Reset()
	require.NoError(t, rt.StreamLogs(t.Context(), id, &out, io.Discard))
	assert.Equal(t, "fake logs", out.String())
}

func TestKubernetesRuntime_ListManagedAndReconcile(t *testing.T) {
	t.Parallel()

	rt, cs := newK8s(t, running, container.KubernetesConfig{})

	_, err := cs.CoreV1().Pods(testNS).Create(t.Context(), &corev1.Pod{
		Name: "unrelated", Namespace: testNS,
	}, metav1.CreateOptions{})
	require.NoError(t, err)

	dead := map[string]string{
		container.LabelOwner: "run-dead", container.LabelEphemeral: "true", container.LabelPID: "2147483646:0",
		container.LabelService: "lambda",
	}
	_, err = rt.CreateAndStart(t.Context(), container.Spec{Image: "i", Name: "orphan", Labels: dead})
	require.NoError(t, err)

	got, err := rt.ListManaged(t.Context())
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "orphan", got[0].Name)
	assert.True(t, got[0].Running)
	assert.Equal(t, "2147483646:0", got[0].Labels[container.LabelPID])

	_, err = container.Reconcile(t.Context(), rt, container.Identity{Owner: "run-new", Ephemeral: true}, nil)
	require.NoError(t, err)

	got, err = rt.ListManaged(t.Context())
	require.NoError(t, err)
	assert.Empty(t, got, "dead ephemeral owner's pod is swept")

	_, err = cs.CoreV1().Pods(testNS).Get(t.Context(), "unrelated", metav1.GetOptions{})
	require.NoError(t, err, "unmanaged pods are never touched")
}

func zipOf(t *testing.T, files map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	for name, body := range files {
		f, err := zw.Create(name)
		require.NoError(t, err)

		_, err = f.Write([]byte(body))
		require.NoError(t, err)
	}

	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func httpGet(t *testing.T, url, auth string) (int, []byte) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)

	if auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, body
}

func TestKubernetesRuntime_ArchiveDelivery(t *testing.T) {
	t.Parallel()

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	code := zipOf(t, map[string]string{"app.py": "print(1)"})
	layer := zipOf(t, map[string]string{"lib/x.py": "x=1"})

	type fetched struct {
		body        []byte
		wrong, none int
		status      int
	}

	res := make(chan fetched, 1)

	var initEnvToken string

	onAdded := func(p *corev1.Pod) {
		args := p.Spec.InitContainers[0].Args
		initEnvToken = p.Spec.InitContainers[0].Env[0].Value

		f := fetched{}
		f.wrong, _ = httpGet(t, args[3], "Bearer wrong")
		f.none, _ = httpGet(t, args[3], "")
		f.status, f.body = httpGet(t, args[3], "Bearer "+initEnvToken)
		res <- f

		p.Status.Phase = corev1.PodRunning
	}

	rt, cs := newK8s(t, onAdded, container.KubernetesConfig{
		CodeListener: ln, AdvertiseHost: ln.Addr().String(), FetchImage: "fetch:1",
	})

	_, err = rt.CreateAndStart(t.Context(), container.Spec{
		Image: "py",
		Name:  "fn",
		Archives: []container.Archive{
			{Dest: "/var/task", Zip: code},
			{Dest: "/opt", Zip: layer},
			{Dest: "/var/task", Zip: layer},
		},
	})
	require.NoError(t, err)

	f := <-res
	assert.Equal(t, http.StatusUnauthorized, f.wrong)
	assert.Equal(t, http.StatusUnauthorized, f.none)
	assert.Equal(t, http.StatusOK, f.status)
	assert.Equal(t, code, f.body)

	pod := onlyPod(t, cs)
	require.Len(t, pod.Spec.InitContainers, 1)
	init := pod.Spec.InitContainers[0]
	assert.Equal(t, "fetch:1", init.Image)
	assert.Len(t, init.VolumeMounts, 2, "one volume per distinct destination")
	assert.Len(t, init.Args, 3+2*3)

	main := pod.Spec.Containers[0]
	require.Len(t, main.VolumeMounts, 2)

	for _, m := range main.VolumeMounts {
		assert.True(t, m.ReadOnly)
	}

	assert.Equal(t, "/var/task", main.VolumeMounts[0].MountPath)
	assert.Equal(t, "/opt", main.VolumeMounts[1].MountPath)

	for _, a := range init.Args[3:] {
		if strings.HasPrefix(a, "http://") {
			status, _ := httpGet(t, a, "Bearer "+initEnvToken)
			assert.Equal(t, http.StatusUnauthorized, status, "grant is revoked once the pod runs")
		}
	}
}

func TestKubernetesRuntime_ArchiveErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want     error
		name     string
		host     string
		archives []container.Archive
	}{
		{
			name:     "no advertise host",
			want:     container.ErrNoAdvertiseHost,
			archives: []container.Archive{{Dest: "/a", Zip: nil}},
		},
		{
			name: "zip slip", host: "127.0.0.1:1", want: container.ErrUnsafeArchive,
			archives: []container.Archive{{Dest: "/a", Zip: zipOf(t, map[string]string{"../evil": "x"})}},
		},
		{
			name: "absolute path", host: "127.0.0.1:1", want: container.ErrUnsafeArchive,
			archives: []container.Archive{{Dest: "/a", Zip: zipOf(t, map[string]string{"/etc/passwd": "x"})}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt, cs := newK8s(t, running, container.KubernetesConfig{AdvertiseHost: tt.host})

			if tt.name == "no advertise host" {
				tt.archives[0].Zip = zipOf(t, map[string]string{"ok": "x"})
			}

			_, err := rt.CreateAndStart(t.Context(), container.Spec{Image: "i", Name: "fn", Archives: tt.archives})
			require.ErrorIs(t, err, tt.want)

			list, listErr := cs.CoreV1().Pods(testNS).List(t.Context(), metav1.ListOptions{})
			require.NoError(t, listErr)
			assert.Empty(t, list.Items)
		})
	}
}

func TestDockerRuntime_RejectsArchives(t *testing.T) {
	t.Parallel()

	rt := container.NewDockerRuntimeWithAPI(&mockAPI{}, container.Config{})

	_, err := rt.CreateAndStart(t.Context(), container.Spec{Image: "i", Archives: []container.Archive{{Dest: "/a"}}})
	require.ErrorIs(t, err, container.ErrArchivesUnsupported)
}
