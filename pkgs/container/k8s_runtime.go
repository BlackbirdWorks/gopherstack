package container

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	// EnvK8sKubeconfig selects a kubeconfig file; unset means in-cluster config when running in a Pod.
	EnvK8sKubeconfig = "CONTAINER_K8S_KUBECONFIG"
	// EnvK8sNamespace is the namespace Pods are created in; defaults to the Pod's own namespace.
	EnvK8sNamespace = "CONTAINER_K8S_NAMESPACE"
	// EnvK8sAdvertiseHost is the address function Pods use to reach this process (usually its Pod IP).
	EnvK8sAdvertiseHost = "CONTAINER_K8S_ADVERTISE_HOST"
	// EnvK8sCodePort is the TCP port the archive server listens on.
	EnvK8sCodePort = "CONTAINER_K8S_CODE_PORT"
	// EnvK8sFetchImage is the image of the initContainer that downloads and unpacks archives.
	EnvK8sFetchImage = "CONTAINER_K8S_FETCH_IMAGE"

	defaultK8sCodePort   = 4599
	defaultK8sFetchImage = "busybox:1.37.0"
	defaultK8sStartWait  = 5 * time.Minute
	k8sNamespaceFile     = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
	k8sShutdownWait      = 5 * time.Second
)

// ErrK8sUnsupported is returned for operations a Pod cannot offer.
var ErrK8sUnsupported = errors.New("operation is not supported by the kubernetes runtime")

// ErrPodFailed is returned when a Pod cannot reach Running.
var ErrPodFailed = errors.New("pod failed to start")

// ErrWatchClosed is returned when a Pod watch ends before the awaited transition.
var ErrWatchClosed = errors.New("pod watch closed")

// ErrNoAdvertiseHost is returned when archives are requested but pods have no address to fetch them from.
var ErrNoAdvertiseHost = errors.New("archive delivery needs " + EnvK8sAdvertiseHost)

// KubernetesConfig configures RuntimeKubernetes. Zero values fall back to the environment.
type KubernetesConfig struct {
	Client        kubernetes.Interface
	CodeListener  net.Listener
	Executor      PodExecutor
	Kubeconfig    string
	Namespace     string
	AdvertiseHost string
	FetchImage    string
	CodePort      int
	StartTimeout  time.Duration
}

// KubernetesRuntime runs containers as Pods; the container ID is the Pod name.
type KubernetesRuntime struct {
	cs         kubernetes.Interface
	exec       PodExecutor
	log        *slog.Logger
	code       *codeServer
	codeMu     *lockmetrics.RWMutex
	restCfg    *rest.Config
	ns         string
	fetchImage string
	start      time.Duration
	codeCfg    KubernetesConfig
}

func (c KubernetesConfig) withEnv() KubernetesConfig {
	c.Kubeconfig = firstNonEmpty(c.Kubeconfig, os.Getenv(EnvK8sKubeconfig))
	c.Namespace = firstNonEmpty(c.Namespace, os.Getenv(EnvK8sNamespace))
	c.AdvertiseHost = firstNonEmpty(c.AdvertiseHost, os.Getenv(EnvK8sAdvertiseHost))
	c.FetchImage = firstNonEmpty(c.FetchImage, os.Getenv(EnvK8sFetchImage), defaultK8sFetchImage)

	if c.CodePort == 0 {
		if p, err := strconv.Atoi(os.Getenv(EnvK8sCodePort)); err == nil {
			c.CodePort = p
		} else {
			c.CodePort = defaultK8sCodePort
		}
	}

	if c.StartTimeout == 0 {
		c.StartTimeout = defaultK8sStartWait
	}

	return c
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}

	return ""
}

func newKubernetesRuntime(cfg Config) (*KubernetesRuntime, error) {
	kc := KubernetesConfig{}
	if cfg.Kubernetes != nil {
		kc = *cfg.Kubernetes
	}

	kc = kc.withEnv()

	cs := kc.Client
	ns := kc.Namespace

	var restCfg *rest.Config

	if cs == nil {
		var err error

		cs, restCfg, ns, err = connectKubernetes(kc)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
	}

	if ns == "" {
		ns = "default"
	}

	return &KubernetesRuntime{
		cs:         cs,
		log:        cfg.Logger,
		codeMu:     lockmetrics.New("container.k8s.runtime"),
		codeCfg:    kc,
		restCfg:    restCfg,
		exec:       kc.Executor,
		ns:         ns,
		fetchImage: kc.FetchImage,
		start:      kc.StartTimeout,
	}, nil
}

// connectKubernetes builds a clientset from the kubeconfig, else the in-cluster config, else the default
// kubeconfig search path, and resolves the namespace Pods go to.
func connectKubernetes(kc KubernetesConfig) (kubernetes.Interface, *rest.Config, string, error) {
	ns := kc.Namespace

	var restCfg *rest.Config

	switch {
	case kc.Kubeconfig != "":
		rules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: kc.Kubeconfig}
		cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})

		c, err := cc.ClientConfig()
		if err != nil {
			return nil, nil, "", fmt.Errorf("kubeconfig %q: %w", kc.Kubeconfig, err)
		}

		restCfg = c

		if ns == "" {
			ns, _, _ = cc.Namespace()
		}
	case os.Getenv("KUBERNETES_SERVICE_HOST") != "":
		c, err := rest.InClusterConfig()
		if err != nil {
			return nil, nil, "", fmt.Errorf("in-cluster config: %w", err)
		}

		restCfg = c

		if ns == "" {
			if b, readErr := os.ReadFile(k8sNamespaceFile); readErr == nil {
				ns = strings.TrimSpace(string(b))
			}
		}
	default:
		cc := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{})

		c, err := cc.ClientConfig()
		if err != nil {
			return nil, nil, "", fmt.Errorf("no kubeconfig and not running in a cluster: %w", err)
		}

		restCfg = c

		if ns == "" {
			ns, _, _ = cc.Namespace()
		}
	}

	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, nil, "", fmt.Errorf("kubernetes client: %w", err)
	}

	return cs, restCfg, ns, nil
}

// DeliversArchives reports that Spec.Archives are unpacked by the runtime.
func (r *KubernetesRuntime) DeliversArchives() bool { return true }

// Namespace is the namespace Pods are created in.
func (r *KubernetesRuntime) Namespace() string { return r.ns }

// Ping lists one managed Pod, which also proves the RBAC the runtime needs.
func (r *KubernetesRuntime) Ping(ctx context.Context) error {
	_, err := r.cs.CoreV1().Pods(r.ns).List(ctx, metav1.ListOptions{Limit: 1, LabelSelector: managedSelector})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}

	return nil
}

// PullImage is a no-op: the kubelet pulls the image when the Pod is scheduled.
func (r *KubernetesRuntime) PullImage(context.Context, string) error { return nil }

// HasImage reports true: whether the image is present is the kubelet's concern, so callers never pre-pull.
func (r *KubernetesRuntime) HasImage(context.Context, string) (bool, error) { return true, nil }

// CreateAndStart creates the Pod and waits, by watch, until it is running.
func (r *KubernetesRuntime) CreateAndStart(ctx context.Context, spec Spec) (string, error) {
	name := podName(spec.Name)

	var g grant

	if len(spec.Archives) > 0 {
		var err error

		if g, err = r.grantArchives(ctx, spec.Archives); err != nil {
			return "", err
		}

		defer r.code.revoke(g)
	}

	pod, err := r.buildPod(spec, name, g)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(ctx, r.start)
	defer cancel()

	pods := r.cs.CoreV1().Pods(r.ns)

	w, err := pods.Watch(ctx, metav1.ListOptions{FieldSelector: "metadata.name=" + name})
	if err != nil {
		return "", fmt.Errorf("watch pod %q: %w", name, err)
	}
	defer func() { w.Stop() }()

	if _, err = pods.Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return "", fmt.Errorf("create pod %q: %w", name, err)
	}

	if err = r.awaitPod(ctx, name, &w, podStarted); err != nil {
		_ = r.StopAndRemove(context.WithoutCancel(ctx), name)

		return "", err
	}

	return name, nil
}

func (r *KubernetesRuntime) grantArchives(ctx context.Context, archives []Archive) (grant, error) {
	for _, a := range archives {
		if err := validateArchive(a.Zip); err != nil {
			return grant{}, err
		}
	}

	if err := r.ensureCodeServer(ctx); err != nil {
		return grant{}, err
	}

	return r.code.register(archives)
}

func (r *KubernetesRuntime) ensureCodeServer(ctx context.Context) error {
	r.codeMu.Lock("ensureCodeServer")
	defer r.codeMu.Unlock()

	if r.code != nil {
		return nil
	}

	if r.codeCfg.AdvertiseHost == "" {
		return ErrNoAdvertiseHost
	}

	ln := r.codeCfg.CodeListener
	if ln == nil {
		var err error

		ln, err = (&net.ListenConfig{}).Listen(ctx, "tcp", ":"+strconv.Itoa(r.codeCfg.CodePort))
		if err != nil {
			return fmt.Errorf("archive server listen: %w", err)
		}
	}

	adv := r.codeCfg.AdvertiseHost
	if _, _, err := net.SplitHostPort(adv); err != nil {
		adv = net.JoinHostPort(adv, strconv.Itoa(r.codeCfg.CodePort))
	}

	r.code = newCodeServer(ln, adv)
	r.code.start()

	return nil
}

// awaitPod consumes watch events until check reports the Pod launched or failed. A closed watch is
// re-established and the Pod re-read, so a server-side watch timeout does not lose the transition.
func (r *KubernetesRuntime) awaitPod(
	ctx context.Context, name string, w *watch.Interface, check podCheck,
) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: pod %q: %w", ErrPodFailed, name, ctx.Err())
		case ev, ok := <-(*w).ResultChan():
			var (
				done bool
				err  error
			)

			if ok {
				done, err = handlePodEvent(name, ev, check)
			} else {
				done, err = r.resync(ctx, name, w, check)
			}

			if done || err != nil {
				return err
			}
		}
	}
}

// resync reopens the watch after it closed and re-reads the Pod so no transition is missed.
func (r *KubernetesRuntime) resync(
	ctx context.Context, name string, w *watch.Interface, check podCheck,
) (bool, error) {
	pods := r.cs.CoreV1().Pods(r.ns)

	nw, err := pods.Watch(ctx, metav1.ListOptions{FieldSelector: "metadata.name=" + name})
	if err != nil {
		return false, fmt.Errorf("watch pod %q: %w", name, err)
	}

	*w = nw

	cur, err := pods.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("get pod %q: %w", name, err)
	}

	return check(cur)
}

// podCheck reports whether a Pod reached the state a launch waits for, or why it never will.
type podCheck func(*corev1.Pod) (bool, error)

func handlePodEvent(name string, ev watch.Event, check podCheck) (bool, error) {
	pod, ok := ev.Object.(*corev1.Pod)
	if !ok || pod.Name != name {
		return false, nil
	}

	if ev.Type == watch.Deleted {
		return false, fmt.Errorf("%w: pod %q was deleted", ErrPodFailed, name)
	}

	return check(pod)
}

var waitingFailures = map[string]bool{ //nolint:gochecknoglobals // fixed lookup set
	"ErrImagePull": true, "ImagePullBackOff": true, "InvalidImageName": true,
	"CreateContainerConfigError": true, "CreateContainerError": true, "RunContainerError": true,
}

// podStarted reports whether pod is running, or why it never will.
func podStarted(pod *corev1.Pod) (bool, error) {
	switch pod.Status.Phase {
	case corev1.PodRunning, corev1.PodSucceeded:
		return true, nil
	case corev1.PodFailed:
		return false, fmt.Errorf(
			"%w: pod %q failed: %s",
			ErrPodFailed,
			pod.Name,
			firstNonEmpty(pod.Status.Reason, pod.Status.Message),
		)
	case corev1.PodPending, corev1.PodUnknown:
	}

	all := append(
		append([]corev1.ContainerStatus{}, pod.Status.InitContainerStatuses...),
		pod.Status.ContainerStatuses...)
	for _, cs := range all {
		if w := cs.State.Waiting; w != nil && waitingFailures[w.Reason] {
			return false, fmt.Errorf(
				"%w: pod %q container %q: %s: %s",
				ErrPodFailed,
				pod.Name,
				cs.Name,
				w.Reason,
				w.Message,
			)
		}

		if t := cs.State.Terminated; t != nil && t.ExitCode != 0 {
			return false, fmt.Errorf(
				"%w: pod %q container %q exited %d: %s",
				ErrPodFailed,
				pod.Name,
				cs.Name,
				t.ExitCode,
				t.Reason,
			)
		}
	}

	return false, nil
}

// StopAndRemove deletes the Pod and waits, by watch, until it is gone; a missing Pod is not an error.
func (r *KubernetesRuntime) StopAndRemove(ctx context.Context, containerID string) error {
	grace := int64(podTerminationSec)

	return r.deletePod(ctx, containerID, &grace)
}

// deletePod deletes the Pod and waits for it to be gone; a nil grace uses the Pod's own terminationGracePeriodSeconds.
func (r *KubernetesRuntime) deletePod(ctx context.Context, containerID string, grace *int64) error {
	pods := r.cs.CoreV1().Pods(r.ns)

	w, err := pods.Watch(ctx, metav1.ListOptions{FieldSelector: "metadata.name=" + containerID})
	if err != nil {
		return fmt.Errorf("watch pod %q: %w", containerID, err)
	}
	defer w.Stop()

	err = pods.Delete(ctx, containerID, metav1.DeleteOptions{GracePeriodSeconds: grace})
	if apierrors.IsNotFound(err) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("delete pod %q: %w", containerID, err)
	}

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for pod %q deletion: %w", containerID, ctx.Err())
		case ev, ok := <-w.ResultChan():
			if !ok {
				if _, getErr := pods.Get(ctx, containerID, metav1.GetOptions{}); apierrors.IsNotFound(getErr) {
					return nil
				}

				return fmt.Errorf("%w before pod %q was deleted", ErrWatchClosed, containerID)
			}

			if p, isPod := ev.Object.(*corev1.Pod); ev.Type == watch.Deleted && isPod && p.Name == containerID {
				return nil
			}
		}
	}
}

// StopContainer is unsupported: a Pod cannot be stopped and restarted in place.
func (r *KubernetesRuntime) StopContainer(context.Context, string) error { return ErrK8sUnsupported }

// StartContainer is unsupported: a Pod cannot be stopped and restarted in place.
func (r *KubernetesRuntime) StartContainer(context.Context, string) error { return ErrK8sUnsupported }

// Exec is unsupported; the chart grants no pods/exec.
func (r *KubernetesRuntime) Exec(context.Context, string, []string) (ExecResult, error) {
	return ExecResult{}, ErrExecUnsupported
}

// AcquireWarm is unsupported: Pods are not pooled.
func (r *KubernetesRuntime) AcquireWarm(context.Context, Spec) (*PooledContainer, error) {
	return nil, ErrK8sUnsupported
}

// ReleaseContainer is unsupported: Pods are not pooled.
func (r *KubernetesRuntime) ReleaseContainer(string) error { return ErrK8sUnsupported }

// ReapIdleContainers does nothing: there is no pool to reap.
func (r *KubernetesRuntime) ReapIdleContainers(context.Context) {}

// StartReaper does nothing: there is no pool to reap.
func (r *KubernetesRuntime) StartReaper(context.Context, time.Duration) {}

// Close stops the archive server.
func (r *KubernetesRuntime) Close() error {
	r.codeMu.RLock("Close")
	code := r.code
	r.codeMu.RUnlock()

	if code != nil {
		ctx, cancel := context.WithTimeout(context.Background(), k8sShutdownWait)
		defer cancel()

		code.close(ctx)
	}

	return nil
}
