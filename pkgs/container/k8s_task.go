package container

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// PodLaunch describes a multi-container Pod started as one unit, for callers (ECS tasks) that map their own
// container model onto a PodSpec.
type PodLaunch struct {
	// Labels are added to the managed identity labels; values Kubernetes rejects as labels land in annotations.
	Labels map[string]string
	// Secret, when non-empty, becomes a Secret named after the Pod that the PodSpec references and the Pod owns.
	Secret map[string][]byte
	Name   string
	Spec   corev1.PodSpec
}

// PodFailedError is returned when a Pod cannot reach a running state. Reason is the Kubernetes waiting or
// termination reason (ErrImagePull, ImagePullBackOff, Error, ...).
type PodFailedError struct {
	Container string
	Reason    string
	Message   string
	Pod       string
}

func (f *PodFailedError) Error() string {
	return fmt.Sprintf("pod %q container %q: %s: %s", f.Pod, f.Container, f.Reason, f.Message)
}

// Is reports ErrPodFailed so callers can match every launch failure with errors.Is.
func (f *PodFailedError) Is(target error) bool { return target == ErrPodFailed }

// podLaunched reports a Pod that is running or finished; failed init containers and image errors are failures.
func podLaunched(pod *corev1.Pod) (bool, error) {
	for _, cs := range pod.Status.InitContainerStatuses {
		if t := cs.State.Terminated; t != nil && t.ExitCode != 0 {
			return false, &PodFailedError{
				Pod: pod.Name, Container: cs.Name,
				Reason: firstNonEmpty(t.Reason, "Error"), Message: fmt.Sprintf("exited with code %d", t.ExitCode),
			}
		}
	}

	all := append(
		append([]corev1.ContainerStatus{}, pod.Status.InitContainerStatuses...),
		pod.Status.ContainerStatuses...)
	for _, cs := range all {
		if w := cs.State.Waiting; w != nil && waitingFailures[w.Reason] {
			return false, &PodFailedError{Pod: pod.Name, Container: cs.Name, Reason: w.Reason, Message: w.Message}
		}
	}

	switch pod.Status.Phase {
	case corev1.PodRunning, corev1.PodSucceeded:
		return true, nil
	case corev1.PodFailed:
		if hasTerminated(pod.Status.ContainerStatuses) {
			return true, nil
		}

		return false, &PodFailedError{
			Pod: pod.Name, Reason: firstNonEmpty(pod.Status.Reason, "Failed"), Message: pod.Status.Message,
		}
	case corev1.PodPending, corev1.PodUnknown:
	}

	return false, nil
}

func hasTerminated(statuses []corev1.ContainerStatus) bool {
	for _, cs := range statuses {
		if cs.State.Terminated != nil {
			return true
		}
	}

	return false
}

func hardenPod(spec *corev1.PodSpec) {
	spec.RestartPolicy = corev1.RestartPolicyNever
	spec.AutomountServiceAccountToken = new(false)
	spec.EnableServiceLinks = new(false)

	if spec.SecurityContext == nil {
		spec.SecurityContext = &corev1.PodSecurityContext{}
	}

	if spec.SecurityContext.SeccompProfile == nil {
		spec.SecurityContext.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}
	}
}

// LaunchPod creates the Pod (and its Secret) and waits, by watch, until it is running or has run. A failed launch
// removes both. The returned Pod is the last observed state.
func (r *KubernetesRuntime) LaunchPod(ctx context.Context, l PodLaunch) (*corev1.Pod, error) {
	hardenPod(&l.Spec)

	labels, annotations := podMetadata(Spec{Labels: l.Labels})
	pod := &corev1.Pod{
		Name: l.Name, Namespace: r.ns, Labels: labels, Annotations: annotations,
		Spec: l.Spec,
	}

	ctx, cancel := context.WithTimeout(ctx, r.start)
	defer cancel()

	hasSecret := len(l.Secret) > 0
	secrets := r.cs.CoreV1().Secrets(r.ns)

	if hasSecret {
		sec := &corev1.Secret{
			Name: l.Name, Namespace: r.ns, Labels: map[string]string{LabelManaged: labelTrue},
			Type: corev1.SecretTypeOpaque,
			Data: l.Secret,
		}

		if _, err := secrets.Create(ctx, sec, metav1.CreateOptions{}); err != nil {
			return nil, fmt.Errorf("create secret %q: %w", l.Name, err)
		}
	}

	cleanup := func(cause error) error {
		_ = r.DeletePod(context.WithoutCancel(ctx), l.Name, hasSecret)

		return cause
	}

	pods := r.cs.CoreV1().Pods(r.ns)

	w, err := pods.Watch(ctx, metav1.ListOptions{FieldSelector: "metadata.name=" + l.Name})
	if err != nil {
		return nil, cleanup(fmt.Errorf("watch pod %q: %w", l.Name, err))
	}
	defer func() { w.Stop() }()

	created, err := pods.Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, cleanup(fmt.Errorf("create pod %q: %w", l.Name, err))
	}

	if hasSecret {
		if err = r.ownSecret(ctx, created); err != nil {
			return nil, cleanup(err)
		}
	}

	if err = r.awaitPod(ctx, l.Name, &w, podLaunched); err != nil {
		return nil, cleanup(err)
	}

	cur, err := pods.Get(ctx, l.Name, metav1.GetOptions{})
	if err != nil {
		return nil, cleanup(fmt.Errorf("get pod %q: %w", l.Name, err))
	}

	return cur, nil
}

// ownSecret makes the Pod the Secret's owner so the garbage collector removes it with the Pod.
func (r *KubernetesRuntime) ownSecret(ctx context.Context, pod *corev1.Pod) error {
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"ownerReferences": []metav1.OwnerReference{{
				APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID,
			}},
		},
	}, jsonx.V1())
	if err != nil {
		return fmt.Errorf("marshal owner reference: %w", err)
	}

	_, err = r.cs.CoreV1().Secrets(r.ns).Patch(ctx, pod.Name, types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("own secret %q: %w", pod.Name, err)
	}

	return nil
}

// DeletePod deletes the Pod, giving its containers their terminationGracePeriodSeconds, and waits until it is gone.
// withSecret also deletes the Secret LaunchPod created. A missing Pod or Secret is not an error.
func (r *KubernetesRuntime) DeletePod(ctx context.Context, name string, withSecret bool) error {
	err := r.deletePod(ctx, name, nil)

	if withSecret {
		serr := r.cs.CoreV1().Secrets(r.ns).Delete(ctx, name, metav1.DeleteOptions{})
		if serr != nil && !apierrors.IsNotFound(serr) {
			err = errors.Join(err, fmt.Errorf("delete secret %q: %w", name, serr))
		}
	}

	return err
}

// GetPod returns the Pod's current state.
func (r *KubernetesRuntime) GetPod(ctx context.Context, name string) (*corev1.Pod, error) {
	pod, err := r.cs.CoreV1().Pods(r.ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get pod %q: %w", name, err)
	}

	return pod, nil
}

// FollowPod calls fn with the Pod's current state, then with every change, until fn returns true, the Pod is
// deleted (fn then sees gone) or ctx ends. A watch that closes is re-established and the Pod re-read.
func (r *KubernetesRuntime) FollowPod(
	ctx context.Context, name string, fn func(pod *corev1.Pod, gone bool) bool,
) error {
	pods := r.cs.CoreV1().Pods(r.ns)

	for {
		w, err := pods.Watch(ctx, metav1.ListOptions{FieldSelector: "metadata.name=" + name})
		if err != nil {
			return fmt.Errorf("watch pod %q: %w", name, err)
		}

		cur, err := pods.Get(ctx, name, metav1.GetOptions{})

		switch {
		case apierrors.IsNotFound(err):
			w.Stop()
			fn(&corev1.Pod{Name: name}, true)

			return nil
		case err != nil:
			w.Stop()

			return fmt.Errorf("get pod %q: %w", name, err)
		}

		if fn(cur, false) {
			w.Stop()

			return nil
		}

		done := drainPodEvents(ctx, w, name, fn)
		w.Stop()

		if done {
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

// drainPodEvents feeds events for name to fn until fn stops, the Pod is deleted, ctx ends or the watch closes.
func drainPodEvents(
	ctx context.Context, w watch.Interface, name string, fn func(*corev1.Pod, bool) bool,
) bool {
	for {
		select {
		case <-ctx.Done():
			return true
		case ev, ok := <-w.ResultChan():
			if !ok {
				return false
			}

			pod, isPod := ev.Object.(*corev1.Pod)
			if !isPod || pod.Name != name {
				continue
			}

			gone := ev.Type == watch.Deleted
			if fn(pod, gone) || gone {
				return true
			}
		}
	}
}

// StreamPodLogs follows one container's output into w until the container ends or ctx is cancelled.
func (r *KubernetesRuntime) StreamPodLogs(ctx context.Context, pod, containerName string, w io.Writer) error {
	opts := &corev1.PodLogOptions{Container: containerName, Follow: true}

	rc, err := r.cs.CoreV1().Pods(r.ns).GetLogs(pod, opts).Stream(ctx)
	if err != nil {
		return fmt.Errorf("pod logs %q/%q: %w", pod, containerName, err)
	}
	defer rc.Close()

	if _, err = io.Copy(w, rc); err != nil && ctx.Err() == nil {
		return fmt.Errorf("pod logs %q/%q: %w", pod, containerName, err)
	}

	return nil
}

// SplitContainerRef splits a "pod/container" reference.
func SplitContainerRef(ref string) (string, string, bool) {
	pod, c, ok := strings.Cut(ref, "/")
	if !ok || pod == "" || c == "" {
		return "", "", false
	}

	return pod, c, true
}
