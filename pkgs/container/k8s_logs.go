package container

import (
	"context"
	"fmt"
	"io"
	"maps"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const managedSelector = LabelManaged + "=true"

// StreamLogs follows the Pod's output; Pods merge stdout and stderr, so everything goes to stdout.
func (r *KubernetesRuntime) StreamLogs(ctx context.Context, containerID string, stdout, stderr io.Writer) error {
	return r.ReadLogs(ctx, containerID, "", true, stdout, stderr)
}

// ReadLogs reads the Pod's logs; Pods merge stdout and stderr, so everything goes to stdout.
func (r *KubernetesRuntime) ReadLogs(
	ctx context.Context,
	containerID, tail string,
	follow bool,
	stdout, _ io.Writer,
) error {
	opts := &corev1.PodLogOptions{Container: podMainContainer, Follow: follow}

	if n, err := strconv.ParseInt(tail, 10, 64); err == nil {
		opts.TailLines = &n
	}

	rc, err := r.cs.CoreV1().Pods(r.ns).GetLogs(containerID, opts).Stream(ctx)
	if err != nil {
		return fmt.Errorf("pod logs %q: %w", containerID, err)
	}
	defer rc.Close()

	if _, err = io.Copy(stdout, rc); err != nil && ctx.Err() == nil {
		return fmt.Errorf("pod logs %q: %w", containerID, err)
	}

	return nil
}

// ListManaged lists the Pods carrying LabelManaged; labels are read back from annotations, which keep values
// (such as pid:start) that Kubernetes label syntax rejects.
func (r *KubernetesRuntime) ListManaged(ctx context.Context) ([]ManagedContainer, error) {
	list, err := r.cs.CoreV1().Pods(r.ns).List(ctx, metav1.ListOptions{LabelSelector: managedSelector})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	out := make([]ManagedContainer, 0, len(list.Items))

	for i := range list.Items {
		p := &list.Items[i]
		if p.DeletionTimestamp != nil {
			continue
		}

		out = append(out, ManagedContainer{
			ID:      p.Name,
			Name:    p.Name,
			Labels:  maps.Clone(p.Annotations),
			Running: p.Status.Phase == corev1.PodRunning || p.Status.Phase == corev1.PodPending,
		})
	}

	return out, nil
}
