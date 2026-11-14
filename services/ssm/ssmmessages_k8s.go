package ssm

import (
	"context"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

// ErrBadContainerRef is returned when a session target is not a "pod/container" reference.
var ErrBadContainerRef = errors.New("session container must be pod/container")

// PodExecer opens an interactive command in a Pod container; *container.KubernetesRuntime implements it.
type PodExecer interface {
	ExecInteractive(
		ctx context.Context, pod, containerName string, cmd []string, cols, rows uint,
	) (container.PodShell, error)
}

type k8sShellOpener struct{ x PodExecer }

// NewKubernetesShellOpener returns a ShellOpener that execs into Pods through pods/exec; container IDs are
// "pod/container".
func NewKubernetesShellOpener(x PodExecer) ShellOpener { return &k8sShellOpener{x: x} }

func (k *k8sShellOpener) OpenShell(ctx context.Context, containerID string, cols, rows uint) (Shell, error) {
	return k.OpenCommand(ctx, containerID,
		[]string{"sh", "-c", "command -v bash >/dev/null 2>&1 && exec bash -l; exec sh -l"}, cols, rows)
}

func (k *k8sShellOpener) OpenCommand(
	ctx context.Context, containerID string, cmd []string, cols, rows uint,
) (Shell, error) {
	pod, name, ok := container.SplitContainerRef(containerID)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrBadContainerRef, containerID)
	}

	sh, err := k.x.ExecInteractive(ctx, pod, name, cmd, cols, rows)
	if err != nil {
		return nil, fmt.Errorf("exec %s/%s: %w", pod, name, err)
	}

	return sh, nil
}
