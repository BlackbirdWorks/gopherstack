package ocicontainerinstances

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

var (
	errVaultUnavailable = errors.New("VAULT image pull secrets need OCI Vault, which is not available")
	errVaultSecretShape = errors.New(`the Vault secret must be JSON like {"username":"...","password":"..."}`)
	errNoExitStatus     = errors.New("the container runtime cannot report exit status")
	errNoLogs           = errors.New("the container runtime cannot read logs")
	errNoCredPull       = errors.New("the container runtime cannot pull with registry credentials")
)

// Credential is a registry login resolved from an image pull secret.
type Credential struct {
	Endpoint string
	Username string
	Password string
}

// Runtime is the slice of the container runtime Container Instances need.
type Runtime interface {
	// Pull fetches ref when it is absent locally or when cred is set.
	Pull(ctx context.Context, ref string, cred *Credential) error
	Start(ctx context.Context, spec container.Spec) (string, error)
	// Wait blocks until the container exits and returns its exit code.
	Wait(ctx context.Context, id string) (int, error)
	Logs(ctx context.Context, id string) (string, error)
	// Kill stops the container but keeps it, so Wait reports its exit code and logs stay readable.
	Kill(ctx context.Context, id string) error
	Remove(ctx context.Context, id string) error
}

type credPuller interface {
	PullImageWithCredentials(ctx context.Context, ref, user, pass string) error
}

type imagePuller interface {
	HasImage(ctx context.Context, ref string) (bool, error)
	PullImage(ctx context.Context, ref string) error
}

type stopper interface {
	StopContainer(ctx context.Context, containerID string) error
}

type dockerRuntime struct{ r gcpserve.Runner }

// NewDockerRuntime wraps a pkgs/container runtime.
func NewDockerRuntime(r gcpserve.Runner) Runtime { return dockerRuntime{r: r} }

func (d dockerRuntime) Pull(ctx context.Context, ref string, cred *Credential) error {
	if cred != nil {
		cp, ok := d.r.(credPuller)
		if !ok {
			return errNoCredPull
		}

		return cp.PullImageWithCredentials(ctx, ref, cred.Username, cred.Password)
	}

	ip, ok := d.r.(imagePuller)
	if !ok {
		return nil
	}

	has, err := ip.HasImage(ctx, ref)
	if err != nil || has {
		return err
	}

	return ip.PullImage(ctx, ref)
}

func (d dockerRuntime) Start(ctx context.Context, spec container.Spec) (string, error) {
	return d.r.CreateAndStart(ctx, spec)
}

func (d dockerRuntime) Wait(ctx context.Context, id string) (int, error) {
	w, ok := d.r.(container.ExitWaiter)
	if !ok {
		return 0, errNoExitStatus
	}

	return w.WaitExit(ctx, id)
}

func (d dockerRuntime) Logs(ctx context.Context, id string) (string, error) {
	lr, ok := d.r.(container.LogReader)
	if !ok {
		return "", errNoLogs
	}

	var out, errOut bytes.Buffer

	if err := lr.ReadLogs(ctx, id, "", false, &out, &errOut); err != nil {
		return "", fmt.Errorf("read logs: %w", err)
	}

	return out.String() + errOut.String(), nil
}

func (d dockerRuntime) Kill(ctx context.Context, id string) error {
	if s, ok := d.r.(stopper); ok {
		return s.StopContainer(ctx, id)
	}

	return d.r.StopAndRemove(ctx, id)
}

func (d dockerRuntime) Remove(ctx context.Context, id string) error {
	return d.r.StopAndRemove(ctx, id)
}
