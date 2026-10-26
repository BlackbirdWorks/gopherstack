package azurearm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

var (
	errACINoExitStatus = errors.New("the container runtime cannot report exit status")
	errACINoLogs       = errors.New("the container runtime cannot read logs")
	errACINoCredPull   = errors.New("the container runtime cannot pull with registry credentials")
	errACINoRuntime    = errors.New("no container runtime is available")
)

// ACICredential is an imageRegistryCredentials entry.
type ACICredential struct {
	Server   string
	Username string
	Password string
}

// ACIRuntime is the slice of the container runtime that container groups need.
type ACIRuntime interface {
	// Pull fetches ref when it is absent or when cred is set; it reports whether a pull happened.
	Pull(ctx context.Context, ref string, cred *ACICredential) (bool, error)
	Start(ctx context.Context, spec container.Spec) (string, error)
	// Wait blocks until the container exits and returns its exit code.
	Wait(ctx context.Context, id string) (int, error)
	Logs(ctx context.Context, id, tail string) (string, error)
	Remove(ctx context.Context, id string) error
}

// ACIPortPool hands out host ports for published container ports.
type ACIPortPool = gcpserve.PortPool

// dockerACIRuntime adapts a pkgs/container runtime to ACIRuntime.
type dockerACIRuntime struct{ r gcpserve.Runner }

type credPuller interface {
	PullImageWithCredentials(ctx context.Context, ref, user, pass string) error
}

type imagePuller interface {
	HasImage(ctx context.Context, ref string) (bool, error)
	PullImage(ctx context.Context, ref string) error
}

func (d dockerACIRuntime) Pull(ctx context.Context, ref string, cred *ACICredential) (bool, error) {
	if cred != nil {
		cp, ok := d.r.(credPuller)
		if !ok {
			return false, errACINoCredPull
		}

		return true, cp.PullImageWithCredentials(ctx, ref, cred.Username, cred.Password)
	}

	ip, ok := d.r.(imagePuller)
	if !ok {
		return false, nil
	}

	has, err := ip.HasImage(ctx, ref)
	if err != nil || has {
		return false, err
	}

	return true, ip.PullImage(ctx, ref)
}

func (d dockerACIRuntime) Start(ctx context.Context, spec container.Spec) (string, error) {
	return d.r.CreateAndStart(ctx, spec)
}

func (d dockerACIRuntime) Wait(ctx context.Context, id string) (int, error) {
	w, ok := d.r.(container.ExitWaiter)
	if !ok {
		return 0, errACINoExitStatus
	}

	return w.WaitExit(ctx, id)
}

func (d dockerACIRuntime) Logs(ctx context.Context, id, tail string) (string, error) {
	lr, ok := d.r.(container.LogReader)
	if !ok {
		return "", errACINoLogs
	}

	var out, errOut bytes.Buffer

	if err := lr.ReadLogs(ctx, id, tail, false, &out, &errOut); err != nil {
		return "", fmt.Errorf("read logs: %w", err)
	}

	return out.String() + errOut.String(), nil
}

func (d dockerACIRuntime) Remove(ctx context.Context, id string) error {
	return d.r.StopAndRemove(ctx, id)
}

// NewDockerACIRuntime wraps a pkgs/container runner.
func NewDockerACIRuntime(r gcpserve.Runner) ACIRuntime { return dockerACIRuntime{r: r} }

func credentialFor(ref string, creds []aciCredWire) *ACICredential {
	host := strings.ToLower(refHost(ref))

	for _, c := range creds {
		if strings.EqualFold(strings.TrimSuffix(c.Server, "/"), host) {
			return &ACICredential{Server: c.Server, Username: c.Username, Password: c.Password}
		}
	}

	return nil
}

// refHost returns the registry host of an image reference; docker.io when it names none.
func refHost(ref string) string {
	first, _, ok := strings.Cut(ref, "/")
	if ok && (strings.ContainsAny(first, ".:") || first == "localhost") {
		return first
	}

	return "docker.io"
}
