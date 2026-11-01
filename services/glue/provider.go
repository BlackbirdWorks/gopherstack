package glue

import (
	"context"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when appCtx is nil.
var ErrNilAppContext = errors.New("glue provider: nil AppContext")

// Provider implements service.Provider for Glue.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return glueServiceName }

// Init initializes the Glue backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w", ErrNilAppContext)
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	var settings Settings

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetGlueSettings()
	}

	backend := NewInMemoryBackend(accountID, region)
	if settings.Engine == EngineDocker {
		enableDockerEngine(ctx, backend, settings)
	}

	handler := NewHandler(backend)
	handler.EnableRegions(ctx.JanitorCtx)

	return handler, nil
}

func enableDockerEngine(ctx *service.AppContext, backend *InMemoryBackend, settings Settings) {
	rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
	if err != nil {
		ctx.Logger.Warn("Glue: container runtime unavailable; session statements will fail", "error", err)

		return
	}

	backend.SetSessionEngine(NewDockerSessionEngine(DockerSessionConfig{
		Runtime: NewContainerSessionRuntime(rt), Image: settings.EngineImage,
	}))
}

var errLivenessProbe = errors.New("liveness probe failed")

// NewContainerSessionRuntime adapts a container runtime for NewDockerSessionEngine.
func NewContainerSessionRuntime(rt container.Runtime) SessionRuntime {
	return containerSessionRuntime{rt: rt}
}

// containerSessionRuntime adapts container.Runtime to SessionRuntime.
type containerSessionRuntime struct{ rt container.Runtime }

func (c containerSessionRuntime) HasImage(ctx context.Context, ref string) (bool, error) {
	return c.rt.HasImage(ctx, ref)
}

func (c containerSessionRuntime) PullImage(ctx context.Context, ref string) error {
	return c.rt.PullImage(ctx, ref)
}

func (c containerSessionRuntime) CreateAndStart(ctx context.Context, spec SessionContainerSpec) (string, error) {
	return c.rt.CreateAndStart(ctx, container.Spec{
		Image: spec.Image, Entrypoint: spec.Entrypoint, Env: spec.Env, Mounts: spec.Mounts,
	})
}

func (c containerSessionRuntime) StopAndRemove(ctx context.Context, id string) error {
	return c.rt.StopAndRemove(ctx, id)
}

func (c containerSessionRuntime) Alive(ctx context.Context, id string) error {
	res, err := c.rt.Exec(ctx, id, []string{"true"})
	if err != nil {
		return err
	}

	if res.ExitCode != 0 {
		return fmt.Errorf("%w: exit %d", errLivenessProbe, res.ExitCode)
	}

	return nil
}
