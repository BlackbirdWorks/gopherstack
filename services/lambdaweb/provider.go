package lambdaweb

import (
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for Lambda Web Functions.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "LambdaWeb" }

// Init initializes the Lambda Web Functions backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID)

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		if st := cp.GetLambdaWebSettings(); st.Engine == EngineDocker {
			enableDockerEngine(ctx, backend, st)
		}
	}

	handler := NewHandler(backend)
	handler.AccountID = accountID
	handler.DefaultRegion = region

	return handler, nil
}

func enableDockerEngine(ctx *service.AppContext, backend *InMemoryBackend, st Settings) {
	rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
	if err != nil {
		ctx.Logger.Warn("LambdaWeb: container runtime unavailable; endpoints will not serve requests", "error", err)

		return
	}

	backend.SetLauncher(NewDockerLauncher(backend, DockerConfig{
		Runtime: rt, Ports: ctx.PortAlloc, Image: st.RuntimeImage,
	}))
}
