package lambda

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/docker"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the Lambda service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "Lambda" }

// Init initializes the Lambda service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	var accountID, region string

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		accountID = cfg.AccountID
		region = cfg.Region
	}

	settings := DefaultSettings()

	if sp, ok := ctx.Config.(LambdaSettingsProvider); ok {
		settings = sp.GetLambdaSettings()
	}

	var dockerClient *docker.Client

	dc, err := docker.NewClient(docker.Config{
		Logger:      ctx.Logger,
		PoolSize:    settings.PoolSize,
		IdleTimeout: settings.IdleTimeout,
	})
	if err != nil {
		ctx.Logger.Warn("Lambda: Docker unavailable; Invoke will not work", "error", err)
	} else {
		dockerClient = dc
	}

	backend := NewInMemoryBackend(
		dockerClient,
		ctx.PortAlloc,
		settings,
		accountID,
		region,
		ctx.Logger,
	)

	handler := NewHandler(backend, ctx.Logger)
	handler.DefaultRegion = region
	handler.AccountID = accountID

	return handler, nil
}

// LambdaSettingsProvider is implemented by config objects that supply Lambda settings.
type LambdaSettingsProvider interface {
	GetLambdaSettings() Settings
}
