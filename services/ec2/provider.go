package ec2

import (
	"context"
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Provider.Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("AppContext is required")

// ConfigProvider is a private interface to extract EC2 configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetEC2Settings() Settings
}

// ComputeProviderConfig is the optional interface a Config may implement to
// switch the EC2 backend onto a real compute provider (e.g. Docker). It is
// independent of ConfigProvider so adding/removing the docker provider does
// not require existing Config types to change.
type ComputeProviderConfig interface {
	// GetEC2ComputeProvider returns "inmemory" (default) or "docker".
	GetEC2ComputeProvider() string
	// GetEC2DockerComputeConfig returns docker provider settings (only consulted
	// when GetEC2ComputeProvider() == "docker").
	GetEC2DockerComputeConfig() DockerComputeConfig
}

// Provider implements service.Provider for the EC2 service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string {
	return "EC2"
}

// Init initializes the EC2 service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	var accountID, region string

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		accountID = cfg.GetAccountID()
		region = cfg.GetRegion()
	}

	var settings Settings

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetEC2Settings()
	}

	backend := NewInMemoryBackend(accountID, region)
	backend.StartLifecycleReconciler()

	if cp, ok := ctx.Config.(ComputeProviderConfig); ok && cp.GetEC2ComputeProvider() == "docker" {
		dc, err := NewDockerCompute(cp.GetEC2DockerComputeConfig())
		if err != nil {
			return nil, err
		}

		if pingErr := dc.Ping(context.Background()); pingErr != nil && ctx.Logger != nil {
			ctx.Logger.Warn("ec2 docker compute ping failed; continuing with hook installed",
				"error", pingErr)
		}

		backend.WithCompute(dc)
	}

	handler := NewHandler(backend)
	handler.AccountID = accountID
	handler.Region = region

	handler.WithJanitor(
		settings.JanitorInterval,
		settings.TerminatedTTL,
		settings.CancelledSpotTTL,
		ctx.JanitorTimeout,
	)

	return handler, nil
}
