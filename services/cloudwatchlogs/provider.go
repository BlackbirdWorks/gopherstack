package cloudwatchlogs

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ConfigProvider is a private interface to extract CloudWatch Logs configuration
// from the abstract AppContext Config.
type ConfigProvider interface {
	GetCloudWatchLogsSettings() Settings
}

// Provider implements service.Provider for the CloudWatch Logs service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "CloudWatchLogs" }

// Init initializes the CloudWatch Logs service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	var backend *InMemoryBackend

	if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		backend = NewInMemoryBackendWithContext(ctx.JanitorCtx, cfg.AccountID, cfg.Region)
	} else {
		backend = NewInMemoryBackendWithContext(ctx.JanitorCtx, config.DefaultAccountID, config.DefaultRegion)
	}

	var settings Settings

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetCloudWatchLogsSettings()
	}

	handler := NewHandler(backend).WithJanitor(settings.JanitorInterval, ctx.JanitorTimeout)

	return handler, nil
}
