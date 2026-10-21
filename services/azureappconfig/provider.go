package azureappconfig

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// ConfigProvider extracts the App Configuration settings from the AppContext config.
type ConfigProvider interface {
	GetAzureAppConfigSettings() Settings
}

// Provider implements service.Provider for Azure App Configuration.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "AzureAppConfig" }

// Init builds the backend and handler; the port is bound by Handler.StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetAzureAppConfigSettings()
	}

	return NewHandler(NewInMemoryBackend(), settings), nil
}
