package azuregraph

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// ConfigProvider extracts the Microsoft Graph settings from the AppContext config.
type ConfigProvider interface {
	GetAzureGraphSettings() Settings
}

// Provider implements service.Provider for Microsoft Graph.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "AzureGraph" }

// Init builds the backend and handler; the port is bound by Handler.StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetAzureGraphSettings()
	}

	return NewHandler(NewBackend(settings), settings), nil
}
