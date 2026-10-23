package azureeventhubs

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// ConfigProvider extracts the Event Hubs settings from the AppContext config.
type ConfigProvider interface {
	GetAzureEventHubsSettings() Settings
}

// Provider implements service.Provider for Azure Event Hubs.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "AzureEventHubs" }

// Init builds the backend and handler; ports are bound by Handler.StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetAzureEventHubsSettings()
	}

	h := NewHandler(NewInMemoryBackend())
	h.Port, h.AMQPPort, h.AMQPTLSPort = settings.Port, settings.AMQPPort, settings.AMQPTLSPort
	h.ValidateSAS = settings.ValidateSAS

	return h, nil
}
