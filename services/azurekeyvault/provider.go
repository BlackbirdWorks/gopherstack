package azurekeyvault

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// ConfigProvider extracts Key Vault configuration from the abstract AppContext Config.
type ConfigProvider interface {
	GetAzureKeyVaultSettings() Settings
}

// Provider implements service.Provider for Azure Key Vault. Like the other Azure
// services it never matches on the shared AWS router; the handler serves its own
// HTTPS listener from StartWorker.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "AzureKeyVault" }

// Init initializes the Key Vault backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetAzureKeyVaultSettings()
	}

	return NewHandler(NewInMemoryBackend(settings.AutoCreateVaults), settings), nil
}
