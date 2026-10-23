package azurecontainerregistry

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// ConfigProvider extracts the Container Registry settings from the AppContext config.
type ConfigProvider interface {
	GetAzureACRSettings() Settings
}

// DataDirConfig locates the directory registry blobs are kept in; empty keeps them in memory.
type DataDirConfig interface {
	GetRegistryDataDir() string
}

// Provider implements service.Provider for Azure Container Registry.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "AzureContainerRegistry" }

// Init builds the backend and handler; the port is bound by Handler.StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetAzureACRSettings()
	}

	backend := NewInMemoryBackend()
	if dc, ok := ctx.Config.(DataDirConfig); ok {
		backend.SetDataDir(dc.GetRegistryDataDir())
	}

	if ctx.JanitorCtx != nil {
		backend.SetContext(ctx.JanitorCtx)
	}

	return NewHandler(backend, settings), nil
}
