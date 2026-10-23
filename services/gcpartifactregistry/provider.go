package gcpartifactregistry

import (
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ConfigProvider extracts Artifact Registry configuration from the AppContext Config.
type ConfigProvider interface {
	GetGCPArtifactRegistrySettings() Settings
}

// DataDirConfig locates the directory image blobs are kept in; empty keeps them in memory.
type DataDirConfig interface {
	GetRegistryDataDir() string
}

// Provider implements service.Provider for Google Artifact Registry. It serves gRPC, REST and the Docker
// Registry API on its own listener (see Handler.StartWorker) and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "GCPArtifactRegistry" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetGCPArtifactRegistrySettings()
	}

	backend := NewInMemoryBackend()
	backend.SetRegistryAddress(settings.RegistryHost, strconv.Itoa(settings.Port))

	if dc, ok := ctx.Config.(DataDirConfig); ok {
		backend.SetDataDir(dc.GetRegistryDataDir())
	}

	if ctx.JanitorCtx != nil {
		backend.SetContext(ctx.JanitorCtx)
	}

	handler := NewHandler(backend)
	handler.Port = settings.Port

	return handler, nil
}
