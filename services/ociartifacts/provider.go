package ociartifacts

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("ociartifacts: nil app context")

// ConfigProvider extracts OCI Artifacts configuration from the AppContext Config.
type ConfigProvider interface {
	GetOCIArtifactsSettings() Settings
}

// DataDirConfig locates the directory registry blobs are kept in; empty keeps them in memory.
type DataDirConfig interface {
	GetRegistryDataDir() string
}

// Provider implements service.Provider for OCI Artifacts. It runs on its own listener (see
// Handler.StartWorker) and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "OCIArtifacts" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetOCIArtifactsSettings()
	}

	backend := NewInMemoryBackend()
	if dc, ok := ctx.Config.(DataDirConfig); ok {
		backend.SetDataDir(dc.GetRegistryDataDir())
	}

	if ctx.JanitorCtx != nil {
		backend.SetContext(ctx.JanitorCtx)
	}

	handler := NewHandler(backend)
	handler.Port = settings.Port
	handler.EnforceAuth = settings.EnforceAuth

	return handler, nil
}
