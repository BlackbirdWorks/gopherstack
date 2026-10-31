package ocimysql

import (
	"errors"
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("ocimysql: nil app context")

// ConfigProvider extracts OCI MySQL configuration from the AppContext Config.
type ConfigProvider interface {
	GetOCIMySQLSettings() Settings
}

// EngineConfigProvider exposes the effective engine mode (stub or docker).
type EngineConfigProvider interface {
	GetOCIMySQLEngine() string
}

// Provider implements service.Provider for OCI MySQL. It runs on its own listener (see Handler.StartWorker)
// and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "OCIMySQL" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetOCIMySQLSettings()
	}

	backend := NewInMemoryBackend()
	if ec, ok := ctx.Config.(EngineConfigProvider); ok && ec.GetOCIMySQLEngine() == EngineDocker {
		enableDockerEngine(ctx, backend)
	}

	handler := NewHandler(backend)
	handler.Port = settings.Port
	handler.EnforceAuth = settings.EnforceAuth

	return handler, nil
}

func enableDockerEngine(ctx *service.AppContext, backend *InMemoryBackend) {
	rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
	if err != nil {
		ctx.Logger.Warn("OCIMySQL: container runtime unavailable; DB systems stay metadata-only", "error", err)

		return
	}

	vols, _ := rt.(container.VolumeRuntime)

	if err = backend.EnableDockerEngine(DockerConfig{
		Runtime: rt, Volumes: vols, Ports: ctx.PortAlloc, Logger: ctx.Logger, Image: os.Getenv("OCI_MYSQL_IMAGE"),
	}); err != nil {
		ctx.Logger.Warn("OCIMySQL: invalid engine configuration; DB systems stay metadata-only", "error", err)
		_ = rt.Close()
	}
}
