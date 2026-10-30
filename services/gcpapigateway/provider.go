package gcpapigateway

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("gcpapigateway: nil app context")

// ConfigProvider extracts API Gateway configuration from the AppContext Config.
type ConfigProvider interface {
	GetGCPAPIGatewaySettings() Settings
}

// Provider implements service.Provider for API Gateway. It serves gRPC, REST and the gateway data plane on
// its own listener (see Handler.StartWorker) and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "GCPAPIGateway" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetGCPAPIGatewaySettings()
	}

	backend := NewInMemoryBackend()
	backend.SetAllowExternal(settings.AllowExternalBackend)

	handler := NewHandler(backend)
	handler.Port = settings.Port

	return handler, nil
}
