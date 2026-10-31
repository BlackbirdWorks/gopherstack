package ociloadbalancer

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("ociloadbalancer: nil app context")

// ConfigProvider extracts OCI Load Balancer configuration from the AppContext Config.
type ConfigProvider interface {
	GetOCILoadBalancerSettings() Settings
}

// Provider implements service.Provider for OCI Load Balancer. It runs on its own
// listener (see Handler.StartWorker) and never matches on the shared AWS router.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "OCILoadBalancer" }

// Init creates the backend and handler; the port is bound in StartWorker.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetOCILoadBalancerSettings()
	}

	backend := NewInMemoryBackend()
	backend.SetAllowExternal(settings.AllowExternalBackend)

	handler := NewHandler(backend)
	handler.Port = settings.Port
	handler.EnforceAuth = settings.EnforceAuth

	return handler, nil
}
