// Package route53recoverycontrolconfig emulates the Route 53 Application Recovery Controller control plane.
package route53recoverycontrolconfig

import (
	"errors"
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when no AppContext is supplied.
var ErrNilAppContext = errors.New("route53recoverycontrolconfig: AppContext must not be nil")

// externalEndpointEnv pins the base URL that cluster endpoints advertise, overriding the caller's host.
const externalEndpointEnv = "ROUTE53_RECOVERY_CONTROL_ENDPOINT"

// Provider implements service.Provider.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Route53RecoveryControlConfig" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID)
	backend.SetAppConfig(ctx.Config)
	backend.SetExternalEndpoint(os.Getenv(externalEndpointEnv))

	handler := NewHandler(backend)
	handler.DefaultRegion = region

	return handler, nil
}
