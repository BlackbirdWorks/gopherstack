package route53recoverycluster

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned by Init when no AppContext is supplied.
var ErrNilAppContext = errors.New("route53recoverycluster: AppContext must not be nil")

// Provider implements service.Provider.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Route53RecoveryCluster" }

// Init builds the handler; the control-plane sibling is resolved at call time.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	_, region := service.AccountRegionOrDefault(ctx)

	h := NewHandler(ctx.Config)
	h.DefaultRegion = region

	return h, nil
}
