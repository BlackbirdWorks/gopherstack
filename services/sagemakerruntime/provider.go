package sagemakerruntime

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// sagemakerHandlerProvider is the subset of the server's backends provider
// needed to obtain the SageMaker (control-plane) handler, so InvokeEndpoint/
// InvokeEndpointAsync/InvokeEndpointWithResponseStream can validate
// EndpointName against real registered endpoints. See
// services/cloudwatchlogs/provider.go's s3HandlerProvider for the
// established precedent of this pattern (a private, minimal interface
// type-asserted against ctx.Config, rather than the full
// services/cloudformation-style BackendsProvider).
type sagemakerHandlerProvider interface {
	GetSageMakerHandler() service.Registerable
}

// Provider implements service.Provider for SageMaker Runtime.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "SageMakerRuntime" }

// Init initializes the SageMaker Runtime backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)
	wireEndpointLookup(ctx, backend)
	handler := NewHandler(backend)

	return handler, nil
}

// wireEndpointLookup connects backend to the emulated SageMaker service's
// endpoint registry when the app context exposes a SageMaker handler,
// enabling real EndpointName existence/InService validation on invoke
// calls. When the app context doesn't expose one (e.g. a standalone
// sagemakerruntime-only test harness), backend keeps its pre-wiring
// behaviour of accepting any EndpointName.
func wireEndpointLookup(ctx *service.AppContext, backend *InMemoryBackend) {
	hp, ok := ctx.Config.(sagemakerHandlerProvider)
	if !ok {
		return
	}

	sh, ok := hp.GetSageMakerHandler().(*sagemaker.Handler)
	if !ok || sh == nil || sh.Backend == nil {
		return
	}

	backend.SetEndpointLookup(sh.Backend)
}
