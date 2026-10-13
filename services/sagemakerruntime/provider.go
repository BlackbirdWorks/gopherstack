package sagemakerruntime

import (
	"context"

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

	handler.EnableRegions()

	return handler, nil
}

// wireEndpointLookup connects backend to the emulated SageMaker service's
// endpoint registry. The handler is resolved per call: the CLI stores its
// handlers only after every provider's Init has run, so resolving here would
// always see nil in a real server.
func wireEndpointLookup(ctx *service.AppContext, backend *InMemoryBackend) {
	hp, ok := ctx.Config.(sagemakerHandlerProvider)
	if !ok {
		return
	}

	backend.SetEndpointLookup(lazyEndpointLookup{provider: hp})
}

// lazyEndpointLookup resolves the SageMaker backend at call time.
type lazyEndpointLookup struct {
	provider sagemakerHandlerProvider
}

func (l lazyEndpointLookup) DescribeEndpoint(ctx context.Context, name string) (*sagemaker.Endpoint, error) {
	b, err := l.backend()
	if err != nil {
		return nil, err
	}

	return b.DescribeEndpoint(ctx, name)
}

func (l lazyEndpointLookup) backend() (*sagemaker.InMemoryBackend, error) {
	sh, ok := l.provider.GetSageMakerHandler().(*sagemaker.Handler)
	if !ok || sh == nil || sh.Backend == nil {
		return nil, errEndpointLookupUnavailable
	}

	return sh.Backend, nil
}

func (l lazyEndpointLookup) DescribeInferenceComponent(
	ctx context.Context, name string,
) (*sagemaker.InferenceComponent, error) {
	b, err := l.backend()
	if err != nil {
		return nil, err
	}

	return b.DescribeInferenceComponent(ctx, name)
}

func (l lazyEndpointLookup) DescribeEndpointConfig(
	ctx context.Context,
	name string,
) (*sagemaker.EndpointConfig, error) {
	b, err := l.backend()
	if err != nil {
		return nil, err
	}

	return b.DescribeEndpointConfig(ctx, name)
}

func (l lazyEndpointLookup) DescribeModel(ctx context.Context, name string) (*sagemaker.Model, error) {
	b, err := l.backend()
	if err != nil {
		return nil, err
	}

	return b.DescribeModel(ctx, name)
}
