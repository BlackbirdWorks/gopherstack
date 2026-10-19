package budgets

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// Provider implements service.Provider for AWS Budgets.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "Budgets" }

// Init builds the backend and starts the notification evaluator.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, _ := service.AccountRegionOrDefault(ctx)
	backend := NewInMemoryBackend(accountID, homeRegion)

	if ctx.JanitorCtx != nil {
		backend.StartEvaluator(ctx.JanitorCtx, evaluateInterval)
	}

	return NewHandler(backend), nil
}
