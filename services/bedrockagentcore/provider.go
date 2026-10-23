package bedrockagentcore

import (
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Provider implements service.Provider for the Bedrock AgentCore data plane.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "BedrockAgentCore" }

// Init builds the backend and handler, and enables containers when a runtime is reachable.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)
	b := NewInMemoryBackend(accountID, region)

	if cp, ok := ctx.Config.(ConfigProvider); ok {
		enableEngine(ctx, b, cp.GetAgentCoreSettings(), cp.GetAgentCoreEndpointURL())
	}

	h := NewHandler(b)
	h.DefaultRegion = region

	return h, nil
}

func enableEngine(ctx *service.AppContext, b *InMemoryBackend, s Settings, endpoint string) {
	if s.Engine == EngineNone {
		return
	}

	rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
	if err != nil {
		ctx.Logger.Warn("BedrockAgentCore: container runtime unavailable; runtimes and code execution are off",
			"error", err)

		return
	}

	b.EnableEngine(EngineConfig{
		Runtime: rt, Ports: ctx.PortAlloc, EndpointURL: endpoint, RegistryHost: s.RegistryHost,
		PythonImage: s.PythonImage, IdleTimeout: s.IdleTimeout,
	})
}
