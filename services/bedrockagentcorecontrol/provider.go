package bedrockagentcorecontrol

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// Provider implements service.Provider for the Bedrock AgentCore control plane.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "BedrockAgentCoreControl" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	h := NewHandler(NewInMemoryBackend(accountID, region))
	h.DefaultRegion = region

	return h, nil
}
