package bedrockagentcorecontrol

import (
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	dataplane "github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

// Provider implements service.Provider for the Bedrock AgentCore control plane.
type Provider struct{}

// Name returns the provider name.
func (p *Provider) Name() string { return "BedrockAgentCoreControl" }

// Init builds the backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID, region := service.AccountRegionOrDefault(ctx)

	backend := NewInMemoryBackend(accountID, region)

	if cp, ok := ctx.Config.(dataplane.ConfigProvider); ok {
		st := cp.GetAgentCoreSettings()
		backend.SetAllowExternalTargets(st.AllowExternalGatewayTargets)

		if st.GatewayExtraCAFile != "" {
			pool, err := loadExtraCAs(st.GatewayExtraCAFile)
			if err != nil {
				return nil, err
			}

			backend.SetTrustedRoots(pool)
		}
	}

	h := NewHandler(backend)
	h.DefaultRegion = region

	return h, nil
}

func loadExtraCAs(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read agentcore gateway CA file: %w", err)
	}

	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}

	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%w: %s", errNoCerts, path)
	}

	return pool, nil
}

var errNoCerts = errors.New("no PEM certificates found in agentcore gateway CA file")
