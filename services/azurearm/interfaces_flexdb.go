package azurearm

import (
	"context"
	"net/netip"
	"strings"
)

// FlexSpec is what the database engine needs to bring up one flexible server. AdminPassword is empty when the
// request did not carry one.
type FlexSpec struct {
	Version       string
	AdminLogin    string
	AdminPassword string
}

// FlexEndpoint is where a server's engine listens.
type FlexEndpoint struct {
	Host string
	Port int
}

// FlexEngine is the seam the flexible server providers use to run real database servers. id is the lower-cased
// ARM resource ID of the server. Primitive types only.
type FlexEngine interface {
	Ensure(ctx context.Context, id string, spec FlexSpec) (FlexEndpoint, error)
	Endpoint(id string) (FlexEndpoint, bool)
	SetPassword(ctx context.Context, id, login, password string) error
	Stop(ctx context.Context, id string) error
	Start(ctx context.Context, id string) error
	Restart(ctx context.Context, id string) error
	CreateDatabase(ctx context.Context, id, name, charset, collation string) error
	DropDatabase(ctx context.Context, id, name string) error
	Delete(ctx context.Context, id string) error
}

// subnetDelegatedTo reports whether the subnet exists and carries a delegation to service.
func (p *NetworkProvider) subnetDelegatedTo(subnetID, service string) (bool, bool) {
	p.mu.RLock("subnetDelegatedTo")
	defer p.mu.RUnlock()

	_, s, ok := p.subnetByID(subnetID)
	if !ok {
		return false, false
	}

	for _, d := range anyList(s.Props["delegations"]) {
		m, _ := d.(map[string]any)
		if strings.EqualFold(strOf(propsOf(m), "serviceName"), service) {
			return true, true
		}
	}

	return true, false
}

// subnetPrefixBits returns the prefix length of the subnet's address range.
func (p *NetworkProvider) subnetPrefixBits(subnetID string) (int, bool) {
	p.mu.RLock("subnetPrefixBits")
	defer p.mu.RUnlock()

	_, s, ok := p.subnetByID(subnetID)
	if !ok {
		return 0, false
	}

	prefixes := strList(s.Props[netFieldAddrPrefixes])
	if len(prefixes) == 0 {
		return 0, false
	}

	pfx, err := netip.ParsePrefix(prefixes[0])

	return pfx.Bits(), err == nil
}
